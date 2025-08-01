# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import boto3

from pathlib import Path
from typing import Dict, List, NamedTuple, Set
from datetime import datetime
from botocore.client import BaseClient

from .retention_record_handler import RetentionRecordHandler
from .job_attachments_sweeper import JobAttachmentsSweeper
from ..job_attachments_s3_bucket_lister import S3PaginationLister
from ..models import (
    AssetHash,
    FarmQueueJobTriple,
    JobAttachmentS3Settings,
    RetentionRecord,
)
from ..manifest_handling import (
    _get_all_manifest_s3_keys_for_job,
    _load_manifests_from_disk,
    _extract_asset_hashes_from_manifests,
)
from ..manifest_download import _download_job_manifests_using_s3_keys
from ..asset_manifests.base_manifest import BaseAssetManifest

from deadline.client.api._list_active_job_ids import _list_active_job_ids


class SweeperDependencies(NamedTuple):
    """Tuple to enforce components returned by the initialization function"""

    sweeper: JobAttachmentsSweeper
    job_attachment_s3_settings: JobAttachmentS3Settings
    retention_record_handler: RetentionRecordHandler


def _initialize_dependencies(
    working_directory: Path,
    bucket_name: str,
    root_prefix: str,
    boto3_session: boto3.Session,
    role_arn: str,
) -> SweeperDependencies:
    """
    Initialize all required services and components for the bucket sweeper.

    Args:
        working_directory: Directory path where temporary files (like manifests)
            will be managed during sweeper operations.
        bucket_name: Name of the S3 bucket containing job attachments to be swept.
        root_prefix: S3 key prefix that defines the root path for job attachments
            within the bucket.
        boto3_session: boto3 session
        role_arn: s3 batch operations batch tagging role arn

    Returns:
        SweeperComponents: Container object with initialized boto3 session, sweeper
            instance, S3 settings, and retention record handler ready for use.
    """
    job_attachment_s3_settings: JobAttachmentS3Settings = JobAttachmentS3Settings(
        s3BucketName=bucket_name,
        rootPrefix=root_prefix,
    )

    # Initialize AWS clients
    s3_client: BaseClient = boto3_session.client("s3")
    s3_control_client: BaseClient = boto3_session.client("s3control")
    deadline_client: BaseClient = boto3_session.client("deadline")

    # Initialize handlers and job attachments lister
    retention_record_handler: RetentionRecordHandler = RetentionRecordHandler(
        storage_file_path=working_directory / "storage_file.json"
    )

    job_attachments_object_fetcher: S3PaginationLister = S3PaginationLister(
        boto3_session=boto3_session, settings=job_attachment_s3_settings
    )

    sweeper: JobAttachmentsSweeper = JobAttachmentsSweeper(
        s3_client=s3_client,
        s3_control_client=s3_control_client,
        deadline_client=deadline_client,
        retention_record_handler=retention_record_handler,
        job_attachments_s3_bucket_lister=job_attachments_object_fetcher,
        boto3_session=boto3_session,
        role_arn=role_arn,
        bucket_name=bucket_name,
        root_prefix=root_prefix,
    )

    return SweeperDependencies(
        sweeper=sweeper,
        job_attachment_s3_settings=job_attachment_s3_settings,
        retention_record_handler=retention_record_handler,
    )


def _collect_farm_queue_job_triples(
    sweeper: JobAttachmentsSweeper,
    boto3_session: boto3.Session,
    retention_datetime: datetime,
) -> List[FarmQueueJobTriple]:
    """
    Collect all farm-queue-job triples that need to be processed for retention.

    This method retrieves all farms and their associated queues from S3, then for each
    farm-queue combination, it finds jobs that have ended before the retention datetime.
    The resulting triples represent the scope of job attachments that must be retained.

    Important Note:
        Jobs without an endedAt date are handled separately during the delete list
        compilation phase by checking their last_modified_date.

    Args:
        sweeper: The JobAttachmentsSweeper instance used to retrieve farm-queue mappings
        boto3_session: AWS session for making API calls to list jobs
        retention_datetime: Jobs that ended before this datetime are candidates for cleanup, ones
            that ended after or at this datetime will be retained

    Returns:
        List of FarmQueueJobTriple objects, each representing a farm-queue-job combination
        that needs to be processed to retain its associated assets. Only includes jobs that have
        an endedAt date after or at the retention datetime.
    """
    farm_queues_map: Dict[str, List[str]] = sweeper.get_queues_in_farms_from_s3()

    farm_queue_job_triples: List[FarmQueueJobTriple] = []

    for farm_id, queue_ids in farm_queues_map.items():
        # Only returns jobs that have an endedAt date. Checking last_modified_date when
        # compiling a delete list handles cases for jobs that have no endedAt date.
        queue_job_id_map: Dict[str, List[str]] = _list_active_job_ids(
            boto3_session=boto3_session,
            farm_id=farm_id,
            queue_ids=queue_ids,
            retention_datetime=retention_datetime,
        )

        for queue_id, job_ids in queue_job_id_map.items():
            farm_queue_job_triples.extend(
                FarmQueueJobTriple(farm_id=farm_id, queue_id=queue_id, job_id=job_id)
                for job_id in job_ids
            )

    return farm_queue_job_triples


def _process_manifests_and_create_retention_records(
    farm_queue_job_triples: List[FarmQueueJobTriple],
    boto3_session: boto3.Session,
    job_attachment_s3_settings: JobAttachmentS3Settings,
    working_directory: Path,
    retention_record_handler: RetentionRecordHandler,
) -> None:
    """
    Process job attachment manifests and create retention records for S3 objects.

    This method orchestrates the complete workflow for processing job attachments:
        1. Downloads manifest files from S3 for each specified job
        2. Extracts asset information from the manifests
        3. Generates S3 object keys for both manifests and assets
        4. Creates retention records to keep track of objects that need to be preserved

    Args:
        farm_queue_job_triples: List of (farm_id, queue_id, job_id) tuples identifying
            the jobs to process.
        boto3_session: AWS session for S3 operations.
        job_attachment_s3_settings: S3 configuration settings for job attachments,
            including bucket name and key prefixes.
        working_directory: Local directory path where manifest files will be downloaded
            and organized by farm/queue/job hierarchy.
        retention_record_handler: Handler for inserting retention records into storage.

    Raises:
        S3 operation errors if manifest download fails.
        File system errors if local directory creation fails.
        Validation errors if manifest parsing fails.
    """
    download_directory: Path = working_directory / "manifests"
    download_directory.mkdir(exist_ok=True)

    for farm_id, queue_id, job_id in farm_queue_job_triples:
        job_specific_directory: Path = download_directory / farm_id / queue_id / job_id
        job_specific_directory.mkdir(parents=True, exist_ok=True)

        # Download manifests
        manifest_keys: List[str] = _get_all_manifest_s3_keys_for_job(
            session=boto3_session,
            job_attachment_settings=job_attachment_s3_settings,
            farm_id=farm_id,
            queue_id=queue_id,
            job_id=job_id,
        )
        _download_job_manifests_using_s3_keys(
            session=boto3_session,
            manifest_keys=manifest_keys,
            job_attachment_settings=job_attachment_s3_settings,
            download_directory=job_specific_directory,
        )

        # Get asset s3 keys
        manifests: List[BaseAssetManifest] = _load_manifests_from_disk(
            manifests_directory=job_specific_directory
        )
        asset_hashes: List[AssetHash] = _extract_asset_hashes_from_manifests(manifests=manifests)
        asset_s3_keys: List[str] = [
            f"{job_attachment_s3_settings.rootPrefix}/Data/{asset.hash}.{asset.hash_alg.value}"
            for asset in asset_hashes
        ]

        # Save retention records
        retain_object_keys: List[str] = manifest_keys + asset_s3_keys
        retention_records: List[RetentionRecord] = [
            RetentionRecord(queue_id=queue_id, job_id=job_id, s3_object_key=key)
            for key in retain_object_keys
        ]
        retention_record_handler.insert_retention_records(records=retention_records)


def _determine_objects_to_delete(
    farm_queue_job_triples: List[FarmQueueJobTriple],
    sweeper: JobAttachmentsSweeper,
    retention_datetime: datetime,
    root_prefix: str,
) -> List[str]:
    """
    Determine which S3 objects should be deleted based on S3 last modified date and
    a object retention list.

    This method analyzes job attachments to identify objects that are safe to delete
    by first determining which objects must be retained (based on active jobs), then
    identifying all other objects as candidates for deletion.

    Args:
        farm_queue_job_triples: List of (farm_id, queue_id, job_id) tuples representing
            the jobs to consider for retention analysis.
        sweeper: The JobAttachmentsSweeper instance used to query S3 and determine
            retention and deletion candidates.
        retention_datetime: The cutoff datetime - objects modified before this time
            may be eligible for deletion if not otherwise retained.
        root_prefix: The S3 key prefix to limit the scope of the deletion analysis.

    Returns:
        List of S3 object keys that have not been used since the retention_datetime and
        are thereby safe to delete.
    """
    # Create queue-job mapping
    queue_job_id_map: Dict[str, List[str]] = {}
    for _, queue_id, job_id in farm_queue_job_triples:
        if queue_id not in queue_job_id_map:
            queue_job_id_map[queue_id] = []
        queue_job_id_map[queue_id].append(job_id)

    # Get retention set
    retention_set: Set[str] = sweeper.get_attachments_to_retain(queue_job_id_map=queue_job_id_map)

    # Get delete list
    delete_list: List[str] = sweeper.get_attachments_to_delete(
        s3_keys_to_retain=retention_set,
        retention_datetime=retention_datetime,
        root_prefix=root_prefix,
    )

    return delete_list


def _create_deletion_batch_job(
    delete_list: List[str],
    sweeper: JobAttachmentsSweeper,
    working_directory: Path,
    root_prefix: str,
    dry_run: bool,
) -> None:
    """
    Orchestrates S3 batch operations job creation.

    This method orchestrates the creation of a batch deletion job by:
        1. Creating a CSV manifest file containing the list of S3 objects to delete
        2. Uploading the manifest to S3 for use by the batch job
        3. Creating the S3 batch operations job

    Args:
        delete_list: List of S3 object keys to be deleted
        sweeper: JobAttachmentsSweeper instance used for manifest operations
        working_directory: Local directory path where temporary files are created
        root_prefix: S3 key prefix used for organizing uploaded manifest files
        dry_run: flag for creating s3 batch job
    """
    # Create CSV for batch deletion
    tag_manifest_path: Path = working_directory / "delete_objects_manifest.csv"
    sweeper._create_tag_manifest(file_path=tag_manifest_path, delete_list=delete_list)

    tag_manifest_s3_key: str = f"{root_prefix}/delete_objects_manifest.csv"
    sweeper._upload_tag_manifest(manifest_path=tag_manifest_path, object_key=tag_manifest_s3_key)

    # Create batch job
    if not dry_run:
        sweeper._create_batch_tag_s3_job(s3_manifest_key=tag_manifest_s3_key)
