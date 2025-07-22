# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from pathlib import Path
import boto3

from botocore.client import BaseClient
from botocore.exceptions import ClientError
from typing import Any, Dict, List, Set

from deadline.job_attachments._aws.aws_clients import get_deadline_client
from deadline.job_attachments.asset_manifests.base_manifest import BaseAssetManifest
from deadline.job_attachments.asset_manifests.decode import decode_manifest
from deadline.job_attachments.download import _get_tasks_manifests_keys_from_s3
from deadline.job_attachments.exceptions import JobAttachmentsError
from deadline.job_attachments.models import AssetHash, JobAttachmentS3Settings


def _get_all_manifest_s3_keys_for_job(
    session: boto3.Session,
    job_attachment_settings: JobAttachmentS3Settings,
    farm_id: str,
    queue_id: str,
    job_id: str,
) -> List[str]:
    """
    Retrieves all manifest (both input and output) S3 keys for a specific job.

    Args:
        session: boto3 Session for AWS credentials
        job_attachment_settings: S3 job attachments settings
        farm_id: Deadline farm identifier
        queue_id: Deadline queue identifier
        job_id: Deadline job identifier

    Returns:
        List[str]: Combined list of input and output manifest S3 keys

    Raises:
        JobAttachmentsError: If there's any error retrieving the manifests
    """
    root_prefix: str = job_attachment_settings.rootPrefix.rstrip("/")
    output_manifest_prefix: str = f"{root_prefix}/Manifests/{farm_id}/{queue_id}/{job_id}/"

    try:
        input_manifest_keys: List[str] = _get_input_manifest_keys_for_job(
            session=session,
            s3_root_prefix=job_attachment_settings.rootPrefix,
            farm_id=farm_id,
            queue_id=queue_id,
            job_id=job_id,
        )
        # TODO: Implement fetching output manifests with JobAttachmentsLister
        output_manifest_keys: List[str] = _get_tasks_manifests_keys_from_s3(
            manifest_prefix=output_manifest_prefix,
            s3_bucket=job_attachment_settings.s3BucketName,
            session=session,
        )
    except Exception as err:
        raise JobAttachmentsError(f"Failed to get all job manifest keys: {str(err)}") from err

    return input_manifest_keys + output_manifest_keys


def _get_input_manifest_keys_for_job(
    session: boto3.Session,
    s3_root_prefix: str,
    farm_id: str,
    queue_id: str,
    job_id: str,
) -> List[str]:
    """
    Retrieves S3 keys for input manifests associated with a specific Deadline job.

    Args:
        session: boto3 Session
        s3_root_prefix: Base S3 path prefix user for job attachments
        farm_id: Deadline farm identifier
        queue_id: Deadline queue identifier
        job_id: Deadline job identifier

    Returns:
        List[str]: Full S3 object keys for the input manifests

    Raises:
        JobAttachmentsError: If job metadata can't be retrieved or doesn't contain expected structure
    """
    cleaned_root_prefix: str = s3_root_prefix.rstrip("/")
    deadline: BaseClient = get_deadline_client(session=session)

    try:
        job_metadata: Dict[str, Any] = deadline.get_job(
            farmId=farm_id, queueId=queue_id, jobId=job_id
        )
    except ClientError as err:
        raise JobAttachmentsError(f"Failed to get job metadata: {str(err)}")

    # Handle case where job has no input manifests
    if "attachments" not in job_metadata or "manifests" not in job_metadata["attachments"]:
        return []

    manifest_data: List[Dict[str, Any]] = job_metadata["attachments"]["manifests"]

    manifest_keys: List[str] = []
    for manifest in manifest_data:
        if "inputManifestPath" not in manifest:
            # Skip if there's no input manifest path - job may have no input assets
            continue

        manifest_path: str = manifest["inputManifestPath"]
        full_s3_path: str = f"{cleaned_root_prefix}/Manifests/{manifest_path}"
        manifest_keys.append(full_s3_path)

    return manifest_keys


def _load_manifests_from_disk(manifests_directory: Path) -> List[BaseAssetManifest]:
    """
    Load and decode asset manifests from files in the specified directory.

    Args:
        manifests_directory: Path to the directory containing manifest files.

    Returns:
        List[BaseAssetManifest]: List of decoded manifest objects.

    Raises:
        JobAttachmentsError: If the manifests directory doesn't exist, if there's an error
            while loading/decoding any manifest file.

    Note:
        If a file in the directory is not a manifest, the decode_manifest will raise a validation
        error upon reading.
    """
    if not manifests_directory.exists():
        raise JobAttachmentsError(f"Manifests directory does not exist: {manifests_directory}")

    manifests: List[BaseAssetManifest] = []

    for manifest_path in manifests_directory.iterdir():
        try:
            manifest_data = manifest_path.read_text(encoding="utf-8")
            manifest: BaseAssetManifest = decode_manifest(manifest_data)
            manifests.append(manifest)
        except Exception as err:
            raise JobAttachmentsError(f"Failed to load manifests from disk: {str(err)}") from err

    return manifests


def _extract_asset_hashes_from_manifests(
    manifests: List[BaseAssetManifest],
) -> List[AssetHash]:
    """
    Extract unique asset hashes from a list of asset manifests.

    Args:
        manifests: A list of asset manifest objects.

    Returns:
        List[AssetHash]: A list of unique AssetHash objects, each containing:
            - hash: The hash value of the asset
            - hash_alg: The hash algorithm used for the asset
    """
    asset_hashes: Set[AssetHash] = set()

    for manifest in manifests:
        for path in manifest.paths:
            if path.hash:
                asset_hash: AssetHash = AssetHash(path.hash, manifest.hashAlg)
                asset_hashes.add(asset_hash)

    return list(asset_hashes)
