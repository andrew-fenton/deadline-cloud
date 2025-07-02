import click
import logging
import os
import csv
import json
import shutil
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Iterator, Any

from ... import api
from ...config import config_file
from .._common import _apply_cli_options_to_config, _handle_error
from ._sigint_handler import SigIntHandler

from deadline.job_attachments.asset_manifests.decode import decode_manifest
from deadline.job_attachments.asset_manifests.base_manifest import (
    BaseAssetManifest,
)


logger = logging.getLogger("deadline.client.cli")

# Set up the signal handler for handling Ctrl + C interruptions.
sigint_handler = SigIntHandler()

DEADLINE_S3_PREFIX = "DeadlineCloud"
LOCAL_TEMP_DIR_ROOT = os.path.expanduser("~/bucket_sweeper")

ASSET_KEY_LENGTH = 3
INPUT_MANIFEST_KEY_LENGTH = 7
OUTPUT_MANIFEST_KEY_LENGTH = 9
JOB_ID_INDEX = 4


@click.group(name="cleanup")
@_handle_error
def cli_cleanup():
    """
    Commands to work with cleanups.
    """


@cli_cleanup.command(name="bucket")
@click.option("--farm-id", help="The farm to use.")
@click.option("--queue-id", help="The queue to use.")
@click.option(
    "--retention-days",
    type=int,
    default=120,
    help="The number of days to retain job files before deletion. Default = 120",
)
def cleanup_bucket(retention_days: int, **args):
    # Get a temporary config object with the standard options handled
    config = _apply_cli_options_to_config(
        required_options={"farm_id", "queue_id"}, **args
    )

    farm_id = config_file.get_setting("defaults.farm_id", config=config)
    queue_id = config_file.get_setting("defaults.queue_id", config=config)

    s3 = api.get_boto3_client("s3", config=config)
    deadline = api.get_boto3_client("deadline", config=config)
    bucket_name = _get_bucket_name(deadline, farm_id, queue_id)

    # Initialize paths
    STORAGE_PATH = os.path.join(LOCAL_TEMP_DIR_ROOT, "keep_file_manifests")

    # Initialize components
    storage = StorageHandler(STORAGE_PATH)
    job_attachments = JobAttachmentsHandler(
        s3,
        deadline,
        DEADLINE_S3_PREFIX,
        farm_id,
        queue_id,
        LOCAL_TEMP_DIR_ROOT,
        bucket_name,
    )
    sweeper = SweeperProcessor(
        s3, deadline, storage, job_attachments, farm_id, queue_id
    )

    # Create merged manifests directory
    os.makedirs(STORAGE_PATH, exist_ok=True)

    active_job_ids = sweeper.get_active_job_ids(retention_days=retention_days)
    print("Active jobs:", active_job_ids)

    for job_id in active_job_ids:
        job_attachments.download_manifests(job_id)
        manifests = job_attachments.retrieve_manifests(job_id)
        input_manifest = ""  # get from retrieve_manifests()?

        keep_asset_hashes = sweeper.extract_files_to_keep(manifests)
        keep_files_manifest = KeepFilesManifest(
            job_id, input_manifest, keep_asset_hashes
        )

        storage.store_keep_manifest(keep_files_manifest)
        job_attachments.cleanup_downloaded_manifests(job_id)

    keep_assets = sweeper.get_files_to_keep(active_job_ids)
    delete_files = sweeper.get_delete_files_list(
        retention_days, keep_assets, active_job_ids
    )

    file_path = sweeper.create_tag_manifest(
        LOCAL_TEMP_DIR_ROOT, bucket_name, delete_files
    )
    print(f"Created delete object key manifest: {file_path}")

    s3_manifest_prefix = sweeper.upload_tag_manifest(file_path)
    sweeper.create_batch_tag_s3_job(s3_manifest_prefix)

    # Cleanup keep manifests
    shutil.rmtree(STORAGE_PATH)

    print("Done.")


# Data Store Schema
class KeepFilesManifest:
    def __init__(self, job_id: str, input_manifest: str, asset_hashes: List[str]):
        self.job_id = job_id
        self.input_manifest = input_manifest
        self.asset_hashes = asset_hashes

    def encode(self) -> Dict:
        return {
            "jobId": self.job_id,
            "inputManifest": self.input_manifest,
            "assetHashes": self.asset_hashes,
        }

    @staticmethod
    def decode(data: Dict):
        return KeepFilesManifest(
            data["jobId"], data["inputManifest"], data["assetHashes"]
        )


# Interfaces
class StorageInterface(ABC):
    """Interface for storage operations."""

    @abstractmethod
    def store_keep_manifest(self, keep_files: KeepFilesManifest) -> None:
        pass

    @abstractmethod
    def retrieve_keep_manifest(self, job_id) -> KeepFilesManifest:
        pass


class JobAttachmentsInterface(ABC):
    """Interface for job attachment operations."""

    @abstractmethod
    def download_manifests(self, job_id) -> None:
        pass

    @abstractmethod
    def cleanup_downloaded_manifests(self, job_id) -> None:
        pass

    @abstractmethod
    def retrieve_manifests(self, job_id) -> List[BaseAssetManifest]:
        pass

    @abstractmethod
    def list_objects(self, page_size=1000) -> Iterator[Dict[str, Any]]:
        pass


# Interface Implementations
class StorageHandler(StorageInterface):
    """Handles storage operations for files to keep."""

    def __init__(self, root_path):
        self.root_path = root_path

    def store_keep_manifest(self, keep_files: KeepFilesManifest) -> None:
        write_file_path = os.path.join(self.root_path, keep_files.job_id)
        with open(write_file_path, "w") as file:
            json.dump(keep_files.encode(), file)

    def retrieve_keep_manifest(self, job_id) -> KeepFilesManifest:
        read_file_path = os.path.join(self.root_path, job_id)

        manifest = None
        with open(read_file_path, "r") as file:
            data = json.load(file)
            manifest = KeepFilesManifest.decode(data)

        return manifest


# Interface Implementations
class JobAttachmentsHandler(JobAttachmentsInterface):
    """Handles operations related to job attachments."""

    def __init__(
        self,
        s3_client,
        deadline_client,
        deadline_prefix,
        farm_id,
        queue_id,
        root_path,
        bucket_name,
    ):
        self.s3 = s3_client
        self.deadline = deadline_client
        self.deadline_prefix = deadline_prefix
        self.farm_id = farm_id
        self.queue_id = queue_id
        self.root_path = root_path
        self.bucket_name = bucket_name

    # For downloading functions, we can use transfer manager and download in parallel
    def download_manifests(self, job_id) -> None:
        job_manifests_path = os.path.join(self.root_path, job_id)
        os.makedirs(job_manifests_path, exist_ok=True)

        input_manifest_key = self._get_input_manifest_key(job_id)
        self._download_input_manifest(job_manifests_path, input_manifest_key)
        self._download_output_manifests(job_manifests_path, job_id)

    def _get_input_manifest_key(self, job_id):
        response = self.deadline.get_job(
            farmId=self.farm_id, queueId=self.queue_id, jobId=job_id
        )
        manifests = response["attachments"]["manifests"]

        manifest_location = None
        for manifest in manifests:
            if "inputManifestPath" in manifest:
                manifest_location = manifest["inputManifestPath"]
                break

        manifest_key = f"DeadlineCloud/Manifests/{manifest_location}"
        print(manifest_key)

        return manifest_key

    def _download_input_manifest(self, write_path, input_manifest_key):
        split_key = input_manifest_key.split("/")
        file_write_path = os.path.join(write_path, split_key[-1])
        return self.s3.download_file(
            self.bucket_name, input_manifest_key, file_write_path
        )

    def _download_output_manifests(self, write_path, job_id):
        s3_prefix = f"DeadlineCloud/Manifests/{self.farm_id}/{self.queue_id}/{job_id}"

        paginator = self.s3.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=s3_prefix):
            for obj in page.get("Contents", []):
                s3_key = obj["Key"]

                split_key = s3_key.split("/")
                filename = f"{split_key[-2]}_{split_key[-1]}"
                write_file_path = os.path.join(write_path, filename)

                self.s3.download_file(self.bucket_name, s3_key, write_file_path)

    def cleanup_downloaded_manifests(self, job_id):
        manifests_path = os.path.join(self.root_path, job_id)
        shutil.rmtree(manifests_path)

    def retrieve_manifests(self, job_id) -> List[BaseAssetManifest]:
        manifests: List[BaseAssetManifest] = []
        job_manifests_path = os.path.join(self.root_path, job_id)

        for file_name in os.listdir(job_manifests_path):
            file_path = os.path.join(job_manifests_path, file_name)

            with open(file_path, "r", encoding="utf-8") as file_data:
                manifest_data = file_data.read()
                manifest: BaseAssetManifest = decode_manifest(manifest_data)
                manifests.append(manifest)

        return manifests

    def list_objects(self, page_size=1000) -> Iterator[Dict[str, Any]]:
        paginator = self.s3.get_paginator("list_objects_v2")
        config = {"PageSize": page_size}

        for page in paginator.paginate(
            Bucket=self.bucket_name,
            Prefix=self.deadline_prefix,
            PaginationConfig=config,
        ):
            yield page.get("Contents", [])


class SweeperProcessor:
    """Processes cleanup operations for job attachments."""

    def __init__(
        self,
        s3_client,
        deadline_client,
        storage: StorageInterface,
        job_attachments: JobAttachmentsInterface,
        farm_id,
        queue_id,
    ):
        self.deadline = deadline_client
        self.s3 = s3_client
        self.storage = storage
        self.job_attachments = job_attachments
        self.farm_id = farm_id
        self.queue_id = queue_id

    def get_active_job_ids(self, retention_days=120) -> List[str]:
        retention_days_ago = datetime.now(timezone.utc) - timedelta(days=retention_days)

        active_jobs = []
        nextToken = ""

        while True:
            response = self.deadline.list_jobs(
                farmId=self.farm_id,
                queueId=self.queue_id,
                nextToken=nextToken,
                maxResults=100,
            )

            for job in response.get("jobs", []):
                created_date = job.get("createdAt")
                ended_date = job.get("endedAt")
                job_id = job.get("jobId")
                job_status = job.get("taskRunStatus")

                # Keep job's files if we can't determine when it was last run
                if not created_date and not ended_date:
                    active_jobs.append(job_id)

                elif self._run_within_retention_period(
                    created_date, ended_date, retention_days_ago
                ) or self._is_a_running_job(job_status):
                    active_jobs.append(job_id)

            # Break out of loop if no more jobs to list
            nextToken = response.get("nextToken")
            if not nextToken:
                break

        return active_jobs

    def _run_within_retention_period(
        self, created_date, ended_date, retention_days_ago
    ) -> bool:
        # Both dates cannot be null due to the guard before calling this function.
        check_date = ended_date or created_date
        return check_date >= retention_days_ago

    def _is_a_running_job(self, status) -> bool:
        return status in [
            "PENDING",
            "READY",
            "ASSIGNED",
            "STARTING",
            "SCHEDULED",
            "RUNNING",
        ]

    def extract_files_to_keep(self, manifests: List[BaseAssetManifest]) -> List[str]:
        asset_hashes = dict()

        for manifest in manifests:
            for path in manifest.paths:
                asset_hashes[path.hash] = None

        return list(asset_hashes)

    def get_files_to_keep(self, job_ids):
        keep_assets = set()

        for job_id in job_ids:
            keep_files = self.storage.retrieve_keep_manifest(job_id)
            for asset_hash in keep_files.asset_hashes:
                keep_assets.add(asset_hash)

        return list(keep_assets)

    def get_delete_files_list(self, retention_days, keep_assets_list, active_job_ids):
        if not keep_assets_list or not active_job_ids:
            return []

        delete_list = []
        for page in self.job_attachments.list_objects():
            for obj in page:
                # Due to eventual consistency, we may not list recently submitted jobs. So, we
                # must check the last modified date to avoid premature deletions.
                retention_date = datetime.now(timezone.utc) - timedelta(
                    days=retention_days
                )
                last_modified_date = obj.get("LastModified")
                if last_modified_date >= retention_date:
                    continue

                s3_key = obj.get("Key")
                split_key = s3_key.split("/")
                split_key_len = len(split_key)

                # We need another way to check whether to include INPUT manifests
                # they have no job_id in their key. We can use the hash in the manifest file name
                if split_key_len == ASSET_KEY_LENGTH:
                    asset_hash = split_key[-1]
                    hash = asset_hash.split(".")[0]

                    if hash not in keep_assets_list:
                        delete_list.append(s3_key)
                elif (
                    split_key_len == OUTPUT_MANIFEST_KEY_LENGTH
                    and split_key[JOB_ID_INDEX] not in active_job_ids
                ):
                    delete_list.append(s3_key)

        return delete_list

    def create_tag_manifest(self, write_directory, bucket_name, delete_list):
        csv_formatted_list = []
        for obj_key in delete_list:
            csv_formatted_list.append([bucket_name, obj_key])

        file_path = os.path.join(write_directory, "tag_manifest.csv")
        with open(file_path, "w") as file:
            writer = csv.writer(file)
            writer.writerows(csv_formatted_list)

        return file_path

    def upload_tag_manifest(self, manifest_path):
        pass

    def create_batch_tag_s3_job(self, manifest_s3_prefix):
        pass


def _get_bucket_name(deadline, farm_id, queue_id):
    response = deadline.get_queue(farmId=farm_id, queueId=queue_id)
    return response["jobAttachmentSettings"]["s3BucketName"]
