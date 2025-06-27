import click
import logging
import os
import json
import csv
import shutil
from datetime import datetime, timedelta, timezone
from typing import List

from ... import api
from ...config import config_file
from .._common import _apply_cli_options_to_config, _handle_error
from ._sigint_handler import SigIntHandler

from deadline.job_attachments.download import merge_asset_manifests
from deadline.job_attachments.asset_manifests.decode import decode_manifest
from deadline.job_attachments.asset_manifests.base_manifest import (
    BaseAssetManifest,
    BaseManifestPath,
)


logger = logging.getLogger("deadline.client.cli")

# Set up the signal handler for handling Ctrl + C interruptions.
sigint_handler = SigIntHandler()

LOCAL_TEMP_DIR_ROOT = os.path.expanduser("~/temp_manifests_dir")
MERGED_MANIFESTS_DIR = os.path.join(LOCAL_TEMP_DIR_ROOT, "merged_manifests")

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
def cleanup_bucket(**args):
    # Get a temporary config object with the standard options handled
    config = _apply_cli_options_to_config(
        required_options={"farm_id", "queue_id"}, **args
    )

    farm_id = config_file.get_setting("defaults.farm_id", config=config)
    queue_id = config_file.get_setting("defaults.queue_id", config=config)

    s3 = api.get_boto3_client("s3", config=config)
    deadline = api.get_boto3_client("deadline", config=config)

    # Create merged manifests directory
    os.makedirs(MERGED_MANIFESTS_DIR, exist_ok=True)
    print("Created temporary working directories.")

    bucket_name = _get_bucket_name(deadline, farm_id, queue_id)
    print("Got bucket name.")

    active_job_ids = _get_active_job_ids(
        deadline, farm_id, queue_id, retention_days=120
    )
    print("Got active job_ids.")

    _download_and_merge_manifests(
        s3, deadline, bucket_name, farm_id, queue_id, active_job_ids
    )
    print("Downloaded and merged manifests.")

    keep_assets = _get_asset_hashes_to_keep(MERGED_MANIFESTS_DIR)
    print("Got assets to keep.")

    delete_keys_list = _get_delete_files_list(
        s3, "DeadlineCloud/", bucket_name, keep_assets, set(active_job_ids)
    )

    _create_tag_manifest(LOCAL_TEMP_DIR_ROOT, bucket_name, delete_keys_list)
    print("Created batch tag manifest.")

    # Cleanup merged manifests
    shutil.rmtree(MERGED_MANIFESTS_DIR)

    print("Done.")


def _download_and_merge_manifests(
    s3, deadline, bucket_name, farm_id, queue_id, active_job_ids
):
    """
    We are losing the keys of manfiests to keep by merging. We can filter using the job_id though.
    """
    for job_id in active_job_ids:
        s3_prefix = f"DeadlineCloud/Manifests/{farm_id}/{queue_id}/{job_id}"

        job_manifests_dir = os.path.join(LOCAL_TEMP_DIR_ROOT, job_id)
        if not os.path.exists(job_manifests_dir):
            os.mkdir(job_manifests_dir)

        merged_manifest_path = os.path.join(MERGED_MANIFESTS_DIR, job_id)

        # Fetch and download manifests
        input_manifest_loc = _get_input_manifest_key(
            deadline, farm_id, queue_id, job_id
        )
        input_manifest_s3_key = f"DeadlineCloud/Manifests/{input_manifest_loc}"
        _download_input_manifest(
            s3, bucket_name, job_manifests_dir, input_manifest_s3_key
        )
        _download_output_manifests(s3, bucket_name, job_manifests_dir, s3_prefix)

        # Aggregate manifests
        manifests = _load_output_manifests(job_manifests_dir)
        # For the same task, different sessions produce output assets with different hashes. This means the
        # manifests produced by the same task point to different asset hashes. The merging logic loses this information because
        # we merge based on asset path, not hash. This implementation only keeps one of these output assets.
        merged_manifest = merge_asset_manifests(manifests)
        _write_manifest(merged_manifest, merged_manifest_path)

        # Cleanup downloaded manifests
        shutil.rmtree(job_manifests_dir)

        print(f"Processed manifests for job: {job_id}")


def _get_bucket_name(deadline, farm_id, queue_id):
    response = deadline.get_queue(farmId=farm_id, queueId=queue_id)
    return response["jobAttachmentSettings"]["s3BucketName"]


def _get_active_job_ids(deadline, farm_id, queue_id, retention_days=120):
    retention_days_ago = datetime.now() - timedelta(days=retention_days)

    # Get all active jobs.
    # This uses CREATED_AT which does not change if jobs are re-queued. May need to use ListJobs and
    # filter ourselves. Also, we need to be careful of eventual consistency. We may miss some newly
    # submitted jobs when we search. We need to check that last_modified_date >= 120 days when we check
    # the active files set against the bucket contents.
    response = deadline.search_jobs(
        farmId=farm_id,
        queueIds=[queue_id],
        itemOffset=0,
        pageSize=100,
        filterExpressions={
            "filters": [
                {
                    "dateTimeFilter": {
                        "dateTime": retention_days_ago.isoformat(),
                        "name": "CREATED_AT",
                        "operator": "GREATER_THAN_EQUAL_TO",
                    }
                }
            ],
            "operator": "AND",
        },
    )

    # Trim job response
    job_ids = []
    for job in response["jobs"]:
        job_ids.append(job["jobId"])

    return job_ids


def _get_input_manifest_key(deadline, farm_id, queue_id, job_id):
    response = deadline.get_job(farmId=farm_id, queueId=queue_id, jobId=job_id)
    manifests = response["attachments"]["manifests"]

    manifest_location = None
    for manifest in manifests:
        if "inputManifestPath" in manifest:
            manifest_location = manifest["inputManifestPath"]
            break

    return manifest_location


# For downloading functions, we can use transfer manager and download in parallel
def _download_input_manifest(s3, bucket_name, job_manifest_dir, input_manifest_key):
    split_key = input_manifest_key.split("/")
    assert len(split_key) >= 1, "Input manifest key is improperly formatted"

    file_write_path = os.path.join(job_manifest_dir, split_key[-1])
    return s3.download_file(bucket_name, input_manifest_key, file_write_path)


def _download_output_manifests(s3, bucket_name, job_manifests_dir, s3_prefix):
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket_name, Prefix=s3_prefix):
        for obj in page.get("Contents", []):
            s3_key = obj["Key"]

            split_key = s3_key.split("/")
            assert len(split_key) >= 2, "manifest S3 key is wrongly formatted"

            filename = f"{split_key[-2]}_{split_key[-1]}"
            write_file_path = os.path.join(job_manifests_dir, filename)

            s3.download_file(bucket_name, s3_key, write_file_path)


# Returns list of BaseManifests
def _load_output_manifests(local_manifests_path):
    manifests: List[BaseAssetManifest] = []

    for file in os.listdir(local_manifests_path):
        file_path = os.path.join(local_manifests_path, file)

        with open(file_path, "r", encoding="utf-8") as file_data:
            manifest_data = file_data.read()
            manifest: BaseAssetManifest = decode_manifest(manifest_data)
            manifests.append(manifest)

    return manifests


def _write_manifest(manifest: BaseAssetManifest, local_file_path):
    assert manifest, "Manifest is null"

    with open(local_file_path, "w") as file:
        encoded_manifest = manifest.encode()
        file.write(encoded_manifest)


def _get_asset_hashes_to_keep(merged_manifests_dir):
    keep_assets = set()

    for manifest in os.listdir(merged_manifests_dir):
        file_path = os.path.join(merged_manifests_dir, manifest)

        with open(file_path, "r", encoding="utf-8") as file_data:
            manifest_data = file_data.read()
            manifest: BaseAssetManifest = decode_manifest(manifest_data)

            for asset in manifest.paths:
                keep_assets.add(asset.hash)

    return keep_assets


def _get_delete_files_list(
    s3, s3_prefix, bucket_name, keep_asset_hashes_set, active_job_ids_set
):
    if not keep_asset_hashes_set or not active_job_ids_set:
        return []

    paginator = s3.get_paginator("list_objects_v2")

    delete_list = []
    for page in paginator.paginate(Bucket=bucket_name, Prefix=s3_prefix):
        for obj in page.get("Contents", []):
            s3_key = obj["Key"]
            split_key = s3_key.split("/")
            split_key_len = len(split_key)

            # We need another way to check whether to include INPUT manifests
            # they have no job_id in their key. We can use the hash in the manifest file name
            if split_key_len == ASSET_KEY_LENGTH:
                asset_hash = split_key[-1]
                hash = asset_hash.split(".")[0]

                if hash not in keep_asset_hashes_set:
                    delete_list.append(s3_key)
            elif (
                split_key_len == OUTPUT_MANIFEST_KEY_LENGTH
                and split_key[JOB_ID_INDEX] not in active_job_ids_set
            ):
                delete_list.append(s3_key)

    return delete_list


def _create_tag_manifest(write_dir, bucket_name, delete_list):
    csv_formatted_list = []
    for obj_key in delete_list:
        csv_formatted_list.append([bucket_name, obj_key])

    file_path = os.path.join(write_dir, "tag_manifest.csv")
    with open(file_path, "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerows(csv_formatted_list)


def _tag_files_to_delete(s3):
    pass
