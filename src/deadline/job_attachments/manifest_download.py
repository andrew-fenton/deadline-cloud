# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import boto3
from pathlib import Path

from ._utils import _get_num_download_workers
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from botocore.client import BaseClient
from typing import List
from botocore.exceptions import ClientError

from deadline.job_attachments._aws.aws_clients import get_s3_client
from deadline.job_attachments.exceptions import JobAttachmentsError
from deadline.job_attachments.models import JobAttachmentS3Settings


def _download_job_manifests_using_s3_keys_to_disk(
    session: boto3.Session,
    manifest_keys: List[str],
    job_attachment_settings: JobAttachmentS3Settings,
    download_directory: Path,
) -> None:
    """
    Downloads job manifests from S3 in parallel to a local directory using provided S3 keys.

    Note:
        If any manifest download fails, function will throw JobAttachmentsError.
        Also, creates download_directory if it doesn't exist.

    Args:
        session: boto3 Session
        manifest_keys: List of S3 object keys for manifest files
        job_attachment_settings: S3 job attachments settings
        download_directory: Local directory path where manifests will be saved

    Raises:
        JobAttachmentsError: If manifest keys are malformed or if there are IO errors
        JobAttachmentS3BotoCoreError: If there are AWS S3 errors (excluding missing files)
    """
    s3_client: BaseClient = get_s3_client(session=session)

    num_workers: int = _get_num_download_workers()

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures: List[Future] = [
            executor.submit(
                _download_manifest_from_s3_to_disk,
                job_attachment_settings,
                manifest_key,
                download_directory,
                s3_client,
            )
            for manifest_key in manifest_keys
        ]

        for future in as_completed(futures):
            future.result()


def _download_manifest_from_s3_to_disk(
    job_attachment_settings: JobAttachmentS3Settings,
    manifest_key: str,
    download_directory: Path,
    s3_client: BaseClient,
) -> None:
    """
    Download a single manifest file from S3 to a local directory.

    The manifest file is downloaded and saved with a filename constructed from the last two
    segments of the S3 key path, joined with an underscore (e.g., "segment1_segment2").

    Args:
        session: The boto3 session for AWS operations (currently unused).
        job_attachment_settings: S3 settings containing the bucket name and other configuration.
        manifest_key: The S3 key path to the manifest file. Must contain at least 2 path segments.
        download_directory: Local directory where the manifest file will be saved.
        s3_client: The S3 client used to perform the download operation.

    Raises:
        JobAttachmentsError: If the manifest key has invalid structure (less than 2 segments)
                           or if the S3 download operation fails.
    """
    split_key: List[str] = manifest_key.split("/")

    if len(split_key) < 2:
        raise JobAttachmentsError(
            f"Invalid manifest key structure: {manifest_key}. Expected at least 2 path segments."
        )

    download_directory.mkdir(parents=True, exist_ok=True)

    file_name: str = f"{split_key[-2]}_{split_key[-1]}"
    local_file_path: Path = download_directory / file_name

    try:
        s3_client.download_file(
            job_attachment_settings.s3BucketName, manifest_key, str(local_file_path)
        )
    except (ClientError, IOError) as err:
        raise JobAttachmentsError(f"Failed to download manifest {manifest_key}: {err}") from err
