# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os
import boto3
from typing import List
from botocore.exceptions import ClientError

from deadline.job_attachments._aws.aws_clients import get_s3_client
from deadline.job_attachments.exceptions import JobAttachmentsError
from deadline.job_attachments.models import JobAttachmentS3Settings


def _download_job_manifests_using_s3_keys(
    session: boto3.Session,
    manifest_keys: List[str],
    job_attachment_settings: JobAttachmentS3Settings,
    download_directory: str,
) -> None:
    """
    Downloads job manifests from S3 to a local directory using provided S3 keys.

    Note: Current implementation downloads sequentially. Future optimization
    opportunity exists for parallel downloads.

    Args:
        session: boto3 Session
        manifest_keys: List of S3 object keys for manifest files
        job_attachment_settings: S3 job attachments settings
        download_directory: Local directory path where manifests will be saved

    Raises:
        JobAttachmentsError: If manifest keys are malformed or if there are IO errors
        JobAttachmentS3BotoCoreError: If there are AWS S3 errors (excluding missing files)
    """
    s3 = get_s3_client(session=session)

    for manifest_key in manifest_keys:
        split_key = manifest_key.split("/")

        if len(split_key) < 2:
            raise JobAttachmentsError(
                f"Invalid manifest key structure: {manifest_key}. Expected at least 2 path segments."
            )

        file_name = f"{split_key[-2]}_{split_key[-1]}"
        local_file_path = os.path.join(download_directory, file_name)

        try:
            s3.download_file(job_attachment_settings.s3BucketName, manifest_key, local_file_path)
        except (ClientError, IOError) as err:
            raise JobAttachmentsError(f"Failed to download manifest {manifest_key}: {err}") from err
