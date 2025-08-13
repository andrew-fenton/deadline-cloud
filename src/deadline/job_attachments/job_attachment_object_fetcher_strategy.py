# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import boto3

from enum import Enum
from typing import Optional

from .job_attachments_s3_bucket_lister import (
    JobAttachmentsS3BucketLister,
    S3PaginationLister,
    S3InventoryLister,
)
from .models import JobAttachmentS3Settings, JobAttachmentFetchingStrategy
from .exceptions import JobAttachmentsError


class JobAttachmentObjectFetcherFactory:
    """Factory that returns appropriate job attachment object fetcher implementation based on strategy."""

    @staticmethod
    def create(
        strategy: JobAttachmentFetchingStrategy,
        boto3_session: boto3.Session,
        settings: JobAttachmentS3Settings,
        job_attachments_file_key: Optional[str] = None,
    ) -> JobAttachmentsS3BucketLister:
        if strategy == JobAttachmentFetchingStrategy.PAGINATION:
            return S3PaginationLister(boto3_session=boto3_session, settings=settings)

        elif strategy == JobAttachmentFetchingStrategy.INVENTORY:
            if not job_attachments_file_key:
                raise JobAttachmentsError(
                    "job_attachments_file_key is required for inventory strategy"
                )

            return S3InventoryLister(
                boto3_session=boto3_session,
                s3_settings=settings,
                job_attachments_file_key=job_attachments_file_key,
            )

        else:
            raise JobAttachmentsError(f"Unknown strategy: {strategy}")
