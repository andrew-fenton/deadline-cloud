# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import boto3

from typing import Optional

from .job_attachment_object_fetcher import (
    JobAttachmentObjectFetcher,
    S3PaginationFetcher,
    S3InventoryFetcher,
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
    ) -> JobAttachmentObjectFetcher:
        if strategy == JobAttachmentFetchingStrategy.PAGINATION:
            return S3PaginationFetcher(boto3_session=boto3_session, settings=settings)

        elif strategy == JobAttachmentFetchingStrategy.INVENTORY:
            if not job_attachments_file_key:
                raise JobAttachmentsError(
                    "job_attachments_file_key is required for inventory strategy"
                )

            return S3InventoryFetcher(
                boto3_session=boto3_session,
                s3_settings=settings,
                job_attachments_file_key=job_attachments_file_key,
            )

        else:
            raise JobAttachmentsError(f"Unknown strategy: {strategy}")
