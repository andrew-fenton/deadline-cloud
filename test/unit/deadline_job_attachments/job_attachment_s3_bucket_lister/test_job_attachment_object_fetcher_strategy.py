# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import pytest
import boto3

from unittest.mock import Mock, patch

from deadline.job_attachments.job_attachment_object_fetcher_strategy import (
    JobAttachmentObjectFetcherFactory,
    JobAttachmentFetchingStrategy,
)
from deadline.job_attachments.job_attachments_s3_bucket_lister import (
    JobAttachmentsS3BucketLister,
    S3PaginationLister,
    S3InventoryLister,
)
from deadline.job_attachments.models import JobAttachmentS3Settings
from deadline.job_attachments.exceptions import JobAttachmentsError


class TestJobAttachmentObjectFetcherFactory:
    @pytest.fixture
    def mock_session(self) -> Mock:
        return Mock(spec=boto3.Session)

    @pytest.fixture
    def settings(self) -> JobAttachmentS3Settings:
        return JobAttachmentS3Settings(s3BucketName="test-bucket", rootPrefix="DeadlineCloud")

    def test_create_pagination_strategy(
        self, mock_session: Mock, settings: JobAttachmentS3Settings
    ):
        """Test that the factory returns a S3PaginationLister for PAGINATION strategy."""
        result: JobAttachmentsS3BucketLister = JobAttachmentObjectFetcherFactory.create(
            strategy=JobAttachmentFetchingStrategy.PAGINATION,
            boto3_session=mock_session,
            settings=settings,
        )

        assert isinstance(result, S3PaginationLister)

    def test_create_inventory_strategy(self, mock_session: Mock, settings: JobAttachmentS3Settings):
        """Test that the factory returns a S3InventoryLister for INVENTORY strategy."""
        manifest_key: str = "test-manifest-key"

        with patch(
            "deadline.job_attachments.job_attachments_s3_bucket_lister.S3InventoryLister._get_s3_inventory_manifest"
        ) as mock_get_manifest:
            mock_get_manifest.return_value = None

            result: JobAttachmentsS3BucketLister = JobAttachmentObjectFetcherFactory.create(
                strategy=JobAttachmentFetchingStrategy.INVENTORY,
                boto3_session=mock_session,
                settings=settings,
                job_attachments_file_key=manifest_key,
            )

        assert isinstance(result, S3InventoryLister)

    def test_create_inventory_strategy_without_manifest_key_raises_error(
        self, mock_session: Mock, settings: JobAttachmentS3Settings
    ):
        """Test that the factory raises JobAttachmentsError when INVENTORY strategy is requested without manifest key."""
        with pytest.raises(JobAttachmentsError) as error:
            JobAttachmentObjectFetcherFactory.create(
                strategy=JobAttachmentFetchingStrategy.INVENTORY,
                boto3_session=mock_session,
                settings=settings,
            )

        assert "job_attachments_file_key is required for inventory strategy" in str(error)

    def test_create_unknown_strategy_raises_error(
        self, mock_session: Mock, settings: JobAttachmentS3Settings
    ):
        """Test that the factory raises JobAttachmentsError for unknown strategy."""
        unknown_strategy: str = "unknown_strategy"

        with pytest.raises(JobAttachmentsError) as error:
            JobAttachmentObjectFetcherFactory.create(
                strategy=unknown_strategy,  # type: ignore
                boto3_session=mock_session,
                settings=settings,
            )

        assert "Unknown strategy" in str(error)
