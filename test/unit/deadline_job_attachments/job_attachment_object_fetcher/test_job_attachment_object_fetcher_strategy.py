# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import pytest
import boto3

from unittest.mock import Mock, patch

from deadline.job_attachments.job_attachment_object_fetcher_strategy import (
    JobAttachmentObjectFetcherFactory,
    JobAttachmentFetchingStrategy,
)
from deadline.job_attachments.job_attachment_object_fetcher import (
    JobAttachmentObjectFetcher,
    S3PaginationFetcher,
    S3InventoryFetcher,
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
        """Test that the factory returns a S3PaginationFetcher for PAGINATION strategy."""
        result: JobAttachmentObjectFetcher = JobAttachmentObjectFetcherFactory.create(
            strategy=JobAttachmentFetchingStrategy.PAGINATION,
            boto3_session=mock_session,
            settings=settings,
        )

        assert isinstance(result, S3PaginationFetcher)

    def test_create_inventory_strategy(self, mock_session: Mock, settings: JobAttachmentS3Settings):
        """Test that the factory returns a S3InventoryFetcher for INVENTORY strategy."""
        manifest_key: str = "test-manifest-key"

        with patch(
            "deadline.job_attachments.job_attachment_object_fetcher.S3InventoryFetcher._get_s3_inventory_manifest"
        ) as mock_get_manifest:
            mock_get_manifest.return_value = None

            result: JobAttachmentObjectFetcher = JobAttachmentObjectFetcherFactory.create(
                strategy=JobAttachmentFetchingStrategy.INVENTORY,
                boto3_session=mock_session,
                settings=settings,
                job_attachments_file_key=manifest_key,
            )

        assert isinstance(result, S3InventoryFetcher)

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
