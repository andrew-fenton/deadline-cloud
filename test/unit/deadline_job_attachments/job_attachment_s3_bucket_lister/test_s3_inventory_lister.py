# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import pytest
import boto3
import gzip
import io

from typing import List, Dict, Tuple
from unittest.mock import Mock, patch
from datetime import datetime
from botocore.exceptions import ClientError
from datetime import timezone

from deadline.job_attachments.exceptions import JobAttachmentsS3BucketListerError
from deadline.job_attachments.job_attachments_s3_bucket_lister import S3InventoryLister
from deadline.job_attachments.models import JobAttachmentS3Settings, S3ObjectData


class TestS3InventoryLister:
    @pytest.fixture
    def mock_session(self) -> Mock:
        session: Mock = Mock(spec=boto3.Session)
        return session

    @pytest.fixture
    def settings(self) -> JobAttachmentS3Settings:
        return JobAttachmentS3Settings(s3BucketName="test-bucket", rootPrefix="DeadlineCloud")

    @pytest.fixture
    def sample_manifest_data(self) -> List[S3ObjectData]:
        return [
            S3ObjectData(
                key="queue-1/job-1/file1.txt",
                size=100,
                last_modified=datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
                etag="etag1",
            ),
            S3ObjectData(
                key="queue-1/job-2/file2.txt",
                size=200,
                last_modified=datetime(2025, 1, 2, 12, 0, 0, tzinfo=timezone.utc),
                etag="etag2",
            ),
            S3ObjectData(
                key="queue-2/job-1/file3.txt",
                size=300,
                last_modified=datetime(2025, 1, 3, 12, 0, 0, tzinfo=timezone.utc),
                etag="etag3",
            ),
        ]

    @pytest.fixture
    def inventory_lister_with_mock_data(
        self,
        mock_session: Mock,
        settings: JobAttachmentS3Settings,
        sample_manifest_data: List[S3ObjectData],
    ) -> S3InventoryLister:
        with patch.object(
            S3InventoryLister, "_get_s3_inventory_manifest", return_value=sample_manifest_data
        ):
            return S3InventoryLister(
                boto3_session=mock_session,
                s3_settings=settings,
                s3_inventory_manifest_key="test-manifest.csv.gz",
            )

    def test_list_common_prefixes_with_delimiter_happy_path(
        self, inventory_lister_with_mock_data: S3InventoryLister
    ):
        """Test listing common prefixes with populated manifest data returns all common prefixes"""
        result: List[str] = list(
            inventory_lister_with_mock_data.list_common_prefixes_with_delimeter("queue-1/")
        )

        expected_prefixes: set[str] = {"queue-1/job-1/", "queue-1/job-2/"}
        assert set(result) == expected_prefixes

    def test_list_job_attachments_happy_path_matching_prefix(
        self,
        inventory_lister_with_mock_data: S3InventoryLister,
        sample_manifest_data: List[S3ObjectData],
    ) -> None:
        """Test listing job attachments with matching prefix returns only matching objects"""
        result: List[S3ObjectData] = list(
            inventory_lister_with_mock_data.list_job_attachments("queue-1/job-1/")
        )

        assert len(result) == 1
        assert result[0].key == "queue-1/job-1/file1.txt"
        assert result[0].size == 100
        assert result[0].etag == "etag1"

    def test_list_job_attachments_no_prefix_returns_all(
        self,
        inventory_lister_with_mock_data: S3InventoryLister,
        sample_manifest_data: List[S3ObjectData],
    ) -> None:
        """Test listing job attachments with no prefix returns all objects"""
        result: List[S3ObjectData] = list(inventory_lister_with_mock_data.list_job_attachments(""))

        assert len(result) == 3
        result_keys: List[str] = [obj.key for obj in result]
        expected_keys: List[str] = [
            "queue-1/job-1/file1.txt",
            "queue-1/job-2/file2.txt",
            "queue-2/job-1/file3.txt",
        ]
        assert result_keys == expected_keys

    def test_list_job_attachments_with_prefixes_happy_path(
        self, inventory_lister_with_mock_data: S3InventoryLister
    ) -> None:
        """Test listing job attachments with multiple prefixes returns objects with their prefixes"""
        prefixes: List[str] = ["queue-1/job-1/", "queue-2/"]

        result: List[Tuple[str, S3ObjectData]] = list(
            inventory_lister_with_mock_data.list_job_attachments_with_prefixes(prefixes)
        )

        assert len(result) == 2

        # Check first result (queue-1/job-1/ prefix)
        prefix1, obj1 = result[0]
        assert prefix1 == "queue-1/job-1/"
        assert obj1.key == "queue-1/job-1/file1.txt"

        # Check second result (queue-2/ prefix)
        prefix2, obj2 = result[1]
        assert prefix2 == "queue-2/"
        assert obj2.key == "queue-2/job-1/file3.txt"

    @patch("deadline.job_attachments.job_attachments_s3_bucket_lister.get_s3_client")
    def test_get_s3_inventory_manifest_happy_path(
        self, mock_get_s3_client: Mock, mock_session: Mock, settings: JobAttachmentS3Settings
    ) -> None:
        """Test successful manifest download, decompression, and parsing"""
        mock_s3_client: Mock = Mock()
        mock_get_s3_client.return_value = mock_s3_client

        # Create sample CSV data. Manifest CSV file does not provide headers i.e first row is object data
        csv_data: str = "test-bucket,queue-1/file1.txt,100,2025-01-01T12:00:00.000Z,etag1\ntest-bucket,queue-1/file2.txt,200,2025-01-02T13:30:45.123Z,etag2"
        compressed_data: bytes = gzip.compress(csv_data.encode("utf-8"))

        mock_s3_client.get_object.return_value = {"Body": io.BytesIO(compressed_data)}

        lister: S3InventoryLister = S3InventoryLister(
            boto3_session=mock_session,
            s3_settings=settings,
            s3_inventory_manifest_key="test-manifest.csv.gz",
        )

        assert len(lister.manifest_data) == 2

        # Check first object
        obj1: S3ObjectData = lister.manifest_data[0]
        assert obj1.key == "queue-1/file1.txt"
        assert obj1.size == 100
        assert obj1.last_modified == datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        assert obj1.etag == "etag1"

        # Check second object with microseconds
        obj2: S3ObjectData = lister.manifest_data[1]
        assert obj2.key == "queue-1/file2.txt"
        assert obj2.size == 200
        assert obj2.last_modified == datetime(2025, 1, 2, 13, 30, 45, 123000, tzinfo=timezone.utc)
        assert obj2.etag == "etag2"

        mock_s3_client.get_object.assert_called_once_with(
            Bucket="test-bucket", Key="test-manifest.csv.gz"
        )

    @patch("deadline.job_attachments.job_attachments_s3_bucket_lister.get_s3_client")
    def test_get_s3_inventory_manifest_key_not_exists(
        self, mock_get_s3_client: Mock, mock_session: Mock, settings: JobAttachmentS3Settings
    ) -> None:
        """Test that missing manifest key raises JobAttachmentsS3BucketListerError"""
        mock_s3_client: Mock = Mock()
        mock_get_s3_client.return_value = mock_s3_client

        mock_s3_client.get_object.side_effect = ClientError(
            error_response={"Error": {"Code": "NoSuchKey", "Message": "Object does not exist"}},
            operation_name="GetObject",
        )

        with pytest.raises(JobAttachmentsS3BucketListerError) as exc_info:
            S3InventoryLister(
                boto3_session=mock_session,
                s3_settings=settings,
                s3_inventory_manifest_key="non-existent-manifest.csv.gz",
            )

        assert "Failed to download S3 Inventory manifest from S3" in str(exc_info.value)

    @patch("deadline.job_attachments.job_attachments_s3_bucket_lister.get_s3_client")
    def test_get_s3_inventory_manifest_memory_error(
        self, mock_get_s3_client: Mock, mock_session: Mock, settings: JobAttachmentS3Settings
    ) -> None:
        """Test that memory error during manifest processing raises JobAttachmentsS3BucketListerError"""
        mock_s3_client: Mock = Mock()
        mock_get_s3_client.return_value = mock_s3_client

        # Create a mock response that will cause MemoryError when read
        mock_body: Mock = Mock()
        mock_body.read.side_effect = MemoryError("Not enough memory")
        mock_response: Dict[str, Mock] = {"Body": mock_body}
        mock_s3_client.get_object.return_value = mock_response

        with pytest.raises(JobAttachmentsS3BucketListerError) as exc_info:
            S3InventoryLister(
                boto3_session=mock_session,
                s3_settings=settings,
                s3_inventory_manifest_key="large-manifest.csv.gz",
            )

        assert "Failed to load S3 Inventory manifest into memory" in str(exc_info.value)
