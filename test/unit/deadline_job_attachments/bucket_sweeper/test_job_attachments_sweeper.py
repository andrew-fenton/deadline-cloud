# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import pytest
import csv

from datetime import datetime
from typing import Dict, List, Any, Set
from pathlib import Path
from botocore.exceptions import BotoCoreError
from unittest.mock import Mock, patch

from deadline.job_attachments.models import (
    RetentionRecord,
    S3ObjectData,
)
from deadline.job_attachments.bucket_sweeper.job_attachment_sweeper import JobAttachmentSweeper
from deadline.job_attachments.exceptions import (
    JobAttachmentObjectFetcherError,
    JobAttachmentSweeperError,
    JobAttachmentS3BotoCoreError,
    RetentionRecordHandlerError,
)


@pytest.fixture
def mock_boto3_session() -> Mock:
    """Fixture to create mock Boto3 Session"""
    return Mock()


@pytest.fixture
def mock_s3() -> Mock:
    """Fixture to create mock AWS S3 client"""
    return Mock()


@pytest.fixture
def mock_s3_control() -> Mock:
    """Fixture to create mock AWS S3 Control client"""
    return Mock()


@pytest.fixture
def mock_deadline() -> Mock:
    """Fixture to create mock Deadline client"""
    return Mock()


@pytest.fixture
def mock_record_handler() -> Mock:
    """Fixture to create mock RetentionRecordHandler"""
    return Mock()


@pytest.fixture
def mock_lister() -> Mock:
    """Fixture to create mock JobAttachmentsLister"""
    return Mock()


@pytest.fixture
def processor(
    mock_boto3_session: Mock,
    mock_s3: Mock,
    mock_s3_control: Mock,
    mock_deadline: Mock,
    mock_record_handler: Mock,
    mock_lister: Mock,
) -> JobAttachmentSweeper:
    """Fixture to create JobAttachmentSweeper instance with mock clients"""
    return JobAttachmentSweeper(
        s3_client=mock_s3,
        s3_control_client=mock_s3_control,
        deadline_client=mock_deadline,
        retention_record_handler=mock_record_handler,
        job_attachment_object_fetcher=mock_lister,
        boto3_session=mock_boto3_session,
        role_arn="test-role-arn",
        bucket_name="test-bucket",
        root_prefix="test-prefix",
    )


@pytest.fixture
def test_dir(tmp_path: Path) -> Path:
    """Create and cleanup a test directory."""
    test_directory: Path = tmp_path / "bucket_sweeper_test"
    test_directory.mkdir(exist_ok=True)

    return test_directory


class TestJobAttachmentSweeper:
    def test_get_attachments_to_retain_happy_path(
        self, processor: JobAttachmentSweeper, mock_record_handler: Mock
    ):
        """Tests retrieving attachments to retain with multiple queues and jobs."""
        queue_job_id_map: Dict[str, List[str]] = {
            "queue-1": ["job-1"],
            "queue-2": ["job-2"],
            "queue-3": ["job-3"],
        }

        mock_records: List[RetentionRecord] = [
            RetentionRecord(queue_id="queue-1", job_id="job-1", s3_object_key="key-1"),
            RetentionRecord(queue_id="queue-2", job_id="job-2", s3_object_key="key-2"),
            RetentionRecord(queue_id="queue-3", job_id="job-3", s3_object_key="key-3"),
        ]

        mock_record_handler.get_retention_records.return_value = mock_records

        result: Set[str] = processor.get_attachments_to_retain(queue_job_id_map)

        mock_record_handler.get_retention_records.assert_called_once_with(
            queue_job_id_map=queue_job_id_map
        )
        assert result == {"key-1", "key-2", "key-3"}

    def test_get_attachments_to_retain_deduplication(
        self, processor: JobAttachmentSweeper, mock_record_handler: Mock
    ):
        """Tests deduplication of attachment keys."""
        queue_job_id_map: Dict[str, List[str]] = {"queue-1": ["job-1", "job-2"]}

        mock_records: List[RetentionRecord] = [
            # Both jobs have the same set of keys
            RetentionRecord(queue_id="queue-1", job_id="job-1", s3_object_key="key-1"),
            RetentionRecord(queue_id="queue-1", job_id="job-1", s3_object_key="key-2"),
            RetentionRecord(queue_id="queue-1", job_id="job-2", s3_object_key="key-1"),
            RetentionRecord(queue_id="queue-1", job_id="job-2", s3_object_key="key-2"),
        ]

        mock_record_handler.get_retention_records.return_value = mock_records

        result: Set[str] = processor.get_attachments_to_retain(queue_job_id_map)

        assert len(result) == 2
        assert result == {"key-1", "key-2"}

    def test_get_attachments_to_retain_empty_map(
        self, processor: JobAttachmentSweeper, mock_record_handler: Mock
    ):
        """Tests behavior with empty queue job map."""
        queue_job_id_map: Dict[str, List[str]] = {}

        mock_record_handler.get_retention_records.return_value = []

        result: Set[str] = processor.get_attachments_to_retain(queue_job_id_map)

        mock_record_handler.get_retention_records.assert_called_once_with(
            queue_job_id_map=queue_job_id_map
        )
        assert result == set()

    def test_get_attachments_to_retain_handler_error(
        self, processor: JobAttachmentSweeper, mock_record_handler: Mock
    ):
        """Tests error handling when record handler fails."""
        queue_job_id_map: Dict[str, List[str]] = {"queue-1": ["job-1"]}

        error_message: str = "Failed to retrieve records"
        mock_record_handler.get_retention_records.side_effect = RetentionRecordHandlerError(
            error_message
        )

        with pytest.raises(JobAttachmentSweeperError) as err:
            processor.get_attachments_to_retain(queue_job_id_map)

        assert "Failed to get retention records" in str(err.value)
        assert error_message in str(err.value)

    def test_get_queues_in_farms_from_s3_happy_path(self, processor: JobAttachmentSweeper):
        """Test successfully retrieving queues for multiple farms."""
        side_effect_values = [
            ["farm-123", "farm-456"],  # First call for farm IDs
            ["queue-1", "queue-2"],  # Second call for farm-123 queues
            ["queue-3"],  # Third call for farm-456 queues
        ]

        with patch(
            "deadline.job_attachments.bucket_sweeper.job_attachment_sweeper.JobAttachmentSweeper._get_ids_from_common_prefixes",
            side_effect=side_effect_values,
        ):
            result: Dict[str, List[str]] = processor.get_queues_in_farms_from_s3()

        expected: Dict[str, List[str]] = {
            "farm-123": ["queue-1", "queue-2"],
            "farm-456": ["queue-3"],
        }

        assert len(result) == 2
        assert result == expected

    def test_get_queues_in_farms_from_s3_no_queues(self, processor: JobAttachmentSweeper):
        """Test when there are no queue_ids returned by _get_ids_from_common_prefixes."""
        side_effect_values = [
            ["farm-123", "farm-456"],  # First call for farm IDs
            [],  # Second call for farm-123 queues
            [],  # Third call for farm-456 queues
        ]

        with patch(
            "deadline.job_attachments.bucket_sweeper.job_attachment_sweeper.JobAttachmentSweeper._get_ids_from_common_prefixes",
            side_effect=side_effect_values,
        ):
            result: Dict[str, List[str]] = processor.get_queues_in_farms_from_s3()

        assert len(result) == 0
        assert result == {}

    def test_get_ids_from_common_prefixes_happy_path(
        self, processor: JobAttachmentSweeper, mock_lister: Mock
    ):
        """Test successfully getting IDs from common prefixes."""
        mock_prefixes = ["test/123/", "test/456/", "test/789/"]
        mock_lister.list_common_prefixes_with_delimeter.return_value = mock_prefixes

        result = processor._get_ids_from_common_prefixes("test/")

        assert result == ["123", "456", "789"]
        mock_lister.list_common_prefixes_with_delimeter.assert_called_once_with(prefix="test/")

    def test_get_ids_from_common_prefixes_none_exist(
        self, processor: JobAttachmentSweeper, mock_lister: Mock
    ):
        """Test getting IDs when no prefixes exist."""
        mock_lister.list_common_prefixes_with_delimeter.return_value = []

        result = processor._get_ids_from_common_prefixes("test/")

        assert result == []
        mock_lister.list_common_prefixes_with_delimeter.assert_called_once_with(prefix="test/")

    def test_get_ids_from_common_prefixes_lister_error(
        self, processor: JobAttachmentSweeper, mock_lister: Mock
    ):
        """Test getting IDs when lister throws an error."""
        error_message = "Failed to list common prefixes"
        mock_lister.list_common_prefixes_with_delimeter.side_effect = (
            JobAttachmentObjectFetcherError(error_message)
        )

        with pytest.raises(JobAttachmentSweeperError) as err:
            processor._get_ids_from_common_prefixes("test/")

        assert error_message in str(err.value)
        mock_lister.list_common_prefixes_with_delimeter.assert_called_once_with(prefix="test/")

    def test_get_attachments_to_delete_filter_with_datetime(
        self, processor: JobAttachmentSweeper, mock_lister: Mock
    ):
        """Test when s3_keys_to_retain is empty - should return all objects modified after retention_datetime."""
        mock_objects: List[S3ObjectData] = [
            S3ObjectData(
                key="delete_this", size=100, last_modified=datetime(2025, 1, 1), etag="etag1"
            ),
            S3ObjectData(
                key="retain_this", size=200, last_modified=datetime(2025, 1, 3), etag="etag2"
            ),
        ]
        mock_lister.list_job_attachments.return_value = mock_objects

        retention_datetime: datetime = datetime(2025, 1, 2)
        result: List[str] = processor.get_attachments_to_delete(
            s3_keys_to_retain=set(),  # calling with empty set
            retention_datetime=retention_datetime,
            root_prefix="test/",
        )

        assert result == ["delete_this"]
        mock_lister.list_job_attachments.assert_called_once_with(prefix="test/")

    def test_get_attachments_to_delete_object_filter_with_retain_set(
        self, processor: JobAttachmentSweeper, mock_lister: Mock
    ):
        """Test when object key is in s3_keys_to_retain - should not be included in delete list."""
        mock_objects: List[S3ObjectData] = [
            S3ObjectData(
                key="delete_this", size=200, last_modified=datetime(2025, 1, 1), etag="etag2"
            ),
            S3ObjectData(
                key="retain_this", size=100, last_modified=datetime(2025, 1, 1), etag="etag1"
            ),
        ]
        mock_lister.list_job_attachments.return_value = mock_objects

        retention_datetime: datetime = datetime(2025, 1, 2)
        result: List[str] = processor.get_attachments_to_delete(
            s3_keys_to_retain={"retain_this"},
            retention_datetime=retention_datetime,
            root_prefix="test/",
        )

        assert result == ["delete_this"]
        mock_lister.list_job_attachments.assert_called_once_with(prefix="test/")

    def test_get_attachments_to_delete_handles_lister_error(
        self, processor: JobAttachmentSweeper, mock_lister: Mock
    ):
        """Test that the function properly handles errors from the lister."""
        error_message: str = "Error with listing function"
        mock_lister.list_job_attachments.side_effect = JobAttachmentObjectFetcherError(
            error_message
        )

        retention_datetime: datetime = datetime(2025, 1, 2)

        with pytest.raises(JobAttachmentSweeperError) as err:
            processor.get_attachments_to_delete(
                s3_keys_to_retain={"key1"},
                retention_datetime=retention_datetime,
                root_prefix="test/",
            )

        assert "Failed to list objects for deletion" in str(err.value)
        assert error_message in str(err.value)

        mock_lister.list_job_attachments.assert_called_once_with(prefix="test/")

        # Verify that the original error is preserved in the exception chain
        assert isinstance(err.value.__cause__, JobAttachmentObjectFetcherError)

    def test_create_tag_manifest_empty_list(self, processor: JobAttachmentSweeper, test_dir: Path):
        """Test creating a tag manifest with an empty delete list."""
        test_file_path: Path = test_dir / "empty_manifest.csv"
        processor._create_tag_manifest(test_file_path, [])

        with open(str(test_file_path), "r") as file:
            assert file.read() == ""

    def test_create_tag_manifest_io_error(
        self,
        processor: JobAttachmentSweeper,
        test_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        """Test creating a tag manifest when creation fails."""

        def mock_open(*args, **kwargs):
            raise IOError("Mocked IO Error")

        monkeypatch.setattr("builtins.open", mock_open)

        with pytest.raises(JobAttachmentSweeperError) as raised_error:
            processor._create_tag_manifest(test_dir, ["object_key"])

            assert str(raised_error) == "Mocked IO Error"

    def test_create_tag_manifest(self, processor: JobAttachmentSweeper, test_dir: Path):
        """Create a tag manifest and validate CSV content."""

        # Sample data
        delete_list: List[str] = [
            "DeadlineCloud/Manifests/farm-123/queue-123/job-123/step-123/session/456_output",
            "DeadlineCloud/Manifests/farm-123/queue-123/Inputs/123/456_input",
            "DeadlineCloud/Data/hash.xx128",
        ]

        test_file_path: Path = test_dir / "tag_manifest.csv"
        processor._create_tag_manifest(test_file_path, delete_list)

        assert test_file_path.exists()

        # Validate CSV output
        with open(str(test_file_path), "r") as file:
            reader = csv.reader(file)
            rows: List[List[str]] = list(reader)

        # fmt: off
        assert sorted(rows) == sorted([
                ["test-bucket", "DeadlineCloud/Manifests/farm-123/queue-123/job-123/step-123/session/456_output"],
                ["test-bucket", "DeadlineCloud/Manifests/farm-123/queue-123/Inputs/123/456_input"],
                ["test-bucket", "DeadlineCloud/Data/hash.xx128"],
        ])
        # fmt: on

    def test_upload_tag_manifest(
        self, processor: JobAttachmentSweeper, mock_s3: Mock, test_dir: Path
    ):
        """Test uploading an existing CSV file to S3."""

        manifest_path: Path = test_dir / "tag_manifest.csv"
        object_key: str = "DeadlineCloud/BucketSweeper/tag_manifest.csv"
        processor._upload_tag_manifest(manifest_path, object_key)

        # Validate S3 call
        mock_s3.upload_file.assert_called_once_with(
            str(manifest_path),
            "test-bucket",
            object_key,
        )

    def test_upload_tag_manifest_s3_error(
        self, processor: JobAttachmentSweeper, mock_s3: Mock, test_dir: Path
    ):
        """Test uploading manifest when s3 upload fails."""
        mock_s3.upload_file.side_effect = BotoCoreError()

        with pytest.raises(JobAttachmentS3BotoCoreError):
            processor._upload_tag_manifest(test_dir / "test.csv", "test_key")

    def test_get_manifest_etag_value_error(self, processor: JobAttachmentSweeper, mock_s3: Mock):
        """Test _get_manifest_etag method."""
        mock_s3.head_object.return_value = {"ETag": None}

        with pytest.raises(JobAttachmentSweeperError):
            processor._get_manifest_etag("test_key")

    def test_get_manifest_etag(self, processor: JobAttachmentSweeper, mock_s3: Mock):
        """Test _get_manifest_etag method."""
        mock_s3.head_object.return_value = {"ETag": "test-etag"}

        etag: str = processor._get_manifest_etag("test_key")
        assert etag == "test-etag"

        mock_s3.head_object.assert_called_once_with(Bucket="test-bucket", Key="test_key")

    def test_get_manifest_etag_botocore_error(self, processor: JobAttachmentSweeper, mock_s3: Mock):
        """Test _get_manifest_etag when head_object call fails."""
        mock_s3.head_object.side_effect = BotoCoreError()

        with pytest.raises(JobAttachmentS3BotoCoreError):
            processor._get_manifest_etag("test_key")

    def test_submit_tagging_batch_job_error(
        self, processor: JobAttachmentSweeper, mock_s3_control: Mock
    ):
        """Test _submit_tagging_batch_job when job creation fails."""
        mock_s3_control.create_job.side_effect = Exception("Mocked error")

        with pytest.raises(JobAttachmentSweeperError):
            processor._submit_tagging_batch_job({}, {})

    def test_create_batch_tag_s3_job(
        self,
        processor: JobAttachmentSweeper,
        mock_boto3_session: Mock,
        mock_s3: Mock,
        mock_s3_control: Mock,
    ):
        """Test creating S3 batch tagging job"""
        # Test data
        s3_manifest_key: str = "test/test.csv"

        # Mock responses
        mock_s3.head_object.return_value = {"ETag": "test-etag"}
        mock_boto3_session.client("sts").get_caller_identity.return_value = {
            "Account": "test-account-id"
        }

        # Call method
        processor._create_batch_tag_s3_job(s3_manifest_key)

        # Verify S3 Control create_job was called correctly
        expected_operation: Dict[str, Any] = {
            "S3PutObjectTagging": {"TagSet": [{"Key": "delete", "Value": "True"}]}
        }

        expected_manifest: Dict[str, Any] = {
            "Spec": {
                "Format": "S3BatchOperations_CSV_20180820",
                "Fields": ["Bucket", "Key"],
            },
            "Location": {
                "ObjectArn": f"arn:aws:s3:::test-bucket/{s3_manifest_key}",
                "ETag": "test-etag",
            },
        }

        expected_priority: int = 10
        expected_confirmation_setting: bool = False
        expected_report_settings: Dict[str, bool] = {"Enabled": False}

        mock_s3_control.create_job.assert_called_once_with(
            AccountId="test-account-id",
            RoleArn="test-role-arn",
            Operation=expected_operation,
            Manifest=expected_manifest,
            ConfirmationRequired=expected_confirmation_setting,
            Report=expected_report_settings,
            Priority=expected_priority,
        )
