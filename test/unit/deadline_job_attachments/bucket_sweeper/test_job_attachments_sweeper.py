# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from botocore.exceptions import BotoCoreError
from deadline.job_attachments.models import RetentionRecord
import pytest
import os
import csv
from typing import Dict, List, Any, Set
from pathlib import Path
from unittest.mock import Mock
from deadline.job_attachments.bucket_sweeper.job_attachments_sweeper import JobAttachmentsSweeper
from deadline.job_attachments.exceptions import (
    JobAttachmentsSweeperError,
    JobAttachmentS3BotoCoreError,
    RetentionRecordHandlerError,
)


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
def processor(
    mock_s3: Mock, mock_s3_control: Mock, mock_deadline: Mock, mock_record_handler: Mock
) -> JobAttachmentsSweeper:
    """Fixture to create JobAttachmentsSweeper instance with mock clients"""
    return JobAttachmentsSweeper(
        s3_client=mock_s3,
        s3_control_client=mock_s3_control,
        deadline_client=mock_deadline,
        retention_record_handler=mock_record_handler,
        farm_id="test-farm",
        account_id="test-account-id",
        role_arn="test-role-arn",
        bucket_name="test-bucket",
    )


@pytest.fixture
def test_dir(tmp_path: Path) -> Path:
    """Create and cleanup a test directory."""
    test_directory: Path = tmp_path / "bucket_sweeper_test"
    test_directory.mkdir(exist_ok=True)

    return test_directory


class TestJobAttachmentsSweeper:
    def test_get_attachments_to_retain_happy_path(self, processor: JobAttachmentsSweeper):
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

        processor.retention_record_handler.get_retention_records.return_value = mock_records

        result: Set[str] = processor.get_attachments_to_retain(queue_job_id_map)

        processor.retention_record_handler.get_retention_records.assert_called_once_with(
            queue_job_id_map=queue_job_id_map
        )
        assert result == {"key-1", "key-2", "key-3"}

    def test_get_attachments_to_retain_deduplication(self, processor: JobAttachmentsSweeper):
        """Tests deduplication of attachment keys."""
        queue_job_id_map: Dict[str, List[str]] = {"queue-1": ["job-1", "job-2"]}

        mock_records: List[RetentionRecord] = [
            # Both jobs have the same set of keys
            RetentionRecord(queue_id="queue-1", job_id="job-1", s3_object_key="key-1"),
            RetentionRecord(queue_id="queue-1", job_id="job-1", s3_object_key="key-2"),
            RetentionRecord(queue_id="queue-1", job_id="job-2", s3_object_key="key-1"),
            RetentionRecord(queue_id="queue-1", job_id="job-2", s3_object_key="key-2"),
        ]

        processor.retention_record_handler.get_retention_records.return_value = mock_records

        result: Set[str] = processor.get_attachments_to_retain(queue_job_id_map)

        assert len(result) == 2
        assert result == {"key-1", "key-2"}

    def test_get_attachments_to_retain_empty_map(self, processor: JobAttachmentsSweeper):
        """Tests behavior with empty queue job map."""
        queue_job_id_map: Dict[str, List[str]] = {}

        processor.retention_record_handler.get_retention_records.return_value = []

        result: Set[str] = processor.get_attachments_to_retain(queue_job_id_map)

        processor.retention_record_handler.get_retention_records.assert_called_once_with(
            queue_job_id_map=queue_job_id_map
        )
        assert result == set()

    def test_get_attachments_to_retain_handler_error(self, processor: JobAttachmentsSweeper):
        """Tests error handling when record handler fails."""
        queue_job_id_map: Dict[str, List[str]] = {"queue-1": ["job-1"]}

        error_message: str = "Failed to retrieve records"
        processor.retention_record_handler.get_retention_records.side_effect = (
            RetentionRecordHandlerError(error_message)
        )

        with pytest.raises(JobAttachmentsSweeperError) as err:
            processor.get_attachments_to_retain(queue_job_id_map)

        assert "Failed to get retention records" in str(err.value)
        assert error_message in str(err.value)

    def test_create_tag_manifest_empty_list(self, processor: JobAttachmentsSweeper, test_dir: Path):
        """Test creating a tag manifest with an empty delete list."""
        test_file_path = test_dir / "empty_manifest.csv"
        manifest_path: str = processor._create_tag_manifest(str(test_file_path), [])

        with open(manifest_path, "r") as file:
            assert file.read() == ""

    def test_create_tag_manifest_io_error(
        self,
        processor: JobAttachmentsSweeper,
        test_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        """Test creating a tag manifest when creation fails."""

        def mock_open(*args, **kwargs):
            raise IOError("Mocked IO Error")

        monkeypatch.setattr("builtins.open", mock_open)

        with pytest.raises(JobAttachmentsSweeperError) as raised_error:
            processor._create_tag_manifest(str(test_dir), ["object_key"])

            assert str(raised_error) == "Mocked IO Error"

    def test_create_tag_manifest(self, processor: JobAttachmentsSweeper, test_dir: Path):
        """Create a tag manifest and validate CSV content."""

        # Sample data
        delete_list: List[str] = [
            "DeadlineCloud/Manifests/farm-123/queue-123/job-123/step-123/session/456_output",
            "DeadlineCloud/Manifests/farm-123/queue-123/Inputs/123/456_input",
            "DeadlineCloud/Data/hash.xx128",
        ]

        test_file_path = test_dir / "tag_manifest.csv"
        manifest_path: str = processor._create_tag_manifest(str(test_file_path), delete_list)

        assert os.path.exists(manifest_path)

        # Validate CSV output
        with open(manifest_path, "r") as file:
            reader = csv.reader(file)
            rows: List[List[str]] = list(reader)

        # fmt: off
        assert sorted(rows) == sorted([
                ["test-bucket", "DeadlineCloud/Manifests/farm-123/queue-123/job-123/step-123/session/456_output"],
                ["test-bucket", "DeadlineCloud/Manifests/farm-123/queue-123/Inputs/123/456_input"],
                ["test-bucket", "DeadlineCloud/Data/hash.xx128"],
        ])
        # fmt: on

    def test_upload_tag_manifest(self, processor: JobAttachmentsSweeper, mock_s3: Mock):
        """Test uploading an existing CSV file to S3."""

        manifest_path: str = "test/tag_manifest.csv"
        object_key: str = "DeadlineCloud/BucketSweeper/tag_manifest.csv"
        processor._upload_tag_manifest(manifest_path, object_key)

        # Validate S3 call
        mock_s3.upload_file.assert_called_once_with(
            manifest_path,
            "test-bucket",
            object_key,
        )

    def test_upload_tag_manifest_s3_error(self, processor: JobAttachmentsSweeper, mock_s3: Mock):
        """Test uploading manifest when s3 upload fails."""
        mock_s3.upload_file.side_effect = BotoCoreError()

        with pytest.raises(JobAttachmentS3BotoCoreError):
            processor._upload_tag_manifest("test.csv", "test_key")

    def test_get_manifest_etag_value_error(self, processor: JobAttachmentsSweeper, mock_s3: Mock):
        """Test _get_manifest_etag method."""
        mock_s3.head_object.return_value = {"ETag": None}

        with pytest.raises(JobAttachmentsSweeperError):
            processor._get_manifest_etag("test_key")

    def test_get_manifest_etag(self, processor: JobAttachmentsSweeper, mock_s3: Mock):
        """Test _get_manifest_etag method."""
        mock_s3.head_object.return_value = {"ETag": "test-etag"}

        etag: str = processor._get_manifest_etag("test_key")
        assert etag == "test-etag"

        mock_s3.head_object.assert_called_once_with(Bucket="test-bucket", Key="test_key")

    def test_get_manifest_etag_botocore_error(
        self, processor: JobAttachmentsSweeper, mock_s3: Mock
    ):
        """Test _get_manifest_etag when head_object call fails."""
        mock_s3.head_object.side_effect = BotoCoreError()

        with pytest.raises(JobAttachmentS3BotoCoreError):
            processor._get_manifest_etag("test_key")

    def test_submit_tagging_batch_job_error(
        self, processor: JobAttachmentsSweeper, mock_s3_control: Mock
    ):
        """Test _submit_tagging_batch_job when job creation fails."""
        mock_s3_control.create_job.side_effect = Exception("Mocked error")

        with pytest.raises(JobAttachmentsSweeperError):
            processor._submit_tagging_batch_job({}, {})

    def test_create_batch_tag_s3_job(
        self, processor: JobAttachmentsSweeper, mock_s3: Mock, mock_s3_control: Mock
    ):
        """Test creating S3 batch tagging job"""
        # Test data
        s3_manifest_key: str = "test/test.csv"

        # Mock S3 head_object response
        mock_s3.head_object.return_value = {"ETag": "test-etag"}

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
