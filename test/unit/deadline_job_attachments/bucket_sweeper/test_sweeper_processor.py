# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from botocore.exceptions import BotoCoreError
import pytest
import os
import csv
from typing import Dict, List, Any
from pathlib import Path
from unittest.mock import Mock
from deadline.job_attachments.bucket_sweeper.sweeper_processor import SweeperProcessor
from deadline.job_attachments.exceptions import (
    SweeperProcessorError,
    JobAttachmentS3BotoCoreError,
)


@pytest.fixture
def mock_clients() -> Dict[str, Mock]:
    """Fixture to create mock AWS clients"""
    return {
        "s3": Mock(),
        "s3_control": Mock(),
        "deadline": Mock(),
        "storage": Mock(),
        "job_attachments": Mock(),
    }


@pytest.fixture
def processor(mock_clients: Dict[str, Mock]) -> SweeperProcessor:
    """Fixture to create SweeperProcessor instance with mock clients"""
    return SweeperProcessor(
        s3_client=mock_clients["s3"],
        s3_control_client=mock_clients["s3_control"],
        deadline_client=mock_clients["deadline"],
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


class TestSweeperProcessor:
    def test_create_tag_manifest_empty_list(self, processor: SweeperProcessor, test_dir: Path):
        """Test creating a tag manifest with an empty delete list."""
        manifest_path: str = processor._create_tag_manifest(str(test_dir), [])

        with open(manifest_path, "r") as file:
            assert file.read() == ""

    def test_create_tag_manifest_io_error(
        self,
        processor: SweeperProcessor,
        test_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        """Test creating a tag manifest when creation fails."""

        def mock_open(*args, **kwargs):
            raise IOError("Mocked IO Error")

        monkeypatch.setattr("builtins.open", mock_open)

        with pytest.raises(SweeperProcessorError):
            processor._create_tag_manifest(str(test_dir), ["object_key"])

    def test_create_tag_manifest(self, processor: SweeperProcessor, test_dir: Path):
        """Create a tag manifest and validate CSV content."""

        # Sample data
        delete_list: List[str] = [
            "DeadlineCloud/Manifests/farm-123/queue-123/job-123/step-123/session/456_output",
            "DeadlineCloud/Manifests/farm-123/queue-123/Inputs/123/456_input",
            "DeadlineCloud/Data/hash.xx128",
        ]

        manifest_path: str = processor._create_tag_manifest(str(test_dir), delete_list)

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

    def test_upload_tag_manifest(self, processor: SweeperProcessor, mock_clients: Dict[str, Mock]):
        """Test uploading an existing CSV file to S3."""

        manifest_path: str = "test/tag_manifest.csv"
        object_key: str = "DeadlineCloud/BucketSweeper/tag_manifest.csv"
        processor._upload_tag_manifest(manifest_path, object_key)

        # Validate S3 call
        mock_clients["s3"].upload_file.assert_called_once_with(
            manifest_path,
            "test-bucket",
            object_key,
        )

    def test_upload_tag_manifest_s3_error(
        self, processor: SweeperProcessor, mock_clients: Dict[str, Mock]
    ):
        """Test uploading manifest when s3 upload fails."""
        mock_clients["s3"].upload_file.side_effect = BotoCoreError()

        with pytest.raises(JobAttachmentS3BotoCoreError):
            processor._upload_tag_manifest("test.csv", "test_key")

    def test_get_manifest_etag_value_error(
        self, processor: SweeperProcessor, mock_clients: Dict[str, Mock]
    ):
        """Test _get_manifest_etag method."""
        mock_clients["s3"].head_object.return_value = {"ETag": None}

        with pytest.raises(ValueError):
            processor._get_manifest_etag("test_key")

    def test_get_manifest_etag(self, processor: SweeperProcessor, mock_clients: Dict[str, Mock]):
        """Test _get_manifest_etag method."""
        mock_clients["s3"].head_object.return_value = {"ETag": "test-etag"}

        etag: str = processor._get_manifest_etag("test_key")
        assert etag == "test-etag"

        mock_clients["s3"].head_object.assert_called_once_with(Bucket="test-bucket", Key="test_key")

    def test_get_manifest_etag_botocore_error(
        self, processor: SweeperProcessor, mock_clients: Dict[str, Mock]
    ):
        """Test _get_manifest_etag when head_object call fails."""
        mock_clients["s3"].head_object.side_effect = BotoCoreError()

        with pytest.raises(JobAttachmentS3BotoCoreError):
            processor._get_manifest_etag("test_key")

    def test_create_manifest_config(self, processor: SweeperProcessor):
        """Test _create_manifest_config method."""
        config: Dict[str, Any] = processor._create_manifest_config("test_key", "test-etag")

        assert config == {
            "Spec": {
                "Format": "S3BatchOperations_CSV_20180820",
                "Fields": ["Bucket", "Key"],
            },
            "Location": {
                "ObjectArn": "arn:aws:s3:::test-bucket/test_key",
                "ETag": "test-etag",
            },
        }

    def test_create_delete_tagging_operation(self, processor: SweeperProcessor):
        """Test _create_delete_tagging_operation method."""
        operation: Dict[str, Any] = processor._create_delete_tagging_operation()

        assert operation == {
            "S3PutObjectTagging": {
                "TagSet": [
                    {"Key": "delete", "Value": "True"},
                ]
            }
        }

    def test_submit_tagging_batch_job_error(self, processor, mock_clients):
        """Test _submit_tagging_batch_job when job creation fails."""
        mock_clients["s3_control"].create_job.side_effect = Exception("Mocked error")

        with pytest.raises(SweeperProcessorError):
            processor._submit_tagging_batch_job(False, {}, {}, {}, 10)

    def test_create_batch_tag_s3_job(self, processor, mock_clients):
        """Test creating S3 batch tagging job"""
        # Test data
        s3_manifest_key: str = "test/test.csv"

        # Mock S3 head_object response
        mock_clients["s3"].head_object.return_value = {"ETag": "test-etag"}

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

        mock_clients["s3_control"].create_job.assert_called_once_with(
            AccountId="test-account-id",
            RoleArn="test-role-arn",
            Operation=expected_operation,
            Manifest=expected_manifest,
            ConfirmationRequired=expected_confirmation_setting,
            Report=expected_report_settings,
            Priority=expected_priority,
        )
