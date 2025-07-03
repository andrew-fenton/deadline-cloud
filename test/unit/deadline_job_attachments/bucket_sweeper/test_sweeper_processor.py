# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from botocore.exceptions import BotoCoreError
import pytest
import shutil
import os
import csv
from unittest.mock import Mock
from deadline.job_attachments.bucket_sweeper.sweeper_processor import SweeperProcessor
from deadline.job_attachments.exceptions import (
    SweeperProcessorError,
    JobAttachmentS3BotoCoreError,
)


@pytest.fixture
def mock_clients():
    """Fixture to create mock AWS clients"""
    return {
        "s3": Mock(),
        "s3_control": Mock(),
        "deadline": Mock(),
        "storage": Mock(),
        "job_attachments": Mock(),
    }


@pytest.fixture
def processor(mock_clients):
    """Fixture to create SweeperProcessor instance with mock clients"""
    return SweeperProcessor(
        s3_client=mock_clients["s3"],
        s3_control_client=mock_clients["s3_control"],
        deadline_client=mock_clients["deadline"],
        storage=mock_clients["storage"],
        job_attachments=mock_clients["job_attachments"],
        farm_id="test-farm",
    )


@pytest.fixture
def test_dir():
    """Create and cleanup a test directory."""
    temp_dir = "./bucket_sweeper_test"
    os.makedirs(temp_dir, exist_ok=True)

    yield temp_dir

    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)


class TestSweeperProcessor:

    def test_create_tag_manifest_empty_list(self, processor, test_dir):
        """Test creating a tag manifest with an empty delete list."""
        manifest_path = processor._create_tag_manifest(test_dir, "test_bucket", [])

        with open(manifest_path, "r") as file:
            assert file.read() == ""

    def test_create_tag_manifest_io_error(self, processor, test_dir, monkeypatch):
        """Test creating a tag manifest when creation fails."""

        def mock_open(*args, **kwargs):
            raise IOError("Mocked IO Error")

        monkeypatch.setattr("builtins.open", mock_open)

        with pytest.raises(SweeperProcessorError):
            processor._create_tag_manifest(test_dir, "test_bucket", ["object_key"])

    def test_create_tag_manifest(self, processor, test_dir):
        """Create a tag manifest and validate CSV content."""

        # Sample data
        bucket_name = "test_bucket"
        delete_list = [
            "DeadlineCloud/Manifests/farm-123/queue-123/job-123/step-123/session/456_output",
            "DeadlineCloud/Manifests/farm-123/queue-123/Inputs/123/456_input",
            "DeadlineCloud/Data/hash.xx128",
        ]

        manifest_path = processor._create_tag_manifest(
            test_dir, bucket_name, delete_list
        )

        assert os.path.exists(manifest_path)

        # Validate CSV output
        with open(manifest_path, "r") as file:
            reader = csv.reader(file)
            rows = list(reader)

            # fmt: off
            assert sorted(rows) == sorted([
                    ["test_bucket", "DeadlineCloud/Manifests/farm-123/queue-123/job-123/step-123/session/456_output"],
                    ["test_bucket", "DeadlineCloud/Manifests/farm-123/queue-123/Inputs/123/456_input"],
                    ["test_bucket", "DeadlineCloud/Data/hash.xx128"],
            ])
            # fmt: on

    def test_upload_tag_manifest(self, processor, mock_clients):
        """Test uploading an existing CSV file to S3."""

        manifest_path = "test/tag_manifest.csv"
        bucket_name = "test_bucket"
        object_key = "DeadlineCloud/BucketSweeper/tag_manifest.csv"
        processor._upload_tag_manifest(manifest_path, bucket_name, object_key)

        # Validate S3 call
        mock_clients["s3"].upload_file.assert_called_once_with(
            manifest_path,
            bucket_name,
            object_key,
        )

    def test_upload_tag_manifest_s3_error(self, processor, mock_clients):
        """Test uploading manifest when s3 upload fails."""
        mock_clients["s3"].upload_file.side_effect = BotoCoreError()

        with pytest.raises(JobAttachmentS3BotoCoreError):
            processor._upload_tag_manifest("test.csv", "test_bucket", "test_key")

    def test_get_manifest_etag(self, processor, mock_clients):
        """Test _get_manifest_etag method."""
        mock_clients["s3"].head_object.return_value = {"ETag": "test-etag"}

        etag = processor._get_manifest_etag("test_bucket", "test_key")
        assert etag == "test-etag"

        mock_clients["s3"].head_object.assert_called_once_with(
            Bucket="test_bucket", Key="test_key"
        )

    def test_get_manifest_etag_botocore_error(self, processor, mock_clients):
        """Test _get_manifest_etag when head_object call fails."""
        mock_clients["s3"].head_object.side_effect = BotoCoreError()

        with pytest.raises(JobAttachmentS3BotoCoreError):
            processor._get_manifest_etag("test_bucket", "test_key")

    def test_create_manifest_config(self, processor):
        """Test _create_manifest_config method."""
        config = processor._create_manifest_config(
            "test_bucket", "test_key", "test-etag"
        )

        assert config == {
            "Spec": {
                "Format": "S3BatchOperations_CSV_20180820",
                "Fields": ["Bucket", "Key"],
            },
            "Location": {
                "ObjectArn": "arn:aws:s3:::test_bucket/test_key",
                "ETag": "test-etag",
            },
        }

    def test_create_tagging_operation(self, processor):
        """Test _create_tagging_operation method."""
        operation = processor._create_tagging_operation()

        assert operation == {
            "S3PutObjectTagging": {
                "TagSet": [
                    {"Key": "delete", "Value": "True"},
                ]
            }
        }

    def test_submit_batch_job_error(self, processor, mock_clients):
        """Test _submit_batch_job when job creation fails."""
        mock_clients["s3_control"].create_job.side_effect = Exception("Mocked error")

        with pytest.raises(SweeperProcessorError):
            processor._submit_batch_job("123", False, "role_arn", {}, {}, {}, 10)

    def test_create_batch_tag_s3_job(self, processor, mock_clients):
        """Test creating S3 batch tagging job"""
        # Test data
        account_id = "123456789"
        role_arn = "arn:aws:iam::123456789:role/test-role"
        bucket_name = "test_bucket"
        s3_manifest_key = "test/test.csv"

        # Mock S3 head_object response
        mock_clients["s3"].head_object.return_value = {"ETag": "test-etag"}

        # Call method
        processor._create_batch_tag_s3_job(
            account_id, role_arn, bucket_name, s3_manifest_key
        )

        # Verify S3 Control create_job was called correctly
        expected_operation = {
            "S3PutObjectTagging": {"TagSet": [{"Key": "delete", "Value": "True"}]}
        }

        expected_manifest = {
            "Spec": {
                "Format": "S3BatchOperations_CSV_20180820",
                "Fields": ["Bucket", "Key"],
            },
            "Location": {
                "ObjectArn": f"arn:aws:s3:::{bucket_name}/{s3_manifest_key}",
                "ETag": "test-etag",
            },
        }

        expected_priority = 10
        expected_confirmation_setting = False
        expected_report_settings = {"Enabled": False}

        mock_clients["s3_control"].create_job.assert_called_once_with(
            AccountId=account_id,
            ConfirmationRequired=expected_confirmation_setting,
            RoleArn=role_arn,
            Operation=expected_operation,
            Manifest=expected_manifest,
            Report=expected_report_settings,
            Priority=expected_priority,
        )
