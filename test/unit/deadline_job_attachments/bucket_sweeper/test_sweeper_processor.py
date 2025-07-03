# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import pytest
import shutil
import os
import csv
from unittest.mock import Mock
from deadline.job_attachments.bucket_sweeper.sweeper_processor import SweeperProcessor


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
        """Uploads an existing CSV file to S3."""

        manifest_path = "test/tag_manifest.csv"
        bucket_name = "test_bucket"
        processor._upload_tag_manifest(manifest_path, bucket_name)

        # Validate S3 call
        expected_object_key = "DeadlineCloud/BucketSweeper/tag_manifest.csv"
        mock_clients["s3"].upload_file.assert_called_once_with(
            manifest_path,
            bucket_name,
            expected_object_key,
        )

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

        mock_clients["s3_control"].create_job.assert_called_once_with(
            AccountId=account_id,
            RoleArn=role_arn,
            Operation=expected_operation,
            Manifest=expected_manifest,
            EnableManifestOutput=False,
        )
