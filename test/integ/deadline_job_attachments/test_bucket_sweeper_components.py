# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import pytest

from typing import List
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone, timedelta
from pathlib import Path

from deadline.job_attachments.bucket_sweeper.bucket_sweeper_components import (
    SweeperDependencies,
    _initialize_dependencies,
    _collect_farm_queue_job_triples,
    _process_manifests_and_create_retention_records,
    _determine_objects_to_delete,
    _create_deletion_batch_job,
)
from deadline.job_attachments.bucket_sweeper.job_attachments_sweeper import JobAttachmentsSweeper
from deadline.job_attachments.models import FarmQueueJobTriple, JobAttachmentFetchingStrategy


@pytest.fixture
def mock_boto3_session() -> MagicMock:
    session: MagicMock = MagicMock()
    session.client.side_effect = lambda service_name: MagicMock()
    return session


@pytest.fixture
def mock_get_session() -> MagicMock:
    return MagicMock()


@pytest.fixture
def mock_sweeper() -> MagicMock:
    return MagicMock(spec=JobAttachmentsSweeper)


@pytest.fixture
def mock_retention_handler() -> MagicMock:
    return MagicMock()


def test_initialize_services(
    mock_get_session: MagicMock, mock_boto3_session: MagicMock, tmp_path: Path
):
    """Test that dependencies are initialized successfully."""
    working_dir: Path = tmp_path / "test_sweeper"
    working_dir.mkdir(exist_ok=True)

    components: SweeperDependencies = _initialize_dependencies(
        working_directory=working_dir,
        bucket_name="test-bucket",
        root_prefix="test-prefix",
        boto3_session=mock_boto3_session,
        role_arn="test-role-arn",
        job_attachment_fetching_strategy=JobAttachmentFetchingStrategy.PAGINATION,
        job_attachments_file_key="",
        
    )

    assert isinstance(components, SweeperDependencies)
    mock_boto3_session.client.assert_any_call("s3")
    mock_boto3_session.client.assert_any_call("s3control")
    mock_boto3_session.client.assert_any_call("deadline")


def test_collect_farm_queue_job_triples(mock_sweeper: MagicMock, mock_boto3_session: MagicMock):
    """Test collection of farm-queue-job triples from S3 and Deadline service."""
    mock_sweeper.get_queues_in_farms_from_s3.return_value = {
        "farm-1": ["queue-1", "queue-2"],
        "farm-2": ["queue-3"],
    }

    # Mock the _list_active_job_ids function
    with patch(
        "deadline.job_attachments.bucket_sweeper.bucket_sweeper_components._list_active_job_ids"
    ) as mock_list_jobs:
        mock_list_jobs.side_effect = lambda **kwargs: {
            "queue-1": ["job-1", "job-2"] if kwargs["farm_id"] == "farm-1" else [],
            "queue-2": ["job-3"] if kwargs["farm_id"] == "farm-1" else [],
            "queue-3": ["job-4", "job-5"] if kwargs["farm_id"] == "farm-2" else [],
        }

        # Random retention date to execute the call
        retention_date: datetime = datetime.now(timezone.utc) - timedelta(days=30)
        result: List[FarmQueueJobTriple] = _collect_farm_queue_job_triples(
            mock_sweeper, mock_boto3_session, retention_date
        )

        assert FarmQueueJobTriple("farm-1", "queue-1", "job-2") in result
        assert FarmQueueJobTriple("farm-1", "queue-2", "job-3") in result
        assert FarmQueueJobTriple("farm-2", "queue-3", "job-4") in result
        assert FarmQueueJobTriple("farm-2", "queue-3", "job-5") in result


def test_process_manifests_and_create_retention_records(
    mock_boto3_session: MagicMock,
    mock_retention_handler: MagicMock,
    tmp_path: Path,
):
    """Test processing job manifests and creating retention records for asset tracking."""
    farm_queue_job_triples: List[FarmQueueJobTriple] = [
        FarmQueueJobTriple("farm-1", "queue-1", "job-1")
    ]

    job_attachment_settings: MagicMock = MagicMock()

    # Mock the manifest handling functions
    with patch(
        "deadline.job_attachments.bucket_sweeper.bucket_sweeper_components._get_all_manifest_s3_keys_for_job"
    ) as mock_get_keys, patch(
        "deadline.job_attachments.bucket_sweeper.bucket_sweeper_components._download_job_manifests_using_s3_keys_to_disk"
    ) as mock_download, patch(
        "deadline.job_attachments.bucket_sweeper.bucket_sweeper_components._load_manifests_from_disk"
    ) as mock_load, patch(
        "deadline.job_attachments.bucket_sweeper.bucket_sweeper_components._extract_asset_hashes_from_manifests"
    ) as mock_extract:
        mock_get_keys.return_value = ["manifest-key-1", "manifest-key-2"]
        mock_load.return_value = [MagicMock()]
        mock_extract.return_value = [
            MagicMock(hash="hash1", hash_alg=MagicMock(value="xxh128")),
            MagicMock(hash="hash2", hash_alg=MagicMock(value="xxh128")),
        ]

        _process_manifests_and_create_retention_records(
            farm_queue_job_triples=farm_queue_job_triples,
            boto3_session=mock_boto3_session,
            job_attachment_s3_settings=job_attachment_settings,
            working_directory=tmp_path,
            retention_record_handler=mock_retention_handler,
        )

        mock_get_keys.assert_called_once()
        mock_download.assert_called_once()
        mock_load.assert_called_once()
        mock_extract.assert_called_once()

        # Check that retention records were created
        mock_retention_handler.insert_retention_records.assert_called_once()
        records = mock_retention_handler.insert_retention_records.call_args[1]["records"]
        assert len(records) == 4  # 2 manifest keys + 2 asset keys


def test_determine_objects_to_delete(mock_sweeper: MagicMock):
    """Test identification of S3 objects eligible for deletion based on retention policy."""
    farm_queue_job_triples: List[FarmQueueJobTriple] = [
        FarmQueueJobTriple("farm-1", "queue-1", "job-1"),
        FarmQueueJobTriple("farm-1", "queue-1", "job-2"),
    ]

    mock_sweeper.get_attachments_to_retain.return_value = {"key1", "key2", "key3"}
    mock_sweeper.get_attachments_to_delete.return_value = ["key4", "key5"]

    retention_date: datetime = datetime.now(timezone.utc) - timedelta(days=30)
    result: List[str] = _determine_objects_to_delete(
        farm_queue_job_triples=farm_queue_job_triples,
        sweeper=mock_sweeper,
        retention_datetime=retention_date,
        root_prefix="test-prefix",
    )

    assert result == ["key4", "key5"]
    mock_sweeper.get_attachments_to_retain.assert_called_once()
    mock_sweeper.get_attachments_to_delete.assert_called_once_with(
        s3_keys_to_retain={"key1", "key2", "key3"},
        retention_datetime=retention_date,
        root_prefix="test-prefix",
    )


def test_create_and_upload_batch_job(mock_sweeper, tmp_path):
    """Test creation and upload of S3 batch job for object deletion."""
    delete_list = ["key1", "key2", "key3"]

    _create_deletion_batch_job(
        delete_list=delete_list,
        sweeper=mock_sweeper,
        working_directory=tmp_path,
        root_prefix="test-prefix",
        dry_run=False,
    )

    mock_sweeper._create_tag_manifest.assert_called_once()
    mock_sweeper._upload_tag_manifest.assert_called_once()
    mock_sweeper._create_batch_tag_s3_job.assert_called_once()
