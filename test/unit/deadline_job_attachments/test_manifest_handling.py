# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from deadline.job_attachments.asset_manifests.hash_algorithms import HashAlgorithm
import pytest

from pathlib import Path
from typing import List
from unittest.mock import MagicMock, mock_open, patch
from botocore.exceptions import ClientError

from deadline.job_attachments.manifest_handling import (
    _extract_asset_hashes_from_manifests,
    _get_all_manifest_s3_keys_for_job,
    _get_input_manifest_keys_for_job,
    _load_manifests_from_disk,
)
from deadline.job_attachments.exceptions import JobAttachmentsError
from deadline.job_attachments.models import AssetHash, JobAttachmentS3Settings
from deadline.job_attachments.asset_manifests.base_manifest import (
    BaseAssetManifest,
)
from deadline.job_attachments.asset_manifests.decode import decode_manifest


class TestManifestHandling:
    def test_get_input_manifest_keys_for_job_happy_path(self):
        """Test successful retrieval of manifest keys"""
        mock_session: MagicMock = MagicMock()
        mock_deadline: MagicMock = MagicMock()
        mock_deadline.get_job.return_value = {
            "attachments": {
                "manifests": [
                    {"inputManifestPath": "farm-id/queue-id/Inputs/123/manifest_input"},
                    {"inputManifestPath": "farm-id/queue-id/Inputs/456/manifest_input"},
                ]
            }
        }

        with patch(
            "deadline.job_attachments.manifest_handling.get_deadline_client",
            return_value=mock_deadline,
        ):
            result: List[str] = _get_input_manifest_keys_for_job(
                session=mock_session,
                s3_root_prefix="DeadlineCloud/",
                farm_id="farm-id",
                queue_id="queue-id",
                job_id="job-id",
            )

        expected: List[str] = [
            "DeadlineCloud/Manifests/farm-id/queue-id/Inputs/123/manifest_input",
            "DeadlineCloud/Manifests/farm-id/queue-id/Inputs/456/manifest_input",
        ]
        assert sorted(result) == sorted(expected)

    def test_get_input_manifest_keys_for_job_trailing_slash_in_prefix(self):
        """Test handling of trailing slash in s3_root_prefix"""
        mock_session: MagicMock = MagicMock()
        mock_deadline: MagicMock = MagicMock()
        mock_deadline.get_job.return_value = {
            "attachments": {
                "manifests": [{"inputManifestPath": "farm-id/queue-id/Inputs/123/manifest_input"}]
            }
        }

        with patch(
            "deadline.job_attachments.manifest_handling.get_deadline_client",
            return_value=mock_deadline,
        ):
            result: List[str] = _get_input_manifest_keys_for_job(
                session=mock_session,
                s3_root_prefix="DeadlineCloud/",  # Note trailing slash
                farm_id="farm-id",
                queue_id="queue-id",
                job_id="job-id",
            )

        assert result == ["DeadlineCloud/Manifests/farm-id/queue-id/Inputs/123/manifest_input"]

    def test_get_input_manifest_keys_for_job_client_error(self):
        """Test handling of ClientError from Deadline client"""
        mock_session: MagicMock = MagicMock()
        mock_deadline: MagicMock = MagicMock()
        mock_deadline.get_job.side_effect = ClientError(
            operation_name="get_job", error_response={"Error": {"Message": "Job not found"}}
        )

        with patch(
            "deadline.job_attachments.manifest_handling.get_deadline_client",
            return_value=mock_deadline,
        ):
            with pytest.raises(JobAttachmentsError) as error:
                _get_input_manifest_keys_for_job(
                    session=mock_session,
                    s3_root_prefix="DeadlineCloud",
                    farm_id="farm-id",
                    queue_id="queue-id",
                    job_id="job-id",
                )

        assert "Failed to get job metadata" in str(error.value)

    def test_get_input_manifest_keys_for_job_missing_attachments(self):
        """Test when job metadata is missing attachments key"""
        mock_session = MagicMock()
        mock_deadline = MagicMock()
        mock_deadline.get_job.return_value = {}

        with patch(
            "deadline.job_attachments.manifest_handling.get_deadline_client",
            return_value=mock_deadline,
        ):
            result: List[str] = _get_input_manifest_keys_for_job(
                session=mock_session,
                s3_root_prefix="DeadlineCloud/",
                farm_id="farm-id",
                queue_id="queue-id",
                job_id="job-id",
            )

        assert result == []

    def test_get_input_manifest_keys_for_job_missing_manifests(self):
        """Test when attachments is missing manifests key"""
        mock_session = MagicMock()
        mock_deadline = MagicMock()
        mock_deadline.get_job.return_value = {"attachments": {}}

        with patch(
            "deadline.job_attachments.manifest_handling.get_deadline_client",
            return_value=mock_deadline,
        ):
            result: List[str] = _get_input_manifest_keys_for_job(
                session=mock_session,
                s3_root_prefix="DeadlineCloud/",
                farm_id="farm-id",
                queue_id="queue-id",
                job_id="job-id",
            )

        assert result == []

    def test_get_input_manifest_keys_for_job_missing_input_manifest_path(self):
        """Test when a manifest is missing inputManifestPath key"""
        mock_session = MagicMock()
        mock_deadline = MagicMock()
        mock_deadline.get_job.return_value = {
            "attachments": {
                "manifests": [
                    {},  # Missing inputManifestPath
                    {"inputManifestPath": "farm-id/queue-id/Inputs/123/manifest_input"},
                ]
            }
        }

        with patch(
            "deadline.job_attachments.manifest_handling.get_deadline_client",
            return_value=mock_deadline,
        ):
            result: List[str] = _get_input_manifest_keys_for_job(
                session=mock_session,
                s3_root_prefix="DeadlineCloud/",
                farm_id="farm-id",
                queue_id="queue-id",
                job_id="job-id",
            )

        assert result == ["DeadlineCloud/Manifests/farm-id/queue-id/Inputs/123/manifest_input"]

    def test_get_all_manifest_s3_keys_happy_path(self):
        """Test retrieving both input and output manifest keys successfully"""
        mock_session: MagicMock = MagicMock()
        job_settings: JobAttachmentS3Settings = JobAttachmentS3Settings(
            rootPrefix="DeadlineCloud", s3BucketName="deadline-bucket"
        )

        input_keys: List[str] = [
            "DeadlineCloud/Manifests/farm-123/queue-456/Inputs/abc123/manifest_input",
            "DeadlineCloud/Manifests/farm-123/queue-456/Inputs/def456/manifest_input",
        ]
        output_keys: List[str] = [
            "DeadlineCloud/Manifests/farm-123/queue-456/job-789/step-1/task-1/session-action/manifest_output",
            "DeadlineCloud/Manifests/farm-123/queue-456/job-789/step-1/task-2/session-action/manifest_output",
        ]

        with patch(
            "deadline.job_attachments.manifest_handling._get_input_manifest_keys_for_job",
            return_value=input_keys,
        ), patch(
            "deadline.job_attachments.manifest_handling._get_tasks_manifests_keys_from_s3",
            return_value=output_keys,
        ):
            result: List[str] = _get_all_manifest_s3_keys_for_job(
                session=mock_session,
                job_attachment_settings=job_settings,
                farm_id="farm-123",
                queue_id="queue-456",
                job_id="job-789",
            )

        assert result == input_keys + output_keys

    def test_get_all_manifest_s3_keys_only_input_manifests(self):
        """Test case where job has only input manifests"""
        mock_session: MagicMock = MagicMock()
        job_settings: JobAttachmentS3Settings = JobAttachmentS3Settings(
            rootPrefix="DeadlineCloud", s3BucketName="deadline-bucket"
        )

        input_keys: List[str] = [
            "DeadlineCloud/Manifests/farm-123/queue-456/Inputs/abc123/manifest_input"
        ]

        with patch(
            "deadline.job_attachments.manifest_handling._get_input_manifest_keys_for_job",
            return_value=input_keys,
        ), patch(
            "deadline.job_attachments.download._get_tasks_manifests_keys_from_s3", return_value=[]
        ):
            result: List[str] = _get_all_manifest_s3_keys_for_job(
                session=mock_session,
                job_attachment_settings=job_settings,
                farm_id="farm-123",
                queue_id="queue-456",
                job_id="job-789",
            )

        assert result == input_keys

    def test_get_all_manifest_s3_keys_input_manifest_fails(self):
        """Test error handling when input manifest retrieval fails"""
        mock_session: MagicMock = MagicMock()
        job_settings: JobAttachmentS3Settings = JobAttachmentS3Settings(
            rootPrefix="DeadlineCloud", s3BucketName="deadline-bucket"
        )

        with patch(
            "deadline.job_attachments.manifest_handling._get_input_manifest_keys_for_job",
            side_effect=Exception("Failed to retrieve input manifests"),
        ):
            with pytest.raises(JobAttachmentsError) as error:
                _get_all_manifest_s3_keys_for_job(
                    session=mock_session,
                    job_attachment_settings=job_settings,
                    farm_id="farm-123",
                    queue_id="queue-456",
                    job_id="job-789",
                )

        assert "Failed to get all job manifest keys: Failed to retrieve input manifests" in str(
            error.value
        )

    def test_get_all_manifest_s3_keys_output_manifest_fails(self):
        """Test error handling when output manifest retrieval fails"""
        mock_session: MagicMock = MagicMock()
        job_settings: JobAttachmentS3Settings = JobAttachmentS3Settings(
            rootPrefix="DeadlineCloud", s3BucketName="deadline-bucket"
        )

        with patch(
            "deadline.job_attachments.manifest_handling._get_input_manifest_keys_for_job",
            return_value=[],
        ), patch(
            "deadline.job_attachments.manifest_handling._get_tasks_manifests_keys_from_s3",
            side_effect=Exception("Failed to retrieve output manifests"),
        ):
            with pytest.raises(JobAttachmentsError) as error:
                _get_all_manifest_s3_keys_for_job(
                    session=mock_session,
                    job_attachment_settings=job_settings,
                    farm_id="farm-123",
                    queue_id="queue-456",
                    job_id="job-789",
                )

        assert "Failed to get all job manifest keys: Failed to retrieve output manifests" in str(
            error.value
        )

    def test_load_manifests_from_disk_multiple_files_happy_path(self, tmp_path: Path):
        """Test loading multiple manifest files from disk successfully"""
        manifests_dir: Path = tmp_path / "manifests"
        manifests_dir.mkdir()

        manifest_content_1: str = """
        {
            "hashAlg": "xxh128",
            "manifestVersion": "2023-03-03",
            "paths": [
                {
                    "hash": "hash1",
                    "mtime": 1,
                    "path": "path1",
                    "size": 1
                },
                {
                    "hash": "hash2",
                    "mtime": 2,
                    "path": "path2",
                    "size": 2
                }
            ],
            "totalSize": 3
        }"""

        manifest_content_2: str = """
        {
            "hashAlg": "xxh128",
            "manifestVersion": "2023-03-03",
            "paths": [
                {
                    "hash": "hash3",
                    "mtime": 3,
                    "path": "path3",
                    "size": 3
                }
            ],
            "totalSize": 3
        }"""

        # Create manifest files
        (manifests_dir / "manifest1.json").write_text(manifest_content_1)
        (manifests_dir / "manifest2.json").write_text(manifest_content_2)

        resulting_manifest_list: List[BaseAssetManifest] = _load_manifests_from_disk(manifests_directory=manifests_dir)

        expected_manifest_1: BaseAssetManifest = decode_manifest(manifest_content_1)
        expected_manifest_2: BaseAssetManifest = decode_manifest(manifest_content_2)

        assert len(resulting_manifest_list) == 2
        assert expected_manifest_1 in resulting_manifest_list
        assert expected_manifest_2 in resulting_manifest_list

    def test_load_manifests_from_disk_empty_directory(self, tmp_path: Path):
        """Test loading from a directory with no manifest files"""
        manifest_dir: Path = tmp_path / "manifests"
        manifest_dir.mkdir()

        manifests: List[BaseAssetManifest] = _load_manifests_from_disk(manifest_dir)

        assert manifests == []

    def test_load_manifests_from_disk_directory_not_exist(self):
        """Test loading from a non-existent directory"""
        with pytest.raises(JobAttachmentsError) as err:
            _load_manifests_from_disk(Path("/nonexistent/directory"))

        assert "Manifests directory does not exist" in str(err.value)

    def test_load_manifests_from_disk_io_error(self, tmp_path):
        """Test handling IO error when reading manifest file"""
        manifest_dir: Path = tmp_path / "manifests"
        manifest_dir.mkdir()

        # Create a manifest file
        manifest_path = manifest_dir / "manifest.json"
        manifest_path.touch()

        with patch.object(Path, 'read_text', side_effect=IOError("Permission denied")):
            with pytest.raises(JobAttachmentsError) as err:
                _load_manifests_from_disk(manifest_dir)

            assert "Failed to load manifests from disk" in str(err.value)
            assert "Permission denied" in str(err.value)

    def test_extract_asset_hashes_happy_path(self):
        """Test extracting hashes from multiple manifests successfully"""
        manifest_content_1: str = """
        {
            "hashAlg": "xxh128",
            "manifestVersion": "2023-03-03",
            "paths": [
                {
                    "hash": "hash1",
                    "mtime": 1,
                    "path": "path1",
                    "size": 1
                },
                {
                    "hash": "hash2",
                    "mtime": 2,
                    "path": "path2",
                    "size": 2
                }
            ],
            "totalSize": 3
        }"""

        manifest_content_2: str = """
        {
            "hashAlg": "xxh128",
            "manifestVersion": "2023-03-03",
            "paths": [
                {
                    "hash": "hash3",
                    "mtime": 3,
                    "path": "path3",
                    "size": 3
                }
            ],
            "totalSize": 3
        }"""

        manifests: List[BaseAssetManifest] = [
            decode_manifest(manifest_content_1),
            decode_manifest(manifest_content_2),
        ]

        result: List[AssetHash] = _extract_asset_hashes_from_manifests(manifests)

        assert len(result) == 3
        assert AssetHash("hash1", HashAlgorithm.XXH128) in result
        assert AssetHash("hash2", HashAlgorithm.XXH128) in result
        assert AssetHash("hash3", HashAlgorithm.XXH128) in result

    def test_extract_asset_hashes_with_duplicates(self):
        """Test extracting hashes from manifests with duplicate hashes"""
        manifest_content_1: str = """
            {
                "hashAlg": "xxh128",
                "manifestVersion": "2023-03-03",
                "paths": [
                    {
                        "hash": "hash1",
                        "mtime": 1,
                        "path": "path1",
                        "size": 1
                    },
                    {
                        "hash": "hash2",
                        "mtime": 2,
                        "path": "path2",
                        "size": 2
                    }
                ],
                "totalSize": 3
            }"""

        manifest_content_2: str = """
            {
                "hashAlg": "xxh128",
                "manifestVersion": "2023-03-03",
                "paths": [
                    {
                        "hash": "hash1",
                        "mtime": 1,
                        "path": "path1",
                        "size": 1
                    }
                ],
                "totalSize": 1
            }"""

        manifests: List[BaseAssetManifest] = [
            decode_manifest(manifest_content_1),
            decode_manifest(manifest_content_2),
        ]

        result: List[AssetHash] = _extract_asset_hashes_from_manifests(manifests)

        assert len(result) == 2
        assert AssetHash("hash1", HashAlgorithm.XXH128) in result
        assert AssetHash("hash2", HashAlgorithm.XXH128) in result
