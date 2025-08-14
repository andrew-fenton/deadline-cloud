# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import pytest

from botocore.exceptions import ClientError
from typing import List
from unittest.mock import MagicMock, patch
from pathlib import Path

from deadline.job_attachments.exceptions import JobAttachmentsError
from deadline.job_attachments.manifest_download import _download_job_manifests_using_s3_keys_to_disk
from deadline.job_attachments.models import JobAttachmentS3Settings


class TestManifestDownload:
    def test_download_job_manifests_happy_path(self, tmp_path: Path):
        """Test successful download of job manifests"""
        mock_session: MagicMock = MagicMock()
        mock_s3_client: MagicMock = MagicMock()
        manifest_keys: List[str] = [
            "DeadlineCloud/Manifests/farm-123/queue-456/Inputs/abc123/manifest_input",
            "DeadlineCloud/Manifests/farm-123/queue-456/job-789/step-1/task-1/section-action/manifest_output",
        ]
        job_settings: JobAttachmentS3Settings = JobAttachmentS3Settings(
            rootPrefix="DeadlineCloud", s3BucketName="deadline-bucket"
        )
        download_directory: Path = tmp_path / "manifests"

        with patch(
            "deadline.job_attachments.manifest_download.get_s3_client", return_value=mock_s3_client
        ):
            _download_job_manifests_using_s3_keys_to_disk(
                session=mock_session,
                manifest_keys=manifest_keys,
                job_attachment_settings=job_settings,
                download_directory=download_directory,
            )

        assert mock_s3_client.download_file.call_count == 2
        mock_s3_client.download_file.assert_any_call(
            "deadline-bucket",
            "DeadlineCloud/Manifests/farm-123/queue-456/Inputs/abc123/manifest_input",
            f"{download_directory}/abc123_manifest_input",
        )
        mock_s3_client.download_file.assert_any_call(
            "deadline-bucket",
            "DeadlineCloud/Manifests/farm-123/queue-456/job-789/step-1/task-1/section-action/manifest_output",
            f"{download_directory}/section-action_manifest_output",
        )

    def test_download_job_manifests_malformed_key(self, tmp_path: Path):
        """Test error handling for malformed manifest key"""
        mock_session: MagicMock = MagicMock()
        manifest_keys: List[str] = ["malformed_key"]
        job_settings: JobAttachmentS3Settings = JobAttachmentS3Settings(
            rootPrefix="DeadlineCloud", s3BucketName="deadline-bucket"
        )
        download_directory: Path = tmp_path / "manifests"

        with pytest.raises(JobAttachmentsError) as error:
            _download_job_manifests_using_s3_keys_to_disk(
                session=mock_session,
                manifest_keys=manifest_keys,
                job_attachment_settings=job_settings,
                download_directory=download_directory,
            )

        assert "Invalid manifest key structure: malformed_key" in str(error.value)

    def test_download_job_manifests_non_existent_directory(self, tmp_path: Path):
        """Test error handling for non existent download directory"""
        mock_session: MagicMock = MagicMock()
        manifest_keys: List[str] = [
            "DeadlineCloud/Manifests/farm-123/queue-456/Inputs/abc123/manifest_input"
        ]
        job_settings: JobAttachmentS3Settings = JobAttachmentS3Settings(
            rootPrefix="DeadlineCloud", s3BucketName="deadline-bucket"
        )
        download_directory: Path = tmp_path / "does_not_exist"

        _download_job_manifests_using_s3_keys_to_disk(
            session=mock_session,
            manifest_keys=manifest_keys,
            job_attachment_settings=job_settings,
            download_directory=download_directory,
        )

        assert download_directory.exists()

    def test_download_job_manifests_client_error(self, tmp_path: Path):
        """Test error handling for ClientError during download"""
        mock_session: MagicMock = MagicMock()
        mock_s3_client: MagicMock = MagicMock()
        mock_s3_client.download_file.side_effect = ClientError(
            error_response={
                "Error": {"Code": "NoSuchKey", "Message": "The specified key does not exist."}
            },
            operation_name="GetObject",
        )

        manifest_keys: List[str] = [
            "DeadlineCloud/Manifests/farm-123/queue-456/Inputs/abc123/manifest_input"
        ]
        job_settings: JobAttachmentS3Settings = JobAttachmentS3Settings(
            rootPrefix="DeadlineCloud", s3BucketName="deadline-bucket"
        )
        download_directory: Path = tmp_path / "manifests"

        with patch(
            "deadline.job_attachments.manifest_download.get_s3_client", return_value=mock_s3_client
        ):
            with pytest.raises(JobAttachmentsError) as error:
                _download_job_manifests_using_s3_keys_to_disk(
                    session=mock_session,
                    manifest_keys=manifest_keys,
                    job_attachment_settings=job_settings,
                    download_directory=download_directory,
                )

        assert "Failed to download manifest" in str(error.value)

    def test_download_job_manifests_fails_fast_on_first_download_error(self, tmp_path: Path):
        """Test download job manifests fails fast"""
        mock_session: MagicMock = MagicMock()
        mock_s3_client: MagicMock = MagicMock()

        def selective_download_error(bucket_name, key, local_path):
            # Only raise error for a single manifest
            if "step-1/task-1/section-action/manifest_output" in key:
                raise IOError()
            # For other manifests, simulate successful download by doing nothing
            return None

        mock_s3_client.download_file.side_effect = selective_download_error

        manifest_keys: List[str] = [
            "DeadlineCloud/Manifests/farm-123/queue-456/Inputs/abc123/manifest_input",
            "DeadlineCloud/Manifests/farm-123/queue-456/job-789/step-1/task-1/section-action/manifest_output",
            "DeadlineCloud/Manifests/farm-123/queue-456/job-789/step-2/task-1/section-action/manifest_output",
        ]
        job_settings: JobAttachmentS3Settings = JobAttachmentS3Settings(
            rootPrefix="DeadlineCloud", s3BucketName="deadline-bucket"
        )
        download_directory: Path = tmp_path / "manifests"

        with patch(
            "deadline.job_attachments.manifest_download.get_s3_client", return_value=mock_s3_client
        ):
            with pytest.raises(JobAttachmentsError) as error:
                _download_job_manifests_using_s3_keys_to_disk(
                    session=mock_session,
                    manifest_keys=manifest_keys,
                    job_attachment_settings=job_settings,
                    download_directory=download_directory,
                )

        assert "Failed to download manifest" in str(error.value)
        assert "step-1/task-1/section-action/manifest_output" in str(error.value)

        assert "Inputs/abc123/manifest_input" not in str(error.value)
        assert "step-2/task-1/section-action/manifest_output" not in str(error.value)
