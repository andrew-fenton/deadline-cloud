# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import pytest
import boto3

from typing import List
from unittest.mock import Mock
from datetime import datetime
from botocore.paginate import Paginator
from botocore.exceptions import ClientError

from deadline.job_attachments.exceptions import JobAttachmentsS3BucketListerError
from deadline.job_attachments.job_attachments_s3_bucket_lister import S3PaginationLister
from deadline.job_attachments.models import JobAttachmentS3Settings, S3ObjectData


class TestS3PaginationLister:
    @pytest.fixture
    def mock_session(self) -> Mock:
        session = Mock(spec=boto3.Session)
        return session

    @pytest.fixture
    def settings(self) -> JobAttachmentS3Settings:
        return JobAttachmentS3Settings(s3BucketName="test-bucket", rootPrefix="DeadlineCloud")

    @pytest.fixture
    def mock_paginator(self) -> Mock:
        return Mock(spec=Paginator)

    @pytest.fixture
    def mock_s3_client(self, mock_paginator: Mock) -> Mock:
        client = Mock()
        client.get_paginator.return_value = mock_paginator
        return client

    def test_list_common_prefixes_with_delimeter_happy_path(
        self,
        mock_session: Mock,
        settings: JobAttachmentS3Settings,
        mock_s3_client: Mock,
        mock_paginator: Mock,
    ):
        """Test listing common prefixes with successful response"""
        mock_session.client.return_value = mock_s3_client

        mock_paginator.paginate.return_value = [
            {
                "CommonPrefixes": [
                    {"Prefix": "queue-1/job-1/"},
                    {"Prefix": "queue-1/job-2/"},
                    {"Prefix": "queue-1/job-3/"},
                ]
            }
        ]

        lister: S3PaginationLister = S3PaginationLister(mock_session, settings)
        results = list(lister.list_common_prefixes_with_delimeter("queue-1/"))

        assert len(results) == 3
        assert results == [
            {"Prefix": "queue-1/job-1/"},
            {"Prefix": "queue-1/job-2/"},
            {"Prefix": "queue-1/job-3/"},
        ]

        mock_paginator.paginate.assert_called_once_with(
            Bucket="test-bucket", Prefix="queue-1/", Delimiter="/"
        )

    def test_list_common_prefixes_with_delimeter_client_error(
        self,
        mock_session: Mock,
        settings: JobAttachmentS3Settings,
        mock_s3_client: Mock,
        mock_paginator: Mock,
    ):
        """Test handling of ClientError when listing common prefixes"""
        mock_session.client.return_value = mock_s3_client

        mock_paginator.paginate.side_effect = ClientError(
            error_response={"Error": {"Code": "IOError", "Message": "Failed to read from S3"}},
            operation_name="ListObjectsV2",
        )

        lister: S3PaginationLister = S3PaginationLister(mock_session, settings)

        with pytest.raises(JobAttachmentsS3BucketListerError) as err:
            list(lister.list_common_prefixes_with_delimeter("queue-1/"))

        assert "Failed to list job attachments from S3" in str(err.value)

        mock_paginator.paginate.assert_called_once_with(
            Bucket="test-bucket", Prefix="queue-1/", Delimiter="/"
        )

    def test_list_job_attachments_basic_listing(
        self,
        mock_session: Mock,
        settings: JobAttachmentS3Settings,
        mock_s3_client: Mock,
        mock_paginator: Mock,
    ):
        """Test basic listing functionality with single page of results"""
        mock_session.client.return_value = mock_s3_client

        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {
                        "Key": "DeadlineCloud/test/file1",
                        "Size": 100,
                        "LastModified": datetime(2025, 1, 1),
                        "ETag": "abc123",
                    }
                ]
            }
        ]

        lister: S3PaginationLister = S3PaginationLister(mock_session, settings)
        results: List[S3ObjectData] = list(lister.list_job_attachments("DeadlineCloud/test/"))

        assert len(results) == 1
        assert results[0] == S3ObjectData(
            key="DeadlineCloud/test/file1",
            size=100,
            last_modified=datetime(2025, 1, 1),
            etag="abc123",
        )

        mock_paginator.paginate.assert_called_once_with(
            Bucket="test-bucket", Prefix="DeadlineCloud/test/"
        )

    def test_list_job_attachments_multiple_pages(
        self,
        mock_session: Mock,
        settings: JobAttachmentS3Settings,
        mock_s3_client: Mock,
        mock_paginator: Mock,
    ):
        """Test handling of multiple pages of results"""
        mock_session.client.return_value = mock_s3_client

        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {
                        "Key": "DeadlineCloud/test/file1",
                        "Size": 100,
                        "LastModified": datetime(2025, 1, 1),
                        "ETag": "abc123",
                    }
                ]
            },
            {
                "Contents": [
                    {
                        "Key": "DeadlineCloud/test/file2",
                        "Size": 200,
                        "LastModified": datetime(2025, 1, 2),
                        "ETag": "def456",
                    }
                ]
            },
        ]

        lister: S3PaginationLister = S3PaginationLister(mock_session, settings)
        results: List[S3ObjectData] = list(lister.list_job_attachments("DeadlineCloud/test/"))

        expected_result = [
            S3ObjectData(
                key="DeadlineCloud/test/file1",
                size=100,
                last_modified=datetime(2025, 1, 1),
                etag="abc123",
            ),
            S3ObjectData(
                key="DeadlineCloud/test/file2",
                size=200,
                last_modified=datetime(2025, 1, 2),
                etag="def456",
            ),
        ]

        assert len(results) == 2
        assert results == expected_result

    def test_list_job_attachments_empty_results(
        self,
        mock_session: Mock,
        settings: JobAttachmentS3Settings,
        mock_s3_client: Mock,
        mock_paginator: Mock,
    ):
        """Test handling of no results"""
        mock_session.client.return_value = mock_s3_client

        mock_paginator.paginate.return_value = [{"Contents": []}]

        lister: S3PaginationLister = S3PaginationLister(mock_session, settings)
        results: List[S3ObjectData] = list(lister.list_job_attachments("test/"))

        assert len(results) == 0

    def test_list_job_attachments_missing_contents(
        self,
        mock_session: Mock,
        settings: JobAttachmentS3Settings,
        mock_s3_client: Mock,
        mock_paginator: Mock,
    ):
        """Test handling of missing Contents key in response"""
        mock_session.client.return_value = mock_s3_client

        mock_paginator.paginate.return_value = [{}]  # No Contents key

        lister: S3PaginationLister = S3PaginationLister(mock_session, settings)
        results: List[S3ObjectData] = list(lister.list_job_attachments("DeadlineCloud/test/"))

        assert len(results) == 0

    def test_list_job_attachments_client_error(
        self,
        mock_session: Mock,
        settings: JobAttachmentS3Settings,
        mock_s3_client: Mock,
        mock_paginator: Mock,
    ):
        """Test handling of ClientError"""
        mock_session.client.return_value = mock_s3_client

        mock_paginator.paginate.side_effect = ClientError(
            error_response={"Error": {"Code": "SomeError", "Message": "An error occurred"}},
            operation_name="ListObjectsV2",
        )

        lister: S3PaginationLister = S3PaginationLister(mock_session, settings)

        with pytest.raises(JobAttachmentsS3BucketListerError) as err:
            list(lister.list_job_attachments("DeadlineCloud/test/"))

        assert "Failed to list job attachments from S3" in str(err.value)

    def test_list_job_attachments_with_prefixes_no_prefixes(
        self,
        mock_session: Mock,
        settings: JobAttachmentS3Settings,
    ):
        """Test listing job attachments with empty prefixes list."""
        lister = S3PaginationLister(mock_session, settings)
        lister.list_job_attachments = Mock()

        prefixes = []
        results = list(lister.list_job_attachments_with_prefixes(prefixes))

        assert len(results) == 0


    def test_list_job_attachments_with_prefixes_happy_path(
        self,
        mock_session: Mock,
        settings: JobAttachmentS3Settings,
    ):
        """Test listing job attachments from multiple prefixes."""
        lister = S3PaginationLister(mock_session, settings)
        lister.list_job_attachments = Mock()

        # Create mock objects for each prefix
        prefix1_objects = [
            S3ObjectData(
                key="DeadlineCloud/prefix1/file1",
                size=100,
                last_modified=datetime(2025, 1, 1),
                etag="abc123",
            )
        ]

        prefix2_objects = [
            S3ObjectData(
                key="DeadlineCloud/prefix2/file2",
                size=200,
                last_modified=datetime(2025, 1, 2),
                etag="def456",
            ),
            S3ObjectData(
                key="DeadlineCloud/prefix2/file3",
                size=300,
                last_modified=datetime(2025, 1, 3),
                etag="ghi789",
            ),
        ]

        prefix3_objects = [
            S3ObjectData(
                key="DeadlineCloud/prefix3/file4",
                size=400,
                last_modified=datetime(2025, 1, 4),
                etag="jkl012",
            )
        ]

        # Configure mock to return different objects for different prefixes
        def mock_list_job_attachments(prefix):
            if prefix == "DeadlineCloud/prefix1/":
                return iter(prefix1_objects)
            elif prefix == "DeadlineCloud/prefix2/":
                return iter(prefix2_objects)
            elif prefix == "DeadlineCloud/prefix3/":
                return iter(prefix3_objects)
            return iter([])

        lister.list_job_attachments.side_effect = mock_list_job_attachments

        prefixes = ["DeadlineCloud/prefix1/", "DeadlineCloud/prefix2/", "DeadlineCloud/prefix3/"]
        results = list(lister.list_job_attachments_with_prefixes(prefixes))

        assert len(results) == 4

        assert results[0] == ("DeadlineCloud/prefix1/", prefix1_objects[0])
        assert results[1] == ("DeadlineCloud/prefix2/", prefix2_objects[0])
        assert results[2] == ("DeadlineCloud/prefix2/", prefix2_objects[1])
        assert results[3] == ("DeadlineCloud/prefix3/", prefix3_objects[0])

        lister.list_job_attachments.assert_any_call(prefix="DeadlineCloud/prefix1/")
        lister.list_job_attachments.assert_any_call(prefix="DeadlineCloud/prefix2/")
        lister.list_job_attachments.assert_any_call(prefix="DeadlineCloud/prefix3/")

