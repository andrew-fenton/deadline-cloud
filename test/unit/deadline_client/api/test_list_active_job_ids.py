# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import pytest
from typing import Dict, List
from datetime import datetime, timezone
from unittest.mock import Mock, patch
from deadline.client.api._list_active_job_ids import _list_active_job_ids
from deadline.client.exceptions import DeadlineOperationError
from deadline.client.api._list_jobs_by_filter_expression import JobFetchFailure

@pytest.fixture
def mock_boto3_session():
    """Mock for Deadline Client"""
    return Mock()


class TestListActiveJobIds():

    @patch("deadline.client.api._list_active_job_ids._list_jobs_by_filter_expression")
    def test_get_active_job_ids_empty_queue_list(self, mock_boto3_session: Mock):
        """Test behavior with empty queue list."""
        result: Dict[str, List[str]] = _list_active_job_ids(
            boto3_session=mock_boto3_session,
            farm_id="farm-id",
            queue_ids=[],
            retention_datetime=datetime.now(timezone.utc)
        )
        assert result == {}

    @patch("deadline.client.api._list_active_job_ids._list_jobs_by_filter_expression")
    def test_get_active_job_ids_empty_jobs_response(
        self, mock_list_jobs: Mock, mock_boto3_session: Mock
    ):
        """Test behavior when no jobs are returned."""
        mock_list_jobs.return_value = []

        result: Dict[str, List[str]] = _list_active_job_ids(
            boto3_session=mock_boto3_session,
            farm_id="farm-id",
            queue_ids=["queue-1"],
            retention_datetime=datetime.now(timezone.utc)
        )
        assert result == {"queue-1": []}

    @patch("deadline.client.api._list_active_job_ids._list_jobs_by_filter_expression")
    def test_get_active_job_ids_deadline_error(
        self, mock_list_jobs: Mock, mock_boto3_session: Mock
    ):
        """Test get active job ids when deadline function call fails."""
        mock_list_jobs.side_effect = DeadlineOperationError()

        with pytest.raises(DeadlineOperationError):
            _list_active_job_ids(
                boto3_session=mock_boto3_session,
                farm_id="farm-id",
                queue_ids=["queue-1"],
                retention_datetime=datetime.now(timezone.utc)
            )

    @patch("deadline.client.api._list_active_job_ids._list_jobs_by_filter_expression")
    def test_get_active_job_ids_job_fetch_failure_error(
        self, mock_list_jobs: Mock
    ):
        """Test get active job ids when listing function fails."""

        mock_list_jobs.side_effect = JobFetchFailure()

        with pytest.raises(JobFetchFailure):
            _list_active_job_ids(
                boto3_session=mock_boto3_session,
                farm_id="farm-id",
                queue_ids=["queue-1"],
                retention_datetime=datetime.now(timezone.utc)
            )

    @patch("deadline.client.api._list_active_job_ids._list_jobs_by_filter_expression")
    def test_get_active_job_ids(self, mock_list_jobs: Mock, mock_boto3_session: Mock):
        """Test getting job ids for multiple queues."""

        mock_list_jobs.return_value = [{"jobId": "job-1"}, {"jobId": "job-2"}]

        # Call method
        queue_ids: List[str] = ["queue-1", "queue-2", "queue-3"]
        retention_datetime: datetime = datetime.now(timezone.utc)
        result: Dict[str, List[str]] = _list_active_job_ids(
            boto3_session=mock_boto3_session,
            farm_id="farm-id",
            queue_ids=queue_ids,
            retention_datetime=retention_datetime
        )

        expected_result: Dict[str, List[str]] = {
            "queue-1": ["job-1", "job-2"],
            "queue-2": ["job-1", "job-2"],
            "queue-3": ["job-1", "job-2"],
        }
        assert list(result.keys()) == list(expected_result.keys())
        assert list(result.values()) == list(expected_result.values())