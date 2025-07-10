# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import boto3

from typing import Dict, List, Any
from datetime import datetime
from ._list_jobs_by_filter_expression import _list_jobs_by_filter_expression


def _list_active_job_ids(
    boto3_session: boto3.Session, farm_id: str, queue_ids: List[str], retention_datetime: datetime
) -> Dict[str, List[str]]:
    """Retrieves active job IDs for specified queues that are newer than the retention date.

    Args:
        boto3_session: AWS boto3 session with credentials that have permissions to access multiple queues.
            Must be a general session (from get_boto3_session()) and not a queue-specific session
            (from get_queue_user_boto3_session()), as queue-specific sessions can only access
            their designated queue.
        farm_id: Deadline farm identifier
        queue_ids: List of queue identifiers to check for active jobs
        retention_datetime: Datetime threshold for considering jobs as active

    Returns:
        Dict[str, List[str]]: Mapping of queue IDs to lists of active job IDs
            Key: Queue ID
            Value: List of job IDs that are active in that queue

    Raises:
        JobAttachmentsSweeperError: If there is a failure fetching job IDs from Deadline, wrapping
            either DeadlineOperationError or JobFetchFailure
    """
    queue_job_id_map: Dict[str, List[str]] = {}
    filter_expression: Dict[str, Any] = {
        "filters": [
            {
                "dateTimeFilter": {
                    "name": "ENDED_AT",
                    "dateTime": retention_datetime,
                    "operator": "GREATER_THAN_EQUAL_TO",
                }
            },
        ],
        "operator": "AND",
    }

    for queue_id in queue_ids:
        jobs: List[Dict[str, Any]] = _list_jobs_by_filter_expression(
            boto3_session=boto3_session,
            farm_id=farm_id,
            queue_id=queue_id,
            filter_expression=filter_expression,
        )

        extracted_job_ids: List[str] = [job["jobId"] for job in jobs]
        queue_job_id_map[queue_id] = extracted_job_ids

    return queue_job_id_map
