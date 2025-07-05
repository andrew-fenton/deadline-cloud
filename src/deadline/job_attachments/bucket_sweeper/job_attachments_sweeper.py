# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os
import csv
import boto3

from datetime import datetime
from typing import List, Dict, Any
from botocore.exceptions import BotoCoreError
from botocore.client import BaseClient

from ..exceptions import JobAttachmentsSweeperError, JobAttachmentS3BotoCoreError
from deadline.client.exceptions import DeadlineOperationError
from deadline.client.api._list_jobs_recent_by_timestamp_field import (
    JobFetchFailure,
    _list_jobs_recent_by_timestamp_field,
)


class JobAttachmentsSweeper:
    """Processes cleanup operations for job attachments."""

    def __init__(
        self,
        boto3_session: boto3.Session,
        s3_client: BaseClient,
        s3_control_client: BaseClient,
        deadline_client: BaseClient,
        farm_id: str,
        account_id: str,
        role_arn: str,
        bucket_name: str,
    ):
        """
        Initializes the JobAttachmentsSweeper.

        Args:
            boto3_session: AWS boto3 session
            s3_client: AWS S3 client for basic S3 operations
            s3_control_client: AWS S3 Control client for batch operations
            deadline_client: Client for interacting with Deadline
            farm_id (str): The target farm_id to cleanup
            account_id (str): AWS account ID for the batch operation
            role_arn (str): The ARN of the IAM role for executing batch jobs.
                Required permissions:
                    - s3:GetObject
                    - s3:PutObjectTagging
                    - s3:CreateJob
            bucket_name (str): target S3 bucket to cleanup
        """
        self.boto3_session = boto3_session
        self.s3 = s3_client
        self.s3_control = s3_control_client
        self.deadline = deadline_client
        self.farm_id = farm_id
        self.account_id = account_id
        self.role_arn = role_arn
        self.bucket_name = bucket_name

    def _get_active_job_ids(
        self, queue_ids: List[str], retention_datetime: datetime
    ) -> Dict[str, List[str]]:
        """Retrieves active job IDs for specified queues that are newer than the retention date.

        Args:
            queue_ids: List of queue identifiers to check for active jobs
            retention_datetime: Datetime threshold for considering jobs as active

        Returns:
            Dict[str, List[str]]: Mapping of queue IDs to lists of active job IDs
                Key: Queue ID
                Value: List of job IDs that are active in that queue

        Raises:
            SweeperProcessorError: If there is a failure fetching job IDs from Deadline, wrapping
                either DeadlineOperationError or JobFetchFailure
        """
        queue_job_id_map: Dict[str, List[str]] = {}

        for queue_id in queue_ids:
            try:
                jobs = _list_jobs_recent_by_timestamp_field(
                    self.boto3_session,
                    self.farm_id,
                    queue_id,
                    "endedAt",
                    timestamp=retention_datetime,
                )
            except (DeadlineOperationError, JobFetchFailure) as err:
                raise JobAttachmentsSweeperError(
                    f"Failed to fetch active job ids for {queue_id}: {str(err)}"
                ) from err

            extracted_job_ids = [job["jobId"] for job in jobs]

            queue_job_id_map[queue_id] = extracted_job_ids

        return queue_job_id_map

    def _create_tag_manifest(self, file_path: str, delete_list: List[str]) -> None:
        """
        Creates a CSV manifest file containing object keys to be deleted and writes it to
        the specified path on disk.

        Each row in the CSV contains two columns: bucket name and object key.

        Args:
            write_path (str): File path where the manifest file will be created
            delete_list (List[str]): List of object keys to be included in the manifest

        Raises:
            JobAttachmentsSweeperError: If the manifest file cannot be created due to
                file system permissions, disk space, or other I/O errors
        """
        csv_formatted_list: List[List[str]] = []
        for obj_key in delete_list:
            csv_formatted_list.append([self.bucket_name, obj_key])

        try:
            with open(file_path, "w") as file:
                writer = csv.writer(file)
                writer.writerows(csv_formatted_list)
        except Exception as e:
            raise JobAttachmentsSweeperError(message=f"Failed to create tag manifest: {str(e)}")

        return file_path

    def _upload_tag_manifest(self, manifest_path: str, object_key: str) -> None:
        """
        Upload CSV manifest to S3. Overwrites existing manifest if already present.

        Args:
            manifest_path (str): Local path to the manifest file
            object_key (str): S3 object key for the uploaded manifest

        Raises:
            JobAttachmentS3BotoCoreError: If any errors occur during the upload process
        """
        try:
            self.s3.upload_file(manifest_path, self.bucket_name, object_key)
        except BotoCoreError as e:
            raise JobAttachmentS3BotoCoreError(
                action="uploading bucket sweeper tag manifest", error_details=str(e)
            )

    def _create_batch_tag_s3_job(self, s3_manifest_key: str) -> None:
        """
        Creates an S3 Batch Operations job to tag objects for deletion.

        Args:
            s3_manifest_key (str): Object key of the manifest file in S3

        Raises:
            JobAttachmentS3BotoCoreError: When retrieving manifest metadata fails
            JobAttachmentsSweeperError: When getting the manifest etag or creating the batch job fails
        """
        manifest_etag: str = self._get_manifest_etag(s3_manifest_key)
        manifest: Dict[str, Any] = {
            "Spec": {
                "Format": "S3BatchOperations_CSV_20180820",
                "Fields": ["Bucket", "Key"],
            },
            "Location": {
                "ObjectArn": f"arn:aws:s3:::{self.bucket_name}/{s3_manifest_key}",
                "ETag": f"{manifest_etag}",
            },
        }
        operation: Dict[str, Any] = {
            "S3PutObjectTagging": {
                "TagSet": [
                    {"Key": "delete", "Value": "True"},
                ]
            }
        }

        self._submit_tagging_batch_job(
            operation=operation,
            manifest=manifest,
        )

    def _get_manifest_etag(self, s3_manifest_key: str) -> str:
        """
        Retrieves the ETag for the manifest file from S3.

        Raises:
            JobAttachmentsSweeperError: when head_object operation succeeds but etag is None
            JobAttachmentS3BotoCoreError: When S3 head_object operation fails
        """
        try:
            manifest_metadata: Dict[str, Any] = self.s3.head_object(
                Bucket=self.bucket_name, Key=s3_manifest_key
            )

            etag: str = manifest_metadata.get("ETag", "")
            if not etag:
                raise JobAttachmentsSweeperError(message="Missing etag in manifest metadata")

            return etag
        except BotoCoreError as e:
            raise JobAttachmentS3BotoCoreError(action="querying head object", error_details=str(e))

    def _submit_tagging_batch_job(
        self,
        operation: Dict[str, Any],
        manifest: Dict[str, Any],
        confirmation_required: bool = False,
        report: Dict[str, Any] = {"Enabled": False},
        priority: int = 10,
    ) -> None:
        """
        Submits the batch job to AWS.

        Args:
            operation (Dict[str, Any]): The operation to be performed by the batch job
            manifest (Dict[str, Any]): The manifest specifying the objects to be processed
            confirmation_required (bool, optional): Whether manual confirmation is needed before job execution. Defaults to False.
            report (Dict[str, Any], optional): Configuration for job completion report. Defaults to {"Enabled": False}.
            priority (int, optional): The priority of the job (1-255, higher values = higher priority). Defaults to 10.

        Raises:
            JobAttachmentsSweeperError: When job creation fails

        Note:
            The CLI client requires s3:CreateJob and iam:PassRole permissions to create a batch tagging job.
        """
        try:
            self.s3_control.create_job(
                AccountId=self.account_id,
                RoleArn=self.role_arn,
                Operation=operation,
                Manifest=manifest,
                ConfirmationRequired=confirmation_required,
                Report=report,
                Priority=priority,
            )
        except Exception as e:
            raise JobAttachmentsSweeperError(f"Failed to create S3 batch operations job: {str(e)}")
