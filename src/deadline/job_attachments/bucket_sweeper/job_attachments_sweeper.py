# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import csv
import boto3

from datetime import datetime
from typing import List, Dict, Any, Set
from botocore.exceptions import BotoCoreError
from botocore.client import BaseClient

from ..exceptions import (
    JobAttachmentsS3BucketListerError,
    JobAttachmentsSweeperError,
    JobAttachmentS3BotoCoreError,
    RetentionRecordHandlerError,
)
from ..job_attachments_s3_bucket_lister import JobAttachmentsS3BucketLister

from deadline.job_attachments.models import RetentionRecord
from deadline.job_attachments.bucket_sweeper.retention_record_handler import (
    RetentionRecordHandlerInterface,
)


class JobAttachmentsSweeper:
    """Processes cleanup operations for job attachments."""

    def __init__(
        self,
        s3_client: BaseClient,
        s3_control_client: BaseClient,
        deadline_client: BaseClient,
        retention_record_handler: RetentionRecordHandlerInterface,
        job_attachments_s3_bucket_lister: JobAttachmentsS3BucketLister,
        boto3_session: boto3.Session,
        role_arn: str,
        bucket_name: str,
        root_prefix: str,
    ):
        """
        Initializes the JobAttachmentsSweeper.

        Note:
            IMPORTANT: Do not mix different lister implementations. S3 Inventory manifests
            represent snapshots of bucket while direct S3 listing shows current state. Using both
            could lead to premature file deletion.

        Args:
            s3_client: AWS S3 client for basic S3 operations
            s3_control_client: AWS S3 Control client for batch operations
            deadline_client: Client for interacting with Deadline
            job_attachments_s3_bucket_lister: Component to list job attachments from an S3 bucket
            boto3_session: boto3 session
            role_arn (str): The ARN of the IAM role for executing batch jobs.
                Required permissions:
                    - s3:GetObject
                    - s3:PutObjectTagging
                    - s3:CreateJob
            bucket_name (str): target S3 bucket to cleanup
            root_prefix (str): S3 job attachments root prefix to cleanup
        """
        self.s3 = s3_client
        self.s3_control = s3_control_client
        self.deadline = deadline_client
        self.retention_record_handler = retention_record_handler
        self.job_attachments_s3_bucket_lister = job_attachments_s3_bucket_lister
        self.boto3_session = boto3_session
        self.role_arn = role_arn
        self.bucket_name = bucket_name
        self.root_prefix = root_prefix

    def get_queues_in_farms_from_s3(self) -> Dict[str, List[str]]:
        """
        Retrieves farms and their queues from S3 bucket as a dictionary mapping farm_id to queue_ids.

        Returns:
            Dict[str, List[str]]: A dictionary mapping farm_id to a list of queue_ids.
            Example:
                {
                    'farm-123': ['queue-1', 'queue-2'],
                    'farm-456': ['queue-3']
                }
        """
        farm_queues_map: Dict[str, List[str]] = {}

        base_prefix: str = f"{self.root_prefix}/Manifests"

        farms_prefix: str = f"{base_prefix}/farm"
        farm_ids: List[str] = self._get_ids_from_common_prefixes(prefix=farms_prefix)

        for farm_id in farm_ids:
            queues_prefix: str = f"{base_prefix}/{farm_id}/queue"
            queue_ids: List[str] = self._get_ids_from_common_prefixes(prefix=queues_prefix)

            if queue_ids:
                farm_queues_map[farm_id] = queue_ids

        return farm_queues_map

    def _get_ids_from_common_prefixes(self, prefix: str) -> List[str]:
        """
        Extracts IDs from S3 common prefixes based on a given prefix path.

        Args:
            prefix (str): The S3 prefix path to search for IDs.

        Returns:
            List[str]: A list of extracted IDs from the common prefixes.

        Raises:
            JobAttachmentsSweeperError: If there is an error listing common prefixes
                from the S3 bucket.

        Notes:
            - Assumes IDs are located in the second-to-last position when splitting
            the prefix path by '/'.
            - Skips prefixes that don't have at least 2 parts when split.
        """
        ids: List[str] = []

        try:
            for (
                common_prefix_data
            ) in self.job_attachments_s3_bucket_lister.list_common_prefixes_with_delimeter(
                prefix=prefix
            ):
                common_prefix: str = common_prefix_data.get("Prefix", "")
                split_common_prefix: List[str] = common_prefix.split("/")

                if len(split_common_prefix) < 2:
                    continue

                # Prefix ends with  "/", last element will be an empty string
                id: str = split_common_prefix[-2]
                ids.append(id)
        except JobAttachmentsS3BucketListerError as err:
            raise JobAttachmentsSweeperError(
                message=f"Failed to list common prefixes: {str(err)}"
            ) from err

        return ids

    def get_attachments_to_retain(self, queue_job_id_map: Dict[str, List[str]]) -> Set[str]:
        """
        Retrieves a set of S3 object keys that should be retained based on queue and job IDs.

        This method queries the retention record handler to get retention records for the specified
        queue and job IDs, then extracts the unique S3 object keys from those records. The returned
        set contains S3 object keys that should be retained during cleanup operations.

        Args:
            queue_job_id_map: A dictionary mapping queue IDs to lists of job IDs. For each queue ID,
                            the associated job IDs will be used to find retention records.
                Example: { "queue-1": ["job-1"], "queue-2": ["job-2"] }

        Returns:
            Set[str]: A set of unique S3 object keys that should be retained.

        Raises:
            JobAttachmentsSweeperError: If there's an error retrieving retention records from the
                                    record handler.

        Note:
            If the queue_job_id_map is empty or if no retention records are found for the specified
            queue and job IDs, an empty set will be returned.
        """
        try:
            records: List[RetentionRecord] = self.retention_record_handler.get_retention_records(
                queue_job_id_map=queue_job_id_map
            )
        except RetentionRecordHandlerError as err:
            raise JobAttachmentsSweeperError(message=f"Failed to get retention records: {str(err)}")

        retain_object_keys: Set[str] = {record.s3_object_key for record in records}

        return retain_object_keys

    def get_attachments_to_delete(
        self, s3_keys_to_retain: Set[str], retention_datetime: datetime, root_prefix: str
    ) -> List[str]:
        """
        Identifies S3 objects within a Job Attachments bucket root prefix that should be deleted.

        This function retains S3 objects if either option is true:
            1. They are explicitly listed in s3_keys_to_retain
            2. They were modified after or at the retention_datetime threshold

        Args:
            s3_keys_to_retain: List of S3 object keys that should be explicitly retained
            retention_datetime: Datetime threshold - objects modified before this date and not in the
                retain list will be deleted. Objects after or at this date will be kept.
            root_prefix: Job attachments S3 bucket root prefix

        Returns:
            List[str]: S3 object keys that should be deleted according to retention rules

        Note:
            The method checks the last_modified date as new jobs may have been submitted
            since listing jobs, ensuring we don't delete recently created attachments.
        """
        delete_list: List[str] = []
        try:
            for s3_object in self.job_attachments_s3_bucket_lister.list_job_attachments(
                prefix=root_prefix
            ):
                if (
                    s3_object.key in s3_keys_to_retain
                    # Check if S3Object was modified more recently than or at the retention_datetime
                    or s3_object.last_modified >= retention_datetime
                ):
                    continue

                delete_list.append(s3_object.key)
        except JobAttachmentsS3BucketListerError as err:
            raise JobAttachmentsSweeperError(
                message=f"Failed to list objects for deletion: {str(err)}"
            ) from err

        return delete_list

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
            account_id: str = self.boto3_session.client("sts").get_caller_identity()["Account"]

            self.s3_control.create_job(
                AccountId=account_id,
                RoleArn=self.role_arn,
                Operation=operation,
                Manifest=manifest,
                ConfirmationRequired=confirmation_required,
                Report=report,
                Priority=priority,
            )
        except Exception as e:
            raise JobAttachmentsSweeperError(f"Failed to create S3 batch operations job: {str(e)}")
