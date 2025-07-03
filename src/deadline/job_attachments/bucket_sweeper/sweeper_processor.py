# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os
import csv

from botocore.exceptions import BotoCoreError

from ..exceptions import SweeperProcessorError, JobAttachmentS3BotoCoreError


class SweeperProcessor:
    """Processes cleanup operations for job attachments."""

    def __init__(
        self,
        s3_client,
        s3_control_client,
        deadline_client,
        storage,
        job_attachments,
        farm_id,
    ):
        self.s3 = s3_client
        self.s3_control = s3_control_client
        self.deadline = deadline_client
        self.storage = storage
        self.job_attachments = job_attachments
        self.farm_id = farm_id

    def _create_tag_manifest(self, write_directory, bucket_name, delete_list):
        """
        Creates a CSV manifest file containing object keys to be deleted.

        The manifest is created in the specified directory with the filename 'tag_manifest.csv'.
        Each row in the CSV contains two columns: bucket name and object key.

        Args:
            write_directory (str): Directory path where the manifest file will be created
            bucket_name (str): Name of the S3 bucket containing the objects
            delete_list (list): List of object keys to be included in the manifest

        Returns:
            str: Full path to the created manifest file

        Raises:
            SweeperProcessorError: If the manifest file cannot be created due to
                file system permissions, disk space, or other I/O errors
        """
        csv_formatted_list = []
        for obj_key in delete_list:
            csv_formatted_list.append([bucket_name, obj_key])

        file_path = os.path.join(write_directory, "tag_manifest.csv")

        try:
            with open(file_path, "w") as file:
                writer = csv.writer(file)
                writer.writerows(csv_formatted_list)
        except Exception as e:
            raise SweeperProcessorError(
                message=f"Failed to create tag manifest: {str(e)}"
            )

        return file_path

    def _upload_tag_manifest(self, manifest_path, bucket_name, object_key):
        """
        Upload CSV manifest to S3. Overwrites existing manifest if already present.

        Args:
            manifest_path (str): Local path to the manifest file
            bucket_name (str): Name of the S3 bucket
            object_key (str): S3 object key for the uploaded manifest

        Raises:
            JobAttachmentS3BotoCoreError: If any errors occur during the upload process
        """
        try:
            self.s3.upload_file(manifest_path, bucket_name, object_key)
        except BotoCoreError as e:
            raise JobAttachmentS3BotoCoreError(
                action="uploading bucket sweeper tag manifest", error_details=str(e)
            )

    def _create_batch_tag_s3_job(
        self, account_id, role_arn, bucket_name, s3_manifest_key
    ):
        """
        Creates an S3 Batch Operations job to tag objects for deletion.

        Args:
            account_id (str): AWS account ID where the batch job will run
            role_arn (str): IAM role ARN with permissions to execute the batch operation
            bucket_name (str): S3 bucket containing the manifest file
            s3_manifest_key (str): Object key of the manifest file in S3

        Raises:
            JobAttachmentS3BotoCoreError: When retrieving manifest metadata fails
            SweeperProcessorError: When creating the batch job fails
        """
        manifest_etag = self._get_manifest_etag(bucket_name, s3_manifest_key)
        manifest = self._create_manifest_config(
            bucket_name, s3_manifest_key, manifest_etag
        )
        operation = self._create_tagging_operation()
        confirmation_required = False
        report = {"Enabled": False}
        priority = 10

        self._submit_batch_job(
            account_id,
            confirmation_required,
            role_arn,
            operation,
            manifest,
            report,
            priority,
        )

    def _get_manifest_etag(self, bucket_name, s3_manifest_key):
        """
        Retrieves the ETag for the manifest file from S3.

        Raises:
            JobAttachmentS3BotoCoreError: When S3 head_object operation fails
        """
        try:
            manifest_metadata = self.s3.head_object(
                Bucket=bucket_name, Key=s3_manifest_key
            )
            return manifest_metadata.get("ETag")
        except BotoCoreError as e:
            raise JobAttachmentS3BotoCoreError(
                action="querying head object", error_details=str(e)
            )

    def _create_manifest_config(self, bucket_name, s3_manifest_key, manifest_etag):
        """Creates the manifest configuration for the batch job."""
        return {
            "Spec": {
                "Format": "S3BatchOperations_CSV_20180820",
                "Fields": ["Bucket", "Key"],
            },
            "Location": {
                "ObjectArn": f"arn:aws:s3:::{bucket_name}/{s3_manifest_key}",
                "ETag": f"{manifest_etag}",
            },
        }

    def _create_tagging_operation(self):
        """Creates the tagging operation configuration."""
        return {
            "S3PutObjectTagging": {
                "TagSet": [
                    {"Key": "delete", "Value": "True"},
                ]
            }
        }

    def _submit_batch_job(
        self,
        account_id,
        confirmation_required,
        role_arn,
        operation,
        manifest,
        report,
        priority,
    ):
        """
        Submits the batch job to AWS.

        Raises:
            SweeperProcessorError: When job creation fails
        """
        try:
            self.s3_control.create_job(
                AccountId=account_id,
                ConfirmationRequired=confirmation_required,
                RoleArn=role_arn,
                Operation=operation,
                Manifest=manifest,
                Report=report,
                Priority=priority,
            )
        except Exception as e:
            raise SweeperProcessorError(
                f"Failed to create S3 batch operations job: {str(e)}"
            )
