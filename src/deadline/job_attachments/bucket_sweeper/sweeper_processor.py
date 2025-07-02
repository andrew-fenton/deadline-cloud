import os
import csv
from typing import List


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

    def create_tag_manifest(self, write_directory, bucket_name, delete_list):
        csv_formatted_list = []
        for obj_key in delete_list:
            csv_formatted_list.append([bucket_name, obj_key])

        file_path = os.path.join(write_directory, "tag_manifest.csv")
        with open(file_path, "w") as file:
            writer = csv.writer(file)
            writer.writerows(csv_formatted_list)

        return file_path

    def upload_tag_manifest(self, manifest_path, bucket_name, object_key):
        if manifest_path:
            self.s3.upload_file(manifest_path, bucket_name, object_key)

    def create_batch_tag_s3_job(
        self, account_id, role_arn, bucket_name, s3_manifest_key
    ):
        manifest_metadata = self.s3.head_object(Bucket=bucket_name, Key=s3_manifest_key)
        manifest_etag = manifest_metadata.get("ETag")

        # Add a delete tag to every object in manifest
        operation = {
            "S3PutObjectTagging": {
                "TagSet": [
                    {"Key": "delete", "Value": "True"},
                ]
            }
        }

        # Manifest config and location in S3
        manifest = (
            {
                "Spec": {
                    "Format": "S3BatchOperations_CSV_20180820",
                    "Fields": ["Bucket", "Key"],
                },
                "Location": {
                    "ObjectArn": f"arn:aws:s3:::{bucket_name}/{s3_manifest_key}",
                    "ETag": f"{manifest_etag}",
                },
            },
        )

        self.s3_control.create_job(
            AccountId=account_id,
            RoleArn=role_arn,
            Operation=operation,
            Manifest=manifest,
            EnableManifestOutput=False,
        )
