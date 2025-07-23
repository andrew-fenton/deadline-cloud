# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import boto3

from abc import ABC, abstractmethod
from typing import Iterator, List, Tuple

from botocore.paginate import Paginator
from botocore.client import BaseClient
from botocore.exceptions import ClientError

from .models import JobAttachmentS3Settings, S3ObjectData
from .exceptions import JobAttachmentsS3BucketListerError

from deadline.client.api._session import get_s3_client


class JobAttachmentsS3BucketLister(ABC):
    """Interface for listing job attachments from S3."""

    @abstractmethod
    def list_common_prefixes_with_delimeter(self, prefix: str) -> Iterator[str]:
        """
        Lists all common prefixes within the given S3 prefix.

        For example, if your S3 structure is:
        queue-1/job-1/...
        queue-1/job-1/...
        queue-1/job-2/...

        Then calling with prefix="queue-1/" will yield:
        - queue-1/job-1/
        - queue-1/job-2/

        Args:
            prefix (str): The S3 prefix to list from

        Returns:
            Iterator[str]: Stream of common prefixes found

        Raises:
            JobAttachmentsS3BucketListerError: If S3 listing fails
        """
        raise NotImplementedError()

    @abstractmethod
    def list_job_attachments(self, prefix: str) -> Iterator[S3ObjectData]:
        """
        Lists S3 objects under the given prefix.

        Args:
            prefix: The S3 prefix to list objects from

        Returns:
            Iterator of S3ObjectInfo containing object metadata
        """
        raise NotImplementedError()

    @abstractmethod
    def list_job_attachments_with_prefixes(
        self, prefixes: List[str]
    ) -> Iterator[Tuple[str, S3ObjectData]]:
        """
        Lists S3 objects under multiple prefixes in parallel.

        Args:
            prefixes: List of S3 prefixes to list objects from

        Returns:
            Iterator of tuples containing (prefix, S3ObjectData) pairs, where:
            - prefix is the original prefix string that produced the object
            - S3ObjectData contains the object's metadata
        """
        raise NotImplementedError()


class S3PaginationLister(JobAttachmentsS3BucketLister):
    """Lister that uses S3 pagination to list objects from S3."""

    def __init__(self, boto3_session: boto3.Session, settings: JobAttachmentS3Settings):
        """
        Initialize the S3PaginationLister with AWS credentials and settings.

        Args:
            boto3_session (boto3.Session): An initialized boto3 Session object containing
                                         AWS credentials and configuration.
            settings (JobAttachmentS3Settings): Configuration settings for S3 operations,
                                              including bucket name and root prefix.
        """
        self.boto3_session = boto3_session
        self.settings = settings

    def list_common_prefixes_with_delimeter(self, prefix: str) -> Iterator[str]:
        """
        Lists all common prefixes within the given S3 prefix.

        For example, if your S3 structure is:
        queue-1/job-1/...
        queue-1/job-1/...
        queue-1/job-2/...

        Then calling with prefix="queue-1/" will yield:
        - queue-1/job-1/
        - queue-1/job-2/

        Args:
            prefix (str): The S3 prefix to list from

        Returns:
            Iterator[str]: Stream of common prefixes found

        Raises:
            JobAttachmentsS3BucketListerError: If S3 listing fails
        """
        s3: BaseClient = get_s3_client(session=self.boto3_session)
        paginator: Paginator = s3.get_paginator("list_objects_v2")

        try:
            for page in paginator.paginate(
                Bucket=self.settings.s3BucketName, Prefix=prefix, Delimiter="/"
            ):
                for object in page.get("CommonPrefixes", []):
                    yield object
        except ClientError as err:
            raise JobAttachmentsS3BucketListerError(
                f"Failed to list job attachments from S3: {str(err)}"
            ) from err

    def list_job_attachments(
        self,
        prefix: str,
    ) -> Iterator[S3ObjectData]:
        """
        List all job attachments in S3 under a specified prefix.

        Args:
            prefix (str): The prefix path to list objects from within the S3 bucket.

        Returns:
            Iterator[S3ObjectData]: An iterator yielding S3ObjectData instances for
                                  each object found.

        Raises:
            JobAttachmentsListerError: If there's an error listing objects from S3.
        """
        s3: BaseClient = get_s3_client(session=self.boto3_session)
        paginator: Paginator = s3.get_paginator("list_objects_v2")

        try:
            for page in paginator.paginate(Bucket=self.settings.s3BucketName, Prefix=prefix):
                for object in page.get("Contents", []):
                    yield S3ObjectData(
                        key=object["Key"],
                        size=object["Size"],
                        last_modified=object["LastModified"],
                        etag=object["ETag"],
                    )
        except ClientError as err:
            raise JobAttachmentsS3BucketListerError(
                f"Failed to list job attachments from S3: {str(err)}"
            ) from err

    def list_job_attachments_with_prefixes(
        self, prefixes: List[str]
    ) -> Iterator[Tuple[str, S3ObjectData]]:
        """
        List job attachments for multiple prefixes.

        Args:
            prefixes (List[str]): A list of prefix paths to list objects from.

        Returns:
            Iterator[Tuple[str, S3ObjectData]]: An iterator yielding tuples containing
                                              the original prefix and the corresponding
                                              S3ObjectData.

        Note:
            Currently implements sequential processing but will be implemented
            with parallelism in the future.
        """
        # TODO: Implement with parallelism
        for prefix in prefixes:
            for object in self.list_job_attachments(prefix=prefix):
                yield (prefix, object)


class S3InventoryLister(JobAttachmentsS3BucketLister):
    """
    Lister that uses an S3 Inventory manifest to list objects from S3.

    See for example data:
        https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-inventory.html

    """

    def list_common_prefixes_with_delimeter(self, prefix) -> Iterator[str]:
        """
        Lists all common prefixes within the given S3 prefix from an S3 Inventory manifest.

        Args:
            prefix (str): The S3 prefix to list from

        Returns:
            Iterator[str]: Stream of common prefixes found

        Raises:
            JobAttachmentsS3BucketListerError: If S3 listing fails
        """
        return super().list_common_prefixes_with_delimeter(prefix)

    def list_job_attachments(self, prefix: str) -> Iterator[S3ObjectData]:
        """
        List job attachments under a specified prefix using S3 Inventory data.

        Args:
            prefix (str): The prefix path to filter objects from the S3 Inventory.

        Returns:
            Iterator[S3ObjectData]: An iterator yielding S3ObjectData instances for
                                  each matching object in the inventory.
        """
        return super().list_job_attachments(prefix)

    def list_job_attachments_with_prefixes(
        self, prefixes: List[str]
    ) -> Iterator[Tuple[str, S3ObjectData]]:
        """
        List job attachments for multiple prefixes using S3 Inventory data.

        Args:
            prefixes (List[str]): A list of prefix paths to filter objects from
                                the S3 Inventory.

        Returns:
            Iterator[Tuple[str, S3ObjectData]]: An iterator yielding tuples containing
                                              the original prefix and the corresponding
                                              S3ObjectData.
        """
        return super().list_job_attachments_with_prefixes(prefixes)
