# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import io
import boto3
import gzip
import csv
import psutil

from abc import ABC, abstractmethod
from typing import Iterator, List, Tuple, Any, Dict, TextIO, Set
from datetime import datetime, timezone

from botocore.paginate import Paginator
from botocore.client import BaseClient
from botocore.exceptions import ClientError

from .models import JobAttachmentS3Settings, S3ObjectData
from .exceptions import JobAttachmentObjectFetcherError

from deadline.client.api._session import get_s3_client

# Derived by comparing unzipped to zipped file sizes
GZIP_UNCOMPRESSED_TO_COMPRESSED_RATIO = 7

# The maximum amount of memory the S3 inventory manfiest should take up
MEMORY_THRESHOLD = 0.4


class JobAttachmentObjectFetcher(ABC):
    """Interface for listing job attachments from S3."""

    @abstractmethod
    def list_common_prefixes_with_delimeter(
        self, prefix: str, delimiter: str = "/"
    ) -> Iterator[str]:
        """
        Lists all common prefixes within the given S3 prefix.

        For example, if your S3 structure is:
        queue-1/job-1/...
        queue-1/job-1/...  # duplicate
        queue-1/job-2/...

        Then calling with prefix="queue-1/" will yield:
        - queue-1/job-1/
        - queue-1/job-2/

        Args:
            prefix (str): The S3 prefix to list from
            delimiter (str): The delimiter to use for grouping (default: "/")

        Returns:
            Iterator[str]: Stream of common prefixes found

        Raises:
            JobAttachmentObjectFetcherError: If S3 listing fails
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


class S3PaginationFetcher(JobAttachmentObjectFetcher):
    """Object fetcher that uses S3 pagination to list objects from S3."""

    def __init__(self, boto3_session: boto3.Session, settings: JobAttachmentS3Settings):
        """
        Initialize the S3PaginationFetcher with AWS credentials and settings.

        Args:
            boto3_session (boto3.Session): An initialized boto3 Session object containing
                                         AWS credentials and configuration.
            settings (JobAttachmentS3Settings): Configuration settings for S3 operations,
                                              including bucket name and root prefix.
        """
        self.boto3_session = boto3_session
        self.settings = settings

    def list_common_prefixes_with_delimeter(
        self, prefix: str, delimiter: str = "/"
    ) -> Iterator[str]:
        """
        Lists all common prefixes within the given S3 prefix using pagination.

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
            JobAttachmentObjectFetcherError: If S3 listing fails
        """
        s3: BaseClient = get_s3_client(session=self.boto3_session)
        paginator: Paginator = s3.get_paginator("list_objects_v2")

        try:
            for page in paginator.paginate(
                Bucket=self.settings.s3BucketName, Prefix=prefix, Delimiter=delimiter
            ):
                for object in page.get("CommonPrefixes", []):
                    yield object.get("Prefix")
        except ClientError as err:
            raise JobAttachmentObjectFetcherError(
                f"Failed to list job attachments from S3: {str(err)}"
            ) from err

    def list_job_attachments(
        self,
        prefix: str,
    ) -> Iterator[S3ObjectData]:
        """
        List all job attachments in S3 under a specified prefix using pagination.

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
            raise JobAttachmentObjectFetcherError(
                f"Failed to list job attachments from S3: {str(err)}"
            ) from err

    def list_job_attachments_with_prefixes(
        self, prefixes: List[str]
    ) -> Iterator[Tuple[str, S3ObjectData]]:
        """
        List job attachments for multiple prefixes using pagination.

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


class S3InventoryFetcher(JobAttachmentObjectFetcher):
    """
    Object fetcher that uses an S3 Inventory manifest to list objects from S3.

    See for example data:
        https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-inventory.html

    Warning:
        This implementation loads the entire manifest file into memory during initialization. Large
        manifest files may exhaust memory resources.
    """

    def __init__(
        self,
        boto3_session: boto3.Session,
        s3_settings: JobAttachmentS3Settings,
        job_attachments_file_key: str,
    ):
        self.boto3_session = boto3_session
        self.s3_client = get_s3_client(session=self.boto3_session)
        self.s3_settings = s3_settings
        self.job_attachments_file_key = job_attachments_file_key
        self.manifest_data = self._get_s3_inventory_manifest()

    def list_common_prefixes_with_delimeter(
        self, prefix: str, delimiter: str = "/"
    ) -> Iterator[str]:
        """
        Lists all common prefixes within the given S3 prefix using an S3 Inventory manifest.

        For example, if your S3 structure is:
        queue-1/job-1/...
        queue-1/job-1/...  # duplicate
        queue-1/job-2/...

        Then calling with prefix="queue-1/" will yield:
        - queue-1/job-1/
        - queue-1/job-2/

        Args:
            prefix (str): The S3 prefix to list from

        Returns:
            Iterator[str]: Stream of common prefixes found

        Raises:
            JobAttachmentObjectFetcherError: If S3 listing fails
        """
        prefix_set: Set[str] = set()

        for obj in self.manifest_data:
            if obj.key.startswith(prefix):
                # Find the next delimiter after the prefix
                remaining_key: str = obj.key[len(prefix) :]
                delimiter_pos: int = remaining_key.find(delimiter)

                if delimiter_pos != -1:
                    # Include everything up to and including the delimiter
                    common_prefix: str = obj.key[: len(prefix) + delimiter_pos + 1]
                    prefix_set.add(common_prefix)

        yield from prefix_set

    def list_job_attachments(self, prefix: str) -> Iterator[S3ObjectData]:
        """
        List all job attachments in S3 under a specified prefix using an S3 Inventory manifest.

        Args:
            prefix (str): The prefix path to list objects from within the S3 bucket.

        Returns:
            Iterator[S3ObjectData]: An iterator yielding S3ObjectData instances for
                                  each object found.

        Raises:
            JobAttachmentsListerError: If there's an error listing objects from S3.
        """
        for object in self.manifest_data:
            if object.key.startswith(prefix):
                yield object

    def list_job_attachments_with_prefixes(
        self, prefixes: List[str]
    ) -> Iterator[Tuple[str, S3ObjectData]]:
        """
        List job attachments for multiple prefixes using an S3 Inventory manifest.

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
            for object in self.list_job_attachments(prefix):
                yield (prefix, object)

    def _get_s3_inventory_manifest(self) -> List[S3ObjectData]:
        """
        Downloads and parses S3 inventory manifest file, returning object metadata.

        Returns:
            List of S3ObjectData containing key, size, last_modified, and etag for each object.

        Raises:
            JobAttachmentObjectFetcherError: If download fails or manifest cannot be loaded into memory.
        """
        self._check_manifest_file_size_fits_into_memory()

        try:
            response: Dict[str, Any] = self.s3_client.get_object(
                Bucket=self.s3_settings.s3BucketName, Key=self.job_attachments_file_key
            )

            # S3 inventory manifests are compressed (.gz)
            compressed_data: bytes = response["Body"].read()
            decompressed_data: str = gzip.decompress(compressed_data).decode("utf-8")

            # Manifest CSV file does not provide headers i.e first row is object data
            csv_file: TextIO = io.StringIO(decompressed_data)
            csv_reader: Iterator[List[str]] = csv.reader(csv_file)

            return [
                # Row: (bucket_name, object key, object size, last modified date, etag)
                S3ObjectData(
                    key=row[1],
                    size=int(row[2]),
                    last_modified=datetime.strptime(row[3], "%Y-%m-%dT%H:%M:%S.%fZ").replace(
                        tzinfo=timezone.utc
                    ),
                    etag=row[4],
                )
                for row in csv_reader
            ]
        except ClientError as err:
            raise JobAttachmentObjectFetcherError(
                f"Failed to download S3 Inventory manifest from S3: {str(err)}"
            ) from err
        except MemoryError as err:
            raise JobAttachmentObjectFetcherError(
                f"Failed to load S3 Inventory manifest into memory: {str(err)}"
            ) from err

    def _check_manifest_file_size_fits_into_memory(self) -> None:
        """
        Check if the manifest file is too large to be loaded into memory.

        Raises:
            JobAttachmentObjectFetcherError: if inventory manifest is too large to fit in memory
        """
        response: Dict[str, Any] = self.s3_client.head_object(
            Bucket=self.s3_settings.s3BucketName, Key=self.job_attachments_file_key
        )

        # All values in bytes
        compressed_file_size: int = int(response["ContentLength"])
        estimated_uncompressed_size: int = (
            compressed_file_size * GZIP_UNCOMPRESSED_TO_COMPRESSED_RATIO
        )
        available_memory_with_threshold: int = psutil.virtual_memory().available * MEMORY_THRESHOLD

        if estimated_uncompressed_size >= available_memory_with_threshold:
            raise JobAttachmentObjectFetcherError(
                "S3 Inventory manifest is too large for available memory"
            )
