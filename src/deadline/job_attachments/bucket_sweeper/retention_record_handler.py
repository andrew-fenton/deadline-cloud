# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import json

from abc import ABC, abstractmethod
from pathlib import Path
from ..models import RetentionRecord
from typing import TypedDict, List, Dict, Tuple
from ..exceptions import RetentionRecordHandlerError


class RetentionRecordHandlerInterface(ABC):
    @abstractmethod
    def insert_retention_records(self, records: List[RetentionRecord]) -> None:
        pass

    @abstractmethod
    def get_retention_records(
        self, queue_job_id_map: Dict[str, List[str]]
    ) -> List[RetentionRecord]:
        pass


"""
TypedDict classes to define data structure saved to disk.

Example:
    "queues": {
            "queue-1": {
                "jobs": {
                    "job-1": ["key1", "key2"]
                    "job-2": ["key3", "key4"]
                }
            }
    }
"""


class JobKeyMapping(TypedDict):
    """Maps JobId to a list of S3 Keys"""

    jobs: Dict[str, List[str]]


class QueueJobKeyMapping(TypedDict):
    """Maps QueueId to a dictionary where keys are JobIds"""

    queues: Dict[str, JobKeyMapping]


class RetentionRecordHandler(RetentionRecordHandlerInterface):
    def __init__(self, storage_file_path: Path):
        """
        Initialize a retention record handler.

        Creates a new handler instance that manages retention records in the specified directory.
        The handler will create or use a 'retention_records.json' file in this directory
        for persistent storage.

        Args:
            storage_file_path: Path to the storage file. The file must be a JSON file and the
            directory must be writable.

        Note:
            This will automatically initialize the storage file if it doesn't exist
            through the _initialize_storage method.
        """
        self.storage_file_path: Path = storage_file_path
        self._initialize_storage()

    def _initialize_storage(self):
        """
        Initializes the retention records storage file if it doesn't exist. If the file already
        exists, the function will throw an error.

        Creates the storage file with an empty JSON object.

        Note:
            Overwrites any existing storage file.

        Raises:
            RetentionRecordHandlerError: If storage file cannot be created
                (e.g., due to permissions, disk space, or invalid path).
        """
        try:
            content: QueueJobKeyMapping = {"queues": {}}
            self.storage_file_path.write_text(json.dumps(content))
        except Exception as err:
            raise RetentionRecordHandlerError(
                f"Failed to create retention record storage file: {str(err)}"
            )

    def insert_retention_records(self, records: List[RetentionRecord]) -> None:
        """
        Insert retention records into the storage system, avoiding duplicates.

        This method takes a list of retention records and merges them with existing records.
        If a record with the same queue_id, job_id, and s3_object_key already exists, it will not be duplicated.

        Note:
            See top of the file for an example of the QueueJobKeyMapping data structure being saved to disk.

        Args:
            records (List[RetentionRecord]): A list of RetentionRecord objects to be inserted.
                Each RetentionRecord should contain queue_id, job_id, and s3_object_key attributes.

        Raises:
            RetentionRecordHandlerError: if loading or writing records fail
        """
        existing_records: QueueJobKeyMapping = self._load_existing_records_from_file()
        queues: Dict[str, JobKeyMapping] = existing_records["queues"]

        for record in records:
            if record.queue_id not in queues:
                queues[record.queue_id] = JobKeyMapping(jobs={})

            queue_jobs: Dict[str, List[str]] = queues[record.queue_id]["jobs"]

            if record.job_id not in queue_jobs:
                queue_jobs[record.job_id] = []

            job_keys: List[str] = queue_jobs[record.job_id]

            # Avoid duplicate keys for the same job
            if record.s3_object_key not in job_keys:
                job_keys.append(record.s3_object_key)

        self._write_records_to_file(existing_records)

    def get_retention_records(
        self, queue_job_id_map: Dict[str, List[str]]
    ) -> List[RetentionRecord]:
        """
        Retrieve retention records based on a mapping of queue IDs to job IDs. Deletes duplicate job IDs
        in queue_job_id_map before retrieving records.

        Args:
            queue_job_id_map (Dict[str, List[str]]): A dictionary mapping queue IDs to lists of job IDs.
                Example: {'queue1': ['job1', 'job2'], 'queue2': ['job3']}

        Returns:
            List[RetentionRecord]: A list of RetentionRecord objects matching the specified criteria.
                Returns an empty list if no matching records are found.

        Raises:
            RetentionRecordHandlerError: if loading records fails

        Note:
            Non-existent queue IDs or job IDs are silently skipped during the filtering process.
        """
        sanitized_query_job_map = self._prune_and_deduplicate_query_map(
            queue_job_id_map=queue_job_id_map
        )
        existing_records: QueueJobKeyMapping = self._load_existing_records_from_file()

        selected_records: List[RetentionRecord] = []

        # Flatten query map
        all_queue_job_pairs: List[Tuple[str, str]] = []
        for queue, jobs in sanitized_query_job_map.items():
            all_queue_job_pairs.extend((queue, job) for job in jobs)

        queues: Dict[str, JobKeyMapping] = existing_records["queues"]
        for queue_id, job_id in all_queue_job_pairs:
            # Skip if queue_id or job_id is not present in the dictionary
            if queue_id not in queues or job_id not in queues[queue_id]["jobs"]:
                continue

            job_s3_keys: List[str] = queues[queue_id]["jobs"][job_id]

            selected_records.extend(
                RetentionRecord(queue_id, job_id, s3_key) for s3_key in job_s3_keys
            )

        return selected_records

    def _prune_and_deduplicate_query_map(
        self, queue_job_id_map: Dict[str, List[str]]
    ) -> Dict[str, List[str]]:
        """
        Sanitize and deduplicate the queue-to-job ID mapping.

        Processes the input mapping by:
        1. Removing queue IDs with empty job lists
        2. Deduplicating job IDs within each queue

        Args:
            queue_job_id_map (Dict[str, List[str]]): Mapping of queue IDs to lists of job IDs,
                potentially containing duplicates or empty lists.
                Example: {'queue1': ['job1', 'job2'], 'queue2': ['job3']}

        Returns:
            Dict[str, List[str]]: Sanitized mapping where:
                - Values are deduplicated lists of job IDs
                - Queues with empty job lists are excluded
        """
        sanitized_map: Dict[str, List[str]] = {}

        for queue_id, job_ids in queue_job_id_map.items():
            if not job_ids:
                continue
            unique_jobs = set(job_ids)
            sanitized_map[queue_id] = list(unique_jobs)

        return sanitized_map

    def _load_existing_records_from_file(self) -> QueueJobKeyMapping:
        """
        Reads and parses the JSON storage file containing retention records.

        Note:
            See top of the file for an example of the QueueJobKeyMapping data structure.

        Returns:
            QueueJobKeyMapping: Nested dictionary mapping queueIds <-> JobIds <-> S3 keys

        Raises:
            RetentionRecordHandlerError: If the file cannot be read or parsed.
        """
        try:
            file_data = self.storage_file_path.read_text(encoding="utf-8")
            existing_records: QueueJobKeyMapping = json.loads(file_data)
        except Exception as err:
            raise RetentionRecordHandlerError(f"Failed to read storage file: {str(err)}")

        return existing_records

    def _write_records_to_file(self, records: QueueJobKeyMapping) -> None:
        """
        Serializes and saves the records dictionary to JSON format.

        Note:
            See top of the file for an example of the QueueJobKeyMapping data structure.

        Args:
            records: QueueJobKeyMapping

        Raises:
            RetentionRecordHandlerError: If the file cannot be written to or if JSON serialization fails.
        """
        try:
            self.storage_file_path.write_text(json.dumps(records), encoding="utf-8")
        except Exception as err:
            raise RetentionRecordHandlerError(f"Failed to write to storage file: {str(err)}")

    def delete_storage(self) -> None:
        """
        Removes the storage file from the filesystem if it exists.

        If the file doesn't exist, the operation is skipped silently.

        Raises:
            RetentionRecordHandlerError: If the file exists but cannot be deleted
        """
        try:
            self.storage_file_path.unlink(missing_ok=True)
        except Exception as err:
            raise RetentionRecordHandlerError(f"Failed to delete storage file: {str(err)}")
