# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import pytest
import json
from unittest.mock import patch
from pathlib import Path
from typing import Dict, List

from deadline.job_attachments.exceptions import RetentionRecordHandlerError
from deadline.job_attachments.bucket_sweeper.retention_record_handler import (
    QueueJobKeyMapping,
    RetentionRecordHandler,
)
from deadline.job_attachments.models import RetentionRecord


@pytest.fixture
def handler(tmp_path: Path) -> RetentionRecordHandler:
    sotrage_file_path: Path = tmp_path / "retention_records.json"
    return RetentionRecordHandler(storage_file_path=sotrage_file_path)


class TestRetentionRecordHandler:
    def test_initialize_storage_happy_path(self, tmp_path: Path):
        """Test successful initialization when storage file doesn't exist"""

        # Constructor initializes storage file automatically
        storage_file_path: Path = tmp_path / "retention_records.json"
        handler = RetentionRecordHandler(storage_file_path=storage_file_path)

        assert handler.storage_file_path.exists()
        with open(handler.storage_file_path, "r") as file:
            assert json.load(file) == {"queues": {}}

    def test_initialize_storage_file_exists(self, handler: RetentionRecordHandler):
        """Test initialization fails when storage file already exists"""

        storage_file: Path = Path(handler.storage_file_path)
        storage_file.touch()

        with pytest.raises(RetentionRecordHandlerError) as err:
            handler._initialize_storage()

        assert "storage file already exists" in str(err)

    def test_initialize_storage_write_fails(self, tmp_path: Path):
        """Test initialization fails when writing to file fails"""

        storage_file_path: Path = tmp_path / "retention_records.json"
        handler: RetentionRecordHandler = RetentionRecordHandler(
            storage_file_path=storage_file_path
        )

        # Mock Path.exists to return False
        with patch.object(Path, "exists", return_value=False):
            # Mock Path.write_text to raise an IOError
            with patch.object(Path, "write_text", side_effect=IOError("Permission denied")):
                with pytest.raises(RetentionRecordHandlerError) as err:
                    handler._initialize_storage()

                assert "Failed to create retention record storage file" in str(err.value)
                assert "Permission denied" in str(err.value)

    def test_load_multiple_records_happy_path(
        self, handler: RetentionRecordHandler, tmp_path: Path
    ):
        """Test loading multiple existing records"""
        test_data: QueueJobKeyMapping = {
            "queues": {
                "queue-1": {"jobs": {"job-1": ["key1", "key2"], "job-2": ["key3"]}},
                "queue-2": {"jobs": {"job-3": ["key4", "key5", "key6"]}},
            }
        }

        file_path: Path = tmp_path / "retention_records.json"
        file_path.write_text(json.dumps(test_data))

        records: QueueJobKeyMapping = handler._load_existing_records_from_file()

        assert records == test_data

        assert "queues" in records
        assert "queue-1" in records["queues"]
        assert "jobs" in records["queues"]["queue-1"]
        assert "job-1" in records["queues"]["queue-1"]["jobs"]

        assert records["queues"]["queue-1"]["jobs"]["job-1"] == ["key1", "key2"]
        assert records["queues"]["queue-1"]["jobs"]["job-2"] == ["key3"]
        assert records["queues"]["queue-2"]["jobs"]["job-3"] == ["key4", "key5", "key6"]

    def test_load_empty_records(self, handler: RetentionRecordHandler, tmp_path: Path):
        """Test loading when storage file exists but is empty"""
        records: QueueJobKeyMapping = handler._load_existing_records_from_file()

        assert records == {"queues": {}}

    def test_load_nonexistent_file(self, handler: RetentionRecordHandler, tmp_path: Path):
        """Test loading from a non-existent file"""

        # Remove the created storage file
        storage_file: Path = tmp_path / "retention_records.json"
        storage_file.unlink(missing_ok=True)

        with pytest.raises(RetentionRecordHandlerError) as err:
            handler._load_existing_records_from_file()

        assert "Failed to read storage file" in str(err.value)

    def test_load_io_error(self, handler: RetentionRecordHandler):
        """Test handling of IO error while reading file"""

        # Mock Path.read_text to raise an IOError
        with patch.object(Path, "read_text", side_effect=IOError("Permission denied")):
            with pytest.raises(RetentionRecordHandlerError) as err:
                handler._load_existing_records_from_file()

            assert "Failed to read storage file" in str(err.value)
            assert "Permission denied" in str(err.value)

    def test_write_records_to_file_multiple_records_happy_path(
        self, handler: RetentionRecordHandler
    ):
        """Test writing multiple records successfully"""
        test_records: QueueJobKeyMapping = {
            "queues": {
                "queue-1": {"jobs": {"job-1": ["key1", "key2"], "job-2": ["key3"]}},
                "queue-2": {"jobs": {"job-3": ["key4", "key5", "key6"]}},
            }
        }

        handler._write_records_to_file(test_records)

        saved_records: QueueJobKeyMapping = json.loads(handler.storage_file_path.read_text())

        assert saved_records == test_records

        assert "queues" in saved_records
        assert "queue-1" in saved_records["queues"]
        assert "jobs" in saved_records["queues"]["queue-1"]

        assert saved_records["queues"]["queue-1"]["jobs"]["job-1"] == ["key1", "key2"]
        assert saved_records["queues"]["queue-1"]["jobs"]["job-2"] == ["key3"]
        assert saved_records["queues"]["queue-2"]["jobs"]["job-3"] == ["key4", "key5", "key6"]

    def test_write_records_to_file_empty_records(self, handler: RetentionRecordHandler):
        """Test writing an empty records dictionary"""
        empty_records: QueueJobKeyMapping = {"queues": {}}

        handler._write_records_to_file(empty_records)

        saved_records: QueueJobKeyMapping = json.loads(handler.storage_file_path.read_text())

        assert saved_records == {"queues": {}}

    def test_write_records_io_error(self, handler: RetentionRecordHandler):
        """Test handling IO error when attempting to write records to file"""

        # Mock Path.write_text to raise an IOError
        with patch.object(Path, "write_text", side_effect=IOError("Permission denied")):
            with pytest.raises(RetentionRecordHandlerError) as err:
                handler._write_records_to_file({"queues": {}})

            error_message: str = str(err.value)
            assert "Failed to write to storage file" in error_message
            assert "Permission denied" in error_message

    def test_prune_and_deduplicate_query_map_multiple_keys(self, handler: RetentionRecordHandler):
        """Test cleaning a queue_job_id_map with multiple queues, duplicates, and empty lists"""
        input_map: Dict[str, List[str]] = {
            "queue-1": ["job-1", "job-2", "job-1", "job-3"],  # Contains duplicates
            "queue-2": ["job-4", "job-5"],
            "queue-3": [],  # Empty list, should be removed
            "queue-4": ["job-6", "job-6", "job-7"],  # Contains duplicates
        }

        sanitized_map: Dict[str, List[str]] = handler._prune_and_deduplicate_query_map(input_map)

        expected_map: Dict[str, List[str]] = {
            "queue-1": ["job-1", "job-2", "job-3"],
            "queue-2": ["job-4", "job-5"],
            "queue-4": ["job-6", "job-7"],
        }

        assert sorted(sanitized_map["queue-1"]) == sorted(expected_map["queue-1"])
        assert sorted(sanitized_map["queue-2"]) == sorted(expected_map["queue-2"])
        assert sorted(sanitized_map["queue-4"]) == sorted(expected_map["queue-4"])
        assert "queue-3" not in sanitized_map

    def test_delete_storage_file_exists_happy_path(self, handler: RetentionRecordHandler):
        """Test deleting an existing storage file"""

        # Storage file is created on initialization
        handler.delete_storage()

        assert not handler.storage_file_path.exists()

    def test_delete_storage_file_does_not_exist(self, handler: RetentionRecordHandler):
        """Test attempting to delete a non-existent storage file"""
        if handler.storage_file_path.exists():
            handler.storage_file_path.unlink()

        handler.delete_storage()

        assert not handler.storage_file_path.exists()

    def test_delete_storage_io_error(self, handler: RetentionRecordHandler):
        """Test handling IO error when attempting to delete storage file"""
        with patch.object(Path, "unlink") as mock_unlink:
            mock_unlink.side_effect = IOError("Permission denied")

            with pytest.raises(RetentionRecordHandlerError) as err:
                handler.delete_storage()

            error_message: str = str(err.value)
            assert "Failed to delete storage file" in error_message
            assert "Permission denied" in error_message

    def test_insert_retention_records_multiple_records_happy_path(
        self, handler: RetentionRecordHandler
    ):
        """Test inserting multiple new retention records"""
        records: List[RetentionRecord] = [
            RetentionRecord("queue-1", "job-1", "key1"),
            RetentionRecord("queue-1", "job-1", "key2"),
            RetentionRecord("queue-1", "job-2", "key3"),
            RetentionRecord("queue-2", "job-3", "key4"),
        ]

        with patch.object(handler, "_load_existing_records_from_file") as mock_load:
            with patch.object(handler, "_write_records_to_file") as mock_write:
                mock_load.return_value = {"queues": {}}

                handler.insert_retention_records(records)

                expected_records: QueueJobKeyMapping = {
                    "queues": {
                        "queue-1": {"jobs": {"job-1": ["key1", "key2"], "job-2": ["key3"]}},
                        "queue-2": {"jobs": {"job-3": ["key4"]}},
                    }
                }

                mock_write.assert_called_once_with(expected_records)

    def test_insert_retention_records_empty_list(self, handler: RetentionRecordHandler):
        """Test inserting an empty list of retention records"""
        records: List[RetentionRecord] = []

        with patch.object(handler, "_load_existing_records_from_file") as mock_load:
            with patch.object(handler, "_write_records_to_file") as mock_write:
                mock_load.return_value = {"queues": {}}

                handler.insert_retention_records(records)

                mock_write.assert_called_once_with({"queues": {}})

    def test_insert_retention_records_with_existing_records(self, handler: RetentionRecordHandler):
        """Test inserting records when some records already exist"""
        existing_records: QueueJobKeyMapping = {
            "queues": {"queue-1": {"jobs": {"job-1": ["key1"], "job-2": ["key2"]}}}
        }

        new_records: List[RetentionRecord] = [
            RetentionRecord("queue-1", "job-1", "key1"),  # Duplicate, should not be added
            RetentionRecord("queue-1", "job-1", "key3"),  # New object for existing job
            RetentionRecord("queue-2", "job-3", "key4"),  # Completely new record
        ]

        with patch.object(handler, "_load_existing_records_from_file") as mock_load:
            with patch.object(handler, "_write_records_to_file") as mock_write:
                mock_load.return_value = existing_records

                handler.insert_retention_records(new_records)

                expected_records: QueueJobKeyMapping = {
                    "queues": {
                        "queue-1": {"jobs": {"job-1": ["key1", "key3"], "job-2": ["key2"]}},
                        "queue-2": {"jobs": {"job-3": ["key4"]}},
                    }
                }
                mock_write.assert_called_once_with(expected_records)

    def test_get_retention_records_multiple_records_happy_path(
        self, handler: RetentionRecordHandler
    ):
        """Test retrieving multiple records across different queues and jobs"""
        existing_records: QueueJobKeyMapping = {
            "queues": {
                "queue-1": {"jobs": {"job-1": ["key1", "key2"], "job-2": ["key3"]}},
                "queue-2": {"jobs": {"job-3": ["key4"]}},
            }
        }

        queue_job_id_map: Dict[str, List[str]] = {
            "queue-1": ["job-1", "job-2"],
            "queue-2": ["job-3"],
        }

        with patch.object(handler, "_load_existing_records_from_file") as mock_load:
            with patch.object(handler, "_prune_and_deduplicate_query_map") as mock_clean:
                mock_load.return_value = existing_records
                mock_clean.return_value = queue_job_id_map

                records = handler.get_retention_records(queue_job_id_map)

                expected_records = [
                    RetentionRecord("queue-1", "job-1", "key1"),
                    RetentionRecord("queue-1", "job-1", "key2"),
                    RetentionRecord("queue-1", "job-2", "key3"),
                    RetentionRecord("queue-2", "job-3", "key4"),
                ]
                assert records == expected_records

    def test_get_retention_records_empty_map(self, handler: RetentionRecordHandler):
        """Test retrieving records with empty queue_job_id_map"""
        existing_records: QueueJobKeyMapping = {
            "queues": {"queue-1": {"jobs": {"job-1": ["key1"]}}}
        }

        queue_job_id_map: Dict[str, List[str]] = {}

        with patch.object(handler, "_load_existing_records_from_file") as mock_load:
            with patch.object(handler, "_prune_and_deduplicate_query_map") as mock_clean:
                mock_load.return_value = existing_records
                mock_clean.return_value = queue_job_id_map

                records = handler.get_retention_records(queue_job_id_map)

                assert records == []

    def test_get_retention_records_partial_matches(self, handler: RetentionRecordHandler):
        """Test retrieving records where some queues/jobs exist and others don't"""
        existing_records: QueueJobKeyMapping = {
            "queues": {
                "queue-1": {"jobs": {"job-1": ["key1"], "job-2": ["key2"]}},
                "queue-2": {"jobs": {"job-3": ["key3"]}},
            }
        }

        queue_job_id_map: Dict[str, List[str]] = {
            "queue-1": ["job-1", "job-nonexistent"],  # One valid, one invalid job
            "queue-2": ["job-3"],  # Valid queue and job
            "queue-nonexistent": ["job-1"],  # Invalid queue
        }

        with patch.object(handler, "_load_existing_records_from_file") as mock_load:
            with patch.object(handler, "_prune_and_deduplicate_query_map") as mock_clean:
                mock_load.return_value = existing_records
                mock_clean.return_value = queue_job_id_map

                records = handler.get_retention_records(queue_job_id_map)

                expected_records = [
                    RetentionRecord("queue-1", "job-1", "key1"),
                    RetentionRecord("queue-2", "job-3", "key3"),
                ]
                assert records == expected_records
