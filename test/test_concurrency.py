import threading
import multiprocessing
from typing import Dict

import pytest

from data import DataManager

# Shared column type definition for tests
COLUMN_TYPES: Dict[str, type] = {
    "name": str,
    "count": int,
    "active": bool,
    "rating": float,
}


def _thread_worker(idx: int, db_path: str) -> None:
    """Insert a single record into the database in a thread."""
    manager = DataManager(COLUMN_TYPES, database_path=db_path)
    manager.add_item(
        {
            "name": f"thread{idx}",
            "count": idx,
            "active": True,
            "rating": float(idx),
        }
    )


def _process_worker(idx: int, db_path: str) -> None:
    """Insert a single record into the database in a process."""
    manager = DataManager(COLUMN_TYPES, database_path=db_path)
    manager.add_item(
        {
            "name": f"process{idx}",
            "count": idx,
            "active": False,
            "rating": float(idx),
        }
    )


def test_multithread_access(tmp_path):
    """DataManager should handle concurrent access from multiple threads."""
    db_path = str(tmp_path / "thread.db")

    threads = [
        threading.Thread(target=_thread_worker, args=(i, db_path)) for i in range(5)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    manager = DataManager(COLUMN_TYPES, database_path=db_path)
    ids = manager.find_item()
    assert len(ids) == 5
    names = {manager.get_attr(i, "name") for i in ids}
    assert names == {f"thread{i}" for i in range(5)}


def test_multiprocess_access(tmp_path):
    """DataManager should handle concurrent access from multiple processes."""
    db_path = str(tmp_path / "process.db")

    processes = [
        multiprocessing.Process(target=_process_worker, args=(i, db_path))
        for i in range(4)
    ]
    for proc in processes:
        proc.start()
    for proc in processes:
        proc.join()

    manager = DataManager(COLUMN_TYPES, database_path=db_path)
    ids = manager.find_item()
    assert len(ids) == 4
    names = {manager.get_attr(i, "name") for i in ids}
    assert names == {f"process{i}" for i in range(4)}
