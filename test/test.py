import os
import time
import threading
import multiprocessing
import pytest
import pytest
from data import DataManager
import sqlite3
from typing import Any

from data import DataManager  # Replace with actual module name

@pytest.fixture
def sample_manager():
    column_types = {
        "name": str,
        "count": int,
        "active": bool,
        "rating": float
    }
    manager = DataManager(column_types, database_path=":memory:")
    return manager

def test_add_and_get(sample_manager: DataManager):
    item_id = sample_manager.add_item({
        "name": "example",
        "count": 3,
        "active": True,
        "rating": 4.5
    })
    assert isinstance(item_id, int)

    assert sample_manager.get_attr(item_id, "name") == "example"
    assert sample_manager.get_attr(item_id, "count") == 3
    assert sample_manager.get_attr(item_id, "active") == True
    assert sample_manager.get_attr(item_id, "rating") == 4.5

def test_set_attr(sample_manager: DataManager):
    item_id = sample_manager.add_item({
        "name": "initial",
        "count": 1,
        "active": False,
        "rating": 2.0
    })

    sample_manager.set_attr(item_id, "name", "updated")
    sample_manager.set_attr(item_id, "count", 42)
    sample_manager.set_attr(item_id, "active", True)
    sample_manager.set_attr(item_id, "rating", 3.14)

    assert sample_manager.get_attr(item_id, "name") == "updated"
    assert sample_manager.get_attr(item_id, "count") == 42
    assert sample_manager.get_attr(item_id, "active") == True
    assert sample_manager.get_attr(item_id, "rating") == 3.14

def test_rm_item(sample_manager: DataManager):
    item_id = sample_manager.add_item({
        "name": "to_remove",
        "count": 7,
        "active": True,
        "rating": 1.0
    })
    sample_manager.rm_item(item_id)

    with pytest.raises(Exception):
        sample_manager.get_attr(item_id, "name")

def test_find_item(sample_manager: DataManager):
    ids = [
        sample_manager.add_item({
            "name": f"item{i}",
            "count": i,
            "active": i % 2 == 0,
            "rating": i * 0.1
        }) for i in range(5)
    ]
    found = sample_manager.find_item()
    assert set(found) == set(ids)


@pytest.fixture
def manager():
    column_types = {
        "name": str,
        "count": int,
        "active": bool,
        "rating": float,
        "tags": list,
        "settings": dict
    }
    return DataManager(column_types, database_path=":memory:")

def test_add_and_get_basic(manager: DataManager):
    item_id = manager.add_item({
        "name": "test1",
        "count": 10,
        "active": True,
        "rating": 4.5,
        "tags": [],
        "settings": {}
    })

    assert isinstance(item_id, int)
    assert manager.get_attr(item_id, "name") == "test1"
    assert manager.get_attr(item_id, "count") == 10
    assert manager.get_attr(item_id, "active") is True
    assert manager.get_attr(item_id, "rating") == 4.5

def test_add_and_get_json_types(manager: DataManager):
    item_id = manager.add_item({
        "name": "complex",
        "count": 5,
        "active": False,
        "rating": 2.2,
        "tags": ["alpha", "beta"],
        "settings": {"volume": 7, "dark_mode": True}
    })

    assert manager.get_attr(item_id, "tags") == ["alpha", "beta"]
    assert manager.get_attr(item_id, "settings") == {"volume": 7, "dark_mode": True}

def test_set_attr(manager: DataManager):
    item_id = manager.add_item({
        "name": "updatable",
        "count": 1,
        "active": False,
        "rating": 0.0,
        "tags": [],
        "settings": {}
    })

    manager.set_attr(item_id, "name", "updated")
    manager.set_attr(item_id, "count", 42)
    manager.set_attr(item_id, "active", True)
    manager.set_attr(item_id, "rating", 3.1415)
    manager.set_attr(item_id, "tags", ["release"])
    manager.set_attr(item_id, "settings", {"updated": True})

    assert manager.get_attr(item_id, "name") == "updated"
    assert manager.get_attr(item_id, "count") == 42
    assert manager.get_attr(item_id, "active") is True
    assert manager.get_attr(item_id, "rating") == 3.1415
    assert manager.get_attr(item_id, "tags") == ["release"]
    assert manager.get_attr(item_id, "settings") == {"updated": True}

def test_rm_item(manager: DataManager):
    item_id = manager.add_item({
        "name": "delete_me",
        "count": 0,
        "active": False,
        "rating": 1.0,
        "tags": [],
        "settings": {}
    })

    manager.rm_item(item_id)

    with pytest.raises(Exception):
        manager.get_attr(item_id, "name")

def test_find_items(manager: DataManager):
    ids = []
    for i in range(3):
        ids.append(manager.add_item({
            "name": f"item{i}",
            "count": i,
            "active": i % 2 == 0,
            "rating": float(i),
            "tags": [],
            "settings": {}
        }))

    found_ids = manager.find_item()
    assert set(found_ids) == set(ids)


DB_FILE = "test_concurrency.db"

# 公共列定义
column_types = {
    "name": str,
    "count": int
}

@pytest.fixture(autouse=True)
def cleanup_db():
    """在每个测试前清理数据库文件"""
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
    yield
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)

def thread_worker(thread_id, loops=50):
    dm = DataManager(column_types, DB_FILE)
    for i in range(loops):
        dm.add_item({"name": f"t{thread_id}", "count": i})
        time.sleep(0.005)  # 模拟真实操作延迟

def process_worker(proc_id, loops=50):
    dm = DataManager(column_types, DB_FILE)
    for i in range(loops):
        dm.add_item({"name": f"p{proc_id}", "count": i})
        time.sleep(0.005)


def count_rows():
    dm = DataManager(column_types, DB_FILE)
    return len(dm.find_item())

def test_multithread_concurrent_writes():
    threads = [threading.Thread(target=thread_worker, args=(i,)) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    total_rows = count_rows()
    assert total_rows == 5 * 50, f"Expected 250 rows, got {total_rows}"

def test_multiprocess_concurrent_writes():
    procs = [multiprocessing.Process(target=process_worker, args=(i,)) for i in range(5)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()

    total_rows = count_rows()
    assert total_rows == 5 * 50, f"Expected 250 rows, got {total_rows}"