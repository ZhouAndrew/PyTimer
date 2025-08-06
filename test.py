import pytest
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
