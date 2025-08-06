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
