import pytest
from data import DataManager


def test_custom_table_names(tmp_path):
    db_file = tmp_path / "tables.db"
    column_types = {"name": str}

    dm_a = DataManager(column_types, database_path=str(db_file), table_name="table_a")
    id_a = dm_a.add_item({"name": "Alice"})

    dm_b = DataManager(column_types, database_path=str(db_file), table_name="table_b")
    id_b = dm_b.add_item({"name": "Bob"})

    assert dm_a.get_attr(id_a, "name") == "Alice"
    assert dm_b.get_attr(id_b, "name") == "Bob"
    assert set(dm_a.find_item()) == {id_a}
    assert set(dm_b.find_item()) == {id_b}
