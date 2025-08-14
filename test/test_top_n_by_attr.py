# test_top_n_by_attr.py
import pytest
from data import DataManager  # your data.py

@pytest.fixture
def dm():
    # columns include types we want to sort on (and one unsupported: bool)
    columns = {
        "name": str,
        "duration": int,
        "rating": float,
        "tags": list,
        "flags": dict,
        "active": bool,
    }
    m = DataManager(columns, database_path=":memory:")

    # Insert 5 deterministic records (ids will be 1..5 in this order)
    rows = [
        {"name": "A",       "duration": 10, "rating": 1.5, "tags": ["x"],             "flags": {},                 "active": True},
        {"name": "Carol",   "duration": 30, "rating": 3.5, "tags": ["x","y","z"],     "flags": {"a": 1},           "active": False},
        {"name": "Bob",     "duration": 20, "rating": 2.5, "tags": ["x","y"],         "flags": {"long": 1},        "active": True},
        {"name": "Eveleen", "duration": 40, "rating": 0.5, "tags": [],                "flags": {"a":[1,2,3]},      "active": False},
        {"name": "Dan",     "duration": 5,  "rating": 9.9, "tags": ["x","y","z","w"], "flags": {},                 "active": True},
    ]
    for r in rows:
        m.add_item(r)
    return m


def test_top_n_int_desc(dm):
    # duration values: [10(id1), 30(id2), 20(id3), 40(id4), 5(id5)]
    # largest 3 -> ids [4, 2, 3]
    ids = dm.top_n_by_attr("duration", 3, largest=True)
    assert ids == (4, 2, 3)

def test_top_n_int_asc(dm):
    # smallest 2 -> ids [5, 1]
    ids = dm.top_n_by_attr("duration", 2, largest=False)
    assert ids == (5, 1)

def test_top_n_float_desc(dm):
    # rating values: [1.5(id1), 3.5(id2), 2.5(id3), 0.5(id4), 9.9(id5)]
    # largest 2 -> ids [5, 2]
    ids = dm.top_n_by_attr("rating", 2, largest=True)
    assert ids == (5, 2)

def test_top_n_str_length_desc(dm):
    # name lengths: A(1,id1), Carol(5,id2), Bob(3,id3), Eveleen(7,id4), Dan(3,id5)
    # largest 2 -> ids [4, 2]
    ids = dm.top_n_by_attr("name", 2, largest=True)
    assert ids == (4, 2)

def test_top_n_list_length_asc(dm):
    # tags lengths: [1(id1), 3(id2), 2(id3), 0(id4), 4(id5)]
    # smallest 3 -> ids [4, 1, 3]
    ids = dm.top_n_by_attr("tags", 3, largest=False)
    assert ids == (4, 1, 3)

def test_top_n_dict_json_length_desc(dm):
    # flags JSON lengths (approx): {}(2,id1), {"a":1}(8,id2),
    # {"long":1}(11,id3), {"a":[1,2,3]}(16,id4), {}(2,id5)
    # largest 2 -> ids [4, 3]
    ids = dm.top_n_by_attr("flags", 2, largest=True)
    assert ids == (4, 3)

def test_limit_greater_than_rows(dm):
    ids = dm.top_n_by_attr("duration", 10, largest=True)
    # should simply return all 5 ids in the right order
    assert ids == (4, 2, 3, 1, 5)

def test_unknown_attribute_raises(dm):
    with pytest.raises(ValueError):
        dm.top_n_by_attr("does_not_exist", 3)

def test_unsupported_type_raises(dm):
    # bool is not allowed for sorting per spec
    with pytest.raises(TypeError):
        dm.top_n_by_attr("active", 2)
