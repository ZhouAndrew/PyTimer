from data import DataManager


def test_find_item_supports_required_attributes_keyword():
    dm = DataManager({"name": str, "count": int}, database_path=":memory:")
    for i in range(3):
        dm.add_item({"name": f"n{i}", "count": i})
    ids = dm.find_item(required_attributes={"name": "n1"})
    assert ids == (2,)
