import unittest
from data import DataManager  # renamed from TimerManager to DataManager


class TestDataManager(unittest.TestCase):
    def setUp(self):
        # Use in-memory DB and define allowed attributes for consistency
        self.attrs = {"name", "duration", "start_time", "status"}
        self.dm = DataManager(":memory:", allowed_attrs=self.attrs)

    def test_create_and_get_timer(self):
        timer_id = self.dm.create_timer({
            "name": "TestTimer",
            "duration": 100,
            "start_time": 1234567890,
            "status": "running"
        })
        self.assertIsInstance(timer_id, int)

        timer = self.dm.get_timer(timer_id)
        self.assertIsNotNone(timer)
        self.assertEqual(timer["name"], "TestTimer")
        self.assertEqual(timer["duration"], 100)
        self.assertEqual(timer["status"], "running")

    def test_get_timer_attr(self):
        tid = self.dm.create_timer({
            "name": "AttrTimer",
            "duration": 200,
            "start_time": 1111111,
            "status": "idle"
        })
        status = self.dm.get_timer_attr(tid, "status")
        self.assertEqual(status, "idle")

    def test_set_timer_attr(self):
        tid = self.dm.create_timer({
            "name": "Setter",
            "duration": 300,
            "start_time": 2222222,
            "status": "paused"
        })
        self.dm.set_timer_attr(tid, "status", "resumed")
        new_status = self.dm.get_timer_attr(tid, "status")
        self.assertEqual(new_status, "resumed")

    def test_find_timer(self):
        self.dm.create_timer({
            "name": "FindMe",
            "duration": 999,
            "start_time": 0,
            "status": "active"
        })
        result = self.dm.find_timer({"name": "FindMe", "status": "active"})
        self.assertTrue(len(result) >= 1)
        self.assertIsInstance(result[0], int)

    def test_rm_timer(self):
        tid = self.dm.create_timer({
            "name": "DeleteMe",
            "duration": 50,
            "start_time": 0,
            "status": "queued"
        })
        self.dm.rm_timer(tid)
        self.assertIsNone(self.dm.get_timer(tid))

    def test_invalid_attr(self):
        tid = self.dm.create_timer({
            "name": "BadAttr",
            "duration": 10,
            "start_time": 0,
            "status": "ok"
        })
        with self.assertRaises(ValueError):
            self.dm.get_timer_attr(tid, "invalid_column")

        with self.assertRaises(ValueError):
            self.dm.set_timer_attr(tid, "invalid_column", "x")


if __name__ == "__main__":
    unittest.main()
