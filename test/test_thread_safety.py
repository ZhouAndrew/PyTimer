"""Thread-safety tests for :mod:`data` and :class:`TimerManager`."""

import threading
from typing import Dict

from data import DataManager
from timer_manager import TimerManager
import pytest

print(TimerManager)
# ---------------------------------------------------------------------------
# DataManager multi-thread tests

COLUMN_TYPES: Dict[str, type] = {
    "name": str,
    "count": int,
    "active": bool,
    "rating": float,
    "tags": list,
    "settings": dict,
}


def _dm_worker(idx: int, db_path: str) -> None:
    dm = DataManager(COLUMN_TYPES, database_path=db_path)
    item_id = dm.add_item(
        {
            "name": f"item{idx}",
            "count": idx,
            "active": bool(idx % 2),
            "rating": float(idx),
            "tags": [idx, idx + 1],
            "settings": {"idx": idx},
        }
    )
    dm.set_attr(item_id, "count", idx * 10)
    assert dm.get_attr(item_id, "count") == idx * 10
    assert dm.find_item({"name": f"item{idx}"}) == (item_id,)
    dm.rm_item(item_id)
    assert dm.find_item({"name": f"item{idx}"}) == ()


def test_datamanager_thread_safety_all_methods(tmp_path) -> None:
    db_path = str(tmp_path / "dm_thread.db")
    threads = [threading.Thread(target=_dm_worker, args=(i, db_path)) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    dm = DataManager(COLUMN_TYPES, database_path=db_path)
    assert dm.find_item() == ()


# ---------------------------------------------------------------------------
# TimerManager multi-thread tests


def _tm_worker(idx: int, db_path: str) -> None:
    tm = TimerManager(database_path=db_path)
    timer_id = tm.create_timer(f"timer{idx}", duration=idx + 1)
    assert tm.is_timer_running(timer_id)
    tm.pause_timer(timer_id)
    assert tm.is_timer_paused(timer_id)
    tm.resume_timer(timer_id)
    assert tm.is_timer_running(timer_id)
    info = tm.get_timer_info(timer_id)
    assert info["name"] == f"timer{idx}"
    tm.rm_timer(timer_id)
    assert not tm.is_timer_exists(timer_id)


def test_timermanager_thread_safety(tmp_path) -> None:
    db_path = str(tmp_path / "tm_thread.db")
    threads = [threading.Thread(target=_tm_worker, args=(i, db_path)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    tm = TimerManager(database_path=db_path)
    assert tm.dm.find_item() == ()

