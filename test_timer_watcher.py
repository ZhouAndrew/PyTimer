import time
import time
import time
from typing import List

import time
from typing import List

from main import TimerManager, TimerWatcher, PAUSED, FINISHED


def test_watcher_marks_finished_and_callback(tmp_path):
    db = str(tmp_path / "timers.db")
    tm = TimerManager(database_path=db)
    finished: List[int] = []
    watcher = TimerWatcher(tm, finished.append)
    tid = tm.create_timer("t1", 1)
    time.sleep(1.3)
    assert tm.dm.get_attr(tid, "status") == FINISHED
    assert finished == [tid]
    watcher.stop()


def test_watcher_handles_pause_resume(tmp_path):
    db = str(tmp_path / "timers_pause.db")
    tm = TimerManager(database_path=db)
    finished: List[int] = []
    watcher = TimerWatcher(tm, finished.append)
    tid = tm.create_timer("t1", 1)
    time.sleep(0.3)
    tm.pause_timer(tid)
    assert tm.dm.get_attr(tid, "status") == PAUSED
    time.sleep(0.5)
    assert finished == []  # callback not triggered while paused
    remaining = tm.dm.get_attr(tid, "duration")
    tm.resume_timer(tid)
    time.sleep(remaining + 0.3)
    assert tm.dm.get_attr(tid, "status") == FINISHED
    assert finished == [tid]
    watcher.stop()


def test_watcher_handles_deletion(tmp_path):
    db = str(tmp_path / "timers_delete.db")
    tm = TimerManager(database_path=db)
    finished: List[int] = []
    watcher = TimerWatcher(tm, finished.append)
    tid = tm.create_timer("t1", 1)
    tm.rm_timer(tid)
    time.sleep(1.2)
    assert finished == []
    watcher.stop()
