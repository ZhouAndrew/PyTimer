"""Unit tests for :class:`TimerManager`."""

import time
import pytest

from main import TimerManager, FINISHED, NOT_SET


def test_create_timer_validations(tmp_path):
    tm = TimerManager(database_path=str(tmp_path / "tm.db"))
    with pytest.raises(ValueError):
        tm.create_timer("", 5)
    with pytest.raises(ValueError):
        tm.create_timer("name", 0)
    with pytest.raises(ValueError):
        tm.create_timer("name", -3)


def test_pause_resume_and_finish(tmp_path):
    tm = TimerManager(database_path=str(tmp_path / "tm.db"))
    tid = tm.create_timer("t1", 2)
    assert tm.is_timer_running(tid)
    time.sleep(0.2)
    tm.pause_timer(tid)
    assert tm.is_timer_paused(tid)
    remaining = tm.dm.get_attr(tid, "duration")
    assert 0 < remaining < 2
    assert tm.dm.get_attr(tid, "start_time") == NOT_SET
    assert tm.dm.get_attr(tid, "end_time") == NOT_SET
    tm.resume_timer(tid)
    assert tm.is_timer_running(tid)
    assert tm.dm.get_attr(tid, "start_time") != NOT_SET
    assert tm.dm.get_attr(tid, "end_time") != NOT_SET
    tm.mark_timer_finished(tid)
    info = tm.get_timer_info(tid)
    assert info["status"] == FINISHED
    tm.mark_timer_finished(tid)  # idempotent


def test_is_timer_exists_validates_input(tmp_path):
    tm = TimerManager(database_path=str(tmp_path / "tm.db"))
    with pytest.raises(ValueError):
        tm.is_timer_exists(0)
    with pytest.raises(ValueError):
        tm.is_timer_exists(-1)
    with pytest.raises(ValueError):
        tm.is_timer_exists("1")  # type: ignore[arg-type]
