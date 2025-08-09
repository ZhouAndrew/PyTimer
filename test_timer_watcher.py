import threading
import time

from time_manager import TimeManager, TimerWatcher


def test_timer_watcher_basic():
    tm = TimeManager()
    event_a = threading.Event()
    event_b = threading.Event()

    tm.add_timer(0.1, event_a.set)
    tm.add_timer(0.2, event_b.set)

    watcher = TimerWatcher(tm)
    watcher.watch_all()
    watcher.join()

    assert event_a.is_set()
    assert event_b.is_set()


def test_timer_watcher_update():
    tm = TimeManager()
    event = threading.Event()
    tid = tm.add_timer(0.3, event.set)

    watcher = TimerWatcher(tm)
    watcher.watch_all()

    # shorten timer after it has already started waiting
    time.sleep(0.1)
    tm.update_timer(tid, 0.1)
    watcher.notify_update(tid)

    watcher.join()
    assert event.is_set()
