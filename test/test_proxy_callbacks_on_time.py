import time
from typing import List

from timer_manager import TimerManager, TimerManagerProxy
from history import TimerWatcher


def test_proxy_callbacks_on_time(tmp_path):
    db = str(tmp_path / "proxy.db")
    tm = TimerManager(database_path=db)
    proxy = TimerManagerProxy(tm)

    events: List[tuple[int, float]] = []

    def record(event: str, timer_id: int) -> None:
        if event == "finished":
            events.append((timer_id, time.time()))

    proxy.add_callback(record)

    watcher = TimerWatcher(proxy)

    tid1 = proxy.create_timer("t1", 1)
    tid2 = proxy.create_timer("t2", 2)

    time.sleep(2.3)
    watcher.stop()

    assert [tid for tid, _ in events] == [tid1, tid2]

    for tid, fired in events:
        end_time = tm.dm.get_attr(tid, "end_time")
        assert fired >= end_time
        assert fired - end_time < 0.5
