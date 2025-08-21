"""Timer management utilities backed by :class:`DataManager`.

This module provides :class:`TimerManager` for manipulating timers stored in a
SQLite database, :class:`TimerManagerProxy` which adds callback support to the
manager, and :class:`TimerWatcher` which monitors timers and fires callbacks
when they expire.  The watcher runs in a background ``asyncio`` event loop so
normal synchronous code can create timers without worrying about awaiting
coroutine objects.
"""

from __future__ import annotations

import asyncio
import threading
import time
from typing import Any, Callable, Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, Executor

# from history import *
from data import DataManager


PAUSED = "paused"
RUNNING = "running"
FINISHED = "finished"
NOT_SET = -1.0
PyTimer_TABLE_NAME = "PyTimer"


class TimerManager:
    """Manage simple timers stored in a SQLite database."""

    def __init__(
        self, database_path: str = "data.db", table_name: str = PyTimer_TABLE_NAME
    ) -> None:
        """Create a new ``TimerManager``.

        Parameters
        ----------
        database_path: str
            Path to the SQLite database used for storing timers.
        """

        self.dm = DataManager(
            column_type_dict={
                "duration": float,
                "start_time": float,
                "end_time": float,
                "status": str,
                "name": str,
            },
            database_path=database_path,
        )

    def is_timer_exists(self, timer_id: int) -> bool:
        """Return ``True`` if ``timer_id`` exists in the database."""
        if not isinstance(timer_id, int) or timer_id <= 0:
            raise ValueError("Timer ID must be a positive integer.")
        try:
            self.dm.get_attr(timer_id, "name")
            return True
        except ValueError:
            return False

    def is_timer_running(self, timer_id: int) -> bool:
        if not self.is_timer_exists(timer_id):
            raise ValueError("Timer with this ID does not exist.")
        status = self.dm.get_attr(timer_id, "status")
        duration = self.dm.get_attr(timer_id, "duration")
        start_time = self.dm.get_attr(timer_id, "start_time")
        end_time = self.dm.get_attr(timer_id, "end_time")
        if end_time - duration != start_time:
            raise RuntimeError(
                "The timer's start and end times do not match the duration."
            )
        return status == RUNNING

    def is_timer_paused(self, timer_id: int) -> bool:
        if not self.is_timer_exists(timer_id):
            raise ValueError("Timer with this ID does not exist.")
        status = self.dm.get_attr(timer_id, "status")
        duration = self.dm.get_attr(timer_id, "duration")
        start_time = self.dm.get_attr(timer_id, "start_time")
        end_time = self.dm.get_attr(timer_id, "end_time")
        assert status == PAUSED
        assert duration >= 0
        assert start_time == NOT_SET
        assert end_time == NOT_SET
        return True

    def create_timer(self, name: str, duration: int) -> int:
        if not isinstance(name, str) or not isinstance(duration, int):
            raise ValueError("Name must be a string and duration must be an integer.")
        if duration <= 0:
            raise ValueError("Duration must be a positive integer.")
        if not name:
            raise ValueError("Name cannot be empty.")
        if len(name) > 100:
            raise ValueError("Name cannot exceed 100 characters.")
        this_moment = time.time()
        float_duration = float(duration)
        timer_id = self.dm.add_item(
            {
                "name": name,
                "duration": float_duration,
                "start_time": this_moment,
                "end_time": this_moment + float_duration,
                "status": RUNNING,
            }
        )
        return timer_id

    def rm_timer(self, timer_id: int) -> None:
        assert self.is_timer_exists(timer_id)
        self.dm.rm_item(timer_id)

    def pause_timer(self, timer_id: int) -> None:
        assert self.is_timer_exists(timer_id)
        assert self.is_timer_running(timer_id)
        end_time = self.dm.get_attr(timer_id, "end_time")
        remaining = end_time - time.time()
        self.dm.set_attr(timer_id, "duration", remaining)
        self.dm.set_attr(timer_id, "start_time", NOT_SET)
        self.dm.set_attr(timer_id, "end_time", NOT_SET)
        self.dm.set_attr(timer_id, "status", PAUSED)

    def resume_timer(self, timer_id: int) -> None:
        assert self.is_timer_exists(timer_id)
        assert self.is_timer_paused(timer_id)
        duration = self.dm.get_attr(timer_id, "duration")
        if duration < 0:
            raise ValueError("Cannot resume a timer with negative duration.")
        start_time = time.time()
        end_time = start_time + duration
        self.dm.set_attr(timer_id, "start_time", start_time)
        self.dm.set_attr(timer_id, "end_time", end_time)
        self.dm.set_attr(timer_id, "status", RUNNING)

    # ------------------------------------------------------------------
    def mark_timer_finished(self, timer_id: int) -> None:
        """Mark ``timer_id`` as finished.

        The timer is not removed from the database; only its ``status`` field is
        updated.  ``TimerWatcher`` calls this when a timer reaches its end.
        """

        if not self.is_timer_exists(timer_id):
            return
        # Only running timers can transition to finished
        status = self.dm.get_attr(timer_id, "status")
        if status == FINISHED:
            return
        self.dm.set_attr(timer_id, "status", FINISHED)

    def get_timer_info(self, timer_id: int) -> Dict[str, Any]:
        if not self.is_timer_exists(timer_id):
            raise ValueError("Timer with this ID does not exist.")
        return {
            "id": timer_id,
            "name": self.dm.get_attr(timer_id, "name"),
            "duration": self.dm.get_attr(timer_id, "duration"),
            "start_time": self.dm.get_attr(timer_id, "start_time"),
            "end_time": self.dm.get_attr(timer_id, "end_time"),
            "status": self.dm.get_attr(timer_id, "status"),
        }

    def top_n_by_attr(self, attr: str, n: int, largest: bool = True) -> tuple[int, ...]:
        """Return the top N timer IDs sorted by the specified attribute.

        Parameters
        ----------
        attr: str
            The attribute to sort by (e.g., "duration", "name").
        n: int
            The number of top items to return.
        largest: bool
            If ``True``, return the largest values; if ``False``, return the smallest.
        Returns
        -------
        tuple[int, ...]
            A tuple of timer IDs sorted by the specified attribute.

        Raises
        ------
        ValueError
            If the attribute does not exist or is not sortable.
        """
        if n <= 0:
            raise ValueError("N must be a positive integer.")
        return self.dm.top_n_by_attr(attr, n, largest)

    def timers_about_finishing(self, number: int = 1) -> tuple[int, ...]:
        """Return a list of timer IDs that are about to finish"""
        if not isinstance(number, int) or number <= 0:
            raise ValueError("Number must be a positive integer.")

        return self.dm.top_n_by_attr(
            "end_time", number, largest=False, required_attributes={"status": RUNNING}
        )
        # running_time r =[id for id in latest_timers if self.is_timer_running(id)]
        # running_timer..
        # sort()


# class TimerWatcher2:
#     def __init__(self,timer_manager:TimerManager=TimerManager(),call_back:function=lambda **args:return args ) -> None:
#         """ check which timer is finished and call the callback function"""
#         self.dm=timer_manager

#         self.watching_list=
print(TimerManager)


class TimerManagerProxy:
    """Proxy for :class:`TimerManager` that dispatches event callbacks."""

    def __init__(
        self, manager: TimerManager, task_pool: Optional[Executor] = None
    ) -> None:
        self._manager = manager
        self._callbacks: List[Callable[[str, int], None]] = []
        self.task_pool = task_pool or ThreadPoolExecutor(max_workers=4)
        self.task = None
        self.new_tracking_task()
        self.add_callback(self._handle_event)

        # Track the closest timers that are about to finish
        # self.the_closest_timers: Optional[int] = None
    def _handle_event(self, event: str, timer_id: int) -> None:
        """Handle timer events and update tracking_task."""
        if event == "created":
            # is it about finishing?
            end_time = self._manager.dm.get_attr(timer_id, "end_time")
            end_time_of_the_pre_timer = self.dm.get_attr(
                self.the_closest_timers, "end_time"
            )
            if end_time < end_time_of_the_pre_timer:
                self.new_tracking_task()
        elif event == "deleted":
            # If the deleted timer was the closest, update tracking_task
            if timer_id in self.the_closest_timers:
                self.new_tracking_task()
        elif event == "finished":
            # If a timer finished, update tracking_task
            self.new_tracking_task()

    def new_tracking_task(self) -> None:
        """Start waiting on the next timer about to finish.

        If there are no running timers, any existing waiting task is cancelled
        and the proxy simply idles until a new timer is created.
        """

        self.the_closest_timers = self._manager.timers_about_finishing()
        # When no timers are active, stop tracking.
        if not self.the_closest_timers:
            if self.task is not None:
                self.task.cancel()
                self.task = None
            return

        self.wait_timer(
            self.the_closest_timers[0],
            call_back=lambda timer_id: self.finish_timer(timer_id),
        )

    def wait_timer(self, timer_id: int, call_back: Optional[Callable] = None) -> None:

        if call_back is None:
            raise ValueError("call_back function is required")
        assert self._manager.is_timer_exists(timer_id)
        if self.task is not None:
            self.task.cancel()

        def waiting_task(
            timer_id=timer_id, end_time=self._manager.dm.get_attr(timer_id, "end_time")
        ):
            import time

            while end_time - time.time() > 0:
                time.sleep(end_time - time.time())

        self.task = self.task_pool.submit(waiting_task)
        self.task.add_done_callback(lambda f: call_back(timer_id))

    def finish_timer(self, timer_id: int) -> None:
        """Mark a timer as finished and notify callbacks."""
        if not self._manager.is_timer_exists(timer_id):
            raise ValueError("Timer with this ID does not exist.")
        end_time = self._manager.dm.get_attr(timer_id, "end_time")
        assert end_time < time.time(), "Cannot finish a timer that has not yet ended."
        self._manager.mark_timer_finished(timer_id)
        self.new_tracking_task()

        self._notify("finished", timer_id)

    def __getattr__(self, name: str) -> Any:  # pragma: no cover - simple delegation
        return getattr(self._manager, name)

    def add_callback(self, callback: Callable[[str, int], None]) -> None:
        if callback not in self._callbacks:
            self._callbacks.append(callback)

    def _notify(self, event: str, timer_id: int) -> None:
        for cb in list(self._callbacks):
            # try:
            cb(event, timer_id)
            # except Exception: 
            # pass

    def create_timer(self, name: str, duration: int) -> int:
        timer_id = self._manager.create_timer(name, duration)
        self._notify("created", timer_id)
        # self.new_tracking_task()
        return timer_id

    def rm_timer(self, timer_id: int) -> None:
        self._manager.rm_timer(timer_id)
        self._notify("deleted", timer_id)
        # self.new_tracking_task()

    def pause_timer(self, timer_id: int) -> None:
        self._manager.pause_timer(timer_id)
        self._notify("paused", timer_id)

    def resume_timer(self, timer_id: int) -> None:
        self._manager.resume_timer(timer_id)
        self._notify("resumed", timer_id)

    def mark_timer_finished(self, timer_id: int) -> None:
        self._manager.mark_timer_finished(timer_id)
        self._notify("finished", timer_id)


# class TimerWatcher:
# """Monitor timers managed by :class:`TimerManager`.

# This class runs an internal ``asyncio`` event loop in a daemon thread to
# watch for timer expirations and fire callbacks when they finish.
# """
# def __ini__(self):
