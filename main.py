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

from data import DataManager


PAUSED = "paused"
RUNNING = "running"
FINISHED = "finished"
NOT_SET = -1.0


class TimerManager:
    """Manage simple timers stored in a SQLite database."""

    def __init__(self, database_path: str = "data.db") -> None:
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
            raise RuntimeError("The timer's start and end times do not match the duration.")
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


class TimerManagerProxy:
    """Proxy for :class:`TimerManager` that dispatches event callbacks."""

    def __init__(self, manager: TimerManager) -> None:
        self._manager = manager
        self._callbacks: List[Callable[[str, int], None]] = []

    def __getattr__(self, name: str) -> Any:  # pragma: no cover - simple delegation
        return getattr(self._manager, name)

    def add_callback(self, callback: Callable[[str, int], None]) -> None:
        if callback not in self._callbacks:
            self._callbacks.append(callback)

    def _notify(self, event: str, timer_id: int) -> None:
        for cb in list(self._callbacks):
            try:
                cb(event, timer_id)
            except Exception:
                pass

    def create_timer(self, name: str, duration: int) -> int:
        timer_id = self._manager.create_timer(name, duration)
        self._notify("created", timer_id)
        return timer_id

    def rm_timer(self, timer_id: int) -> None:
        self._manager.rm_timer(timer_id)
        self._notify("deleted", timer_id)

    def pause_timer(self, timer_id: int) -> None:
        self._manager.pause_timer(timer_id)
        self._notify("paused", timer_id)

    def resume_timer(self, timer_id: int) -> None:
        self._manager.resume_timer(timer_id)
        self._notify("resumed", timer_id)

    def mark_timer_finished(self, timer_id: int) -> None:
        self._manager.mark_timer_finished(timer_id)
        self._notify("finished", timer_id)



# ---------------------------------------------------------------------------
# TimerWatcher


# class TimerWatcher:
#     """Monitor timers managed by :class:`TimerManager`.

#     The watcher keeps track of all running timers and waits for them to expire
#     in a background ``asyncio`` event loop.  When a timer finishes it is marked
#     as ``finished`` in the database and a user supplied callback is invoked.

#     Parameters
#     ----------
#     manager:
#         The :class:`TimerManagerProxy` instance to observe.
#     on_finished:
#         Callback invoked with ``timer_id`` when a timer reaches its end.  If not
#         provided a simple printer function is used.
#     """

#     def __init__(
#         self,
#         manager: TimerManagerProxy,
#         on_finished: Optional[Callable[[int], None]] = None,
#     ) -> None:
#         self._proxy = manager
#         self._manager = manager._manager
#         self._on_finished = on_finished or (lambda tid: print(f"Timer {tid} finished"))

#         # Dedicated asyncio loop in a daemon thread so synchronous code can
#         # continue executing while timers are monitored.  Because SQLite
#         # connections are not thread-safe, the watcher uses its own
#         # :class:`DataManager` instance backed by the same database file.
#         self._loop = asyncio.new_event_loop()
#         self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
#         self._thread.start()

#         # DataManager for use inside the watcher thread.  It must be constructed
#         # in that thread because SQLite connections are not thread-safe.  A
#         # blocking ``Event`` is used so that ``__init__`` waits until the
#         # connection is ready before proceeding.
#         self._worker_dm: Optional[DataManager] = None
#         ready = threading.Event()

#         async def _init_dm() -> None:
#             self._worker_dm = DataManager(
#                 {
#                     "duration": float,
#                     "start_time": float,
#                     "end_time": float,
#                     "status": str,
#                     "name": str,
#                 },
#                 database_path=self._manager.dm._db_path,  # type: ignore[attr-defined]
#             )
#             ready.set()

#         asyncio.run_coroutine_threadsafe(_init_dm(), self._loop)
#         ready.wait()

#         # Map of timer_id -> Future returned by ``run_coroutine_threadsafe``.
#         self._tasks: Dict[int, asyncio.Future] = {}

#         # React to timer events via the proxy
#         self._proxy.add_callback(self._handle_event)

#         # Start watching currently running timers
#         for tid in self._manager.dm.find_item({"status": RUNNING}):
#             self._schedule_watch(int(tid))

#     # ------------------------------------------------------------------
#     # Event handling
#     def _handle_event(self, event: str, timer_id: int) -> None:
#         if event in {"created", "resumed"}:
#             self._schedule_watch(timer_id)
#         elif event in {"paused", "deleted", "finished"}:
#             self._cancel_watch(timer_id)

#     # ------------------------------------------------------------------
#     def _schedule_watch(self, timer_id: int) -> None:
#         if timer_id in self._tasks:
#             return
#         coro = self._wait_for_timer(timer_id)
#         future = asyncio.run_coroutine_threadsafe(coro, self._loop)
#         self._tasks[timer_id] = future

#     def _cancel_watch(self, timer_id: int) -> None:
#         fut = self._tasks.pop(timer_id, None)
#         if fut is not None:
#             fut.cancel()

#     async def _wait_for_timer(self, timer_id: int) -> None:
#         while True:
#             # Retrieve remaining time for the timer
#             try:
#                 status = self._worker_dm.get_attr(timer_id, "status")
#                 end_time = self._worker_dm.get_attr(timer_id, "end_time")
#             except ValueError:
#                 # Timer no longer exists
#                 return
#             if status != RUNNING:
#                 return

#             now = time.time()
#             delay = max(0.0, end_time - now)
#             try:
#                 await asyncio.sleep(delay)
#             except asyncio.CancelledError:
#                 return

#             # After waiting re-check that the timer is still valid and running
#             try:
#                 status = self._worker_dm.get_attr(timer_id, "status")
#                 end_time = self._worker_dm.get_attr(timer_id, "end_time")
#             except ValueError:
#                 return
#             if status != RUNNING:
#                 return

#             if time.time() >= end_time:
#                 self._worker_dm.set_attr(timer_id, "status", FINISHED)
#                 # Notify via the proxy so external callbacks fire
#                 self._proxy._notify("finished", timer_id)
#                 self._on_finished(timer_id)
#                 return
#             # Timer has been extended; loop and wait again for remaining time

#     # ------------------------------------------------------------------
#     def stop(self) -> None:
#         """Cancel all tasks and stop the background event loop."""
#         for tid in list(self._tasks):
#             self._cancel_watch(tid)
#         self._loop.call_soon_threadsafe(self._loop.stop)
#         self._thread.join()
#         try:
#             self._loop.close()
#         finally:
#             try:
#                 if self._worker_dm is not None:
#                     self._worker_dm._conn.close()  # type: ignore[attr-defined]
#             except Exception:
#                 pass

