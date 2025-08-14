

import asyncio
import threading
import time
from typing import Any, Callable, Dict, List, Optional

from timer_manager import *
from data import DataManager
# if __name__=="__main__":
#     from timer_manager import *
# class TimerManagerProxy:
#     """Proxy for :class:`TimerManager` that dispatches event callbacks."""

#     def __init__(self, manager: TimerManager) -> None:
#         self._manager = manager
#         self._callbacks: List[Callable[[str, int], None]] = []

#     def __getattr__(self, name: str) -> Any:  # pragma: no cover - simple delegation
#         return getattr(self._manager, name)

#     def add_callback(self, callback: Callable[[str, int], None]) -> None:
#         if callback not in self._callbacks:
#             self._callbacks.append(callback)

#     def _notify(self, event: str, timer_id: int) -> None:
#         for cb in list(self._callbacks):
#             try:
#                 cb(event, timer_id)
#             except Exception:
#                 pass

#     def create_timer(self, name: str, duration: int) -> int:
#         timer_id = self._manager.create_timer(name, duration)
#         self._notify("created", timer_id)
#         return timer_id

#     def rm_timer(self, timer_id: int) -> None:
#         self._manager.rm_timer(timer_id)
#         self._notify("deleted", timer_id)

#     def pause_timer(self, timer_id: int) -> None:
#         self._manager.pause_timer(timer_id)
#         self._notify("paused", timer_id)

#     def resume_timer(self, timer_id: int) -> None:
#         self._manager.resume_timer(timer_id)
#         self._notify("resumed", timer_id)

#     def mark_timer_finished(self, timer_id: int) -> None:
#         self._manager.mark_timer_finished(timer_id)
#         self._notify("finished", timer_id)


# ---------------------------------------------------------------------------
# TimerWatcher


class TimerWatcher:
    """Monitor timers managed by :class:`TimerManager`.

    The watcher keeps track of all running timers and waits for them to expire
    in a background ``asyncio`` event loop.  When a timer finishes it is marked
    as ``finished`` in the database and a user supplied callback is invoked.

    Parameters
    ----------
    manager:
        The :class:`TimerManagerProxy` instance to observe.
    on_finished:
        Callback invoked with ``timer_id`` when a timer reaches its end.  If not
        provided a simple printer function is used.
    """

    def __init__(
        self,
        manager: TimerManager,
        on_finished: Optional[Callable[[int], None]] = None,
    ) -> None:
        self._proxy = manager
        self._manager = manager._manager
        self._on_finished = on_finished or (lambda tid: print(f"Timer {tid} finished"))

        # Dedicated asyncio loop in a daemon thread so synchronous code can
        # continue executing while timers are monitored.  Because SQLite
        # connections are not thread-safe, the watcher uses its own
        # :class:`DataManager` instance backed by the same database file.
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
        self._thread.start()

        # DataManager for use inside the watcher thread.  It must be constructed
        # in that thread because SQLite connections are not thread-safe.  A
        # blocking ``Event`` is used so that ``__init__`` waits until the
        # connection is ready before proceeding.
        self._worker_dm: Optional[DataManager] = None
        ready = threading.Event()

        async def _init_dm() -> None:
            self._worker_dm = DataManager(
                {
                    "duration": float,
                    "start_time": float,
                    "end_time": float,
                    "status": str,
                    "name": str,
                },
                database_path=self._manager.dm._db_path,  # type: ignore[attr-defined]
            )
            ready.set()

        asyncio.run_coroutine_threadsafe(_init_dm(), self._loop)
        ready.wait()

        # Map of timer_id -> Future returned by ``run_coroutine_threadsafe``.
        self._tasks: Dict[int, asyncio.Future] = {}

        # React to timer events via the proxy
        self._proxy.add_callback(self._handle_event)

        # Start watching currently running timers
        for tid in self._manager.dm.find_item({"status": RUNNING}):
            self._schedule_watch(int(tid))

    # ------------------------------------------------------------------
    # Event handling
    def _handle_event(self, event: str, timer_id: int) -> None:
        if event in {"created", "resumed"}:
            self._schedule_watch(timer_id)
        elif event in {"paused", "deleted", "finished"}:
            self._cancel_watch(timer_id)

    # ------------------------------------------------------------------
    def _schedule_watch(self, timer_id: int) -> None:
        if timer_id in self._tasks:
            return
        coro = self._wait_for_timer(timer_id)
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        self._tasks[timer_id] = future

    def _cancel_watch(self, timer_id: int) -> None:
        fut = self._tasks.pop(timer_id, None)
        if fut is not None:
            fut.cancel()

    async def _wait_for_timer(self, timer_id: int) -> None:
        while True:
            # Retrieve remaining time for the timer
            try:
                status = self._worker_dm.get_attr(timer_id, "status")
                end_time = self._worker_dm.get_attr(timer_id, "end_time")
            except ValueError:
                # Timer no longer exists
                return
            if status != RUNNING:
                return

            now = time.time()
            delay = max(0.0, end_time - now)
            try:
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                return

            # After waiting re-check that the timer is still valid and running
            try:
                status = self._worker_dm.get_attr(timer_id, "status")
                end_time = self._worker_dm.get_attr(timer_id, "end_time")
            except ValueError:
                return
            if status != RUNNING:
                return

            if time.time() >= end_time:
                self._worker_dm.set_attr(timer_id, "status", FINISHED)
                # Notify via the proxy so external callbacks fire
                self._proxy._notify("finished", timer_id)
                self._on_finished(timer_id)
                return
            # Timer has been extended; loop and wait again for remaining time

    # ------------------------------------------------------------------
    def stop(self) -> None:
        """Cancel all tasks and stop the background event loop."""
        for tid in list(self._tasks):
            self._cancel_watch(tid)
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join()
        try:
            self._loop.close()
        finally:
            try:
                if self._worker_dm is not None:
                    self._worker_dm._conn.close()  # type: ignore[attr-defined]
            except Exception:
                pass

