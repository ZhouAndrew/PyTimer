"""Simple timer utilities with multithreaded waiting.

This module provides two classes:

``TimeManager``
    Lightweight container that tracks timers defined by a duration and a
    callback.  The API allows timers to be added or updated.  It is
    intentionally small and exists purely so tests can interact with a
    predictable interface.

``TimerWatcher``
    Consumes a ``TimeManager`` instance and spins up one thread per timer
    to wait for its expiration.  If a timer's duration is modified through
    the ``TimeManager`` the watcher can be notified to restart the
    corresponding thread so the new timeout takes effect.

The implementation is deliberately minimal; it merely satisfies the unit
tests for this kata and is not intended to be a full featured scheduling
library.
"""

from __future__ import annotations

import threading
from typing import Callable, Dict, Tuple


class TimeManager:
    """Hold timer definitions.

    Timers are identified by an integer id and are defined by a duration in
    seconds and a callback that is executed once the timer expires.
    """

    def __init__(self) -> None:
        self._timers: Dict[int, Tuple[float, Callable[[], None]]] = {}
        self._lock = threading.Lock()
        self._counter = 0

    # ------------------------------------------------------------------
    def add_timer(self, duration: float, callback: Callable[[], None]) -> int:
        """Register a new timer and return its id."""

        if duration <= 0:
            raise ValueError("duration must be positive")

        with self._lock:
            timer_id = self._counter
            self._counter += 1
            self._timers[timer_id] = (duration, callback)
        return timer_id

    def update_timer(self, timer_id: int, duration: float) -> None:
        """Update the duration of an existing timer."""

        if duration <= 0:
            raise ValueError("duration must be positive")
        with self._lock:
            if timer_id not in self._timers:
                raise KeyError(f"unknown timer {timer_id}")
            _, callback = self._timers[timer_id]
            self._timers[timer_id] = (duration, callback)

    def get_timer(self, timer_id: int) -> Tuple[float, Callable[[], None]]:
        """Return the ``(duration, callback)`` tuple for ``timer_id``."""

        with self._lock:
            return self._timers[timer_id]

    def ids(self) -> Tuple[int, ...]:
        """Return the ids of all registered timers."""

        with self._lock:
            return tuple(self._timers.keys())


class TimerWatcher:
    """Wait for timers using individual threads.

    Parameters
    ----------
    manager:
        Instance of :class:`TimeManager` providing timer definitions.
    """

    def __init__(self, manager: TimeManager) -> None:
        self._manager = manager
        self._events: Dict[int, threading.Event] = {}
        self._threads: Dict[int, threading.Thread] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    def _run(self, timer_id: int) -> None:
        """Worker executed in a dedicated thread for ``timer_id``."""

        while True:
            duration, callback = self._manager.get_timer(timer_id)
            event = self._events[timer_id]
            # Wait for the duration or until the event is set indicating the
            # timer was updated.  ``wait`` returns ``True`` if the event was
            # set, ``False`` if the timeout elapsed.
            if event.wait(duration):
                event.clear()
                continue
            callback()
            break

    def watch_all(self) -> None:
        """Start a watcher thread for every timer in the manager."""

        for timer_id in self._manager.ids():
            if timer_id in self._threads:
                continue
            event = threading.Event()
            thread = threading.Thread(target=self._run, args=(timer_id,), daemon=True)
            self._events[timer_id] = event
            self._threads[timer_id] = thread
            thread.start()

    def notify_update(self, timer_id: int) -> None:
        """Restart the thread for ``timer_id`` after its duration changed."""

        with self._lock:
            event = self._events.get(timer_id)
            if event is not None:
                event.set()

    def join(self) -> None:
        """Block until all watcher threads exit."""

        for thread in self._threads.values():
            thread.join()
