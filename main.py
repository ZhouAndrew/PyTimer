"""Timer management utilities backed by :class:`DataManager`."""

from typing import Any, Dict
import time

from data import DataManager


PAUSED = "paused"
RUNNING = "running"
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
        self.watching_tasks = self.dm.find_item({"status": RUNNING})

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
        return self.dm.add_item(
            {
                "name": name,
                "duration": float_duration,
                "start_time": this_moment,
                "end_time": this_moment + float_duration,
                "status": RUNNING,
            }
        )

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

