import time
from typing import Any, Dict, List, Optional
import sqlite3
PAUSED="paused"

class DataManager:
    def __init__(
        self, base_path: str = "data.db", allowed_attrs: Optional[set] = None
    ) -> None:
        self.base_path = base_path
        # disable auto fill the allowed_attrs        
        if allowed_attrs is None:
            raise ValueError("allowed_attrs must be provided.")
        if not isinstance(allowed_attrs, set):
            raise TypeError("allowed_attrs must be a set.")
        if not allowed_attrs:
            raise ValueError("allowed_attrs cannot be an empty set.")
        if not all(isinstance(attr, str) for attr in allowed_attrs):
            raise TypeError("All attributes in allowed_attrs must be strings.")
        if "id" in allowed_attrs:
            raise ValueError("Attribute 'id' is reserved and cannot be in allowed_attrs.")
        self.allowed_attrs = allowed_attrs or {
            "name",
            "duration",
            "start_time",
            "status",
        }
        self.conn = sqlite3.connect(self.base_path)
        self.cursor = self.conn.cursor()
        self._ensure_table()

    def _ensure_table(self) -> None:
        # Create columns dynamically based on allowed_attrs
        columns = ", ".join(
            f"{attr} TEXT" if attr == "name" or attr == "status" else f"{attr} INTEGER"
            for attr in self.allowed_attrs
        )
        self.cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS timers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                {columns}
            )
        """
        )
        self.conn.commit()

    def get_timer_attr(self, timer_id: int, attr: str) -> Optional[Any]:
        if attr not in self.allowed_attrs.union({"id"}):
            raise ValueError(f"Invalid attribute: {attr}")
        self.cursor.execute(f"SELECT {attr} FROM timers WHERE id = ?", (timer_id,))
        row = self.cursor.fetchone()
        return row[0] if row else None

    def set_timer_attr(self, timer_id: int, attr: str, value: Any) -> None:
        if attr not in self.allowed_attrs:
            raise ValueError(f"Invalid attribute: {attr}")
        self.cursor.execute(
            f"UPDATE timers SET {attr} = ? WHERE id = ?", (value, timer_id)
        )
        self.conn.commit()

    def create_timer(self, attr: Dict[str, Any]) -> int:
        keys = [k for k in attr if k in self.allowed_attrs]
        values = [attr[k] for k in keys]
        placeholders = ", ".join("?" for _ in keys)
        query = f"INSERT INTO timers ({', '.join(keys)}) VALUES ({placeholders})"
        self.cursor.execute(query, values)
        self.conn.commit()
        if self.cursor.lastrowid is None:
            raise RuntimeError("Failed to create timer, lastrowid is None.") 
        return self.cursor.lastrowid

    def find_timer(self, attr: Dict[str, Any]) -> List[int]:
        conditions = [f"{k} = ?" for k in attr if k in self.allowed_attrs]
        values = [v for k, v in attr.items() if k in self.allowed_attrs]
        query = f"SELECT id FROM timers WHERE {' AND '.join(conditions)}"
        self.cursor.execute(query, values)
        return [int(row[0]) for row in self.cursor.fetchall() if row[0] is not None]

    def get_timer(self, timer_id: int) -> Optional[Dict[str, Any]]:
        query = f"SELECT id, {', '.join(self.allowed_attrs)} FROM timers WHERE id = ?"
        self.cursor.execute(query, (timer_id,))
        row = self.cursor.fetchone()
        if row:
            return dict(zip(["id"] + list(self.allowed_attrs), row))
        return None

    def rm_timer(self, timer_id: int) -> None:
        self.cursor.execute("DELETE FROM timers WHERE id = ?", (timer_id,))
        self.conn.commit()

    def __del__(self):
        self.conn.close()


class TimerManager:
    
    def __init__(self) -> None:
        self.dm = DataManager()        
        self.active_timers = self.dm.find_timer({"status": "running"})
        
    def create_timer(self, name: str, duration: int) -> int:
        if not isinstance(name, str) or not isinstance(duration, int):
            raise ValueError("Name must be a string and duration must be an integer.")
        if duration <= 0:
            raise ValueError("Duration must be a positive integer.")
        if not name:
            raise ValueError("Name cannot be empty.")
        if len(name) > 100:
            raise ValueError("Name cannot exceed 100 characters.")
        return self.dm.create_timer(
            {
                "name": name,
                "start_time": time.time(),
                "status": "running",
                "duration": duration,
            }
        )
    def pause_timer(self, timer_id: int) -> None:
        if self.dm.get_timer_attr(timer_id, "status") != "running":
            raise ValueError("Timer is not running.")
        self.dm.set_timer_attr(timer_id, "status", "paused")
        self.dm.set_timer_attr(timer_id=timer_id, attr="start_time",value=PAUSED)
        self.active_timers.remove(timer_id)
    def resume_timer(self, timer_id: int) -> None:
        if self.dm.get_timer_attr(timer_id, "status") != PAUSED:
            raise ValueError("Timer is not paused.")
        self.dm.set_timer_attr(timer_id, "status", "running")
        self.active_timers.append(timer_id)
