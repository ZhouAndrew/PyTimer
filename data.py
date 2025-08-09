"""SQLite backed data manager.

This module exposes :class:`DataManager` which provides a minimal
interface for storing arbitrary records in a SQLite database.  The
structure of the records is defined by a mapping of column names to
Python types.  Supported value types are:

``str``
    Stored as ``TEXT``.
``int``
    Stored as ``INTEGER``.
``bool``
    Stored as ``INTEGER`` (``0`` or ``1``).
``float``
    Stored as ``REAL``.
``list`` and ``dict``
    Serialised to JSON strings and stored as ``TEXT``.

The implementation is intentionally lightweight to satisfy the needs of
the unit tests in this kata.
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any, Dict, Iterable, Optional, Tuple
import fcntl,os


class DataManagerInterface:
    """Manage a simple SQLite table.

    Parameters
    ----------
    column_type_dict:
        Mapping of column names to Python types.
    database_path:
        Path to the SQLite database file or ``":memory:"`` for an
        in-memory database.
    """

    TABLE_NAME = "items"
    # 在 DataManagerInterface 里新增：
    def __init__(self, column_type_dict, database_path: str = "data.db"):
        self._column_types = dict(column_type_dict)
        path = database_path
        if isinstance(path, str) and path != ":memory:" and not (path.startswith("file:") and "mode=memory" in path):
            path = os.path.abspath(path)
        self._db_path = path
        self._conn = sqlite3.connect(database_path)
        self._cursor = self._conn.cursor()

        # 进行所有必要的建表和初始化逻辑
        self._auto_init()

    def _auto_init(self) -> None:
        # 一律先设置超时（非事务）
        try:
            self._cursor.execute("PRAGMA busy_timeout=3000")
        except sqlite3.OperationalError:
            pass

        # 构建表结构定义
        column_defs = ["id INTEGER PRIMARY KEY AUTOINCREMENT"]
        for name, typ in self._column_types.items():
            if typ is str:
                sql_type = "TEXT"
            elif typ in (int, bool):
                sql_type = "INTEGER"
            elif typ is float:
                sql_type = "REAL"
            elif typ in (list, dict):
                sql_type = "TEXT"
            else:
                raise TypeError(f"Unsupported column type for '{name}': {typ}")
            column_defs.append(f"{name} {sql_type}")

        # 内存库跳过加锁逻辑，但仍需建表
        db_is_memory = (
            self._db_path == ":memory:" or
            (isinstance(self._db_path, str)
             and self._db_path.startswith("file:")
             and "mode=memory" in self._db_path)
        )
        if db_is_memory:
            self._cursor.execute(
                f"CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} ({', '.join(column_defs)})"
            )
            self._conn.commit()
            return

        # 用进程间文件锁串行化初始化，避免并发切 WAL/建表引发 I/O error
        lockfd = None
        if fcntl is not None and isinstance(self._db_path, str) and self._db_path not in (":memory:", ""):
            lock_path = self._db_path + ".initlock"
            os.makedirs(os.path.dirname(os.path.abspath(self._db_path)) or ".", exist_ok=True)
            lockfd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
            fcntl.flock(lockfd, fcntl.LOCK_EX)

        try:
            # PRAGMA 必须在事务外；失败就忽略并用默认模式
            wal_ok = 0
            try:
                mode = self._cursor.execute("PRAGMA journal_mode=WAL").fetchone()[0]
                wal_ok = 1 if (isinstance(mode, str) and mode.lower() == "wal") else 0
            except sqlite3.OperationalError:
                wal_ok = 0
            try:
                self._cursor.execute("PRAGMA synchronous=NORMAL")
            except sqlite3.OperationalError:
                pass
            self._conn.commit()

            # 创建主表
            self._cursor.execute(
                f"CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} ({', '.join(column_defs)})"
            )

            # 幂等的 meta 表
            self._cursor.execute(
                """CREATE TABLE IF NOT EXISTS dm_meta (
                       id INTEGER PRIMARY KEY CHECK (id=1),
                       wal_enabled INTEGER NOT NULL DEFAULT 0,
                       initialized_at REAL
                   )"""
            )
            self._cursor.execute("INSERT OR IGNORE INTO dm_meta (id, wal_enabled) VALUES (1, 0)")

            # 仅当两列都存在时才建索引
            if "status" in self._column_types and "end_time" in self._column_types:
                self._cursor.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_{self.TABLE_NAME}_status_end "
                    f"ON {self.TABLE_NAME}(status, end_time)"
                )

            self._cursor.execute(
                "UPDATE dm_meta SET wal_enabled=?, initialized_at=strftime('%s','now') WHERE id=1",
                (wal_ok,),
            )
            self._conn.commit()
        finally:
            if lockfd is not None:
                fcntl.flock(lockfd, fcntl.LOCK_UN)
                os.close(lockfd)
 
    # ------------------------------------------------------------------
    # Internal helpers
    def _validate_attr(self, attr: str) -> type:
        if attr not in self._column_types:
            raise ValueError(f"Unknown attribute '{attr}'")
        return self._column_types[attr]

    def _encode_value(self, attr: str, value: Any) -> Any:
        expected = self._validate_attr(attr)
        if not isinstance(value, expected):
            raise TypeError(
                f"Attribute '{attr}' expects value of type {expected.__name__}"
            )
        if expected is bool:
            return int(value)
        if expected in (list, dict):
            return json.dumps(value)
        return value

    def _decode_value(self, attr: str, value: Any) -> Any:
        expected = self._validate_attr(attr)
        if expected is bool:
            return bool(value)
        if expected in (list, dict):
            if value is None:
                return None
            return json.loads(value)
        return value

    # ------------------------------------------------------------------
    # Public API
    def get_attr(self, id: int, attr: str) -> Any:
        """Return the value of ``attr`` for a given item ``id``."""

        self._validate_attr(attr)
        cur = self._cursor.execute(
            f"SELECT {attr} FROM {self.TABLE_NAME} WHERE id = ?", (id,)
        )
        row = cur.fetchone()
        if row is None:
            raise ValueError(f"No item with id {id}")
        return self._decode_value(attr, row[0])

    def set_attr(self, id: int, attr: str, value: Any) -> None:
        """Update ``attr`` for the item ``id`` with ``value``."""

        encoded = self._encode_value(attr, value)
        cur = self._cursor.execute(
            f"UPDATE {self.TABLE_NAME} SET {attr} = ? WHERE id = ?", (encoded, id)
        )
        if cur.rowcount == 0:
            raise ValueError(f"No item with id {id}")
        self._conn.commit()

    def add_item(self, attr_dict: Dict[str, Any]) -> int:  # pyright: ignore[reportReturnType]
        """Insert a new item and return its ``id``."""

        if set(attr_dict) != set(self._column_types):
            missing = set(self._column_types) - set(attr_dict)
            extra = set(attr_dict) - set(self._column_types)
            problems = []
            if missing:
                problems.append(f"missing keys: {', '.join(sorted(missing))}")
            if extra:
                problems.append(f"unknown keys: {', '.join(sorted(extra))}")
            raise ValueError("Invalid attributes: " + "; ".join(problems))

        columns: Iterable[str] = []
        placeholders: Iterable[str] = []
        values: Iterable[Any] = []
        for attr in self._column_types:
            columns.append(attr)
            placeholders.append("?")
            values.append(self._encode_value(attr, attr_dict[attr]))

        cur = self._cursor.execute(
            f"INSERT INTO {self.TABLE_NAME} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})",
            tuple(values),
        )
        self._conn.commit()
        return int(cur.lastrowid) # pyright: ignore[reportArgumentType]

    def rm_item(self, id: int) -> None:
        """Remove item ``id`` from the database."""

        cur = self._cursor.execute(
            f"DELETE FROM {self.TABLE_NAME} WHERE id = ?", (id,)
        )
        if cur.rowcount == 0:
            raise ValueError(f"No item with id {id}")
        self._conn.commit()

    def find_item(self, required_attitude: Optional[Dict[str, Any]] = None) -> Tuple[int, ...]:
        """Return a tuple of item ids, optionally filtered by attribute equality.

        Parameters
        ----------
        required_attitude : dict | None
            Mapping of column names to required values. If None or empty,
            all item ids are returned.
        """
        if required_attitude is None or len(required_attitude) == 0:
            cur = self._cursor.execute(f"SELECT id FROM {self.TABLE_NAME}")
            return tuple(row[0] for row in cur.fetchall())

        if not isinstance(required_attitude, dict):
            raise TypeError("required_attitude must be a dict or None")

        clauses = []
        values: list[Any] = []
        for attr, value in required_attitude.items():
            # validate & encode so types/json/bool match stored representation
            self._validate_attr(attr)
            clauses.append(f"{attr} = ?")
            values.append(self._encode_value(attr, value))

        where = " AND ".join(clauses) if clauses else "1"
        cur = self._cursor.execute(
            f"SELECT id FROM {self.TABLE_NAME} WHERE {where}",
            tuple(values),
        )
        return tuple(row[0] for row in cur.fetchall())


# The tests expect a ``DataManager`` class; expose one as an alias.
DataManager = DataManagerInterface

