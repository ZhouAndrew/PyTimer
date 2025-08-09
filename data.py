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

    def __init__(self, column_type_dict: Dict[str, type], database_path: str = "data.db", allow_cross_thread: bool = False) -> None:
        self._column_types = dict(column_type_dict)
        # 允许跨线程使用同一个连接（默认 False）
        self._conn = sqlite3.connect(
            database_path,
            check_same_thread=not allow_cross_thread,  # 如果 allow_cross_thread=True，则 check_same_thread=False
            isolation_level=None  # 手动控制事务，避免自动提交
        )
        self._cursor = self._conn.cursor()

        # 开启 WAL 模式 & 设置同步等级 & 超时
        self._cursor.execute("PRAGMA journal_mode=WAL")
        self._cursor.execute("PRAGMA synchronous=NORMAL")
        self._cursor.execute("PRAGMA busy_timeout=3000")  # 等待锁3秒
        self._conn.commit()

        # 建表
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
        self._cursor.execute(
            f"CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} ({', '.join(column_defs)})"
        )
        self._conn.commit()


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

