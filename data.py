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
import os
import sqlite3
import time
from typing import Any, Dict, Iterable, Optional, Tuple

# Optional on non-POSIX systems; guarded in code
try:
    import fcntl  # type: ignore
except Exception:  # pragma: no cover
    fcntl = None  # type: ignore

from urllib.parse import quote as urlquote


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

    def __init__(self, column_type_dict: Dict[str, type], database_path: str = "data.db") -> None:
        self._column_types = dict(column_type_dict)

        # Normalize path & ensure dir exists
        path = database_path
        if (
            isinstance(path, str)
            and path != ":memory:"
            and not (path.startswith("file:") and "mode=memory" in path)
        ):
            path = os.path.abspath(path)
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self._db_path = path

        # Make sure SQLite temp files go to the same directory (when writable)
        # (safe no-op if already set by the env)
        try:
            if isinstance(self._db_path, str) and self._db_path != ":memory:":
                os.environ.setdefault("SQLITE_TMPDIR", os.path.dirname(self._db_path) or ".")
        except Exception:
            pass

        # Open connection; use URI to guarantee create (mode=rwc)
        if isinstance(self._db_path, str) and self._db_path != ":memory:" and not self._db_path.startswith("file:"):
            uri = "file:" + urlquote(self._db_path) + "?mode=rwc"
            self._conn = sqlite3.connect(uri, uri=True, timeout=10.0)
        else:
            self._conn = sqlite3.connect(self._db_path, uri=str(self._db_path).startswith("file:"), timeout=10.0)
        self._cursor = self._conn.cursor()

        self._auto_init()

    # ------------------------------------------------------------------
    # One-time initialization (idempotent & safe under concurrency)
    def _auto_init(self) -> None:
        # Always set a generous busy timeout to let SQLite wait for locks
        try:
            self._cursor.execute("PRAGMA busy_timeout=10000")
        except sqlite3.OperationalError:
            pass

        # Build CREATE TABLE definition from column definitions
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

        # Skip external locking for in-memory connections
        db_is_memory = (
            self._db_path == ":memory:" or (
                isinstance(self._db_path, str) and self._db_path.startswith("file:") and "mode=memory" in self._db_path
            )
        )
        if db_is_memory:
            self._execute_write_with_retry(
                f"CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} ({', '.join(column_defs)})"
            )
            return

        # Cross-process init lock to serialize first-time initialization
        lockfd = None
        if fcntl and isinstance(self._db_path, str):
            lock_path = self._db_path + ".initlock"
            lockfd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
            fcntl.flock(lockfd, fcntl.LOCK_EX)

        try:
            # Pick conservative journal & sync that work on finicky FS
            # (DELETE+FULL is slower than WAL+NORMAL but very robust.)
            try:
                self._cursor.execute("PRAGMA journal_mode=DELETE")
            except sqlite3.OperationalError:
                pass
            try:
                self._cursor.execute("PRAGMA synchronous=FULL")
            except sqlite3.OperationalError:
                pass

            # Create main & meta tables (idempotent)
            self._execute_write_with_retry(
                f"CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} ({', '.join(column_defs)})"
            )
            self._execute_write_with_retry(
                """
                CREATE TABLE IF NOT EXISTS dm_meta (
                    id INTEGER PRIMARY KEY CHECK (id=1),
                    wal_enabled INTEGER NOT NULL DEFAULT 0,
                    initialized_at REAL
                )
                """
            )
            self._execute_write_with_retry(
                "INSERT OR IGNORE INTO dm_meta (id, wal_enabled) VALUES (1, 0)"
            )

            if "status" in self._column_types and "end_time" in self._column_types:
                # Index create may still conflict under concurrency -> retry
                self._execute_write_with_retry(
                    f"CREATE INDEX IF NOT EXISTS idx_{self.TABLE_NAME}_status_end "
                    f"ON {self.TABLE_NAME}(status, end_time)"
                )

            # meta update is best-effort; keep for compatibility
            try:
                self._cursor.execute(
                    "UPDATE dm_meta SET initialized_at=strftime('%s','now') WHERE id=1"
                )
                self._conn.commit()
            except sqlite3.OperationalError:
                pass
        finally:
            if lockfd is not None and fcntl:
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

    def _execute_write_with_retry(self, sql: str, params: tuple = (), max_tries: int = 80):
        """Execute a write statement with retry on transient locks.

        Retries on 'database is locked', 'database is busy', and transient
        disk I/O errors which some filesystems surface under high contention.
        """
        delay = 0.05
        for _ in range(max_tries):
            try:
                cur = self._cursor.execute(sql, params)
                self._conn.commit()
                return cur
            except sqlite3.OperationalError as e:
                msg = str(e).lower()
                if (
                    "database is locked" in msg
                    or "database is busy" in msg
                    or "disk i/o error" in msg
                ):
                    time.sleep(delay)
                    delay = delay * 1.6 if delay < 0.8 else 0.8
                    continue
                raise
        # last try (propagate if still failing)
        cur = self._cursor.execute(sql, params)
        self._conn.commit()
        return cur

    def _execute_read_with_retry(self, sql: str, params: tuple = (), max_tries: int = 80):
        delay = 0.02
        for _ in range(max_tries):
            try:
                return self._cursor.execute(sql, params)
            except sqlite3.OperationalError as e:
                msg = str(e).lower()
                if "locked" in msg or "busy" in msg:
                    time.sleep(delay)
                    delay = delay * 1.6 if delay < 0.6 else 0.6
                    continue
                raise
        return self._cursor.execute(sql, params)

    # ------------------------------------------------------------------
    # Public API
    def get_attr(self, id: int, attr: str) -> Any:
        """Return the value of ``attr`` for a given item ``id``."""
        self._validate_attr(attr)
        cur = self._execute_read_with_retry(
            f"SELECT {attr} FROM {self.TABLE_NAME} WHERE id = ?", (id,)
        )
        row = cur.fetchone()
        if row is None:
            raise ValueError(f"No item with id {id}")
        return self._decode_value(attr, row[0])

    def set_attr(self, id: int, attr: str, value: Any) -> None:
        """Update ``attr`` for the item ``id`` with ``value``."""
        encoded = self._encode_value(attr, value)
        cur = self._execute_write_with_retry(
            f"UPDATE {self.TABLE_NAME} SET {attr} = ? WHERE id = ?",
            (encoded, id),
        )
        if cur.rowcount == 0:
            raise ValueError(f"No item with id {id}")

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

        cur = self._execute_write_with_retry(
            f"INSERT INTO {self.TABLE_NAME} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})",
            tuple(values),
        )
        return int(cur.lastrowid)  # pyright: ignore[reportArgumentType]

    def rm_item(self, id: int) -> None:
        """Remove item ``id`` from the database."""
        cur = self._execute_write_with_retry(
            f"DELETE FROM {self.TABLE_NAME} WHERE id = ?", (id,)
        )
        if cur.rowcount == 0:
            raise ValueError(f"No item with id {id}")

    def find_item(self, required_attitude: Optional[Dict[str, Any]] = None) -> Tuple[int, ...]:
        """Return a tuple of item ids, optionally filtered by attribute equality.

        Parameters
        ----------
        required_attitude : dict | None
            Mapping of column names to required values. If None or empty,
            all item ids are returned.
        """
        if required_attitude is None or len(required_attitude) == 0:
            cur = self._execute_read_with_retry(f"SELECT id FROM {self.TABLE_NAME}")
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
        cur = self._execute_read_with_retry(
            f"SELECT id FROM {self.TABLE_NAME} WHERE {where}", tuple(values)
        )
        return tuple(row[0] for row in cur.fetchall())


# The tests expect a ``DataManager`` class; expose one as an alias.
DataManager = DataManagerInterface
