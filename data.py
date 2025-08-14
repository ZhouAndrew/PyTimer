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
    table_name:
        Name of the table to operate on. Defaults to ``"items"``.
    """

    TABLE_NAME = "items"

    def __init__(
        self,
        column_type_dict: Dict[str, type],
        database_path: str = "data.db",
        table_name: str = TABLE_NAME,
    ) -> None:
        self._column_types = dict(column_type_dict)
        self._table_name = table_name
        # Maintain ``TABLE_NAME`` as an instance attribute for backward
        # compatibility with code that accessed it directly.
        self.TABLE_NAME = self._table_name

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
                f"CREATE TABLE IF NOT EXISTS {self._table_name} ({', '.join(column_defs)})"
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
                f"CREATE TABLE IF NOT EXISTS {self._table_name} ({', '.join(column_defs)})"
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
                    f"CREATE INDEX IF NOT EXISTS idx_{self._table_name}_status_end "
                    f"ON {self._table_name}(status, end_time)"
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
            f"SELECT {attr} FROM {self._table_name} WHERE id = ?", (id,)
        )
        row = cur.fetchone()
        if row is None:
            raise ValueError(f"No item with id {id}")
        return self._decode_value(attr, row[0])

    def set_attr(self, id: int, attr: str, value: Any) -> None:
        """Update ``attr`` for the item ``id`` with ``value``."""
        encoded = self._encode_value(attr, value)
        cur = self._execute_write_with_retry(
            f"UPDATE {self._table_name} SET {attr} = ? WHERE id = ?",
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
            f"INSERT INTO {self._table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})",
            tuple(values),
        )
        return int(cur.lastrowid)  # pyright: ignore[reportArgumentType]

    def rm_item(self, id: int) -> None:
        """Remove item ``id`` from the database."""
        cur = self._execute_write_with_retry(
            f"DELETE FROM {self._table_name} WHERE id = ?", (id,)
        )
        if cur.rowcount == 0:
            raise ValueError(f"No item with id {id}")

    def find_item(
        self,
        required_attributes: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Tuple[int, ...]:
        """Return a tuple of item ids filtered by attribute equality.

        Parameters
        ----------
        required_attributes:
            Mapping of column names to required values. If ``None`` or empty, all
            item ids are returned.

        Notes
        -----
        ``required_attitude`` is accepted as a deprecated keyword for
        backwards compatibility.
        """

        if required_attributes is None:
            required_attributes = kwargs.pop("required_attitude", None)
        if kwargs:
            unexpected = ", ".join(sorted(kwargs))
            raise TypeError(f"Unexpected keyword argument(s): {unexpected}")

        if required_attributes is None or len(required_attributes) == 0:
            cur = self._execute_read_with_retry(f"SELECT id FROM {self.TABLE_NAME}")

            return tuple(row[0] for row in cur.fetchall())

        if not isinstance(required_attributes, dict):
            raise TypeError("required_attributes must be a dict or None")

        clauses = []
        values: list[Any] = []
        for attr, value in required_attributes.items():
            # validate & encode so types/json/bool match stored representation
            self._validate_attr(attr)
            clauses.append(f"{attr} = ?")
            values.append(self._encode_value(attr, value))

        where = " AND ".join(clauses) if clauses else "1"
        cur = self._execute_read_with_retry(
            f"SELECT id FROM {self._table_name} WHERE {where}", tuple(values)
        )
        return tuple(row[0] for row in cur.fetchall())
    # from typing import Any, Dict, Optional, Tuple

    def top_n_by_attr(
        self,
        attr: str,
        n: int,
        largest: bool = True,
        required_attributes: Optional[Dict[str, Any]] = None,
    ) -> Tuple[int, ...]:
        """
        Return IDs of the first `n` items sorted by the given attribute, optionally
        filtered by exact matches on other attributes.

        Parameters
        ----------
        attr : str
            The column name to sort by. Must be int, float, or length of str/list/dict.
        n : int
            Number of results to return.
        largest : bool, default=True
            If True, return items with the largest values first.
            If False, return items with the smallest values first.
        required_attributes : dict[str, Any] | None, default=None
            If provided, only rows where every (key == value) pair matches will be considered.
            Values are encoded via the manager's storage rules (e.g., bool -> 0/1,
            list/dict -> JSON string), same as `find_item`.
        """
        # Validate the sort attribute and decide the ORDER BY
        expected = self._validate_attr(attr)

        if expected in (int, float):
            order_clause = f"{attr} DESC" if largest else f"{attr} ASC"
        elif expected in (str, list, dict):
            # For TEXT/list/dict (stored as JSON TEXT), sort by LENGTH
            order_clause = f"LENGTH({attr}) DESC" if largest else f"LENGTH({attr}) ASC"
        else:
            raise TypeError(
                f"Unsupported type for sorting: {expected.__name__}. "
                "Must be int, float, str, list, or dict."
            )

        # Build WHERE from required_attributes, encoding values as stored in DB
        where_parts = []
        params: list[Any] = []
        if required_attributes:
            for key, value in required_attributes.items():
                self._validate_attr(key)  # ensure column exists & type is supported
                enc_value = self._encode_value(key, value)
                where_parts.append(f"{key} = ?")
                params.append(enc_value)

        where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

        sql = f"SELECT id FROM {self._table_name} {where_sql} ORDER BY {order_clause} LIMIT ?"
        params.append(n)

        cur = self._execute_read_with_retry(sql, tuple(params))
        return tuple(row[0] for row in cur.fetchall())


# The tests expect a ``DataManager`` class; expose one as an alias.
DataManager = DataManagerInterface
