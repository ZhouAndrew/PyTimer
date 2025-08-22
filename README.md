# PyTimer

Small utilities for experimenting with timers backed by SQLite.

## DataManager

``DataManager`` provides a thin wrapper around a SQLite table for storing
arbitrary records.  The table is called ``items`` by default, but a custom
table name can be supplied via the ``table_name`` argument when creating an
instance.

Records are defined by a mapping of column names to Python types.  Supported
types are ``str``, ``int``, ``float``, ``bool`` (stored as ``0``/``1``) and
``list``/``dict`` (serialised to JSON).

Aside from the basic CRUD helpers, ``DataManager`` also exposes
``top_n_by_attr`` which returns the IDs of the first ``n`` rows ordered by a
given attribute.  Textual types are ordered by the length of the stored value
and ties are broken by row ID to keep the results deterministic.  Optional
``required_attributes`` allow filtering to rows that exactly match other
attribute/value pairs.

## Timer utilities

``TimerManager`` stores simple countdown timers using ``DataManager``.  A
``TimerManagerProxy`` adds callback support and ``TimerWatcher`` can observe a
``TimerManagerProxy`` instance in a background thread, firing callbacks when
timers expire.
