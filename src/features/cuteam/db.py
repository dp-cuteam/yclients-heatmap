from __future__ import annotations

import sqlite3
from contextlib import contextmanager

from .settings import settings


def _connect() -> sqlite3.Connection:
    db_path = settings.db_path
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


@contextmanager
def get_conn() -> sqlite3.Connection:
    conn = _connect()
    try:
        yield conn
    finally:
        conn.close()


def init_schema() -> None:
    schema_path = settings.schema_path
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")
    schema = schema_path.read_text(encoding="utf-8")
    with get_conn() as conn:
        conn.executescript(schema)
        conn.commit()
