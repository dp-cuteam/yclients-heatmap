from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path

from .config import settings


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


@contextmanager
def get_conn() -> sqlite3.Connection:
    conn = _connect(settings.db_path)
    try:
        yield conn
    finally:
        conn.close()


def init_db() -> None:
    with get_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_records (
                branch_id INTEGER NOT NULL,
                staff_id INTEGER NOT NULL,
                record_id INTEGER NOT NULL,
                start_dt TEXT NOT NULL,
                end_dt TEXT NOT NULL,
                attendance INTEGER NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (branch_id, record_id)
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS staff_hour_busy (
                branch_id INTEGER NOT NULL,
                staff_id INTEGER NOT NULL,
                date TEXT NOT NULL,
                hour INTEGER NOT NULL,
                busy_flag INTEGER NOT NULL,
                in_benchmark INTEGER NOT NULL,
                in_gray INTEGER NOT NULL,
                PRIMARY KEY (branch_id, staff_id, date, hour)
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS group_hour_load (
                branch_id INTEGER NOT NULL,
                group_id TEXT NOT NULL,
                date TEXT NOT NULL,
                dow INTEGER NOT NULL,
                hour INTEGER NOT NULL,
                busy_count INTEGER NOT NULL,
                staff_total INTEGER NOT NULL,
                load_pct REAL NOT NULL,
                in_benchmark INTEGER NOT NULL,
                PRIMARY KEY (branch_id, group_id, date, hour)
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS etl_runs (
                run_id TEXT PRIMARY KEY,
                run_type TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT NOT NULL,
                progress TEXT,
                error_log TEXT
            );
            """
        )
        conn.commit()
