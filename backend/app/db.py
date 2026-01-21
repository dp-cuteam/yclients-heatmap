from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable

from .config import settings

DB_URL = os.getenv("DATABASE_URL", "").strip()
USE_POSTGRES = DB_URL.startswith("postgres")

try:
    if USE_POSTGRES:
        try:
            import psycopg
            from psycopg.rows import dict_row
            PG_DRIVER = 'psycopg'
        except Exception:  # noqa: BLE001
            import psycopg2
            from psycopg2.extras import RealDictCursor
            PG_DRIVER = 'psycopg2'
    else:
        psycopg = None
        psycopg2 = None
        dict_row = None
        RealDictCursor = None
        PG_DRIVER = None
except Exception:  # noqa: BLE001
    psycopg = None
    psycopg2 = None
    dict_row = None
    RealDictCursor = None
    PG_DRIVER = None


class DBConn:
    def __init__(self, conn, kind: str):
        self._conn = conn
        self._kind = kind

    def _prepare(self, sql: str) -> str:
        if self._kind == "postgres":
            return sql.replace("?", "%s")
        return sql

    def execute(self, sql: str, params: Iterable | None = None):
        params = [] if params is None else params
        sql = self._prepare(sql)
        if self._kind == "postgres":
            if PG_DRIVER == "psycopg2":
                cur = self._conn.cursor(cursor_factory=RealDictCursor)
            else:
                cur = self._conn.cursor()
            cur.execute(sql, params)
            return cur
        return self._conn.execute(sql, params)

    def executemany(self, sql: str, seq: Iterable[Iterable]):
        sql = self._prepare(sql)
        if self._kind == "postgres":
            cur = self._conn.cursor()
            cur.executemany(sql, seq)
            return cur
        return self._conn.executemany(sql, seq)

    def commit(self):
        self._conn.commit()

    def close(self):
        self._conn.close()


def db_source_label() -> str:
    return "Postgres" if USE_POSTGRES else "SQLite"


def upsert_sql(table: str, columns: list[str], conflict_cols: list[str]) -> str:
    cols = ", ".join(columns)
    placeholders = ", ".join("?" for _ in columns)
    if not USE_POSTGRES:
        return f"INSERT OR REPLACE INTO {table} ({cols}) VALUES ({placeholders})"
    conflict = ", ".join(conflict_cols)
    update_cols = [c for c in columns if c not in conflict_cols]
    if update_cols:
        updates = ", ".join(f"{c}=EXCLUDED.{c}" for c in update_cols)
        return f"INSERT INTO {table} ({cols}) VALUES ({placeholders}) ON CONFLICT ({conflict}) DO UPDATE SET {updates}"
    return f"INSERT INTO {table} ({cols}) VALUES ({placeholders}) ON CONFLICT ({conflict}) DO NOTHING"


def _connect_sqlite(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def _connect_postgres():
    if PG_DRIVER == "psycopg" and psycopg:
        return psycopg.connect(DB_URL, row_factory=dict_row)
    if PG_DRIVER == "psycopg2" and psycopg2:
        conn = psycopg2.connect(DB_URL)
        conn.autocommit = False
        return conn
    raise RuntimeError("Postgres driver not available (psycopg/psycopg2)")


@contextmanager
def get_conn() -> DBConn:
    raw = _connect_postgres() if USE_POSTGRES else _connect_sqlite(settings.db_path)
    conn = DBConn(raw, "postgres" if USE_POSTGRES else "sqlite")
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def get_hist_conn() -> DBConn:
    raw = _connect_sqlite(settings.historical_db_path)
    conn = DBConn(raw, "sqlite")
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


def init_historical_db() -> None:
    with get_hist_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS historical_loads (
                branch_id INTEGER NOT NULL,
                month TEXT NOT NULL,
                resource_type TEXT NOT NULL,
                date TEXT NOT NULL,
                dow INTEGER NOT NULL,
                hour INTEGER NOT NULL,
                load_pct REAL NOT NULL,
                PRIMARY KEY (branch_id, resource_type, date, hour)
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS historical_types (
                branch_id INTEGER NOT NULL,
                month TEXT NOT NULL,
                resource_type TEXT NOT NULL,
                order_index INTEGER NOT NULL,
                PRIMARY KEY (branch_id, month, resource_type)
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS historical_imports (
                run_id TEXT PRIMARY KEY,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT NOT NULL,
                rows_count INTEGER,
                file_path TEXT,
                file_mtime REAL,
                error_log TEXT
            );
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_hist_month ON historical_loads(branch_id, month);"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_hist_date ON historical_loads(branch_id, date);"
        )
        conn.commit()
