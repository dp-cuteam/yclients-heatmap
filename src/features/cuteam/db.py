from __future__ import annotations

import sqlite3
from contextlib import contextmanager

from .settings import settings

DB_URL = settings.db_url or ""
USE_POSTGRES = DB_URL.startswith("postgres")

try:
    if USE_POSTGRES:
        try:
            import psycopg
            from psycopg.rows import dict_row

            PG_DRIVER = "psycopg"
        except Exception:  # noqa: BLE001
            import psycopg2
            from psycopg2.extras import RealDictCursor

            PG_DRIVER = "psycopg2"
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

    def execute(self, sql: str, params=None):
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

    def executemany(self, sql: str, seq):
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


def is_postgres() -> bool:
    return USE_POSTGRES


def db_source_label() -> str:
    return "Postgres" if USE_POSTGRES else "SQLite"


def db_target_label() -> str:
    if USE_POSTGRES:
        return settings.db_url_env or "DATABASE_URL"
    return str(settings.db_path)


def _connect_sqlite() -> sqlite3.Connection:
    db_path = settings.db_path
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
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
    raw = _connect_postgres() if USE_POSTGRES else _connect_sqlite()
    conn = DBConn(raw, "postgres" if USE_POSTGRES else "sqlite")
    try:
        yield conn
    finally:
        conn.close()


def _split_sql_statements(sql: str) -> list[str]:
    parts = [chunk.strip() for chunk in sql.split(";")]
    return [part for part in parts if part]


def init_schema() -> None:
    schema_path = settings.schema_path
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")
    schema = schema_path.read_text(encoding="utf-8")
    if USE_POSTGRES:
        schema = schema.replace("CREATE VIEW IF NOT EXISTS", "CREATE OR REPLACE VIEW")
    statements = _split_sql_statements(schema)
    with get_conn() as conn:
        for stmt in statements:
            conn.execute(stmt)
        conn.commit()
