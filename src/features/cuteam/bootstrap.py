from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Sequence

from .db import get_conn, init_schema, is_postgres
from .settings import settings


@dataclass(frozen=True)
class SeedStats:
    branches: int
    metrics: int


def _load_json(path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"Reference file not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def seed_dimensions() -> SeedStats:
    branches = _load_json(settings.branch_mapping_path)
    metrics = _load_json(settings.metric_mapping_path)

    branch_rows = [(item["code"], item["name"]) for item in branches if item.get("code") and item.get("name")]

    metric_rows = []
    for item in metrics:
        code = item.get("metric_code")
        label = item.get("label")
        source = (item.get("source") or "").strip().lower()
        is_derived = 1 if source == "computed" else 0
        if not code or not label:
            continue
        metric_rows.append((code, label, is_derived))

    branch_sql = (
        "INSERT INTO branches (code, name) VALUES (?, ?) ON CONFLICT(code) DO NOTHING"
        if is_postgres()
        else "INSERT OR IGNORE INTO branches (code, name) VALUES (?, ?)"
    )
    metric_sql = (
        "INSERT INTO metrics (code, label, is_derived) VALUES (?, ?, ?) ON CONFLICT(code) DO NOTHING"
        if is_postgres()
        else "INSERT OR IGNORE INTO metrics (code, label, is_derived) VALUES (?, ?, ?)"
    )

    with get_conn() as conn:
        conn.executemany(branch_sql, branch_rows)
        conn.executemany(metric_sql, metric_rows)
        conn.commit()

    return SeedStats(branches=len(branch_rows), metrics=len(metric_rows))


def bootstrap() -> SeedStats:
    init_schema()
    return seed_dimensions()


def main(argv: Sequence[str] | None = None) -> int:
    _ = argv
    stats = bootstrap()
    stamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"[cuteam] bootstrap complete at {stamp} (branches={stats.branches}, metrics={stats.metrics})")
    if settings.db_url:
        label = settings.db_url_env or "DATABASE_URL"
        print(f"[cuteam] db_source=Postgres ({label})")
    else:
        print(f"[cuteam] db_path={settings.db_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
