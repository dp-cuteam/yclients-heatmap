from __future__ import annotations

import datetime as dt
import json
import sqlite3
from functools import lru_cache
from typing import Dict, List

from .heatmap_db import get_heatmap_conn
from .settings import settings


def _to_int(value) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


@lru_cache(maxsize=1)
def _branch_yclients_map() -> Dict[str, int]:
    path = settings.branch_mapping_path
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    mapping: Dict[str, int] = {}
    for item in data or []:
        code = item.get("code")
        yclients_id = _to_int(item.get("yclients_id"))
        if code and yclients_id:
            mapping[code] = yclients_id
    return mapping


@lru_cache(maxsize=1)
def _branch_name_map() -> Dict[str, str]:
    path = settings.branch_mapping_path
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    mapping: Dict[str, str] = {}
    for item in data or []:
        code = item.get("code")
        name = item.get("name")
        if code and name:
            mapping[code] = str(name)
    return mapping


def _normalize_name(value: str) -> str:
    return " ".join(value.lower().replace("ё", "е").split())


@lru_cache(maxsize=1)
def _group_config() -> Dict:
    path = settings.heatmap_groups_resolved_path
    if not path.exists():
        path = settings.heatmap_groups_path
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}


def _resolve_branch_id(branch_code: str) -> int | None:
    branch_id = _to_int(branch_code) or _branch_yclients_map().get(branch_code)
    if branch_id:
        return branch_id
    name = _branch_name_map().get(branch_code)
    if not name:
        return None
    target = _normalize_name(name)
    config = _group_config()
    for branch in config.get("branches", []):
        display_name = branch.get("display_name") or ""
        if _normalize_name(str(display_name)) == target:
            return _to_int(branch.get("branch_id"))
    return None


def _hairdresser_group_ids(branch_id: int) -> List[str]:
    config = _group_config()
    for branch in config.get("branches", []):
        if _to_int(branch.get("branch_id")) != branch_id:
            continue
        groups = branch.get("groups") or []
        ids: List[str] = []
        target = _normalize_name("Рабочее место парикмахера")
        for group in groups:
            name = _normalize_name(group.get("name") or "")
            if name != target:
                continue
            group_id = group.get("group_id")
            if group_id:
                ids.append(str(group_id))
        return ids
    return []


def _daterange(start: dt.date, end: dt.date) -> List[dt.date]:
    total = (end - start).days + 1
    return [start + dt.timedelta(days=offset) for offset in range(total)]


def fetch_hairdresser_daily_load(
    branch_code: str, start_date: str, end_date: str
) -> Dict[str, float]:
    branch_id = _resolve_branch_id(branch_code)
    if not branch_id:
        return {}
    group_ids = _hairdresser_group_ids(branch_id)
    if not group_ids:
        return {}

    placeholders = ", ".join("?" for _ in group_ids)
    sql = (
        "SELECT date, load_pct FROM group_hour_load "
        "WHERE branch_id = ? AND date BETWEEN ? AND ? "
        "AND in_benchmark = 1 "
        f"AND group_id IN ({placeholders})"
    )
    params = [branch_id, start_date, end_date, *group_ids]

    rows = None
    try:
        with get_heatmap_conn() as conn:
            rows = conn.execute(sql, params).fetchall()
    except Exception:
        rows = None

    if rows is None:
        try:
            db_path = settings.heatmap_db_path
            if db_path.exists():
                raw = sqlite3.connect(str(db_path))
                raw.row_factory = sqlite3.Row
                rows = raw.execute(sql, params).fetchall()
                raw.close()
        except Exception:
            return {}

    if not rows:
        return {}

    by_date: Dict[str, List[float]] = {}
    for row in rows:
        try:
            date_key = row["date"]
            by_date.setdefault(date_key, []).append(float(row["load_pct"]))
        except Exception:
            continue

    try:
        start = dt.date.fromisoformat(start_date)
        end = dt.date.fromisoformat(end_date)
    except ValueError:
        return {}

    daily: Dict[str, float] = {}
    for day in _daterange(start, end):
        key = day.isoformat()
        vals = by_date.get(key, [])
        if not vals:
            continue
        daily[key] = round(sum(vals) / len(vals), 2)
    return daily
