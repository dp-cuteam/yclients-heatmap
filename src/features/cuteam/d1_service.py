from __future__ import annotations

import datetime as dt
import json
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from .db import get_conn, init_schema
from .metrics import D1_METRICS, GROUP_LABELS, PLAN_METRIC_CODES
from .settings import settings


BRANCH_ORDER = [
    "Символ (Шоссе Энтузиастов д.3 к. 1)",
    "Матч Поинт (ул. Василисы Кожиной д.13)",
    "Шелепиха (Шелепихинская набережная, 34к4)",
    "CUTEAM СПб (м. Чернышевская)",
    "CUTEAM СПб (м. Чкаловская)",
]
BRANCH_ORDER_INDEX = {name: idx for idx, name in enumerate(BRANCH_ORDER)}
MOSCOW_TZ = ZoneInfo("Europe/Moscow")
AVG_HINTS = ("percent", "ratio", "share")


def _fallback_branches() -> List[Dict[str, Any]]:
    path = settings.branch_mapping_path
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    branches = []
    for item in data or []:
        code = item.get("code")
        name = item.get("name") or code
        if code:
            branches.append({"code": code, "name": name})
    return branches


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
            mapping[code] = name
    return mapping


def _metric_reference() -> tuple[List[str], Dict[str, str]]:
    path = settings.metric_mapping_path
    if not path.exists():
        return [], {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return [], {}
    order: List[str] = []
    labels: Dict[str, str] = {}
    for item in data or []:
        code = item.get("metric_code")
        if not code:
            continue
        label = item.get("label")
        if label and code not in labels:
            labels[code] = label
        source = (item.get("source") or "").strip().lower()
        if source and source != "manual":
            continue
        if code not in order:
            order.append(code)
    return order, labels


def _is_avg_metric(code: str) -> bool:
    lowered = code.lower()
    if lowered.endswith("_pct") or lowered.endswith("_percent"):
        return True
    return any(hint in lowered for hint in AVG_HINTS)


def list_branches() -> List[Dict[str, Any]]:
    init_schema()
    try:
        with get_conn() as conn:
            rows = conn.execute("SELECT code, name FROM branches ORDER BY name").fetchall()
    except Exception:
        return _fallback_branches()
    name_map = _branch_name_map()
    if not rows:
        branches = _fallback_branches()
    else:
        branches = [
            {"code": row["code"], "name": name_map.get(row["code"], row["name"])}
            for row in rows
        ]
    branches.sort(key=lambda row: (BRANCH_ORDER_INDEX.get(row["name"], 999), row["name"]))
    return branches


def list_months(branch_code: str) -> List[str]:
    init_schema()
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT DISTINCT substr(date, 1, 7) AS month "
            "FROM manual_sheet_daily "
            "WHERE branch_code = ? "
            "ORDER BY month DESC",
            (branch_code,),
        ).fetchall()
    current = dt.datetime.now(MOSCOW_TZ).strftime("%Y-%m")
    return [row["month"] for row in rows if row["month"] <= current]


def _parse_month(value: str) -> dt.date:
    try:
        year, month = value.split("-")
        return dt.date(int(year), int(month), 1)
    except Exception as exc:  # noqa: BLE001
        raise ValueError("month must be in YYYY-MM format") from exc


def _month_days(month_start: dt.date) -> List[dt.date]:
    year = month_start.year
    month = month_start.month
    if month == 12:
        month_end = dt.date(year + 1, 1, 1) - dt.timedelta(days=1)
    else:
        month_end = dt.date(year, month + 1, 1) - dt.timedelta(days=1)
    total = (month_end - month_start).days + 1
    return [month_start + dt.timedelta(days=offset) for offset in range(total)]


def _week_chunks(days: List[dt.date]) -> List[Dict[str, Any]]:
    weeks: List[Dict[str, Any]] = []
    if not days:
        return weeks
    start_idx = 0
    for idx, day in enumerate(days):
        is_week_end = day.weekday() == 6 or idx == len(days) - 1
        if is_week_end:
            weeks.append(
                {
                    "start_idx": start_idx,
                    "end_idx": idx,
                    "start": days[start_idx].isoformat(),
                    "end": days[idx].isoformat(),
                }
            )
            start_idx = idx + 1
    return weeks


def _sum(values: List[Optional[float]]) -> Optional[float]:
    cleaned = [v for v in values if v is not None]
    if not cleaned:
        return None
    return float(sum(cleaned))


def _avg(values: List[Optional[float]]) -> Optional[float]:
    cleaned = [v for v in values if v is not None]
    if not cleaned:
        return None
    return float(sum(cleaned) / len(cleaned))


def _fetch_daily_values(
    branch_code: str, start_date: str, end_date: str
) -> Dict[str, Dict[str, float]]:
    base_codes = sorted({metric.code for metric in D1_METRICS if not metric.derived})
    placeholders = ", ".join("?" for _ in base_codes)
    sql = (
        "SELECT metric_code, date, value "
        "FROM manual_sheet_daily "
        "WHERE branch_code = ? AND date >= ? AND date <= ? "
        f"AND metric_code IN ({placeholders})"
    )
    params = [branch_code, start_date, end_date, *base_codes]
    values: Dict[str, Dict[str, float]] = {code: {} for code in base_codes}
    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    for row in rows:
        values.setdefault(row["metric_code"], {})[row["date"]] = float(row["value"])
    return values


def _fetch_raw_values(branch_code: str, start_date: str, end_date: str) -> Dict[str, Dict[str, float]]:
    sql = (
        "SELECT metric_code, date, value "
        "FROM manual_sheet_daily "
        "WHERE branch_code = ? AND date >= ? AND date <= ?"
    )
    params = [branch_code, start_date, end_date]
    values: Dict[str, Dict[str, float]] = {}
    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    for row in rows:
        values.setdefault(row["metric_code"], {})[row["date"]] = float(row["value"])
    return values


def _fetch_plans(branch_code: str, month_start: str) -> Dict[str, float]:
    if not PLAN_METRIC_CODES:
        return {}
    placeholders = ", ".join("?" for _ in PLAN_METRIC_CODES)
    sql = (
        "SELECT metric_code, value FROM plans_monthly "
        "WHERE branch_code = ? AND month_start = ? "
        f"AND metric_code IN ({placeholders})"
    )
    params = [branch_code, month_start, *sorted(PLAN_METRIC_CODES)]
    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    return {row["metric_code"]: float(row["value"]) for row in rows}


def _fetch_branch(branch_code: str) -> Optional[Dict[str, Any]]:
    name_map = _branch_name_map()
    with get_conn() as conn:
        row = conn.execute(
            "SELECT code, name FROM branches WHERE code = ?",
            (branch_code,),
        ).fetchone()
    if not row:
        return None
    return {"code": row["code"], "name": name_map.get(row["code"], row["name"])}


def build_d1_payload(branch_code: str, month: str) -> Dict[str, Any]:
    init_schema()
    month_start = _parse_month(month)
    days = _month_days(month_start)
    if not days:
        return {"branch": None, "month": month, "days": [], "weeks": [], "metrics": []}

    start_iso = days[0].isoformat()
    end_iso = days[-1].isoformat()
    week_chunks = _week_chunks(days)
    daily_values = _fetch_daily_values(branch_code, start_iso, end_iso)
    plans = _fetch_plans(branch_code, month_start.isoformat())

    revenue_daily = daily_values.get("coffee_revenue_total", {})
    checks_daily = daily_values.get("coffee_checks", {})

    metrics_payload: List[Dict[str, Any]] = []
    for metric in D1_METRICS:
        values_map = daily_values.get(metric.code, {})
        day_values: List[Optional[float]] = [
            values_map.get(day.isoformat()) for day in days
        ]

        if metric.derived and metric.code == "avg_check":
            day_values = []
            for day in days:
                rev = revenue_daily.get(day.isoformat())
                checks = checks_daily.get(day.isoformat())
                if rev is None or checks in (None, 0):
                    day_values.append(None)
                else:
                    day_values.append(float(rev) / float(checks))

        week_totals: List[Optional[float]] = []
        for chunk in week_chunks:
            slice_values = day_values[chunk["start_idx"] : chunk["end_idx"] + 1]
            if metric.unit == "pct":
                week_totals.append(_avg(slice_values))
            elif metric.code == "avg_check":
                rev_sum = _sum(
                    [
                        revenue_daily.get(days[idx].isoformat())
                        for idx in range(chunk["start_idx"], chunk["end_idx"] + 1)
                    ]
                )
                checks_sum = _sum(
                    [
                        checks_daily.get(days[idx].isoformat())
                        for idx in range(chunk["start_idx"], chunk["end_idx"] + 1)
                    ]
                )
                if rev_sum is None or not checks_sum:
                    week_totals.append(None)
                else:
                    week_totals.append(float(rev_sum) / float(checks_sum))
            else:
                week_totals.append(_sum(slice_values))

        if metric.unit == "pct":
            month_total = _avg(day_values)
        elif metric.code == "avg_check":
            rev_sum = _sum([revenue_daily.get(day.isoformat()) for day in days])
            checks_sum = _sum([checks_daily.get(day.isoformat()) for day in days])
            if rev_sum is None or not checks_sum:
                month_total = None
            else:
                month_total = float(rev_sum) / float(checks_sum)
        else:
            month_total = _sum(day_values)

        plan_value = plans.get(metric.code) if metric.plan else None
        plan_pct = None
        plan_delta = None
        if plan_value is not None and month_total is not None:
            if plan_value != 0:
                plan_pct = float(month_total) / float(plan_value) * 100
            plan_delta = float(month_total) - float(plan_value)

        metrics_payload.append(
            {
                "key": metric.key,
                "code": metric.code,
                "label": metric.label,
                "unit": metric.unit,
                "group": metric.group,
                "plan_enabled": metric.plan,
                "values": day_values,
                "week_totals": week_totals,
                "month_total": month_total,
                "plan": plan_value,
                "plan_pct": plan_pct,
                "plan_delta": plan_delta,
            }
        )

    return {
        "branch": _fetch_branch(branch_code),
        "month": month,
        "days": [
            {"date": day.isoformat(), "day": day.day, "dow": day.weekday()}
            for day in days
        ],
        "weeks": week_chunks,
        "groups": GROUP_LABELS,
        "metrics": metrics_payload,
    }


def build_raw_payload(branch_code: str, month: str) -> Dict[str, Any]:
    init_schema()
    month_start = _parse_month(month)
    days = _month_days(month_start)
    if not days:
        return {"branch": None, "month": month, "days": [], "weeks": [], "metrics": []}

    start_iso = days[0].isoformat()
    end_iso = days[-1].isoformat()
    week_chunks = _week_chunks(days)
    values_map = _fetch_raw_values(branch_code, start_iso, end_iso)

    order, labels = _metric_reference()
    if order:
        ordered_codes = list(order)
    else:
        ordered_codes = sorted(values_map.keys())

    if ordered_codes:
        with get_conn() as conn:
            rows = conn.execute(
                "SELECT code, label FROM metrics WHERE code IN ({})".format(
                    ", ".join("?" for _ in ordered_codes)
                ),
                ordered_codes,
            ).fetchall()
        for row in rows:
            code = row["code"]
            label = row["label"]
            if code not in labels and label:
                labels[code] = label

    metrics_payload: List[Dict[str, Any]] = []
    for code in ordered_codes:
        values = values_map.get(code, {})
        day_values: List[Optional[float]] = [values.get(day.isoformat()) for day in days]
        week_totals: List[Optional[float]] = []
        for chunk in week_chunks:
            slice_values = day_values[chunk["start_idx"] : chunk["end_idx"] + 1]
            if _is_avg_metric(code):
                week_totals.append(_avg(slice_values))
            else:
                week_totals.append(_sum(slice_values))
        month_total = _avg(day_values) if _is_avg_metric(code) else _sum(day_values)
        metrics_payload.append(
            {
                "code": code,
                "label": labels.get(code, code),
                "values": day_values,
                "week_totals": week_totals,
                "month_total": month_total,
            }
        )

    return {
        "branch": _fetch_branch(branch_code),
        "month": month,
        "days": [
            {"date": day.isoformat(), "day": day.day, "dow": day.weekday()}
            for day in days
        ],
        "weeks": week_chunks,
        "metrics": metrics_payload,
    }


def upsert_plan(branch_code: str, month: str, metric_code: str, value: float) -> None:
    init_schema()
    month_start = _parse_month(month).isoformat()
    now = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO plans_monthly (branch_code, metric_code, month_start, value, updated_at) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(branch_code, metric_code, month_start) DO UPDATE SET "
            "value=excluded.value, updated_at=excluded.updated_at",
            (branch_code, metric_code, month_start, float(value), now),
        )
        conn.commit()
