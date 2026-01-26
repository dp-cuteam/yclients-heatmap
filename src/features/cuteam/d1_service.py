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
IGNORE_BRANCH_CODES = {"SUM", "\u0421\u0423\u041c"}
YEAR_METRIC_CODES = [
    "revenue_total",
    "revenue_open_space",
    "revenue_cabinets",
    "revenue_lecture",
    "revenue_lab",
    "revenue_retail",
    "revenue_salon",
    "coffee_revenue_total",
]
YEAR_GROUPS = [
    {"label": "Итого", "metrics": ["revenue_total"]},
    {
        "label": "Коворкинг",
        "metrics": [
            "revenue_open_space",
            "revenue_cabinets",
            "revenue_lecture",
            "revenue_lab",
            "revenue_retail",
            "revenue_salon",
        ],
    },
    {"label": "Кофейня", "metrics": ["coffee_revenue_total"]},
]

RAW_ORDER = [
    {"code": "revenue_total", "label": "Выручка", "header": True},
    {"code": "revenue_cashless", "label": "из них безналичные"},
    {"code": "revenue_cash", "label": "из них наличные"},
    {"code": "cash_balance_end_day", "label": "остаток наличных на конец дня"},
    {"code": "revenue_open_space", "label": "Аренда"},
    {"code": "revenue_cabinets", "label": "Кабинеты"},
    {"code": "revenue_lecture", "label": "Лекторий"},
    {"code": "revenue_lab", "label": "Лаборатория"},
    {"code": "revenue_retail", "label": "Ритейл"},
    {"code": "revenue_salon", "label": "Услуги салона"},
    {"code": "load_percent", "label": "Загрузка%"},
    {"code": "coffee_revenue_total", "label": "Кофейня", "header": True},
    {"code": "coffee_checks", "label": "Чеки"},
    {"code": "sold_food_total", "label": "Еда", "header": True},
    {"code": "revenue_desserts", "label": "Десерты"},
    {"code": "revenue_food_breakfast", "label": "Еда завтраки"},
    {"code": "revenue_food_lunch", "label": "Еда обеды"},
    {"code": "revenue_food_croissants", "label": "Круассаны"},
    {"code": "revenue_food_salads", "label": "Салаты"},
    {"code": "revenue_food_sandwiches", "label": "Сэндвичи"},
    {"code": "revenue_drinks_total", "label": "Напитки", "header": True},
    {"code": "revenue_coffee", "label": "Кофе"},
    {"code": "revenue_coffee_hot", "label": "Кофе/чай/какао"},
    {"code": "revenue_drinks_cold", "label": "Напитки ритейл"},
    {"code": "revenue_drinks_seasonal", "label": "Сезонные напитки"},
    {"code": "written_off_food_total", "label": "Порча", "header": True},
    {"code": "withdrawals_total", "label": "Изъятия", "header": True},
    {"code": "expense_cleaning_salary", "label": "ЗП клининг"},
    {"code": "expense_staff_salary", "label": "ЗП персонал"},
    {"code": "expense_maintenance", "label": "Текущий ремонт"},
    {"code": "expense_facility", "label": "Содержание помещения"},
    {"code": "expense_delivery_taxi", "label": "Курьер, доставка, такси"},
    {"code": "expense_food_purchase", "label": "Закупка продуктов"},
    {"code": "expense_marketing", "label": "Расходы на маркетинг"},
    {"code": "expense_hiring", "label": "Расходы на найм"},
    {"code": "expense_cash_collection", "label": "Инкассация"},
    {"code": "expense_other", "label": "Прочее"},
    {"code": "deposit_total", "label": "Внесения", "header": True},
]
RAW_HEADER_CODES = {item["code"] for item in RAW_ORDER if item.get("header")}

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
    branches = [
        branch
        for branch in branches
        if branch.get("code") not in IGNORE_BRANCH_CODES
        and branch.get("name") not in IGNORE_BRANCH_CODES
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


def _prev_month_start(month_start: dt.date) -> dt.date:
    if month_start.month == 1:
        return dt.date(month_start.year - 1, 12, 1)
    return dt.date(month_start.year, month_start.month - 1, 1)


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
    if branch_code in IGNORE_BRANCH_CODES:
        return None
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
    base_month_start = _parse_month(month)
    summary_month_start = _prev_month_start(base_month_start)
    days = _month_days(summary_month_start)
    if not days:
        return {"branch": None, "month": month, "days": [], "weeks": [], "metrics": []}

    week_chunks = _week_chunks(days)
    if len(week_chunks) > 5:
        week_chunks = week_chunks[-5:]
    week_ranges = [
        {
            "start": days[chunk["start_idx"]].isoformat(),
            "end": days[chunk["end_idx"]].isoformat(),
        }
        for chunk in week_chunks
    ]
    pad_count = max(0, 5 - len(week_ranges))
    week_labels: List[str] = []
    for idx in range(5):
        label_idx = 5 - idx
        if idx < pad_count:
            week_labels.append(f"Нед -{label_idx} ()")
        else:
            week = week_ranges[idx - pad_count]
            start = dt.date.fromisoformat(week["start"])
            end = dt.date.fromisoformat(week["end"])
            week_labels.append(f"Нед -{label_idx} ({start:%d.%m}–{end:%d.%m})")

    start_iso = days[0].isoformat()
    end_iso = days[-1].isoformat()
    daily_values = _fetch_daily_values(branch_code, start_iso, end_iso)
    plans = _fetch_plans(branch_code, summary_month_start.isoformat())

    metrics_payload: List[Dict[str, Any]] = []
    for metric in D1_METRICS:
        values_map = daily_values.get(metric.code, {})
        day_values: List[Optional[float]] = [
            values_map.get(day.isoformat()) for day in days
        ]

        week_totals: List[Optional[float]] = []
        for chunk in week_chunks:
            slice_values = day_values[chunk["start_idx"] : chunk["end_idx"] + 1]
            week_totals.append(_sum(slice_values))
        if pad_count:
            week_totals = [None] * pad_count + week_totals

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
        "month": summary_month_start.strftime("%Y-%m"),
        "days": [
            {"date": day.isoformat(), "day": day.day, "dow": day.weekday()}
            for day in days
        ],
        "weeks": week_ranges,
        "week_labels": week_labels,
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

    metrics_payload: List[Dict[str, Any]] = []
    for item in RAW_ORDER:
        code = item["code"]
        label = item["label"]
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
                "label": label,
                "values": day_values,
                "week_totals": week_totals,
                "month_total": month_total,
                "is_header": code in RAW_HEADER_CODES,
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


def _fetch_year_facts(branch_code: str) -> Dict[str, Dict[str, float]]:
    if not YEAR_METRIC_CODES:
        return {}
    placeholders = ", ".join("?" for _ in YEAR_METRIC_CODES)
    sql = (
        "SELECT metric_code, substr(date, 1, 7) AS month, SUM(value) AS total "
        "FROM manual_sheet_daily "
        "WHERE branch_code = ? "
        f"AND metric_code IN ({placeholders}) "
        "GROUP BY metric_code, month"
    )
    params = [branch_code, *YEAR_METRIC_CODES]
    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    result: Dict[str, Dict[str, float]] = {code: {} for code in YEAR_METRIC_CODES}
    for row in rows:
        result.setdefault(row["metric_code"], {})[row["month"]] = float(row["total"])
    return result


def _fetch_year_plans(branch_code: str) -> Dict[str, Dict[str, float]]:
    if not YEAR_METRIC_CODES:
        return {}
    placeholders = ", ".join("?" for _ in YEAR_METRIC_CODES)
    sql = (
        "SELECT metric_code, substr(month_start, 1, 7) AS month, value "
        "FROM plans_monthly "
        "WHERE branch_code = ? "
        f"AND metric_code IN ({placeholders})"
    )
    params = [branch_code, *YEAR_METRIC_CODES]
    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    result: Dict[str, Dict[str, float]] = {code: {} for code in YEAR_METRIC_CODES}
    for row in rows:
        result.setdefault(row["metric_code"], {})[row["month"]] = float(row["value"])
    return result


def _year_range_from_months(months: List[str]) -> List[int]:
    if not months:
        current_year = dt.datetime.now(MOSCOW_TZ).year
        return [current_year]
    years = sorted({int(m[:4]) for m in months})
    current_year = dt.datetime.now(MOSCOW_TZ).year
    start_year = min(years[0], current_year)
    end_year = max(years[-1], current_year)
    return list(range(start_year, end_year + 1))


def build_year_summary_payload(branch_code: str) -> Dict[str, Any]:
    init_schema()
    facts = _fetch_year_facts(branch_code)
    plans = _fetch_year_plans(branch_code)

    fact_months = {m for metric in facts.values() for m in metric.keys()}
    plan_months = {m for metric in plans.values() for m in metric.keys()}
    months_all = sorted(fact_months | plan_months)
    years = _year_range_from_months(months_all)
    months = [f"{year:04d}-{month:02d}" for year in years for month in range(1, 13)]
    plan_year = dt.datetime.now(MOSCOW_TZ).year

    _, labels = _metric_reference()
    metrics_payload = []
    for code in YEAR_METRIC_CODES:
        metrics_payload.append(
            {
                "code": code,
                "label": labels.get(code, code),
                "fact": facts.get(code, {}),
                "plan": plans.get(code, {}),
            }
        )

    return {
        "branch": _fetch_branch(branch_code),
        "years": years,
        "months": months,
        "groups": YEAR_GROUPS,
        "metrics": metrics_payload,
        "plan_year": plan_year,
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

