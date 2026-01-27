from __future__ import annotations

import datetime as dt
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from .db import get_conn, init_schema
from .d1_service import (
    IGNORE_BRANCH_CODES,
    _branch_name_map,
    _is_avg_metric,
    _parse_month,
    _prev_month_start,
)

MOSCOW_TZ = ZoneInfo("Europe/Moscow")

BASE_CODES = [
    "revenue_total",
    "revenue_cashless",
    "revenue_cash",
    "cash_balance_end_day",
    "revenue_open_space",
    "revenue_cabinets",
    "revenue_lecture",
    "revenue_lab",
    "revenue_retail",
    "revenue_salon",
    "load_percent",
    "coffee_revenue_total",
    "coffee_checks",
    "sold_food_total",
    "revenue_desserts",
    "revenue_food_breakfast",
    "revenue_food_lunch",
    "revenue_food_croissants",
    "revenue_food_salads",
    "revenue_food_sandwiches",
    "revenue_drinks_total",
    "revenue_coffee",
    "revenue_coffee_hot",
    "revenue_drinks_cold",
    "revenue_drinks_seasonal",
    "written_off_food_total",
    "withdrawals_total",
    "deposit_total",
    "expense_cleaning_salary",
    "expense_staff_salary",
    "expense_maintenance",
    "expense_facility",
    "expense_delivery_taxi",
    "expense_food_purchase",
    "expense_marketing",
    "expense_hiring",
    "expense_cash_collection",
    "expense_other",
]

EXPENSE_CODES = [
    "expense_cleaning_salary",
    "expense_staff_salary",
    "expense_maintenance",
    "expense_facility",
    "expense_delivery_taxi",
    "expense_food_purchase",
    "expense_marketing",
    "expense_hiring",
    "expense_cash_collection",
    "expense_other",
]

COWORKING_CODES = [
    "revenue_open_space",
    "revenue_cabinets",
    "revenue_lecture",
    "revenue_lab",
    "revenue_retail",
    "revenue_salon",
]

FOOD_CODES = [
    "sold_food_total",
    "revenue_desserts",
    "revenue_food_breakfast",
    "revenue_food_lunch",
    "revenue_food_croissants",
    "revenue_food_salads",
    "revenue_food_sandwiches",
]

DRINK_CODES = [
    "revenue_drinks_total",
    "revenue_coffee",
    "revenue_coffee_hot",
    "revenue_drinks_cold",
    "revenue_drinks_seasonal",
]


def _month_end(month_start: dt.date) -> dt.date:
    if month_start.month == 12:
        return dt.date(month_start.year + 1, 1, 1) - dt.timedelta(days=1)
    return dt.date(month_start.year, month_start.month + 1, 1) - dt.timedelta(days=1)


def _daterange(start: dt.date, end: dt.date) -> List[dt.date]:
    total = (end - start).days + 1
    return [start + dt.timedelta(days=offset) for offset in range(total)]


def _week_start(date_value: dt.date) -> dt.date:
    return date_value - dt.timedelta(days=date_value.weekday())


def _last_n_weeks(end_date: dt.date, count: int) -> List[Dict[str, dt.date]]:
    end_start = _week_start(end_date)
    starts = [end_start - dt.timedelta(weeks=offset) for offset in range(count - 1, -1, -1)]
    return [{"start": start, "end": start + dt.timedelta(days=6)} for start in starts]


def _last_n_months(end_month_start: dt.date, count: int) -> List[dt.date]:
    months: List[dt.date] = []
    current = end_month_start
    for _ in range(count):
        months.append(current)
        if current.month == 1:
            current = dt.date(current.year - 1, 12, 1)
        else:
            current = dt.date(current.year, current.month - 1, 1)
    months.reverse()
    return months


def _fetch_values(
    branch_code: str, start_date: str, end_date: str, codes: List[str]
) -> Dict[str, Dict[str, float]]:
    placeholders = ", ".join("?" for _ in codes)
    sql = (
        "SELECT metric_code, date, value "
        "FROM manual_sheet_daily "
        "WHERE branch_code = ? AND date >= ? AND date <= ? "
        f"AND metric_code IN ({placeholders})"
    )
    params = [branch_code, start_date, end_date, *codes]
    values: Dict[str, Dict[str, float]] = {code: {} for code in codes}
    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    for row in rows:
        values.setdefault(row["metric_code"], {})[row["date"]] = float(row["value"])
    return values


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


def _agg_code(code: str, values: List[Optional[float]]) -> Optional[float]:
    if _is_avg_metric(code):
        return _avg(values)
    return _sum(values)


def _safe_div(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None or b == 0:
        return None
    return float(a) / float(b)


def _safe_sum(values: List[Optional[float]]) -> Optional[float]:
    cleaned = [v for v in values if v is not None]
    if not cleaned:
        return None
    return float(sum(cleaned))


def _compute_derived(values: Dict[str, Optional[float]]) -> Dict[str, Optional[float]]:
    derived: Dict[str, Optional[float]] = {}
    revenue_total = values.get("revenue_total")
    coffee_rev = values.get("coffee_revenue_total")
    coffee_checks = values.get("coffee_checks")
    sold_food = values.get("sold_food_total")
    written_off = values.get("written_off_food_total")
    load = values.get("load_percent")
    revenue_lab = values.get("revenue_lab")
    revenue_open = values.get("revenue_open_space")

    derived["avg_check"] = _safe_div(coffee_rev, coffee_checks)
    derived["writeoff_rate"] = _safe_div(written_off, sold_food)
    derived["writeoff_rate_full"] = _safe_div(
        written_off,
        _safe_sum([sold_food, written_off]),
    )
    derived["lab_to_open_space_ratio"] = _safe_div(revenue_lab, revenue_open)
    derived["revenue_per_load"] = _safe_div(revenue_open, load)
    derived["coffee_revenue_per_load"] = _safe_div(coffee_rev, load)

    total_expenses = _safe_sum([values.get(code) for code in EXPENSE_CODES])
    derived["total_expenses"] = total_expenses
    derived["expense_ratio"] = _safe_div(total_expenses, revenue_total)

    if revenue_total is not None and values.get("expense_food_purchase") is not None:
        derived["gross_profit"] = revenue_total - values.get("expense_food_purchase")  # type: ignore[operator]
    else:
        derived["gross_profit"] = None

    if revenue_total is not None and total_expenses is not None:
        derived["operating_profit"] = revenue_total - total_expenses
    else:
        derived["operating_profit"] = None
    derived["operating_margin"] = _safe_div(derived["operating_profit"], revenue_total)

    coworking_total = _safe_sum([values.get(code) for code in COWORKING_CODES])
    derived["coworking_total"] = coworking_total
    return derived


def _period_values(
    values_map: Dict[str, Dict[str, float]], start: dt.date, end: dt.date
) -> Dict[str, Optional[float]]:
    values: Dict[str, Optional[float]] = {}
    dates = _daterange(start, end)
    for code in BASE_CODES:
        vals = [values_map.get(code, {}).get(day.isoformat()) for day in dates]
        values[code] = _agg_code(code, vals)
    values.update(_compute_derived(values))
    return values


def _delta(current: Optional[float], prev: Optional[float]) -> Dict[str, Optional[float]]:
    if current is None or prev is None:
        return {"delta": None, "pct": None}
    delta = current - prev
    pct = None
    if prev != 0:
        pct = delta / prev * 100
    return {"delta": delta, "pct": pct}


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
        return {"code": branch_code, "name": name_map.get(branch_code, branch_code)}
    return {"code": row["code"], "name": name_map.get(row["code"], row["name"])}


def build_overview_payload(branch_code: str, month: str) -> Dict[str, Any]:
    init_schema()
    month_start = _parse_month(month)
    month_end = _month_end(month_start)

    last_week_start = _week_start(month_end)
    last_week_end = last_week_start + dt.timedelta(days=6)
    prev_week_start = last_week_start - dt.timedelta(days=7)
    prev_week_end = last_week_end - dt.timedelta(days=7)

    yoy_week_start = last_week_start - dt.timedelta(days=364)
    yoy_week_end = last_week_end - dt.timedelta(days=364)

    last_4w_start = last_week_start - dt.timedelta(days=21)
    last_4w_end = last_week_end
    prev_4w_start = last_4w_start - dt.timedelta(days=28)
    prev_4w_end = last_4w_end - dt.timedelta(days=28)
    yoy_4w_start = last_4w_start - dt.timedelta(days=364)
    yoy_4w_end = last_4w_end - dt.timedelta(days=364)

    prev_month_start = _prev_month_start(month_start)
    prev_month_end = _month_end(prev_month_start)
    yoy_month_start = dt.date(month_start.year - 1, month_start.month, 1)
    yoy_month_end = _month_end(yoy_month_start)

    analysis_start = min(prev_4w_start, yoy_4w_start, yoy_month_start, prev_week_start, yoy_week_start)
    analysis_end = max(month_end, last_week_end, prev_week_end)

    values_map = _fetch_values(
        branch_code,
        analysis_start.isoformat(),
        analysis_end.isoformat(),
        BASE_CODES,
    )

    month_values = _period_values(values_map, month_start, month_end)
    prev_month_values = _period_values(values_map, prev_month_start, prev_month_end)
    yoy_month_values = _period_values(values_map, yoy_month_start, yoy_month_end)

    last_week_values = _period_values(values_map, last_week_start, last_week_end)
    prev_week_values = _period_values(values_map, prev_week_start, prev_week_end)
    yoy_week_values = _period_values(values_map, yoy_week_start, yoy_week_end)

    last_4w_values = _period_values(values_map, last_4w_start, last_4w_end)
    prev_4w_values = _period_values(values_map, prev_4w_start, prev_4w_end)
    yoy_4w_values = _period_values(values_map, yoy_4w_start, yoy_4w_end)

    daily_dates = _daterange(month_start, month_end)
    daily_values: Dict[str, List[Optional[float]]] = {code: [] for code in BASE_CODES}
    derived_daily: Dict[str, List[Optional[float]]] = {
        "avg_check": [],
        "writeoff_rate": [],
        "writeoff_rate_full": [],
        "lab_to_open_space_ratio": [],
        "revenue_per_load": [],
        "coffee_revenue_per_load": [],
        "total_expenses": [],
        "expense_ratio": [],
        "gross_profit": [],
        "operating_profit": [],
        "operating_margin": [],
        "coworking_total": [],
    }
    for day in daily_dates:
        day_values = {
            code: values_map.get(code, {}).get(day.isoformat()) for code in BASE_CODES
        }
        for code in BASE_CODES:
            daily_values[code].append(day_values.get(code))
        derived = _compute_derived(day_values)
        for key in derived_daily:
            derived_daily[key].append(derived.get(key))

    weekly_ranges = _last_n_weeks(month_end, 8)
    weekly_values: Dict[str, List[Optional[float]]] = {code: [] for code in BASE_CODES}
    derived_weekly: Dict[str, List[Optional[float]]] = {
        "avg_check": [],
        "writeoff_rate": [],
        "writeoff_rate_full": [],
        "lab_to_open_space_ratio": [],
        "revenue_per_load": [],
        "coffee_revenue_per_load": [],
        "total_expenses": [],
        "expense_ratio": [],
        "gross_profit": [],
        "operating_profit": [],
        "operating_margin": [],
        "coworking_total": [],
    }
    for week in weekly_ranges:
        period = _period_values(values_map, week["start"], week["end"])
        for code in BASE_CODES:
            weekly_values[code].append(period.get(code))
        for key in derived_weekly:
            derived_weekly[key].append(period.get(key))

    monthly_ranges = _last_n_months(month_start, 12)
    monthly_values: Dict[str, List[Optional[float]]] = {"revenue_total": [], "coffee_revenue_total": []}
    for month_start_item in monthly_ranges:
        month_end_item = _month_end(month_start_item)
        period = _period_values(values_map, month_start_item, month_end_item)
        monthly_values["revenue_total"].append(period.get("revenue_total"))
        monthly_values["coffee_revenue_total"].append(period.get("coffee_revenue_total"))

    overview = {
        "revenue_total": {
            "value": month_values.get("revenue_total"),
            "wow": _delta(last_week_values.get("revenue_total"), prev_week_values.get("revenue_total")),
            "yoy": _delta(month_values.get("revenue_total"), yoy_month_values.get("revenue_total")),
        },
        "coworking_total": {
            "value": month_values.get("coworking_total"),
        },
        "coffee_total": {
            "value": month_values.get("coffee_revenue_total"),
            "checks": month_values.get("coffee_checks"),
            "avg_check": month_values.get("avg_check"),
        },
        "load_percent": {
            "value": month_values.get("load_percent"),
        },
        "writeoff": {
            "value": month_values.get("written_off_food_total"),
            "rate": month_values.get("writeoff_rate"),
        },
    }

    revenue_compare_rows = []
    for code in ["revenue_total", "revenue_cashless", "revenue_cash"]:
        current = month_values.get(code)
        previous = prev_month_values.get(code)
        yoy = yoy_month_values.get(code)
        delta = _delta(current, previous)
        revenue_compare_rows.append(
            {
                "code": code,
                "current": current,
                "previous": previous,
                "yoy": yoy,
                "delta_pct": delta.get("pct"),
            }
        )

    best_week = None
    if weekly_values.get("revenue_total"):
        best_week = max(
            (value for value in weekly_values["revenue_total"] if value is not None),
            default=None,
        )
    best_month = None
    if monthly_values.get("revenue_total"):
        best_month = max(
            (value for value in monthly_values["revenue_total"] if value is not None),
            default=None,
        )
    best_4w = None
    rev_weeks = weekly_values.get("revenue_total") or []
    if len(rev_weeks) >= 4:
        sums: List[float] = []
        for idx in range(len(rev_weeks) - 3):
            window = [v for v in rev_weeks[idx : idx + 4] if v is not None]
            if len(window) < 4:
                continue
            sums.append(float(sum(window)))
        if sums:
            best_4w = max(sums)

    period_compare = [
        {
            "label": "Неделя",
            "current": last_week_values.get("revenue_total"),
            "previous": prev_week_values.get("revenue_total"),
            "yoy": yoy_week_values.get("revenue_total"),
            "best": best_week,
        },
        {
            "label": "4 недели",
            "current": last_4w_values.get("revenue_total"),
            "previous": prev_4w_values.get("revenue_total"),
            "yoy": yoy_4w_values.get("revenue_total"),
            "best": best_4w,
        },
        {
            "label": "Месяц",
            "current": month_values.get("revenue_total"),
            "previous": prev_month_values.get("revenue_total"),
            "yoy": yoy_month_values.get("revenue_total"),
            "best": best_month,
        },
    ]

    cash_control: List[Dict[str, Any]] = []
    cash_series = values_map.get("cash_balance_end_day", {})
    cash_rev = values_map.get("revenue_cash", {})
    cash_dep = values_map.get("deposit_total", {})
    cash_with = values_map.get("withdrawals_total", {})
    for idx, day in enumerate(daily_dates):
        if idx == 0:
            continue
        day_iso = day.isoformat()
        prev_iso = daily_dates[idx - 1].isoformat()
        actual = cash_series.get(day_iso)
        prev_balance = cash_series.get(prev_iso)
        if actual is None or prev_balance is None:
            continue
        expected = prev_balance
        expected += cash_rev.get(day_iso, 0.0)
        expected += cash_dep.get(day_iso, 0.0)
        expected -= cash_with.get(day_iso, 0.0)
        diff = actual - expected
        if abs(diff) >= 1:
            cash_control.append(
                {
                    "date": day_iso,
                    "expected": expected,
                    "actual": actual,
                    "diff": diff,
                }
            )

    signals: List[Dict[str, Any]] = []
    thresholds = {
        "revenue_total": 10,
        "revenue_open_space": 12,
        "revenue_lab": 12,
        "coffee_revenue_total": 10,
        "coffee_checks": 8,
        "avg_check": 8,
        "writeoff_rate_full": 10,
        "expense_staff_salary": 10,
        "expense_food_purchase": 10,
    }
    wow_values = _period_values(values_map, prev_week_start, prev_week_end)
    current_values = _period_values(values_map, last_week_start, last_week_end)
    for code, threshold in thresholds.items():
        current = current_values.get(code)
        prev = wow_values.get(code)
        delta = _delta(current, prev)
        pct = delta.get("pct")
        status = "ok"
        if pct is not None and pct < -threshold:
            status = "alert"
        signals.append(
            {
                "code": code,
                "current": current,
                "previous": prev,
                "delta_pct": pct,
                "threshold": threshold,
                "status": status,
            }
        )
    if cash_control:
        signals.append(
            {
                "code": "cash_balance_end_day",
                "status": "alert",
                "message": "Есть расхождения в кассовой логике",
                "count": len(cash_control),
            }
        )

    return {
        "branch": _fetch_branch(branch_code),
        "month": month,
        "periods": {
            "month": {"start": month_start.isoformat(), "end": month_end.isoformat()},
            "prev_month": {"start": prev_month_start.isoformat(), "end": prev_month_end.isoformat()},
            "yoy_month": {"start": yoy_month_start.isoformat(), "end": yoy_month_end.isoformat()},
            "last_week": {"start": last_week_start.isoformat(), "end": last_week_end.isoformat()},
            "prev_week": {"start": prev_week_start.isoformat(), "end": prev_week_end.isoformat()},
            "yoy_week": {"start": yoy_week_start.isoformat(), "end": yoy_week_end.isoformat()},
            "last_4w": {"start": last_4w_start.isoformat(), "end": last_4w_end.isoformat()},
            "prev_4w": {"start": prev_4w_start.isoformat(), "end": prev_4w_end.isoformat()},
            "yoy_4w": {"start": yoy_4w_start.isoformat(), "end": yoy_4w_end.isoformat()},
        },
        "period_values": {
            "week": last_week_values,
            "prev_week": prev_week_values,
            "yoy_week": yoy_week_values,
            "four_weeks": last_4w_values,
            "prev_four_weeks": prev_4w_values,
            "yoy_four_weeks": yoy_4w_values,
            "month": month_values,
            "prev_month": prev_month_values,
            "yoy_month": yoy_month_values,
        },
        "daily": {
            "dates": [d.isoformat() for d in daily_dates],
            "values": {**daily_values, **derived_daily},
        },
        "weekly": {
            "weeks": [
                {
                    "start": week["start"].isoformat(),
                    "end": week["end"].isoformat(),
                }
                for week in weekly_ranges
            ],
            "values": {**weekly_values, **derived_weekly},
        },
        "monthly": {
            "months": [m.strftime("%Y-%m") for m in monthly_ranges],
            "values": monthly_values,
        },
        "overview": overview,
        "revenue_compare": revenue_compare_rows,
        "period_compare": period_compare,
        "cash_control": cash_control,
        "signals": signals,
    }
