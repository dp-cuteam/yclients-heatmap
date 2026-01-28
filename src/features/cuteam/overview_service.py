from __future__ import annotations

import datetime as dt
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from .db import get_conn, init_schema
from .d1_service import (
    IGNORE_BRANCH_CODES,
    _branch_name_map,
    _is_avg_metric,
    _normalize_branch_code,
    _parse_month,
    _prev_month_start,
)
from .heatmap_load import fetch_hairdresser_daily_load

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
    branch_code = _normalize_branch_code(branch_code) or branch_code
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
    if "load_percent" in codes:
        heatmap_values = fetch_hairdresser_daily_load(branch_code, start_date, end_date)
        if heatmap_values:
            values["load_percent"] = heatmap_values
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
    branch_code = _normalize_branch_code(branch_code) or branch_code
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
    branch_code = (_normalize_branch_code(branch_code) or branch_code).strip()
    month_start = _parse_month(month)
    month_end = _month_end(month_start)
    yoy_month_start = dt.date(month_start.year - 1, month_start.month, 1)

    analysis_start = min(month_start - dt.timedelta(days=56), yoy_month_start)
    analysis_end = month_end

    values_map = _fetch_values(
        branch_code,
        analysis_start.isoformat(),
        analysis_end.isoformat(),
        BASE_CODES,
    )

    daily_dates = _daterange(month_start, month_end)
    cutoff_codes = [code for code in BASE_CODES if code != "load_percent"]
    filled_days: List[dt.date] = []
    for day in daily_dates:
        day_iso = day.isoformat()
        if any(values_map.get(code, {}).get(day_iso) is not None for code in cutoff_codes):
            filled_days.append(day)

    cutoff_day = max((day.day for day in filled_days), default=0)
    days_in_month = len(daily_dates)
    cutoff_date = month_start + dt.timedelta(days=cutoff_day - 1) if cutoff_day else None
    range_label = f"1–{cutoff_day}" if cutoff_day else ""

    if cutoff_day:
        current_values = _period_values(values_map, month_start, cutoff_date)
        yoy_end = yoy_month_start + dt.timedelta(days=cutoff_day - 1)
        yoy_values = _period_values(values_map, yoy_month_start, yoy_end)
    else:
        current_values = {}
        yoy_values = {}

    def _delta_for(code: str) -> Dict[str, Optional[float]]:
        return _delta(current_values.get(code), yoy_values.get(code))

    cutoff_for_weeks = cutoff_date or month_end
    weekly_ranges = _last_n_weeks(cutoff_for_weeks, 8)
    weekly_values: Dict[str, List[Optional[float]]] = {code: [] for code in BASE_CODES}
    for week in weekly_ranges:
        period = _period_values(values_map, week["start"], week["end"])
        for code in BASE_CODES:
            weekly_values[code].append(period.get(code))

    avg8_load = _avg([v for v in weekly_values.get("load_percent", []) if v is not None])
    avg8_open = _avg([v for v in weekly_values.get("revenue_open_space", []) if v is not None])
    avg8_coffee = _avg([v for v in weekly_values.get("coffee_revenue_total", []) if v is not None])

    cash_control: List[Dict[str, Any]] = []
    cash_threshold = 1000
    if cutoff_day:
        mtd_dates = daily_dates[:cutoff_day]
        cash_series = values_map.get("cash_balance_end_day", {})
        cash_rev = values_map.get("revenue_cash", {})
        cash_dep = values_map.get("deposit_total", {})
        cash_with = values_map.get("withdrawals_total", {})
        for idx, day in enumerate(mtd_dates):
            if idx == 0:
                continue
            day_iso = day.isoformat()
            prev_iso = mtd_dates[idx - 1].isoformat()
            actual = cash_series.get(day_iso)
            prev_balance = cash_series.get(prev_iso)
            if actual is None or prev_balance is None:
                continue
            expected = prev_balance
            expected += cash_rev.get(day_iso, 0.0)
            expected += cash_dep.get(day_iso, 0.0)
            expected -= cash_with.get(day_iso, 0.0)
            diff = actual - expected
            if abs(diff) >= cash_threshold:
                cash_control.append(
                    {
                        "date": day_iso,
                        "expected": expected,
                        "actual": actual,
                        "diff": diff,
                    }
                )

    drivers_source = [
        ("revenue_open_space", "Аренда"),
        ("revenue_cabinets", "Кабинеты"),
        ("revenue_lecture", "Лекторий"),
        ("revenue_lab", "Лаборатория"),
        ("revenue_retail", "Ритейл"),
        ("revenue_salon", "Услуги салона"),
        ("revenue_drinks_total", "Напитки"),
        ("sold_food_total", "Еда"),
        ("revenue_desserts", "Десерты"),
    ]
    drivers: List[Dict[str, Any]] = []
    for code, label in drivers_source:
        delta = _delta_for(code)
        delta_value = delta.get("delta")
        delta_pct = delta.get("pct")
        if delta_value is None or delta_pct is None:
            continue
        if delta_value <= 0:
            continue
        drivers.append(
            {
                "code": code,
                "label": label,
                "delta": delta_value,
                "pct": delta_pct,
            }
        )
    drivers.sort(key=lambda item: item["delta"], reverse=True)
    drivers = drivers[:3]

    alerts: List[Dict[str, Any]] = []
    if cash_control:
        max_diff = max((abs(item["diff"]) for item in cash_control), default=0)
        alerts.append(
            {
                "type": "cash",
                "count": len(cash_control),
                "max_diff": max_diff,
            }
        )

    writeoff_rate = current_values.get("writeoff_rate_full")
    if writeoff_rate is not None and writeoff_rate > 0.10:
        alerts.append(
            {
                "type": "writeoff",
                "rate": writeoff_rate,
            }
        )

    current_load = current_values.get("load_percent")
    current_open = current_values.get("revenue_open_space")
    if (
        current_load is not None
        and avg8_load is not None
        and current_open is not None
        and avg8_open is not None
    ):
        if current_load >= avg8_load and current_open <= avg8_open * 0.9:
            alerts.append(
                {
                    "type": "load_open",
                    "load": current_load,
                    "load_avg": avg8_load,
                    "value": current_open,
                    "avg": avg8_open,
                }
            )

    current_coffee = current_values.get("coffee_revenue_total")
    if (
        current_load is not None
        and avg8_load is not None
        and current_coffee is not None
        and avg8_coffee is not None
    ):
        if current_load >= avg8_load and current_coffee <= avg8_coffee * 0.9:
            alerts.append(
                {
                    "type": "load_coffee",
                    "load": current_load,
                    "load_avg": avg8_load,
                    "value": current_coffee,
                    "avg": avg8_coffee,
                }
            )

    alerts = alerts[:3]

    checks: List[Dict[str, Any]] = []
    if cash_control:
        max_diff = max((abs(item["diff"]) for item in cash_control), default=0)
        checks.append(
            {
                "key": "cash",
                "title": "Проверка кассы",
                "status": "alert",
                "count": len(cash_control),
                "max_diff": max_diff,
            }
        )
    else:
        checks.append(
            {
                "key": "cash",
                "title": "Проверка кассы",
                "status": "ok" if cutoff_day else "no_data",
                "count": 0,
                "max_diff": None,
            }
        )

    load_open_status = "no_data"
    if current_load is not None and avg8_load is not None and current_open is not None and avg8_open is not None:
        load_open_status = "alert" if (current_load >= avg8_load and current_open <= avg8_open * 0.9) else "ok"
    checks.append(
        {
            "key": "load_open",
            "title": "Высокая загрузка, низкая аренда",
            "status": load_open_status,
            "load": current_load,
            "load_avg": avg8_load,
            "value": current_open,
            "avg": avg8_open,
        }
    )

    load_coffee_status = "no_data"
    if current_load is not None and avg8_load is not None and current_coffee is not None and avg8_coffee is not None:
        load_coffee_status = "alert" if (current_load >= avg8_load and current_coffee <= avg8_coffee * 0.9) else "ok"
    checks.append(
        {
            "key": "load_coffee",
            "title": "Высокая загрузка, слабая кофейня",
            "status": load_coffee_status,
            "load": current_load,
            "load_avg": avg8_load,
            "value": current_coffee,
            "avg": avg8_coffee,
        }
    )

    return {
        "branch": _fetch_branch(branch_code),
        "month": month,
        "mtd": {
            "cutoff_day": cutoff_day,
            "days_in_month": days_in_month,
            "filled_days": len(filled_days),
            "range_label": range_label,
            "current": current_values,
            "yoy": yoy_values,
        },
        "yoy_delta": {
            "revenue_total": _delta_for("revenue_total"),
            "coworking_total": _delta_for("coworking_total"),
            "coffee_revenue_total": _delta_for("coffee_revenue_total"),
            "load_percent": _delta_for("load_percent"),
            "written_off_food_total": _delta_for("written_off_food_total"),
        },
        "coefficients": {
            "avg_check": current_values.get("avg_check"),
            "writeoff_rate": current_values.get("writeoff_rate_full"),
            "lab_to_open_space_ratio": current_values.get("lab_to_open_space_ratio"),
        },
        "drivers": drivers,
        "alerts": alerts,
        "checks": checks,
        "cash_control": cash_control,
        "weekly": {
            "weeks": [
                {"start": week["start"].isoformat(), "end": week["end"].isoformat()}
                for week in weekly_ranges
            ],
            "values": weekly_values,
        },
        "averages": {
            "load_percent": avg8_load,
            "revenue_open_space": avg8_open,
            "coffee_revenue_total": avg8_coffee,
        },
    }
