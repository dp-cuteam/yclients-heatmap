from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from openpyxl import load_workbook

from .config import settings
from .utils import week_start_monday


BRANCH_CODE_MAP = {
    "СМ": 1213086,
    "МП": 1224674,
    "СС": 1224689,
}

_LATIN_TO_CYRILLIC = str.maketrans(
    {
        "A": "А",
        "B": "В",
        "C": "С",
        "E": "Е",
        "H": "Н",
        "K": "К",
        "M": "М",
        "O": "О",
        "P": "Р",
        "T": "Т",
        "X": "Х",
        "Y": "У",
    }
)

_TIME_RE = re.compile(r"^(\\d{1,2}):(\\d{2})-(\\d{1,2}):(\\d{2})$")
_SHEET_RE = re.compile(r"^([A-Za-zА-Яа-я]{1,3})\\.(\\d{2})\\.(\\d{2,4})$")


@dataclass
class HistoricalMonth:
    hours: list[int]
    type_order: list[str]
    types: dict[str, dict[str, Any]]


_CACHE: dict[str, Any] = {"mtime": None, "data": None}


def _normalize_prefix(value: str) -> str:
    text = value.strip().upper().translate(_LATIN_TO_CYRILLIC)
    return text


def _parse_sheet_name(name: str) -> tuple[int, str, str] | None:
    cleaned = name.strip()
    match = _SHEET_RE.match(cleaned)
    if not match:
        return None
    prefix, month_raw, year_raw = match.groups()
    prefix = _normalize_prefix(prefix)
    branch_id = BRANCH_CODE_MAP.get(prefix)
    if not branch_id:
        return None
    year = int(year_raw)
    if year < 100:
        year += 2000
    month = int(month_raw)
    if not (1 <= month <= 12):
        return None
    month_key = f"{year:04d}-{month:02d}"
    return branch_id, prefix, month_key


def _iter_day_columns(ws) -> list[tuple[int, date]]:
    days: list[tuple[int, date]] = []
    for col in range(2, ws.max_column + 1):
        value = ws.cell(row=1, column=col).value
        if value is None:
            if days:
                break
            continue
        if isinstance(value, datetime):
            days.append((col, value.date()))
        elif isinstance(value, date):
            days.append((col, value))
    return days


def _parse_time_label(label: str) -> int | None:
    match = _TIME_RE.match(label.strip())
    if not match:
        return None
    hour = int(match.group(1))
    return hour


def _build_weeks(days: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not days:
        return []
    first_day = date.fromisoformat(days[0]["date"])
    last_day = date.fromisoformat(days[-1]["date"])
    current = week_start_monday(first_day)
    weeks: list[dict[str, Any]] = []
    while current <= last_day:
        week_end = current + timedelta(days=6)
        week_days = []
        week_vals = []
        for day in days:
            day_date = date.fromisoformat(day["date"])
            if day_date < current or day_date > week_end:
                continue
            week_days.append(day)
            week_vals.extend([c["load_pct"] for c in day["cells"]])
        week_avg = round(sum(week_vals) / len(week_vals), 2) if week_vals else 0.0
        weeks.append(
            {
                "week_start": current.isoformat(),
                "week_end": week_end.isoformat(),
                "days": week_days,
                "week_avg": week_avg,
            }
        )
        current += timedelta(days=7)
    return weeks


def _parse_sheet(ws) -> HistoricalMonth | None:
    day_columns = _iter_day_columns(ws)
    if not day_columns:
        return None
    hours: list[int] = []
    hour_index: dict[int, int] = {}
    type_order: list[str] = []
    type_rows: list[tuple[str, list[tuple[int, int]]]] = []
    current_type: str | None = None
    current_rows: list[tuple[int, int]] = []

    def flush_type() -> None:
        nonlocal current_type, current_rows
        if not current_type or not current_rows:
            return
        type_rows.append((current_type, current_rows))
        current_rows = []

    for row in range(2, ws.max_row + 1):
        cell_val = ws.cell(row=row, column=1).value
        if cell_val is None:
            continue
        if isinstance(cell_val, str):
            label = cell_val.strip()
        else:
            label = str(cell_val).strip()
        if not label:
            continue
        hour = _parse_time_label(label)
        if hour is None:
            flush_type()
            current_type = label
            if current_type not in type_order:
                type_order.append(current_type)
            continue
        if current_type:
            current_rows.append((hour, row))
            if hour not in hour_index:
                hour_index[hour] = len(hours)
                hours.append(hour)
    flush_type()

    if not hours:
        return None

    types: dict[str, dict[str, Any]] = {}
    for type_name, rows in type_rows:
        day_data = []
        all_vals = []
        for _, day_date in day_columns:
            cells = [{"load_pct": 0.0, "busy_count": 0, "staff_total": 0} for _ in hours]
            day_data.append(
                {
                    "date": day_date.isoformat(),
                    "dow": day_date.isoweekday(),
                    "cells": cells,
                }
            )

        for hour, row in rows:
            if hour not in hour_index:
                continue
            idx = hour_index[hour]
            for day_idx, (col, _) in enumerate(day_columns):
                val = ws.cell(row=row, column=col).value
                try:
                    val = float(val) if val is not None else 0.0
                except (TypeError, ValueError):
                    val = 0.0
                day_data[day_idx]["cells"][idx]["load_pct"] = val

        for day in day_data:
            vals = [c["load_pct"] for c in day["cells"]]
            all_vals.extend(vals)
            day_avg = round(sum(vals) / len(vals), 2) if vals else 0.0
            early = any(
                day["cells"][i]["load_pct"] > 0 for i, h in enumerate(hours) if h < 10
            )
            late = any(
                day["cells"][i]["load_pct"] > 0 for i, h in enumerate(hours) if h >= 22
            )
            day["day_avg"] = day_avg
            day["gray"] = {"early": early, "late": late}

        month_avg = round(sum(all_vals) / len(all_vals), 2) if all_vals else 0.0
        types[type_name] = {
            "days": day_data,
            "month_avg": month_avg,
        }

    return HistoricalMonth(hours=hours, type_order=type_order, types=types)


def _load_excel(path: Path) -> dict[int, dict[str, HistoricalMonth]]:
    wb = load_workbook(path, data_only=True, read_only=True)
    data: dict[int, dict[str, HistoricalMonth]] = {}
    for sheet_name in wb.sheetnames:
        parsed = _parse_sheet_name(sheet_name)
        if not parsed:
            continue
        branch_id, _, month_key = parsed
        ws = wb[sheet_name]
        month_data = _parse_sheet(ws)
        if not month_data:
            continue
        data.setdefault(branch_id, {})[month_key] = month_data
    return data


def _get_data() -> dict[int, dict[str, HistoricalMonth]]:
    path = settings.historical_excel_path
    if not path.exists():
        raise FileNotFoundError(f"Файл не найден: {path}")
    mtime = path.stat().st_mtime
    if _CACHE["data"] is not None and _CACHE["mtime"] == mtime:
        return _CACHE["data"]
    data = _load_excel(path)
    _CACHE["data"] = data
    _CACHE["mtime"] = mtime
    return data


def list_branches() -> list[dict[str, Any]]:
    data = _get_data()
    branches = []
    for code, branch_id in BRANCH_CODE_MAP.items():
        if branch_id in data:
            branches.append({"branch_id": branch_id, "code": code})
    branches.sort(key=lambda b: b["branch_id"])
    return branches


def list_months(branch_id: int) -> list[str]:
    data = _get_data()
    months = sorted(data.get(branch_id, {}).keys())
    return months


def month_payload(branch_id: int, month: str) -> dict[str, Any]:
    data = _get_data()
    month_data = data.get(branch_id, {}).get(month)
    if not month_data:
        raise KeyError("Нет данных за выбранный месяц")
    types_out = []
    for type_name in month_data.type_order:
        type_data = month_data.types.get(type_name)
        if not type_data:
            continue
        days = type_data["days"]
        weeks = _build_weeks(days)
        types_out.append(
            {
                "name": type_name,
                "weeks": weeks,
                "month_avg": type_data["month_avg"],
            }
        )
    return {
        "branch_id": branch_id,
        "month": month,
        "hours": month_data.hours,
        "types": types_out,
    }
