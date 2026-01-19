from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


def parse_datetime(value: str, tz_name: str) -> datetime:
    if not value:
        raise ValueError("Empty datetime value")
    value = value.strip()
    # Support "YYYY-MM-DD HH:MM:SS" and ISO with offset
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        # Fallback: replace space with T
        dt = datetime.fromisoformat(value.replace(" ", "T"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo(tz_name))
    else:
        dt = dt.astimezone(ZoneInfo(tz_name))
    return dt


def daterange(start_date, end_date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def week_start_monday(date_obj):
    return date_obj - timedelta(days=date_obj.weekday())
