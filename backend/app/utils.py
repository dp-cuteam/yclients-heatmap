from __future__ import annotations

from datetime import datetime, timedelta
import re
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


_CABINET_TOKEN = "\u043a\u0430\u0431\u0438\u043d\u0435\u0442"
_HAIR_TOKEN = "\u043f\u0430\u0440\u0438\u043a\u043c\u0430\u0445\u0435\u0440"
_MAKEUP_TOKEN = "\u0432\u0438\u0437\u0430\u0436"
_NAIL_TOKENS = ("\u043c\u0430\u043d\u0438\u043a\u044e\u0440", "\u043f\u0435\u0434\u0438\u043a\u044e\u0440")
_PLACE_TOKEN = "\u043c\u0435\u0441\u0442"
_CABINET_NUMBER_RE = re.compile(r"\u2116\s*(\d+)")


def _extract_cabinet_number(text: str) -> int:
    match = _CABINET_NUMBER_RE.search(text)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return 9999
    return 9999


def resource_sort_key(name: str, fallback_index: int | None = None) -> tuple:
    text = (name or "").strip().lower()
    if _CABINET_TOKEN in text:
        return (4, _extract_cabinet_number(text), text)
    if _HAIR_TOKEN in text and _PLACE_TOKEN in text:
        return (0, fallback_index if fallback_index is not None else 0, text)
    if _MAKEUP_TOKEN in text:
        return (1, fallback_index if fallback_index is not None else 0, text)
    if any(token in text for token in _NAIL_TOKENS):
        return (2, fallback_index if fallback_index is not None else 0, text)
    return (3, fallback_index if fallback_index is not None else 0, text)
