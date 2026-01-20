from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from .config import settings
from .utils import parse_datetime
from .yclients import build_client, YClientsClient


ATTENDANCE_FACT = {1, 2}


def _attendance_value(rec: dict[str, Any]) -> int | None:
    value = rec.get("attendance")
    if value is None:
        value = rec.get("visit_attendance")
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _fetch_records_for_day(
    client: YClientsClient,
    branch_id: int,
    target_day: str,
) -> list[dict[str, Any]]:
    page = 1
    count = 50
    records: list[dict[str, Any]] = []
    total_count = None

    while True:
        resp = client.get_records(
            branch_id,
            start_date=target_day,
            end_date=target_day,
            page=page,
            count=count,
        )
        data = resp.get("data") or []
        meta = resp.get("meta") or {}
        if total_count is None:
            total_count = meta.get("total_count") or meta.get("total") or 0
        if not data:
            break
        records.extend(data)
        if total_count and page * count >= total_count:
            break
        if not total_count and len(data) < count:
            break
        page += 1

    return records


def _default_day() -> str:
    tz = ZoneInfo(settings.timezone)
    now = datetime.now(tz=tz)
    return (now - timedelta(days=1)).date().isoformat()


def run_staff_audit(branch_id: int, day: str | None = None) -> dict[str, Any]:
    target_day = day or _default_day()
    client = build_client()

    staff_resp = client.get_staff(branch_id)
    staff_data = staff_resp.get("data") or []
    staff_list: list[dict[str, Any]] = []
    staff_map: dict[int, str] = {}
    for item in staff_data:
        staff_id = item.get("id")
        if staff_id is None:
            continue
        try:
            staff_id = int(staff_id)
        except (TypeError, ValueError):
            continue
        name = item.get("name") or item.get("title") or ""
        staff_list.append({"id": staff_id, "name": name})
        staff_map[staff_id] = name

    staff_list.sort(key=lambda s: (s.get("name") or "", s["id"]))

    records = _fetch_records_for_day(client, branch_id, target_day)
    total_records = len(records)
    fact_records = 0
    slots_by_staff: dict[int, list[dict[str, Any]]] = {}
    unknown_staff_ids: set[int] = set()

    for rec in records:
        attendance = _attendance_value(rec)
        if attendance in ATTENDANCE_FACT:
            fact_records += 1
        if attendance not in ATTENDANCE_FACT:
            continue
        staff_id = rec.get("staff_id")
        if staff_id is None:
            continue
        try:
            staff_id = int(staff_id)
        except (TypeError, ValueError):
            continue
        start_raw = rec.get("datetime") or rec.get("date")
        if not start_raw:
            continue
        try:
            start_dt = parse_datetime(start_raw, settings.timezone)
        except Exception:  # noqa: BLE001
            continue
        duration = rec.get("seance_length") or rec.get("length") or 0
        try:
            duration = int(duration)
        except (TypeError, ValueError):
            duration = 0
        end_dt = start_dt + timedelta(seconds=duration)
        slots_by_staff.setdefault(staff_id, []).append(
            {
                "record_id": rec.get("id"),
                "start": start_dt.isoformat(),
                "end": end_dt.isoformat(),
                "start_time": start_dt.strftime("%H:%M"),
                "end_time": end_dt.strftime("%H:%M"),
                "attendance": attendance,
            }
        )
        if staff_id not in staff_map:
            unknown_staff_ids.add(staff_id)

    for slots in slots_by_staff.values():
        slots.sort(key=lambda s: s.get("start") or "")

    staff_output: list[dict[str, Any]] = []
    for staff in staff_list:
        staff_id = staff["id"]
        slots = slots_by_staff.get(staff_id, [])
        staff_output.append(
            {
                "id": staff_id,
                "name": staff.get("name") or "",
                "slots": slots,
                "slot_count": len(slots),
            }
        )

    unknown_output = []
    for staff_id in sorted(unknown_staff_ids):
        slots = slots_by_staff.get(staff_id, [])
        unknown_output.append(
            {
                "id": staff_id,
                "name": "Неизвестный сотрудник",
                "slots": slots,
                "slot_count": len(slots),
            }
        )

    tz = ZoneInfo(settings.timezone)
    generated_at = datetime.now(tz=tz).isoformat()

    return {
        "branch_id": branch_id,
        "date": target_day,
        "timezone": settings.timezone,
        "generated_at": generated_at,
        "staff_count": len(staff_list),
        "records_total": total_records,
        "records_fact": fact_records,
        "staff": staff_output,
        "unknown_staff": unknown_output,
    }
