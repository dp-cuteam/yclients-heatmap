from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Iterable

from .config import settings
from .db import get_conn
from .groups import load_group_config, resolve_staff_ids, save_group_config
from .utils import parse_datetime, daterange
from .yclients import YClientsClient


ATTENDANCE_FACT = {1, 2}


def _start_run(run_type: str) -> str:
    run_id = str(uuid.uuid4())
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO etl_runs(run_id, run_type, started_at, status, progress, error_log)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (run_id, run_type, now, "running", "0%", ""),
        )
        conn.commit()
    return run_id


def _update_run(run_id: str, status: str | None = None, progress: str | None = None, error: str | None = None, finished: bool = False) -> None:
    fields = []
    params = []
    if status is not None:
        fields.append("status = ?")
        params.append(status)
    if progress is not None:
        fields.append("progress = ?")
        params.append(progress)
    if error is not None:
        fields.append("error_log = COALESCE(error_log, '') || ?")
        params.append(f"\n{error}")
    if finished:
        fields.append("finished_at = ?")
        params.append(datetime.utcnow().isoformat())
    if not fields:
        return
    params.append(run_id)
    with get_conn() as conn:
        conn.execute(
            f"UPDATE etl_runs SET {', '.join(fields)} WHERE run_id = ?",
            params,
        )
        conn.commit()


def _iter_hours(start_dt: datetime, end_dt: datetime) -> Iterable[datetime]:
    current = start_dt.replace(minute=0, second=0, microsecond=0)
    while current < end_dt:
        yield current
        current += timedelta(hours=1)


def _upsert_raw_records(records: list[dict]) -> None:
    with get_conn() as conn:
        conn.executemany(
            """
            INSERT OR REPLACE INTO raw_records
            (branch_id, staff_id, record_id, start_dt, end_dt, attendance, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            records,
        )
        conn.commit()


def _rebuild_staff_hour_busy(branch_id: int, date_from: date, date_to: date, records: list[dict]) -> None:
    rows = {}
    for rec in records:
        start_dt = rec["start_dt"]
        end_dt = rec["end_dt"]
        staff_id = rec["staff_id"]
        for hour_dt in _iter_hours(start_dt, end_dt):
            day = hour_dt.date().isoformat()
            hour = hour_dt.hour
            in_benchmark = 1 if 10 <= hour <= 21 else 0
            in_gray = 1 if (hour < 10 or hour >= 22) else 0
            key = (branch_id, staff_id, day, hour)
            rows[key] = (branch_id, staff_id, day, hour, 1, in_benchmark, in_gray)

    with get_conn() as conn:
        conn.execute(
            "DELETE FROM staff_hour_busy WHERE branch_id = ? AND date BETWEEN ? AND ?",
            (branch_id, date_from.isoformat(), date_to.isoformat()),
        )
        if rows:
            conn.executemany(
                """
                INSERT OR REPLACE INTO staff_hour_busy
                (branch_id, staff_id, date, hour, busy_flag, in_benchmark, in_gray)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                rows.values(),
            )
        conn.commit()


def _rebuild_group_hour_load(branch_id: int, group_config: dict, date_from: date, date_to: date) -> None:
    branch = next((b for b in group_config.get("branches", []) if int(b["branch_id"]) == branch_id), None)
    if not branch:
        return
    groups = branch.get("groups", [])
    group_sets = []
    for g in groups:
        staff_ids = [int(x) for x in g.get("staff_ids", [])]
        group_sets.append((g["group_id"], set(staff_ids)))

    busy_by_day_hour = {}
    with get_conn() as conn:
        cur = conn.execute(
            """
            SELECT staff_id, date, hour
            FROM staff_hour_busy
            WHERE branch_id = ? AND date BETWEEN ? AND ? AND busy_flag = 1
            """,
            (branch_id, date_from.isoformat(), date_to.isoformat()),
        )
        for row in cur.fetchall():
            key = (row["date"], int(row["hour"]))
            busy_by_day_hour.setdefault(key, set()).add(int(row["staff_id"]))

        conn.execute(
            "DELETE FROM group_hour_load WHERE branch_id = ? AND date BETWEEN ? AND ?",
            (branch_id, date_from.isoformat(), date_to.isoformat()),
        )

        insert_rows = []
        for day in daterange(date_from, date_to):
            day_str = day.isoformat()
            dow = day.isoweekday()
            for hour in range(24):
                busy_set = busy_by_day_hour.get((day_str, hour), set())
                in_benchmark = 1 if 10 <= hour <= 21 else 0
                for group_id, staff_set in group_sets:
                    staff_total = len(staff_set)
                    if staff_total == 0:
                        busy_count = 0
                        load_pct = 0.0
                    else:
                        busy_count = len(staff_set.intersection(busy_set))
                        load_pct = round((busy_count / staff_total) * 100, 2)
                    insert_rows.append(
                        (
                            branch_id,
                            group_id,
                            day_str,
                            dow,
                            hour,
                            busy_count,
                            staff_total,
                            load_pct,
                            in_benchmark,
                        )
                    )
        if insert_rows:
            conn.executemany(
                """
                INSERT OR REPLACE INTO group_hour_load
                (branch_id, group_id, date, dow, hour, busy_count, staff_total, load_pct, in_benchmark)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                insert_rows,
            )
        conn.commit()


def _fetch_records_for_period(client: YClientsClient, branch_id: int, start_date: date, end_date: date, progress_cb) -> list[dict]:
    records_out = []
    page = 1
    count = 50
    while True:
        resp = client.get_records(
            branch_id,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            page=page,
            count=count,
        )
        data = resp.get("data") or []
        meta = resp.get("meta") or {}
        total = meta.get("total_count") or 0
        progress_cb(branch_id, page, total)
        if not data:
            break
        records_out.extend(data)
        if total and page * count >= total:
            break
        page += 1
    return records_out


def _normalize_records(branch_id: int, records: list[dict]) -> list[dict]:
    normalized = []
    for rec in records:
        attendance = rec.get("attendance")
        if attendance is None:
            attendance = rec.get("visit_attendance")
        try:
            attendance = int(attendance)
        except (TypeError, ValueError):
            continue
        if attendance not in ATTENDANCE_FACT:
            continue
        staff_id = rec.get("staff_id")
        record_id = rec.get("id")
        if staff_id is None or record_id is None:
            continue
        start_raw = rec.get("datetime") or rec.get("date")
        if not start_raw:
            continue
        start_dt = parse_datetime(start_raw, settings.timezone)
        duration = rec.get("seance_length") or rec.get("length") or 0
        try:
            duration = int(duration)
        except (TypeError, ValueError):
            duration = 0
        end_dt = start_dt + timedelta(seconds=duration)
        updated_at = rec.get("last_change_date") or rec.get("create_date") or datetime.utcnow().isoformat()
        normalized.append(
            {
                "branch_id": branch_id,
                "staff_id": int(staff_id),
                "record_id": int(record_id),
                "start_dt": start_dt,
                "end_dt": end_dt,
                "attendance": int(attendance),
                "updated_at": updated_at,
            }
        )
    return normalized


def _to_raw_row(rec: dict) -> tuple:
    return (
        rec["branch_id"],
        rec["staff_id"],
        rec["record_id"],
        rec["start_dt"].isoformat(),
        rec["end_dt"].isoformat(),
        rec["attendance"],
        rec["updated_at"],
    )


def run_full_2025(client: YClientsClient) -> str:
    run_id = _start_run("full_2025")
    try:
        config = load_group_config()
        resolved = resolve_staff_ids(config, client)
        save_group_config(resolved)

        for branch in resolved.get("branches", []):
            branch_id = int(branch["branch_id"])
            start_date = date(2025, 1, 1)
            if settings.branch_start_date and (
                not settings.active_branch_ids or branch_id in settings.active_branch_ids
            ):
                if settings.branch_start_date > start_date:
                    start_date = settings.branch_start_date
            end_date = date(2025, 12, 31)

            def progress_cb(bid, page, total):
                if total:
                    _update_run(run_id, progress=f"{bid}: page {page} / ~{total}")
                else:
                    _update_run(run_id, progress=f"{bid}: page {page}")

            raw_records = _fetch_records_for_period(client, branch_id, start_date, end_date, progress_cb)
            normalized = _normalize_records(branch_id, raw_records)
            _upsert_raw_records([_to_raw_row(r) for r in normalized])

            _rebuild_staff_hour_busy(branch_id, start_date, end_date, normalized)
            _rebuild_group_hour_load(branch_id, resolved, start_date, end_date)

        _update_run(run_id, status="success", progress="100%", finished=True)
    except Exception as exc:  # noqa: BLE001
        _update_run(run_id, status="failed", error=str(exc), finished=True)
    return run_id


def run_daily(client: YClientsClient, target_day: date | None = None) -> str:
    run_id = _start_run("daily")
    try:
        config = load_group_config()
        resolved = resolve_staff_ids(config, client)
        save_group_config(resolved)

        if target_day is None:
            # yesterday in configured timezone
            tz = ZoneInfo(settings.timezone)
            now = datetime.now(tz=tz)
            target_day = (now - timedelta(days=1)).date()

        for branch in resolved.get("branches", []):
            branch_id = int(branch["branch_id"])
            if settings.branch_start_date and target_day < settings.branch_start_date:
                continue

            def progress_cb(bid, page, total):
                _update_run(run_id, progress=f"{bid}: page {page}")

            raw_records = _fetch_records_for_period(client, branch_id, target_day, target_day, progress_cb)
            normalized = _normalize_records(branch_id, raw_records)
            _upsert_raw_records([_to_raw_row(r) for r in normalized])

            _rebuild_staff_hour_busy(branch_id, target_day, target_day, normalized)
            _rebuild_group_hour_load(branch_id, resolved, target_day, target_day)

        _update_run(run_id, status="success", progress="100%", finished=True)
    except Exception as exc:  # noqa: BLE001
        _update_run(run_id, status="failed", error=str(exc), finished=True)
    return run_id
