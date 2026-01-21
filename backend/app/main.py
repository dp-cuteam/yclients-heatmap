from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import logging

from fastapi import BackgroundTasks, FastAPI, Form, HTTPException, Request, Body
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, PlainTextResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from .auth import authenticate, require_admin
from .config import settings
from .db import get_conn, init_db, init_historical_db, db_source_label
from .diagnostics import (
    run_diagnostics,
    _latest_log_info,
    run_support_packet,
    latest_support_packet_info,
)
from .etl import run_full_2025
from .groups import load_group_config
from .historical import (
    list_branches as hist_list_branches,
    list_months as hist_list_months,
    month_payload as hist_month_payload,
    start_import as hist_start_import,
    run_import as hist_run_import,
    last_import_status as hist_last_import_status,
    list_root_files as hist_list_root_files,
)
from .scheduler import start_scheduler, stop_scheduler
from .staff_audit import run_staff_audit
from .utils import daterange, week_start_monday
from .yclients import build_client


BASE_DIR = Path(__file__).resolve().parents[1]

app = FastAPI(title="CUTEAM Heatmap")
logging.basicConfig(level=logging.INFO)
app.add_middleware(SessionMiddleware, secret_key=settings.session_secret, max_age=60 * 60 * 12)

app.mount(
    "/static",
    StaticFiles(directory=str(BASE_DIR / "app" / "static")),
    name="static",
)

templates = Jinja2Templates(directory=str(BASE_DIR / "app" / "templates"))


@app.on_event("startup")
def on_startup():
    init_db()
    init_historical_db()
    if settings.enable_scheduler:
        start_scheduler()
    else:
        logging.getLogger("scheduler").info("Scheduler disabled (ENABLE_SCHEDULER=0)")


@app.on_event("shutdown")
def on_shutdown():
    stop_scheduler()


def _get_group(branch_id: int, group_id: str) -> dict:
    config = load_group_config()
    branch = next((b for b in config.get("branches", []) if int(b["branch_id"]) == branch_id), None)
    if not branch:
        raise HTTPException(status_code=404, detail="Филиал не найден")
    group = next((g for g in branch.get("groups", []) if g["group_id"] == group_id), None)
    if not group:
        raise HTTPException(status_code=404, detail="Группа не найдена")
    return group


def _branch_start_date(branch_id: int) -> date | None:
    if settings.branch_start_date is None:
        return None
    if settings.active_branch_ids and branch_id not in settings.active_branch_ids:
        return None
    return settings.branch_start_date


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    if not request.session.get("user"):
        return RedirectResponse("/login", status_code=302)
    branch_start = settings.branch_start_date.isoformat() if settings.branch_start_date else ""
    return templates.TemplateResponse(
        "dashboard.html",
        {"request": request, "branch_start_date": branch_start},
    )


@app.get("/historical", response_class=HTMLResponse)
def historical_page(request: Request):
    if not request.session.get("user"):
        return RedirectResponse("/login", status_code=302)
    return templates.TemplateResponse("historical.html", {"request": request})


@app.get("/admin", response_class=HTMLResponse)
def admin_page(request: Request):
    if not request.session.get("user"):
        return RedirectResponse("/login", status_code=302)
    return templates.TemplateResponse("admin.html", {"request": request})


@app.get("/admin/diagnostics", response_class=HTMLResponse)
def diagnostics_page(request: Request):
    if not request.session.get("user"):
        return RedirectResponse("/login", status_code=302)
    return templates.TemplateResponse("diagnostics.html", {"request": request})


@app.get("/staff-types", response_class=HTMLResponse)
def staff_types_page(request: Request):
    if not request.session.get("user"):
        return RedirectResponse("/login", status_code=302)
    return templates.TemplateResponse("staff_types.html", {"request": request})


@app.get("/admin/staff-slots", response_class=HTMLResponse)
def staff_slots_page(request: Request):
    if not request.session.get("user"):
        return RedirectResponse("/login", status_code=302)
    return templates.TemplateResponse("staff_slots.html", {"request": request})


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request, "error": None})


@app.post("/login", response_class=HTMLResponse)
def login_submit(request: Request, username: str = Form(...), password: str = Form(...)):
    if authenticate(username, password):
        request.session["user"] = username
        return RedirectResponse("/", status_code=302)
    return templates.TemplateResponse("login.html", {"request": request, "error": "Неверный логин/пароль"})


@app.post("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login", status_code=302)


@app.get("/api/branches")
def api_branches(request: Request):
    if not request.session.get("user"):
        raise HTTPException(status_code=401, detail="Не авторизован")
    config = load_group_config()
    branches = [
        {"branch_id": int(b["branch_id"]), "display_name": b.get("display_name", str(b["branch_id"]))}
        for b in config.get("branches", [])
    ]
    return {"branches": branches}


@app.get("/api/historical/branches")
def api_historical_branches(request: Request):
    if not request.session.get("user"):
        raise HTTPException(status_code=401, detail="Не авторизован")
    try:
        branches = hist_list_branches()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"branches": branches}


@app.get("/api/historical/branches/{branch_id}/months")
def api_historical_months(branch_id: int, request: Request):
    if not request.session.get("user"):
        raise HTTPException(status_code=401, detail="Не авторизован")
    try:
        months = hist_list_months(branch_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"months": months}


@app.get("/api/historical/month")
def api_historical_month(branch_id: int, month: str, request: Request):
    if not request.session.get("user"):
        raise HTTPException(status_code=401, detail="Не авторизован")
    try:
        payload = hist_month_payload(branch_id, month)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return payload


@app.get("/api/admin/historical/status")
def api_historical_status(request: Request):
    require_admin(request)
    status = hist_last_import_status()
    status["db_path"] = str(settings.historical_db_path)
    status["db_exists"] = settings.historical_db_path.exists()
    return status


@app.get("/api/admin/historical/files")
def api_historical_files(request: Request):
    require_admin(request)
    return {"files": hist_list_root_files()}


@app.post("/api/admin/historical/import")
def api_historical_import(request: Request, background: BackgroundTasks, payload: dict = Body(default={})):
    require_admin(request)
    mode = (payload.get("mode") or "replace").lower()
    if mode not in {"replace", "append"}:
        mode = "replace"
    run_id = hist_start_import(mode)
    background.add_task(hist_run_import, run_id, mode)
    return {"status": "started", "run_id": run_id}


@app.get("/api/branches/{branch_id}/groups")
def api_groups(branch_id: int, request: Request):
    if not request.session.get("user"):
        raise HTTPException(status_code=401, detail="Не авторизован")
    config = load_group_config()
    branch = next((b for b in config.get("branches", []) if int(b["branch_id"]) == branch_id), None)
    if not branch:
        raise HTTPException(status_code=404, detail="Филиал не найден")
    groups = [{"group_id": g["group_id"], "name": g["name"]} for g in branch.get("groups", [])]
    return {"groups": groups}


@app.get("/api/months/{month}/weeks")
def api_weeks(month: str, request: Request):
    if not request.session.get("user"):
        raise HTTPException(status_code=401, detail="Не авторизован")
    try:
        year, mon = month.split("-")
        year = int(year)
        mon = int(mon)
        first = date(year, mon, 1)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail="Некорректный месяц") from exc
    # Monday-based weeks covering the month
    start = week_start_monday(first)
    weeks = []
    current = start
    last_day = (date(year, mon + 1, 1) - timedelta(days=1)) if mon < 12 else date(year, 12, 31)
    while current <= last_day:
        weeks.append(current.isoformat())
        current += timedelta(days=7)
    return {"weeks": weeks}


@app.get("/api/heatmap")
def api_heatmap(branch_id: int, group_id: str, week_start: str, request: Request):
    if not request.session.get("user"):
        raise HTTPException(status_code=401, detail="Не авторизован")
    try:
        week_start_date = date.fromisoformat(week_start)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Некорректная дата начала недели") from exc
    week_end = week_start_date + timedelta(days=6)
    effective_start = week_start_date
    branch_start = _branch_start_date(branch_id)
    if branch_start and branch_start > effective_start:
        effective_start = branch_start
    if effective_start > week_end:
        return {"week_start": week_start_date.isoformat(), "hours": list(range(8, 24)), "days": []}
    group = _get_group(branch_id, group_id)
    staff_ids = [int(x) for x in group.get("staff_ids", [])]

    hours = list(range(8, 24))
    days = []

    with get_conn() as conn:
        cur = conn.execute(
            """
            SELECT date, hour, load_pct, busy_count, staff_total
            FROM group_hour_load
            WHERE branch_id = ? AND group_id = ? AND date BETWEEN ? AND ? AND hour BETWEEN 8 AND 23
            """,
            (branch_id, group_id, effective_start.isoformat(), week_end.isoformat()),
        )
        rows = cur.fetchall()
        by_day_hour = {(r["date"], int(r["hour"])): r for r in rows}


    for day in daterange(effective_start, week_end):
        day_str = day.isoformat()
        cells = []
        for hour in hours:
            row = by_day_hour.get((day_str, hour))
            if row:
                cells.append(
                    {
                        "load_pct": row["load_pct"],
                        "busy_count": row["busy_count"],
                        "staff_total": row["staff_total"],
                    }
                )
            else:
                cells.append({"load_pct": 0, "busy_count": 0, "staff_total": len(staff_ids)})
        days.append(
            {
                "date": day_str,
                "dow": day.isoweekday(),
                "cells": cells,
            }
        )

    return {"week_start": week_start_date.isoformat(), "hours": hours, "days": days}


@app.get("/api/heatmap/month")
def api_heatmap_month(branch_id: int, group_id: str, month: str, request: Request):
    if not request.session.get("user"):
        raise HTTPException(status_code=401, detail="Не авторизован")
    try:
        year, mon = month.split("-")
        year = int(year)
        mon = int(mon)
        first = date(year, mon, 1)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail="Некорректный месяц") from exc
    last_day = (date(year, mon + 1, 1) - timedelta(days=1)) if mon < 12 else date(year, 12, 31)
    effective_start = first
    branch_start = _branch_start_date(branch_id)
    if branch_start and branch_start > effective_start:
        effective_start = branch_start
    if effective_start > last_day:
        return {"month": month, "hours": list(range(8, 24)), "weeks": [], "month_avg": 0.0}

    group = _get_group(branch_id, group_id)
    staff_ids = [int(x) for x in group.get("staff_ids", [])]
    hours = list(range(8, 24))
    bench_hours = {h for h in hours if 10 <= h <= 21}

    with get_conn() as conn:
        cur = conn.execute(
            """
            SELECT date, hour, load_pct, busy_count, staff_total
            FROM group_hour_load
            WHERE branch_id = ? AND group_id = ? AND date BETWEEN ? AND ? AND hour BETWEEN 8 AND 23
            """,
            (branch_id, group_id, effective_start.isoformat(), last_day.isoformat()),
        )
        rows = cur.fetchall()
        by_day_hour = {(r["date"], int(r["hour"])): r for r in rows}


    days_map = {}
    all_vals = []
    for day in daterange(effective_start, last_day):
        day_str = day.isoformat()
        cells = []
        bench_vals = []
        for hour in hours:
            row = by_day_hour.get((day_str, hour))
            if row:
                val = float(row["load_pct"])
                cells.append(
                    {
                        "load_pct": val,
                        "busy_count": row["busy_count"],
                        "staff_total": row["staff_total"],
                    }
                )
            else:
                cells.append({"load_pct": 0.0, "busy_count": 0, "staff_total": len(staff_ids)})
            if hour in bench_hours:
                bench_vals.append(cells[-1]["load_pct"])
        all_vals.extend(bench_vals)
        day_avg = round(sum(bench_vals) / len(bench_vals), 2) if bench_vals else 0.0
        days_map[day_str] = {
            "date": day_str,
            "dow": day.isoweekday(),
            "cells": cells,
            "day_avg": day_avg,
        }

    # build week blocks (Monday-Sunday), include only days within month
    weeks = []
    current = week_start_monday(effective_start)
    while current <= last_day:
        week_end = current + timedelta(days=6)
        week_days = []
        week_vals = []
        for day in daterange(current, week_end):
            if day < effective_start or day > last_day:
                continue
            day_str = day.isoformat()
            day_obj = days_map[day_str]
            week_days.append(day_obj)
            for idx, hour in enumerate(hours):
                if hour in bench_hours:
                    week_vals.append(day_obj["cells"][idx]["load_pct"])
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

    month_avg = round(sum(all_vals) / len(all_vals), 2) if all_vals else 0.0
    return {"month": month, "hours": hours, "weeks": weeks, "month_avg": month_avg}


@app.get("/api/heatmap/status")
def api_heatmap_status(branch_id: int, month: str, request: Request):
    if not request.session.get("user"):
        raise HTTPException(status_code=401, detail="Не авторизован")
    try:
        year, mon = month.split("-")
        year = int(year)
        mon = int(mon)
        first = date(year, mon, 1)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail="Некорректный месяц") from exc
    last_day = (date(year, mon + 1, 1) - timedelta(days=1)) if mon < 12 else date(year, 12, 31)
    effective_start = first
    branch_start = _branch_start_date(branch_id)
    if branch_start and branch_start > effective_start:
        effective_start = branch_start
    source_label = db_source_label()
    db_exists = True if source_label == "Postgres" else settings.db_path.exists()
    db_path = "DATABASE_URL" if source_label == "Postgres" else str(settings.db_path)
    if effective_start > last_day:
        return {
            "branch_id": branch_id,
            "month": month,
            "source": source_label,
            "db_path": db_path,
            "db_exists": db_exists,
            "last_updated": None,
            "total_rows": 0,
            "group_counts": [],
        }
    config = load_group_config()
    branch = next((b for b in config.get("branches", []) if int(b["branch_id"]) == branch_id), None)
    if not branch:
        raise HTTPException(status_code=404, detail="Филиал не найден")
    groups = branch.get("groups", [])
    with get_conn() as conn:
        cur = conn.execute(
            """
            SELECT group_id, COUNT(*) as cnt
            FROM group_hour_load
            WHERE branch_id = ? AND date BETWEEN ? AND ? AND hour BETWEEN 8 AND 23
            GROUP BY group_id
            """,
            (branch_id, effective_start.isoformat(), last_day.isoformat()),
        )
        counts = {row["group_id"]: int(row["cnt"]) for row in cur.fetchall()}
        cur2 = conn.execute(
            "SELECT MAX(updated_at) as last_updated FROM raw_records WHERE branch_id = ?",
            (branch_id,),
        )
        last_updated = cur2.fetchone()["last_updated"]
        if not last_updated:
            cur3 = conn.execute(
                "SELECT finished_at FROM etl_runs ORDER BY started_at DESC LIMIT 1"
            )
            row = cur3.fetchone()
            last_updated = row["finished_at"] if row else None

    total_rows = sum(counts.values())
    status_log = logging.getLogger("heatmap_status")
    if total_rows == 0:
        status_log.warning(
            "No heatmap data: branch=%s month=%s db=%s",
            branch_id,
            month,
            source_label,
        )

    group_counts = []
    for g in groups:
        gid = g.get("group_id")
        cnt = counts.get(gid, 0)
        if cnt == 0:
            status_log.info(
                "Zero aggregates: branch=%s group=%s month=%s db=%s",
                branch_id,
                gid,
                month,
                source_label,
            )
        group_counts.append({"group_id": gid, "name": g.get("name"), "count": cnt})

    return {
        "branch_id": branch_id,
        "month": month,
        "source": source_label,
        "db_path": db_path,
        "db_exists": db_exists,
        "last_updated": last_updated,
        "total_rows": total_rows,
        "group_counts": group_counts,
    }


@app.get("/api/summary/month")
def api_summary(branch_id: int, group_id: str, month: str, request: Request):
    if not request.session.get("user"):
        raise HTTPException(status_code=401, detail="Не авторизован")
    try:
        year, mon = month.split("-")
        year = int(year)
        mon = int(mon)
        first = date(year, mon, 1)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail="Некорректный месяц") from exc
    last_day = (date(year, mon + 1, 1) - timedelta(days=1)) if mon < 12 else date(year, 12, 31)

    with get_conn() as conn:
        cur = conn.execute(
            """
            SELECT date, hour, load_pct
            FROM group_hour_load
            WHERE branch_id = ? AND group_id = ? AND date BETWEEN ? AND ? AND in_benchmark = 1
            """,
            (branch_id, group_id, first.isoformat(), last_day.isoformat()),
        )
        rows = cur.fetchall()

    by_date = {}
    for r in rows:
        by_date.setdefault(r["date"], []).append(float(r["load_pct"]))

    avg_day = []
    for day in daterange(first, last_day):
        day_str = day.isoformat()
        vals = by_date.get(day_str, [])
        avg = round(sum(vals) / len(vals), 2) if vals else 0.0
        avg_day.append({"date": day_str, "avg": avg})

    # weekly averages within month
    avg_week = []
    current = week_start_monday(first)
    while current <= last_day:
        week_end = current + timedelta(days=6)
        vals = []
        for day in daterange(current, week_end):
            day_str = day.isoformat()
            if day < first or day > last_day:
                continue
            vals.extend(by_date.get(day_str, []))
        avg = round(sum(vals) / len(vals), 2) if vals else 0.0
        avg_week.append({"week_start": current.isoformat(), "avg": avg})
        current += timedelta(days=7)

    all_vals = []
    for vals in by_date.values():
        all_vals.extend(vals)
    avg_month = round(sum(all_vals) / len(all_vals), 2) if all_vals else 0.0

    return {"avg_day": avg_day, "avg_week": avg_week, "avg_month": avg_month}


@app.post("/api/admin/etl/full_2025/start")
def api_start_full(request: Request, background: BackgroundTasks):
    require_admin(request)
    client = build_client()
    background.add_task(run_full_2025, client)
    return {"status": "started"}


@app.get("/api/admin/etl/status")
def api_status(request: Request):
    require_admin(request)
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT run_id, run_type, started_at, finished_at, status, progress, error_log FROM etl_runs ORDER BY started_at DESC LIMIT 1"
        )
        row = cur.fetchone()
    if not row:
        return {"status": "none"}
    return dict(row)


@app.post("/api/admin/diagnostics/run")
def api_diagnostics_run(request: Request, payload: dict = Body(default={})):
    require_admin(request)
    branch_id = payload.get("branch_id")
    day = payload.get("date")
    staff_id = payload.get("staff_id")
    try:
        if branch_id is not None:
            branch_id = int(branch_id)
    except Exception:
        branch_id = None
    try:
        if staff_id is not None:
            staff_id = int(staff_id)
    except Exception:
        staff_id = None
    result = run_diagnostics(branch_id=branch_id, day=day, staff_id=staff_id)
    return result


@app.get("/api/admin/diagnostics/log/tail")
def api_diagnostics_log_tail(request: Request, lines: int = 200):
    require_admin(request)
    info = _latest_log_info()
    path = Path(info["path"])
    if not path.exists():
        return PlainTextResponse("")
    with path.open("r", encoding="utf-8") as f:
        content = f.readlines()[-lines:]
    return PlainTextResponse("".join(content))


@app.get("/api/admin/diagnostics/log/download")
def api_diagnostics_log_download(request: Request):
    require_admin(request)
    info = _latest_log_info()
    path = Path(info["path"])
    if not path.exists():
        raise HTTPException(status_code=404, detail="Лог не найден")
    return FileResponse(path)



@app.post("/api/admin/diagnostics/support-packet")
def api_support_packet(request: Request, payload: dict = Body(default={})):
    require_admin(request)
    branch_ids = payload.get("branch_ids")
    day = payload.get("date")
    parsed_ids: list[int] | None = None
    if branch_ids:
        if isinstance(branch_ids, str):
            parts = [p.strip() for p in branch_ids.replace(";", ",").split(",") if p.strip()]
            parsed_ids = []
            for part in parts:
                try:
                    parsed_ids.append(int(part))
                except Exception:
                    continue
        elif isinstance(branch_ids, list):
            parsed_ids = []
            for item in branch_ids:
                try:
                    parsed_ids.append(int(item))
                except Exception:
                    continue
    result = run_support_packet(branch_ids=parsed_ids, day=day)
    return result


@app.get("/api/admin/diagnostics/support-packet/download")
def api_support_packet_download(request: Request, format: str = "md"):
    require_admin(request)
    info = latest_support_packet_info()
    path = info["md_path"] if format == "md" else info["json_path"]
    file_path = Path(path)
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(file_path)


@app.get("/api/branches/{branch_id}/staff-types")
def api_staff_types(branch_id: int, request: Request):
    if not request.session.get("user"):
        raise HTTPException(status_code=401, detail="РќРµ Р°РІС‚РѕСЂРёР·РѕРІР°РЅ")
    config = load_group_config()
    branch = next((b for b in config.get("branches", []) if int(b["branch_id"]) == branch_id), None)
    if not branch:
        raise HTTPException(status_code=404, detail="Р¤РёР»РёР°Р» РЅРµ РЅР°Р№РґРµРЅ")
    type_map = {}
    type_list = []
    for group in branch.get("groups", []):
        type_name = group.get("name") or group.get("group_id")
        type_list.append(type_name)
        for staff_name in group.get("staff_names", []):
            if staff_name:
                type_map[staff_name.strip()] = type_name

    client = build_client()
    staff_resp = client.get_staff(branch_id)
    staff_list = []
    for item in staff_resp.get("data") or []:
        staff_id = item.get("id")
        if staff_id is None:
            continue
        name = (item.get("name") or "").strip()
        staff_list.append(
            {
                "id": staff_id,
                "name": name,
                "type": type_map.get(name) or "Не классифицирован",
                "specialization": item.get("specialization") or "",
                "position": (item.get("position") or {}).get("title") or "",
            }
        )
    staff_list.sort(key=lambda s: (s.get("type") or "", s.get("name") or "", s.get("id") or 0))
    return {
        "branch_id": branch_id,
        "types": type_list,
        "staff": staff_list,
    }


@app.post("/api/admin/staff-slots/run")
def api_staff_slots_run(request: Request, payload: dict = Body(default={})):
    require_admin(request)
    branch_id = payload.get("branch_id")
    day = payload.get("date")
    try:
        branch_id = int(branch_id)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail="РќРµ Р·Р°РґР°РЅ branch_id") from exc
    if day:
        try:
            date.fromisoformat(day)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="РќРµРєРѕСЂСЂРµРєС‚РЅР°СЏ РґР°С‚Р°") from exc
    try:
        return run_staff_audit(branch_id=branch_id, day=day)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/api/webhooks/yclients")
async def yclients_webhook(request: Request):
    # Minimal webhook receiver to satisfy YCLIENTS app requirement
    payload = await request.body()
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    log_path = settings.data_dir / "yclients_webhooks.log"
    with log_path.open("ab") as f:
        f.write(payload + b"\n")
    return {"ok": True}
