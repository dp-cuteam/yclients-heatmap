from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import logging

from fastapi import BackgroundTasks, FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from .auth import authenticate, require_admin
from .config import settings
from .db import get_conn, init_db
from .etl import run_full_2025
from .groups import load_group_config
from .scheduler import start_scheduler, stop_scheduler
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
    start_scheduler()


@app.on_event("shutdown")
def on_shutdown():
    stop_scheduler()


def _get_group(branch_id: int, group_id: str) -> dict:
    config = load_group_config()
    branch = next((b for b in config.get("branches", []) if int(b["branch_id"]) == branch_id), None)
    if not branch:
        raise HTTPException(status_code=404, detail="Branch not found")
    group = next((g for g in branch.get("groups", []) if g["group_id"] == group_id), None)
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    return group


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    if not request.session.get("user"):
        return RedirectResponse("/login", status_code=302)
    return templates.TemplateResponse("dashboard.html", {"request": request})


@app.get("/admin", response_class=HTMLResponse)
def admin_page(request: Request):
    if not request.session.get("user"):
        return RedirectResponse("/login", status_code=302)
    return templates.TemplateResponse("admin.html", {"request": request})


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
        raise HTTPException(status_code=401, detail="Unauthorized")
    config = load_group_config()
    branches = [
        {"branch_id": int(b["branch_id"]), "display_name": b.get("display_name", str(b["branch_id"]))}
        for b in config.get("branches", [])
    ]
    return {"branches": branches}


@app.get("/api/branches/{branch_id}/groups")
def api_groups(branch_id: int, request: Request):
    if not request.session.get("user"):
        raise HTTPException(status_code=401, detail="Unauthorized")
    config = load_group_config()
    branch = next((b for b in config.get("branches", []) if int(b["branch_id"]) == branch_id), None)
    if not branch:
        raise HTTPException(status_code=404, detail="Branch not found")
    groups = [{"group_id": g["group_id"], "name": g["name"]} for g in branch.get("groups", [])]
    return {"groups": groups}


@app.get("/api/months/{month}/weeks")
def api_weeks(month: str, request: Request):
    if not request.session.get("user"):
        raise HTTPException(status_code=401, detail="Unauthorized")
    try:
        year, mon = month.split("-")
        year = int(year)
        mon = int(mon)
        first = date(year, mon, 1)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail="Invalid month") from exc
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
        raise HTTPException(status_code=401, detail="Unauthorized")
    try:
        week_start_date = date.fromisoformat(week_start)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid week_start") from exc
    week_end = week_start_date + timedelta(days=6)
    group = _get_group(branch_id, group_id)
    staff_ids = [int(x) for x in group.get("staff_ids", [])]

    hours = list(range(10, 22))
    days = []

    with get_conn() as conn:
        cur = conn.execute(
            """
            SELECT date, hour, load_pct, busy_count, staff_total
            FROM group_hour_load
            WHERE branch_id = ? AND group_id = ? AND date BETWEEN ? AND ? AND hour BETWEEN 10 AND 21
            """,
            (branch_id, group_id, week_start_date.isoformat(), week_end.isoformat()),
        )
        rows = cur.fetchall()
        by_day_hour = {(r["date"], int(r["hour"])): r for r in rows}

        gray = {}
        if staff_ids:
            placeholders = ",".join("?" for _ in staff_ids)
            cur2 = conn.execute(
                f"""
                SELECT date, hour
                FROM staff_hour_busy
                WHERE branch_id = ? AND staff_id IN ({placeholders})
                  AND date BETWEEN ? AND ?
                  AND busy_flag = 1
                  AND (hour < 10 OR hour >= 22)
                """,
                [branch_id, *staff_ids, week_start_date.isoformat(), week_end.isoformat()],
            )
            for r in cur2.fetchall():
                key = r["date"]
                if key not in gray:
                    gray[key] = {"early": False, "late": False}
                if int(r["hour"]) < 10:
                    gray[key]["early"] = True
                else:
                    gray[key]["late"] = True

    for day in daterange(week_start_date, week_end):
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
                "gray": gray.get(day_str, {"early": False, "late": False}),
            }
        )

    return {"week_start": week_start_date.isoformat(), "hours": hours, "days": days}


@app.get("/api/summary/month")
def api_summary(branch_id: int, group_id: str, month: str, request: Request):
    if not request.session.get("user"):
        raise HTTPException(status_code=401, detail="Unauthorized")
    try:
        year, mon = month.split("-")
        year = int(year)
        mon = int(mon)
        first = date(year, mon, 1)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail="Invalid month") from exc
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
