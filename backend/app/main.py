from __future__ import annotations

from datetime import date, datetime, timedelta, time as dt_time
from dataclasses import asdict
from zoneinfo import ZoneInfo
from pathlib import Path
from functools import lru_cache

import json
import logging
import os
import subprocess
import time
from typing import Any

from fastapi import BackgroundTasks, FastAPI, Form, HTTPException, Request, Body
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from .auth import authenticate, require_admin
from .config import settings
from .db import get_conn, get_hist_conn, init_db, init_historical_db, db_source_label, upsert_sql
from .etl import run_full_2025, run_daily
from .groups import load_group_config, ensure_branch_names
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
from .utils import daterange, week_start_monday, resource_sort_key, parse_datetime
from .yclients import build_client
from src.features.cuteam.api import router as cuteam_api
from src.features.cuteam import admin_service as cuteam_admin
from src.features.cuteam.views import router as cuteam_views

BASE_DIR = Path(__file__).resolve().parents[1]

@lru_cache
def _current_commit() -> str:
    env_keys = (
        "RENDER_GIT_COMMIT",
        "RENDER_COMMIT",
        "SOURCE_VERSION",
        "GIT_COMMIT",
        "COMMIT_SHA",
        "VERCEL_GIT_COMMIT_SHA",
    )
    for key in env_keys:
        value = os.getenv(key)
        if value:
            value = value.strip()
            return value[:7] if len(value) > 7 else value
    try:
        repo_root = BASE_DIR.parent
        result = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=str(repo_root),
            text=True,
        ).strip()
        return result or "unknown"
    except Exception:  # noqa: BLE001
        return "unknown"

app = FastAPI(title="CUTEAM Heatmap")
logging.basicConfig(level=logging.INFO)
app.add_middleware(SessionMiddleware, secret_key=settings.session_secret, max_age=60 * 60 * 12)

app.mount(
    "/static",
    StaticFiles(directory=str(BASE_DIR / "app" / "static")),
    name="static",
)

templates = Jinja2Templates(directory=str(BASE_DIR / "app" / "templates"))

app.include_router(cuteam_api)
app.include_router(cuteam_views)

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


def _require_session(request: Request) -> None:
    if not request.session.get("user"):
        raise HTTPException(status_code=401, detail="РќРµ Р°РІС‚РѕСЂРёР·РѕРІР°РЅ")


class _TTLCache:
    def __init__(self) -> None:
        self._store: dict[str, dict[str, Any]] = {}

    def get(self, key: str) -> Any | None:
        item = self._store.get(key)
        if not item:
            return None
        if item["expires_at"] <= time.time():
            self._store.pop(key, None)
            return None
        return item["value"]

    def set(self, key: str, value: Any, ttl_seconds: int) -> None:
        self._store[key] = {"value": value, "expires_at": time.time() + ttl_seconds}


_MINI_CACHE = _TTLCache()
_MINI_SEARCH_TTL = 10 * 60
_MINI_GOOD_TTL = 15 * 60
_MINI_STORAGE_TTL = 60 * 60


def _to_int(value: Any, default: int | None = None) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _to_float(value: Any, default: float | None = None) -> float | None:
    if value is None:
        return default
    if isinstance(value, str):
        value = value.replace(",", ".").strip()
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _record_times(record: dict) -> tuple[datetime | None, datetime | None]:
    start_raw = record.get("datetime") or record.get("date")
    if not start_raw:
        return None, None
    start_dt = parse_datetime(str(start_raw), settings.timezone)
    duration = _to_int(record.get("seance_length") or record.get("length") or 0, 0) or 0
    end_dt = start_dt + timedelta(seconds=duration)
    return start_dt, end_dt


def _record_staff_name(record: dict) -> str:
    staff = record.get("staff") or {}
    return _clean_text(record.get("staff_name") or staff.get("name") or record.get("staff_title") or staff.get("title"))


def _record_client_name(record: dict) -> str:
    client = record.get("client") or {}
    return _clean_text(record.get("client_name") or client.get("name") or client.get("phone"))


def _record_attendance(record: dict) -> int:
    attendance = record.get("attendance")
    if attendance is None:
        attendance = record.get("visit_attendance")
    value = _to_int(attendance, 0)
    return value if value is not None else 0


def _record_comment(record: dict) -> str:
    return _clean_text(record.get("comment"))


def _record_visit_id(record: dict) -> int | None:
    visit_id = record.get("visit_id")
    if visit_id is None:
        visit_id = (record.get("visit") or {}).get("id")
    return _to_int(visit_id)


def _record_document_id(record: dict) -> int | None:
    """Get document_id (type_id=7 is 'Визит') from record for goods transactions."""
    documents = record.get("documents") or []
    for doc in documents:
        if _to_int(doc.get("type_id")) == 7:  # type_id=7 is "Визит"
            return _to_int(doc.get("id"))
    # Fallback: return first document if any
    if documents:
        return _to_int(documents[0].get("id"))
    return None


def _find_goods_tx_id(
    record_data: dict | list,
    good_id: int,
    amount: float | None,
    storage_id: int | None = None,
) -> int | None:
    items = record_data if isinstance(record_data, list) else record_data.get("goods_transactions") or []
    matches = []
    for item in items:
        item_good_id = _to_int(item.get("good_id") or (item.get("good") or {}).get("id"))
        if item_good_id != good_id:
            continue
        item_amount = _to_float(item.get("amount"))
        if amount is not None:
            if item_amount is None or abs(item_amount - amount) > 1e-6:
                continue
        if storage_id is not None:
            item_storage = _to_int(item.get("storage_id"))
            if item_storage is not None and item_storage != storage_id:
                continue
        tx_id = _to_int(item.get("id") or item.get("goods_transaction_id"))
        if tx_id:
            matches.append(tx_id)
    return max(matches) if matches else None


def _record_status(start_dt: datetime, end_dt: datetime, now: datetime) -> str:
    if start_dt <= now <= end_dt:
        return "В процессе"
    if start_dt > now:
        return "Ожидает"
    return "Завершена"


def _fetch_records(client, branch_id: int, start_date: date, end_date: date, max_pages: int = 10) -> list[dict]:
    records_out: list[dict] = []
    page = 1
    count = 50
    while page <= max_pages:
        resp = client.get_records(
            branch_id,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            page=page,
            count=count,
        )
        data = resp.get("data") or []
        if not data:
            break
        records_out.extend(data)
        total = (resp.get("meta") or {}).get("total_count") or 0
        if total and page * count >= total:
            break
        page += 1
    return records_out


def _guess_price(good: dict) -> float:
    price = _to_float(good.get("unit_actual_cost") or good.get("unit_cost"))
    if price is not None:
        return price
    price = _to_float(good.get("actual_cost") or good.get("cost") or good.get("price") or 0) or 0
    unit_equals = _to_float(good.get("unit_equals"))
    if unit_equals and unit_equals > 0:
        return price / unit_equals
    return price


def _extract_good_item(raw: dict) -> dict:
    good_id = _to_int(raw.get("good_id") or raw.get("id") or raw.get("item_id"))
    title = _clean_text(raw.get("title") or raw.get("label") or raw.get("value"))
    unit = _clean_text(raw.get("service_unit_short_title") or raw.get("unit_short_title") or raw.get("unit") or raw.get("service_unit"))
    has_price = any(raw.get(key) is not None for key in ("unit_actual_cost", "unit_cost", "actual_cost", "cost", "price"))
    price = _guess_price(raw) if has_price else None
    return {
        "good_id": good_id,
        "title": title,
        "unit": unit,
        "price": price,
    }


def _sort_goods(items: list[dict], term: str) -> list[dict]:
    needle = term.strip().lower()

    def key(item: dict) -> tuple:
        title = (item.get("title") or "").lower()
        if title == needle:
            rank = 0
        elif title.startswith(needle):
            rank = 1
        elif needle in title:
            rank = 2
        else:
            rank = 3
        return (rank, title)

    return sorted(items, key=key)


def _get_storage_id(client, branch_id: int) -> int | None:
    cache_key = f"mini:storage:{branch_id}"
    cached = _MINI_CACHE.get(cache_key)
    if cached is not None:
        return cached
    storages = []
    try:
        resp = client.get_company(branch_id, include="storages")
        data = resp.get("data") or {}
        storages = data.get("storages") or []
    except Exception:
        storages = []
    if not storages:
        try:
            resp = client.list_storages(branch_id)
            storages = resp.get("data") or []
        except Exception:
            storages = []
    if isinstance(storages, dict):
        storages = [storages]
    storage_id = None
    if storages:
        preferred = None
        for storage in storages:
            if (
                storage.get("for_sale")
                or storage.get("for_services")
                or storage.get("is_default")
                or storage.get("is_main")
                or storage.get("default")
            ):
                preferred = storage
                break
        if not preferred:
            preferred = storages[0]
        storage_id = _to_int(preferred.get("id") or preferred.get("storage_id"))
    if storage_id is not None:
        _MINI_CACHE.set(cache_key, storage_id, _MINI_STORAGE_TTL)
    return storage_id


def _goods_cache_count(branch_id: int) -> int:
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT COUNT(1) AS cnt FROM goods_cache WHERE branch_id = ?",
            (branch_id,),
        )
        row = cur.fetchone()
    return int(row["cnt"] if row and row["cnt"] is not None else 0)


def _goods_cache_search(branch_id: int, term: str, limit: int) -> list[dict]:
    term_lower = term.lower()
    like_any = f"%{term_lower}%"
    like_start = f"{term_lower}%"
    with get_conn() as conn:
        cur = conn.execute(
            """
            SELECT good_id, title, price, unit
            FROM goods_cache
            WHERE branch_id = ?
              AND LOWER(title) LIKE ?
            ORDER BY
              CASE
                WHEN LOWER(title) = ? THEN 0
                WHEN LOWER(title) LIKE ? THEN 1
                ELSE 2
              END,
              title
            LIMIT ?
            """,
            (branch_id, like_any, term_lower, like_start, limit),
        )
        rows = cur.fetchall()
    items = []
    for row in rows:
        items.append(
            {
                "good_id": int(row["good_id"]),
                "title": row["title"],
                "price": row["price"],
                "unit": row["unit"],
            }
        )
    return items


def _goods_cache_upsert(branch_id: int, items: list[dict]) -> int:
    if not items:
        return 0
    now = datetime.utcnow().isoformat()
    rows = []
    for item in items:
        good_id = _to_int(item.get("good_id"))
        title = _clean_text(item.get("title"))
        if not good_id or not title:
            continue
        price_val = _to_float(item.get("price"))
        rows.append(
            (
                branch_id,
                good_id,
                title,
                price_val,
                _clean_text(item.get("unit")),
                now,
            )
        )
    if not rows:
        return 0
    sql = upsert_sql(
        "goods_cache",
        ["branch_id", "good_id", "title", "price", "unit", "updated_at"],
        ["branch_id", "good_id"],
    )
    with get_conn() as conn:
        conn.executemany(sql, rows)
        conn.commit()
    return len(rows)


def _set_goods_cache_status(branch_id: int, status: str, total_count: int | None = None, error: str | None = None) -> None:
    now = datetime.utcnow().isoformat()
    sql = upsert_sql(
        "goods_cache_status",
        ["branch_id", "last_sync", "total_count", "status", "error"],
        ["branch_id"],
    )
    with get_conn() as conn:
        conn.execute(
            sql,
            (
                branch_id,
                now,
                total_count,
                status,
                error,
            ),
        )
        conn.commit()


def _get_goods_cache_status(branch_id: int) -> dict:
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT branch_id, last_sync, total_count, status, error FROM goods_cache_status WHERE branch_id = ?",
            (branch_id,),
        )
        row = cur.fetchone()
    if not row:
        return {
            "branch_id": branch_id,
            "last_sync": None,
            "total_count": _goods_cache_count(branch_id),
            "status": "none",
            "error": None,
        }
    row = dict(row)
    return {
        "branch_id": branch_id,
        "last_sync": row.get("last_sync"),
        "total_count": row.get("total_count"),
        "status": row.get("status"),
        "error": row.get("error"),
    }


def _run_goods_sync(branch_id: int) -> None:
    log = logging.getLogger("goods_sync")
    client = build_client()
    _set_goods_cache_status(branch_id, "running", error=None)
    total = 0
    page = 1
    count = 200
    try:
        while True:
            resp = client.list_goods(branch_id, page=page, count=count)
            data = resp.get("data") or []
            if not data:
                break
            items = [_extract_good_item(raw) for raw in data]
            total += _goods_cache_upsert(branch_id, items)
            if len(data) < count:
                break
            page += 1
        total_count = _goods_cache_count(branch_id)
        _set_goods_cache_status(branch_id, "success", total_count=total_count, error=None)
    except Exception as exc:  # noqa: BLE001
        log.warning("Goods sync failed for branch %s: %s", branch_id, exc)
        total_count = _goods_cache_count(branch_id)
        _set_goods_cache_status(branch_id, "failed", total_count=total_count, error=str(exc))


def _audit_mini(
    action: str,
    branch_id: int,
    record_id: int,
    service_id: int | None = None,
    good_id: int | None = None,
    amount: float | None = None,
    price: float | None = None,
    storage_id: int | None = None,
    tg_user: dict | None = None,
    status: str = "ok",
    error: str | None = None,
) -> None:
    user_id = None
    username = None
    name = None
    if isinstance(tg_user, dict):
        user_id = _clean_text(tg_user.get("id"))
        username = _clean_text(tg_user.get("username"))
        first = _clean_text(tg_user.get("first_name"))
        last = _clean_text(tg_user.get("last_name"))
        name = " ".join([part for part in [first, last] if part]).strip() or None
    created_at = datetime.utcnow().isoformat()
    try:
        with get_conn() as conn:
            conn.execute(
                """
                INSERT INTO mini_app_audit (
                    created_at, action, branch_id, record_id, service_id, good_id,
                    amount, price, storage_id, tg_user_id, tg_username, tg_name, status, error
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    created_at,
                    action,
                    branch_id,
                    record_id,
                    service_id,
                    good_id,
                    amount,
                    price,
                    storage_id,
                    user_id,
                    username,
                    name,
                    status,
                    error,
                ),
            )
            conn.commit()
    except Exception:  # noqa: BLE001
        logging.getLogger("mini_app").warning("Failed to write mini app audit log.")

def _log_mini_yclients_response(
    action: str,
    branch_id: int,
    record_id: int,
    visit_id: int | None,
    payload: dict[str, Any] | None,
    response: dict[str, Any] | None,
    status: str = "ok",
    error: str | None = None,
) -> None:
    log_path = settings.data_dir / "mini_app_yclients.log"
    entry = {
        "ts": datetime.utcnow().isoformat(),
        "action": action,
        "branch_id": branch_id,
        "record_id": record_id,
        "visit_id": visit_id,
        "status": status,
        "error": error,
        "payload": payload,
        "response": response,
    }
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry, ensure_ascii=False, default=str))
            handle.write("\n")
    except Exception:  # noqa: BLE001
        logging.getLogger("mini_app").warning("Failed to log mini app YCLIENTS response.")

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
    return templates.TemplateResponse(
        "admin.html",
        {"request": request, "commit": _current_commit()},
    )

@app.get("/summary", response_class=HTMLResponse)
def summary_page(request: Request):
    if not request.session.get("user"):
        return RedirectResponse("/login", status_code=302)
    return templates.TemplateResponse("summary.html", {"request": request})

@app.get("/mini", response_class=HTMLResponse)
def mini_app_page(request: Request):
    if not request.session.get("user"):
        return RedirectResponse("/login?next=/mini", status_code=302)
    return templates.TemplateResponse("mini.html", {"request": request})

@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    next_url = request.query_params.get("next")
    return templates.TemplateResponse(
        "login.html",
        {"request": request, "error": None, "next_url": next_url},
    )

@app.post("/login", response_class=HTMLResponse)
def login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    next_url: str | None = Form(default=None),
):
    if authenticate(username, password):
        request.session["user"] = username
        target = next_url or "/"
        return RedirectResponse(target, status_code=302)
    return templates.TemplateResponse(
        "login.html",
        {"request": request, "error": "???????????????? ??????????/????????????", "next_url": next_url},
    )

@app.post("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login", status_code=302)

@app.get("/api/branches")
def api_branches(request: Request):
    if not request.session.get("user"):
        raise HTTPException(status_code=401, detail="Не авторизован")
    config = ensure_branch_names(load_group_config())
    branches = [
        {"branch_id": int(b["branch_id"]), "display_name": b.get("display_name", str(b["branch_id"]))}
        for b in config.get("branches", [])
    ]
    return {"branches": branches}


@app.get("/api/mini/branches")
def api_mini_branches(request: Request):
    _require_session(request)
    branches: list[dict] = []
    try:
        config = ensure_branch_names(load_group_config())
        for branch in config.get("branches", []):
            branch_id = _to_int(branch.get("branch_id"))
            if not branch_id:
                continue
            display_name = branch.get("display_name", str(branch_id))
            branches.append({"branch_id": branch_id, "display_name": display_name})
    except Exception as exc:  # noqa: BLE001
        logging.getLogger("mini_app").warning("Failed to load branch config: %s", exc)
        try:
            client = build_client()
            resp = client.get_companies()
            for company in resp.get("data") or []:
                branch_id = _to_int(company.get("id"))
                if not branch_id:
                    continue
                title = _clean_text(company.get("title") or company.get("name") or branch_id)
                branches.append({"branch_id": branch_id, "display_name": title})
        except Exception as exc2:  # noqa: BLE001
            logging.getLogger("mini_app").warning("Failed to load companies: %s", exc2)
    branches.sort(key=lambda item: (item["display_name"] or "").lower())
    return {"branches": branches}


@app.get("/api/mini/records")
def api_mini_records(
    request: Request,
    branch_id: int,
    mode: str = "now",
    q: str = "",
    hours: int = 4,
    limit: int = 60,
):
    _require_session(request)
    client = build_client()
    tz = ZoneInfo(settings.timezone)
    now = datetime.now(tz=tz)
    mode = (mode or "now").lower()
    if mode not in {"now", "today"}:
        mode = "now"
    hours = max(1, min(int(hours or 4), 12))

    if mode == "now":
        range_start = now - timedelta(minutes=30)
        range_end = now + timedelta(hours=hours)
    else:
        today = now.date()
        range_start = datetime.combine(today, dt_time.min).replace(tzinfo=tz)
        range_end = datetime.combine(today, dt_time.max).replace(tzinfo=tz)

    start_date = range_start.date()
    end_date = range_end.date()
    raw_records = _fetch_records(client, branch_id, start_date, end_date)

    needle = (q or "").strip().lower()
    records = []
    for record in raw_records:
        start_dt, end_dt = _record_times(record)
        if not start_dt or not end_dt:
            continue
        if end_dt < range_start or start_dt > range_end:
            continue
        record_id = _to_int(record.get("id"))
        if not record_id:
            continue
        staff_name = _record_staff_name(record)
        client_name = _record_client_name(record)
        if needle:
            haystack = f"{staff_name} {client_name}".lower()
            if needle not in haystack:
                continue
        status = _record_status(start_dt, end_dt, now)
        records.append(
            {
                "record_id": record_id,
                "start_dt": start_dt.isoformat(),
                "end_dt": end_dt.isoformat(),
                "time": start_dt.strftime("%H:%M"),
                "staff_name": staff_name,
                "client_name": client_name,
                "status": status,
            }
        )

    records.sort(key=lambda item: item["start_dt"])
    if limit:
        records = records[: max(1, min(int(limit), 200))]

    return {
        "records": records,
        "now": now.isoformat(),
        "range_start": range_start.isoformat(),
        "range_end": range_end.isoformat(),
    }


@app.get("/api/mini/records/{record_id}")
def api_mini_record_detail(record_id: int, branch_id: int, request: Request):
    _require_session(request)
    client = build_client()
    resp = client.get_record(branch_id, record_id, include_consumables=0, include_finance=0)
    data = resp.get("data") or {}
    start_dt, end_dt = _record_times(data)
    staff_name = _record_staff_name(data)
    client_name = _record_client_name(data)
    services = []
    for service in data.get("services") or []:
        service_id = _to_int(service.get("id") or service.get("service_id"))
        if not service_id:
            continue
        services.append(
            {
                "service_id": service_id,
                "title": _clean_text(service.get("title") or service.get("name")),
            }
        )
    service_id = services[0]["service_id"] if services else None
    storage_id = _to_int(data.get("storage_id")) or _to_int((data.get("storage") or {}).get("id"))
    if not storage_id:
        for service in data.get("services") or []:
            storage_id = _to_int(service.get("storage_id")) or _to_int((service.get("storage") or {}).get("id"))
            if storage_id:
                break
    if not storage_id:
        storage_id = _get_storage_id(client, branch_id)
    return {
        "record": {
            "record_id": record_id,
            "start_dt": start_dt.isoformat() if start_dt else None,
            "end_dt": end_dt.isoformat() if end_dt else None,
            "time": start_dt.strftime("%H:%M") if start_dt else "",
            "staff_name": staff_name,
            "client_name": client_name,
        },
        "services": services,
        "service_id": service_id,
        "storage_id": storage_id,
    }


@app.get("/api/mini/goods/search")
def api_mini_goods_search(request: Request, branch_id: int, term: str, limit: int = 30):
    _require_session(request)
    term = (term or "").strip()
    if len(term) < 2:
        return {"items": [], "source": "api"}
    limit_val = max(1, min(int(limit or 30), 50))
    client = build_client()
    resp = client.search_goods(branch_id, term, count=limit_val)
    items = []
    for raw in resp.get("data") or []:
        item = _extract_good_item(raw)
        if not item.get("good_id") or not item.get("title"):
            continue
        items.append(item)
    items = _sort_goods(items, term)[:limit_val]
    return {"items": items, "source": "api"}


@app.post("/api/mini/records/{record_id}/goods")
def api_mini_add_good(request: Request, record_id: int, payload: dict = Body(default={})):
    """Add a good to a record using YCLIENTS update_visit API.
    
    Uses goods_transactions per YCLIENTS support recommendation:
    - amount: negative value (e.g., -10 for 10 grams)
    - price: unit price
    - cost: total cost (price * abs(amount))
    """
    _require_session(request)
    branch_id = _to_int(payload.get("branch_id"))
    good_id = _to_int(payload.get("good_id"))
    amount = _to_float(payload.get("amount"))
    tg_user = payload.get("tg_user") or {}
    attendance_override = _to_int(payload.get("attendance"))
    comment_override = payload.get("comment")
    good_special_number = _clean_text(payload.get("good_special_number") or "")
    price_override = _to_float(payload.get("price"))

    if not branch_id:
        raise HTTPException(status_code=400, detail="branch_id is required")
    if not good_id:
        raise HTTPException(status_code=400, detail="good_id is required")
    if amount is None or amount <= 0:
        raise HTTPException(status_code=400, detail="amount must be positive")

    client = build_client()
    storage_id = _to_int(payload.get("storage_id")) or _get_storage_id(client, branch_id)
    if not storage_id:
        raise HTTPException(status_code=400, detail="storage_id not found for branch")

    record_resp = client.get_record(branch_id, record_id, include_consumables=0, include_finance=0)
    record_data = record_resp.get("data") or {}
    visit_id = _record_visit_id(record_data)
    if not visit_id:
        raise HTTPException(status_code=404, detail="visit_id not found for record")

    attendance = attendance_override if attendance_override is not None else _record_attendance(record_data)
    comment = comment_override if comment_override is not None else record_data.get("comment")
    comment = _clean_text(comment)

    good_cache_key = f"mini:good:{branch_id}:{good_id}"
    good_data = _MINI_CACHE.get(good_cache_key)
    if good_data is None:
        good_resp = client.get_good(branch_id, good_id)
        good_data = good_resp.get("data") or {}
        _MINI_CACHE.set(good_cache_key, good_data, _MINI_GOOD_TTL)
    price = price_override if price_override is not None else _guess_price(good_data)

    # Per YCLIENTS support: amount must be NEGATIVE for goods consumed/sold
    tx_amount = -abs(amount)
    total_cost = price * abs(amount)

    # Build goods_transactions item per YCLIENTS support recommendation
    goods_item = {
        "good_id": good_id,
        "storage_id": storage_id,
        "amount": tx_amount,
        "price": price,
        "cost": total_cost,
        "good_special_number": good_special_number,
    }

    visit_payload = {
        "attendance": attendance,
        "comment": comment,
        "services": [],
        "goods_transactions": [goods_item],
    }

    try:
        resp = client.update_visit(visit_id, record_id, visit_payload)
    except Exception as exc:  # noqa: BLE001
        _log_mini_yclients_response(
            "add", branch_id, record_id, visit_id, visit_payload, None,
            status="error", error=str(exc),
        )
        _audit_mini(
            "add", branch_id, record_id,
            good_id=good_id, amount=amount, price=price,
            storage_id=storage_id, tg_user=tg_user,
            status="error", error=str(exc),
        )
        raise HTTPException(status_code=502, detail=f"Ошибка YCLIENTS: {exc}") from exc

    _log_mini_yclients_response(
        "add", branch_id, record_id, visit_id, visit_payload, resp,
    )

    added_tx = _find_goods_tx_id(resp.get("data") or {}, good_id, tx_amount, storage_id)
    if not added_tx:
        try:
            latest = client.get_record(branch_id, record_id, include_consumables=0, include_finance=0)
            added_tx = _find_goods_tx_id(latest.get("data") or {}, good_id, tx_amount, storage_id)
        except Exception:  # noqa: BLE001
            added_tx = None

    _audit_mini(
        "add", branch_id, record_id,
        good_id=good_id, amount=amount, price=price,
        storage_id=storage_id, tg_user=tg_user, status="ok",
    )

    return {
        "status": "ok",
        "added": {
            "good_id": good_id,
            "amount": amount,
            "price": price,
            "goods_transaction_id": added_tx,
        },
    }

@app.post("/api/mini/records/{record_id}/goods/undo")
def api_mini_undo_good(request: Request, record_id: int, payload: dict = Body(default={})):
    _require_session(request)
    branch_id = _to_int(payload.get("branch_id"))
    goods_transaction_id = _to_int(payload.get("goods_transaction_id"))
    service_id = _to_int(payload.get("service_id"))
    tg_user = payload.get("tg_user") or {}
    if not branch_id or not goods_transaction_id:
        raise HTTPException(status_code=400, detail="branch_id and goods_transaction_id are required")
    client = build_client()

    record_resp = client.get_record(branch_id, record_id, include_consumables=0, include_finance=0)
    record_data = record_resp.get("data") or {}
    visit_id = _record_visit_id(record_data)
    if not visit_id:
        raise HTTPException(status_code=404, detail="visit_id not found for record")

    removed = None
    for item in record_data.get("goods_transactions") or []:
        item_tx = _to_int(item.get("id") or item.get("goods_transaction_id"))
        if item_tx != goods_transaction_id:
            continue
        removed = {
            "good_id": _to_int(item.get("good_id") or (item.get("good") or {}).get("id")),
            "amount": _to_float(item.get("amount")),
            "price": _to_float(item.get("price") or item.get("cost") or item.get("cost_per_unit")),
            "storage_id": _to_int(item.get("storage_id")),
        }
        break

    visit_payload = {
        "attendance": _record_attendance(record_data),
        "comment": _record_comment(record_data),
        "services": [],
        "deleted_transaction_ids": [goods_transaction_id],
    }

    try:
        client.update_visit(visit_id, record_id, visit_payload)
    except Exception as exc:  # noqa: BLE001
        _audit_mini(
            "undo",
            branch_id,
            record_id,
            service_id=service_id,
            good_id=removed.get("good_id") if removed else None,
            amount=removed.get("amount") if removed else None,
            price=removed.get("price") if removed else None,
            storage_id=removed.get("storage_id") if removed else None,
            tg_user=tg_user,
            status="error",
            error=str(exc),
        )
        raise HTTPException(status_code=502, detail=f"Ошибка YCLIENTS: {exc}") from exc

    _audit_mini(
        "undo",
        branch_id,
        record_id,
        service_id=service_id,
        good_id=removed.get("good_id") if removed else None,
        amount=removed.get("amount") if removed else None,
        price=removed.get("price") if removed else None,
        storage_id=removed.get("storage_id") if removed else None,
        tg_user=tg_user,
        status="ok",
    )

    return {"status": "ok"}


@app.get("/api/months")
def api_months(request: Request):
    if not request.session.get("user"):
        raise HTTPException(status_code=401, detail="Не авторизован")
    start_date = settings.branch_start_date or date(2025, 1, 1)
    start_month = date(start_date.year, start_date.month, 1)
    tz = ZoneInfo(settings.timezone)
    now = datetime.now(tz=tz).date()
    end_month = date(now.year, now.month, 1)
    months = []
    cursor = start_month
    while cursor <= end_month:
        months.append(cursor.strftime("%Y-%m"))
        if cursor.month == 12:
            cursor = date(cursor.year + 1, 1, 1)
        else:
            cursor = date(cursor.year, cursor.month + 1, 1)
    if not months:
        months = [end_month.strftime("%Y-%m")]
    months.sort(reverse=True)
    return {"months": months, "current": end_month.strftime("%Y-%m")}

@app.get("/api/historical/branches")
def api_historical_branches(request: Request):
    if not request.session.get("user"):
        raise HTTPException(status_code=401, detail="Не авторизован")
    config = ensure_branch_names(load_group_config())
    name_map = {
        int(b["branch_id"]): b.get("display_name", str(b["branch_id"]))
        for b in config.get("branches", [])
    }
    try:
        branches = hist_list_branches()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    for item in branches:
        branch_id = int(item.get("branch_id"))
        display_name = name_map.get(branch_id)
        if display_name:
            item["display_name"] = display_name
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


@app.get("/api/admin/cuteam/status")
def api_cuteam_status(request: Request):
    require_admin(request)
    status = cuteam_admin.get_status()
    return asdict(status)


@app.post("/api/admin/cuteam/sync")
def api_cuteam_sync(request: Request, background: BackgroundTasks, payload: dict = Body(default={})):
    require_admin(request)
    sheet_names = payload.get("sheet_names") or []
    dry_run = bool(payload.get("dry_run"))
    if isinstance(sheet_names, str):
        sheet_names = [name.strip() for name in sheet_names.split(",") if name.strip()]
    try:
        task = cuteam_admin.start_sync(sheet_names, dry_run=dry_run)
    except RuntimeError as exc:
        detail = str(exc)
        code = 409 if "already running" in detail else 400
        raise HTTPException(status_code=code, detail=detail) from exc
    background.add_task(task)
    return {"status": "started", "sheets": sheet_names, "dry_run": dry_run}


@app.post("/api/admin/cuteam/import-plans-checks")
def api_cuteam_import_plans_checks(request: Request, background: BackgroundTasks):
    require_admin(request)
    try:
        task = cuteam_admin.start_import_plans_checks()
    except RuntimeError as exc:
        detail = str(exc)
        code = 409 if "already running" in detail else 400
        raise HTTPException(status_code=code, detail=detail) from exc
    background.add_task(task)
    return {"status": "started"}


@app.get("/api/admin/yclients-debug-log")
def api_yclients_debug_log(request: Request, lines: int = 50):
    """Get last N lines from YCLIENTS API debug log."""
    require_admin(request)
    log_path = settings.data_dir / "yclients_api_debug.log"
    if not log_path.exists():
        return {"lines": [], "message": "Log file not found. No API calls logged yet."}
    try:
        with log_path.open("r", encoding="utf-8") as f:
            all_lines = f.readlines()
        last_lines = all_lines[-lines:] if len(all_lines) > lines else all_lines
        entries = []
        for line in last_lines:
            line = line.strip()
            if line:
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    entries.append({"raw": line})
        return {"lines": entries, "total": len(all_lines)}
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}


@app.delete("/api/admin/yclients-debug-log")
def api_clear_yclients_debug_log(request: Request):
    """Clear YCLIENTS API debug log."""
    require_admin(request)
    log_path = settings.data_dir / "yclients_api_debug.log"
    if log_path.exists():
        log_path.unlink()
    return {"status": "cleared"}


@app.get("/api/branches/{branch_id}/groups")
def api_groups(branch_id: int, request: Request):
    if not request.session.get("user"):
        raise HTTPException(status_code=401, detail="Не авторизован")
    config = load_group_config()
    branch = next((b for b in config.get("branches", []) if int(b["branch_id"]) == branch_id), None)
    if not branch:
        raise HTTPException(status_code=404, detail="Филиал не найден")
    groups = [{"group_id": g["group_id"], "name": g["name"]} for g in branch.get("groups", [])]
    indexed = list(enumerate(groups))
    indexed.sort(key=lambda item: resource_sort_key(item[1].get("name"), item[0]))
    return {"groups": [item[1] for item in indexed]}

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
    indexed_groups = list(enumerate(groups))
    indexed_groups.sort(key=lambda item: resource_sort_key(item[1].get("name"), item[0]))
    groups = [item[1] for item in indexed_groups]
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


HISTORICAL_RESOURCE_MAP = {
    "ЗАЛ ВК": "Рабочее место визажиста",
    "ЗАЛ ПДК": "Рабочее место мастера педикюра",
    "ЗАЛ МК": "Рабочее место мастера маникюра",
    "КАБ К/М": "Кабинет косметолога/массажиста",
    "КАБ ПК": "Кабинет стилиста-парикмахера",
    "ЗАЛ ПК": "Рабочее место парикмахера",
}


def _map_historical_resource(resource_type: str, branch_name: str | None) -> str | None:
    key = (resource_type or "").strip()
    if not key:
        return None
    name = branch_name or ""
    if "Матч Поинт" in name and key in {"ЗАЛ ПК/ВК", "ЗАЛ ПК"}:
        return "Рабочее место парикмахера"
    if key == "ЗАЛ ПК/ВК":
        return "Рабочее место парикмахера"
    return HISTORICAL_RESOURCE_MAP.get(key)


@app.get("/api/heatmap/summary")
def api_heatmap_summary(
    request: Request,
    start_year: int = 2024,
    end_year: int | None = None,
):
    if not request.session.get("user"):
        raise HTTPException(status_code=401, detail="Не авторизован")
    if end_year is None:
        end_year = datetime.now(ZoneInfo(settings.timezone)).year
    if start_year > end_year:
        raise HTTPException(status_code=400, detail="Некорректный диапазон лет")
    years = list(range(start_year, end_year + 1))
    months = [
        {"num": 1, "label": "Январь", "short": "Янв"},
        {"num": 2, "label": "Февраль", "short": "Фев"},
        {"num": 3, "label": "Март", "short": "Мар"},
        {"num": 4, "label": "Апрель", "short": "Апр"},
        {"num": 5, "label": "Май", "short": "Май"},
        {"num": 6, "label": "Июнь", "short": "Июн"},
        {"num": 7, "label": "Июль", "short": "Июл"},
        {"num": 8, "label": "Август", "short": "Авг"},
        {"num": 9, "label": "Сентябрь", "short": "Сен"},
        {"num": 10, "label": "Октябрь", "short": "Окт"},
        {"num": 11, "label": "Ноябрь", "short": "Ноя"},
        {"num": 12, "label": "Декабрь", "short": "Дек"},
    ]
    start_date = date(start_year, 1, 1)
    end_date = date(end_year, 12, 31)
    start_ym = f"{start_year:04d}-01"
    end_ym = f"{end_year:04d}-12"
    hist_end_ym = min(end_ym, "2025-02")
    hist_values: dict[int, dict[str, dict[str, float]]] = {}
    if start_ym <= hist_end_ym:
        init_historical_db()
        with get_hist_conn() as hist_conn:
            cur = hist_conn.execute(
                """
                SELECT branch_id, month, resource_type, AVG(load_pct) AS avg_load
                FROM historical_loads
                WHERE month BETWEEN ? AND ? AND hour BETWEEN 10 AND 21
                GROUP BY branch_id, month, resource_type
                """,
                (start_ym, hist_end_ym),
            )
            for row in cur.fetchall():
                try:
                    branch_id = int(row["branch_id"])
                except Exception:
                    continue
                month = row["month"]
                avg = row["avg_load"]
                if avg is None:
                    continue
                hist_values.setdefault(branch_id, {}).setdefault(str(row["resource_type"]), {})[
                    month
                ] = round(float(avg), 2)
    config = ensure_branch_names(load_group_config())
    branches_out: list[dict[str, Any]] = []
    with get_conn() as conn:
        for branch in config.get("branches", []):
            branch_id = _to_int(branch.get("branch_id"))
            if not branch_id:
                continue
            effective_start = start_date
            branch_start = _branch_start_date(branch_id)
            if branch_start and branch_start > effective_start:
                effective_start = branch_start
            values_by_group: dict[str, dict[str, float]] = {}
            if effective_start <= end_date:
                cur = conn.execute(
                    """
                    SELECT group_id, substr(date, 1, 7) AS ym, AVG(load_pct) AS avg_load
                    FROM group_hour_load
                    WHERE branch_id = ? AND date BETWEEN ? AND ? AND hour BETWEEN 10 AND 21
                    GROUP BY group_id, ym
                    """,
                    (branch_id, effective_start.isoformat(), end_date.isoformat()),
                )
                for row in cur.fetchall():
                    group_id = str(row["group_id"])
                    ym = row["ym"]
                    avg = row["avg_load"]
                    if avg is None:
                        continue
                    values_by_group.setdefault(group_id, {})[ym] = round(float(avg), 2)
            groups = branch.get("groups", [])
            display_name = branch.get("display_name") or str(branch_id)
            group_id_by_name = {
                (g.get("name") or ""): str(g.get("group_id") or "") for g in groups
            }
            hist_branch = hist_values.get(branch_id) or {}
            if hist_branch:
                for resource_type, month_values in hist_branch.items():
                    group_name = _map_historical_resource(resource_type, display_name)
                    if not group_name:
                        continue
                    group_id = group_id_by_name.get(group_name)
                    if not group_id:
                        continue
                    for ym, avg in month_values.items():
                        values_by_group.setdefault(group_id, {})[ym] = avg
            indexed_groups = list(enumerate(groups))
            indexed_groups.sort(key=lambda item: resource_sort_key(item[1].get("name"), item[0]))
            groups_out: list[dict[str, Any]] = []
            for _, group in indexed_groups:
                group_id = str(group.get("group_id") or "")
                groups_out.append(
                    {
                        "group_id": group_id,
                        "name": group.get("name") or group_id,
                        "values": values_by_group.get(group_id, {}),
                    }
                )
            branches_out.append(
                {
                    "branch_id": branch_id,
                    "display_name": display_name,
                    "groups": groups_out,
                }
            )
    branch_order = [
        "Символ",
        "Матч Поинт (ул. Василисы Кожиной д.13)",
        "Шелепиха (Шелепихинская набережная, 34к4)",
        "CUTEAM СПб (м. Чернышевская)",
        "CUTEAM СПб (м. Чкаловская)",
    ]

    def branch_sort_key(item: dict[str, Any]) -> tuple[int, int, str]:
        name = item.get("display_name") or ""
        for idx, token in enumerate(branch_order):
            if token in name:
                return (0, idx, name)
        return (1, len(branch_order), name)

    branches_out.sort(key=branch_sort_key)
    return {"years": years, "months": months, "branches": branches_out}

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
def api_start_full(request: Request, background: BackgroundTasks, payload: dict = Body(default={})):
    require_admin(request)
    branch_id = _to_int(payload.get("branch_id"))
    if branch_id is not None:
        config = load_group_config()
        if not any(int(b["branch_id"]) == branch_id for b in config.get("branches", [])):
            raise HTTPException(status_code=400, detail="Unknown branch_id")
    client = build_client()
    background.add_task(run_full_2025, client, branch_id)
    return {"status": "started", "branch_id": branch_id}


@app.post("/api/admin/etl/daily/start")
def api_start_daily(request: Request, background: BackgroundTasks):
    require_admin(request)
    client = build_client()
    background.add_task(run_daily, client, None)
    return {"status": "started"}

@app.get("/api/admin/etl/status")
def api_status(request: Request):
    require_admin(request)
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT run_id, run_type, branch_id, started_at, finished_at, status, progress, error_log FROM etl_runs ORDER BY started_at DESC LIMIT 1"
        )
        row = cur.fetchone()
    if not row:
        return {"status": "none"}
    return dict(row)


@app.get("/api/admin/etl/full/last")
def api_full_last(request: Request):
    require_admin(request)
    config = ensure_branch_names(load_group_config())
    branches = [
        {
            "branch_id": int(b["branch_id"]),
            "display_name": b.get("display_name", str(b["branch_id"])),
        }
        for b in config.get("branches", [])
    ]
    last_map: dict[int, dict] = {}
    fallback: dict | None = None
    with get_conn() as conn:
        cur = conn.execute(
            """
            SELECT branch_id, status, started_at, finished_at
            FROM etl_runs
            WHERE run_type = ?
            ORDER BY started_at DESC
            """,
            ("full_2025",),
        )
        for row in cur.fetchall():
            branch_id = row["branch_id"]
            record = {
                "status": row["status"],
                "started_at": row["started_at"],
                "finished_at": row["finished_at"],
            }
            if branch_id is None:
                if fallback is None:
                    fallback = record
                continue
            branch_id_int = int(branch_id)
            if branch_id_int not in last_map:
                last_map[branch_id_int] = record
    for branch in branches:
        record = last_map.get(branch["branch_id"]) or fallback
        branch["last_status"] = record.get("status") if record else None
        branch["last_full"] = (record or {}).get("finished_at") or (record or {}).get("started_at")
    return {"branches": branches}


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
