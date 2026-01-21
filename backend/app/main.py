from __future__ import annotations

from datetime import date, datetime, timedelta, time as dt_time
from zoneinfo import ZoneInfo
from pathlib import Path

import logging
import time
from typing import Any

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
    good_id = _to_int(raw.get("good_id") or raw.get("id"))
    title = _clean_text(raw.get("title") or raw.get("label") or raw.get("value"))
    unit = _clean_text(raw.get("service_unit_short_title") or raw.get("unit_short_title") or raw.get("unit") or raw.get("service_unit"))
    price = _guess_price(raw)
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
    resp = client.get_company(branch_id, include="storages")
    data = resp.get("data") or {}
    storages = data.get("storages") or []
    if isinstance(storages, dict):
        storages = [storages]
    storage_id = None
    if storages:
        preferred = None
        for storage in storages:
            if storage.get("is_default") or storage.get("is_main") or storage.get("default"):
                preferred = storage
                break
        if not preferred:
            preferred = storages[0]
        storage_id = _to_int(preferred.get("id"))
    _MINI_CACHE.set(cache_key, storage_id, _MINI_STORAGE_TTL)
    return storage_id


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

@app.get("/mini", response_class=HTMLResponse)
def mini_app_page(request: Request):
    return templates.TemplateResponse("mini.html", {"request": request})

@app.get("/admin/diagnostics", response_class=HTMLResponse)
def diagnostics_page(request: Request):
    if not request.session.get("user"):
        return RedirectResponse("/login", status_code=302)
    return RedirectResponse("/admin", status_code=302)

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
    config = ensure_branch_names(load_group_config())
    branches = [
        {"branch_id": int(b["branch_id"]), "display_name": b.get("display_name", str(b["branch_id"]))}
        for b in config.get("branches", [])
    ]
    return {"branches": branches}


@app.get("/api/mini/branches")
def api_mini_branches():
    client = build_client()
    branches: list[dict] = []
    try:
        resp = client.get_companies()
        for company in resp.get("data") or []:
            branch_id = _to_int(company.get("id"))
            if not branch_id:
                continue
            if settings.active_branch_ids and branch_id not in settings.active_branch_ids:
                continue
            title = _clean_text(company.get("title") or company.get("name") or branch_id)
            branches.append({"branch_id": branch_id, "display_name": title})
    except Exception as exc:  # noqa: BLE001
        logging.getLogger("mini_app").warning("Failed to load companies: %s", exc)
        config = ensure_branch_names(load_group_config())
        for branch in config.get("branches", []):
            branch_id = _to_int(branch.get("branch_id"))
            if not branch_id:
                continue
            branches.append(
                {
                    "branch_id": branch_id,
                    "display_name": branch.get("display_name", str(branch_id)),
                }
            )
    branches.sort(key=lambda item: (item["display_name"] or "").lower())
    return {"branches": branches}


@app.get("/api/mini/records")
def api_mini_records(
    branch_id: int,
    mode: str = "now",
    q: str = "",
    hours: int = 4,
    limit: int = 60,
):
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
def api_mini_record_detail(record_id: int, branch_id: int):
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
def api_mini_goods_search(branch_id: int, term: str, limit: int = 30):
    term = (term or "").strip()
    if len(term) < 2:
        return {"items": []}
    cache_key = f"mini:goods:{branch_id}:{term.lower()}"
    cached = _MINI_CACHE.get(cache_key)
    if cached is not None:
        return {"items": cached}
    client = build_client()
    resp = client.search_goods(branch_id, term, count=min(int(limit or 30), 50))
    items = []
    for raw in resp.get("data") or []:
        item = _extract_good_item(raw)
        if not item.get("good_id") or not item.get("title"):
            continue
        items.append(item)
    items = _sort_goods(items, term)
    limit_val = max(1, min(int(limit or 30), 50))
    items = items[:limit_val]
    _MINI_CACHE.set(cache_key, items, _MINI_SEARCH_TTL)
    return {"items": items}


@app.post("/api/mini/records/{record_id}/goods")
def api_mini_add_good(record_id: int, payload: dict = Body(default={})):
    branch_id = _to_int(payload.get("branch_id"))
    good_id = _to_int(payload.get("good_id"))
    amount = _to_float(payload.get("amount"))
    service_id = _to_int(payload.get("service_id"))
    tg_user = payload.get("tg_user") or {}
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

    if not service_id:
        record_resp = client.get_record(branch_id, record_id, include_consumables=0, include_finance=0)
        services = record_resp.get("data") or {}
        services = services.get("services") or []
        if services:
            service_id = _to_int(services[0].get("id") or services[0].get("service_id"))
    if not service_id:
        cons_resp = client.get_record_consumables(branch_id, record_id)
        for service in cons_resp.get("data") or []:
            service_id = _to_int(service.get("service_id"))
            if service_id:
                break
    if not service_id:
        raise HTTPException(status_code=404, detail="service_id not found for record")

    good_cache_key = f"mini:good:{branch_id}:{good_id}"
    good_data = _MINI_CACHE.get(good_cache_key)
    if good_data is None:
        good_resp = client.get_good(branch_id, good_id)
        good_data = good_resp.get("data") or {}
        _MINI_CACHE.set(good_cache_key, good_data, _MINI_GOOD_TTL)
    price = _guess_price(good_data)

    consumables_resp = client.get_record_consumables(branch_id, record_id)
    existing: list[dict] = []
    for service in consumables_resp.get("data") or []:
        if _to_int(service.get("service_id")) != service_id:
            continue
        for item in service.get("consumables") or []:
            item_good_id = _to_int(item.get("good_id") or (item.get("good") or {}).get("id"))
            if not item_good_id:
                continue
            existing.append(
                {
                    "goods_transaction_id": _to_int(item.get("goods_transaction_id"), 0) or 0,
                    "record_id": record_id,
                    "service_id": service_id,
                    "storage_id": _to_int(item.get("storage_id")) or storage_id,
                    "good_id": item_good_id,
                    "price": _to_float(item.get("price")) or 0,
                    "amount": _to_float(item.get("amount")) or 0,
                }
            )
        break

    new_item = {
        "goods_transaction_id": 0,
        "record_id": record_id,
        "service_id": service_id,
        "storage_id": storage_id,
        "good_id": good_id,
        "price": price,
        "amount": amount,
    }
    payload_items = existing + [new_item]
    try:
        resp = client.set_record_consumables(branch_id, record_id, service_id, payload_items)
    except Exception as exc:  # noqa: BLE001
        _audit_mini(
            "add",
            branch_id,
            record_id,
            service_id=service_id,
            good_id=good_id,
            amount=amount,
            price=price,
            storage_id=storage_id,
            tg_user=tg_user,
            status="error",
            error=str(exc),
        )
        raise HTTPException(status_code=502, detail=f"Ошибка YCLIENTS: {exc}") from exc

    added_tx = None
    for service in resp.get("data") or []:
        if _to_int(service.get("service_id")) != service_id:
            continue
        for item in service.get("consumables") or []:
            if _to_int(item.get("good_id")) == good_id and _to_float(item.get("amount")) == amount:
                tx_id = _to_int(item.get("goods_transaction_id"))
                if tx_id:
                    added_tx = tx_id
        break

    _audit_mini(
        "add",
        branch_id,
        record_id,
        service_id=service_id,
        good_id=good_id,
        amount=amount,
        price=price,
        storage_id=storage_id,
        tg_user=tg_user,
        status="ok",
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
def api_mini_undo_good(record_id: int, payload: dict = Body(default={})):
    branch_id = _to_int(payload.get("branch_id"))
    service_id = _to_int(payload.get("service_id"))
    goods_transaction_id = _to_int(payload.get("goods_transaction_id"))
    tg_user = payload.get("tg_user") or {}
    if not branch_id or not service_id or not goods_transaction_id:
        raise HTTPException(status_code=400, detail="branch_id, service_id, goods_transaction_id are required")
    client = build_client()
    storage_id_default = _get_storage_id(client, branch_id)

    consumables_resp = client.get_record_consumables(branch_id, record_id)
    updated: list[dict] = []
    removed = None
    for service in consumables_resp.get("data") or []:
        if _to_int(service.get("service_id")) != service_id:
            continue
        for item in service.get("consumables") or []:
            item_tx = _to_int(item.get("goods_transaction_id"))
            item_good_id = _to_int(item.get("good_id") or (item.get("good") or {}).get("id"))
            if item_tx == goods_transaction_id:
                removed = {
                    "good_id": item_good_id,
                    "amount": _to_float(item.get("amount")),
                    "price": _to_float(item.get("price")),
                    "storage_id": _to_int(item.get("storage_id")) or storage_id_default,
                }
                continue
            if not item_good_id:
                continue
            updated.append(
                {
                    "goods_transaction_id": item_tx or 0,
                    "record_id": record_id,
                    "service_id": service_id,
                    "storage_id": _to_int(item.get("storage_id")) or storage_id_default,
                    "good_id": item_good_id,
                    "price": _to_float(item.get("price")) or 0,
                    "amount": _to_float(item.get("amount")) or 0,
                }
            )
        break

    if removed is None:
        raise HTTPException(status_code=404, detail="Позиция не найдена")

    try:
        client.set_record_consumables(branch_id, record_id, service_id, updated)
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
