from __future__ import annotations

import json
import os
import time
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Any

import requests

from .config import settings
from .utils import parse_datetime


def _mask_token(token: str | None) -> str:
    if not token:
        return "не задан"
    token = token.strip()
    if len(token) <= 8:
        return "****"
    return f"{token[:4]}...{token[-4:]}"


def _env_label() -> str:
    if os.getenv("RENDER") or os.getenv("RENDER_SERVICE_ID"):
        return "Render"
    return "Local"


def _log_dir() -> Path:
    path = settings.data_dir / "logs"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _log_path() -> Path:
    today = date.today().isoformat()
    return _log_dir() / f"yclients_diag_{today}.log"


def _latest_log_info() -> dict[str, Any]:
    log_dir = _log_dir()
    logs = sorted(log_dir.glob("yclients_diag_*.log"))
    if not logs:
        return {"path": str(_log_path()), "size": 0, "last_attempt": None}
    latest = logs[-1]
    mtime = datetime.fromtimestamp(latest.stat().st_mtime).isoformat()
    return {"path": str(latest), "size": latest.stat().st_size, "last_attempt": mtime}


def _sanitize(text: str) -> str:
    for token in [settings.yclients_partner_token, settings.yclients_user_token]:
        if token:
            text = text.replace(token, "***")
    return text


def _write_log(entry: dict[str, Any]) -> None:
    path = _log_path()
    entry["ts"] = datetime.utcnow().isoformat()
    entry_str = json.dumps(entry, ensure_ascii=False)
    entry_str = _sanitize(entry_str)
    path.write_text("", encoding="utf-8") if not path.exists() else None
    with path.open("a", encoding="utf-8") as f:
        f.write(entry_str + "\n")


def _headers() -> dict[str, str]:
    auth = f"Bearer {settings.yclients_partner_token}" if settings.yclients_partner_token else ""
    if settings.yclients_user_token:
        auth = f"{auth}, User {settings.yclients_user_token}".strip(", ")
    return {
        "Accept": "application/vnd.yclients.v2+json",
        "Content-Type": "application/json",
        "Authorization": auth,
    }


def _request(method: str, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    url = f"{settings.yclients_base_url}{path}"
    start = time.perf_counter()
    status_code = None
    response_text = ""
    json_data = None
    error = None
    try:
        resp = requests.request(
            method,
            url,
            headers=_headers(),
            params=params,
            timeout=settings.yclients_timeout,
        )
        status_code = resp.status_code
        response_text = resp.text or ""
        try:
            json_data = resp.json()
        except Exception:  # noqa: BLE001
            json_data = None
    except requests.Timeout:
        error = "Таймаут запроса"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
    latency_ms = int((time.perf_counter() - start) * 1000)

    snippet = response_text[:50_000] if response_text else ""
    _write_log(
        {
            "method": method,
            "url": url,
            "params": params or {},
            "status": status_code,
            "latency_ms": latency_ms,
            "error": error,
            "response_snippet": snippet,
        }
    )

    return {
        "status_code": status_code,
        "latency_ms": latency_ms,
        "json": json_data,
        "text": response_text,
        "error": error,
    }


def _status_label(status_code: int | None) -> str:
    if status_code is None:
        return "Ошибка"
    if status_code == 200:
        return "ОК"
    if status_code == 429:
        return "Предупреждение"
    if status_code >= 500:
        return "Ошибка"
    if status_code in {401, 403, 404}:
        return "Ошибка"
    return "Предупреждение"


def _status_message(status_code: int | None) -> str:
    if status_code is None:
        return "Таймаут или ошибка соединения"
    if status_code == 200:
        return "Успешно"
    if status_code == 401:
        return "Неверный токен или токен не активирован"
    if status_code == 403:
        return "Недостаточно прав для метода"
    if status_code == 404:
        return "Неверный endpoint или идентификатор"
    if status_code == 429:
        return "Превышен лимит запросов, попробуйте позже"
    if status_code >= 500:
        return "Ошибка сервиса YCLIENTS"
    return "Неожиданный ответ"


def _validate_config() -> tuple[bool, str]:
    try:
        raw = settings.group_config_path.read_text(encoding="utf-8-sig")
        data = json.loads(raw)
    except Exception as exc:  # noqa: BLE001
        return False, f"Ошибка чтения конфигурации: {exc}"
    if not isinstance(data, dict):
        return False, "Конфигурация должна быть объектом JSON"
    branches = data.get("branches")
    if not isinstance(branches, list) or not branches:
        return False, "Отсутствует список филиалов в конфигурации"
    for branch in branches:
        if "branch_id" not in branch:
            return False, "В одном из филиалов нет branch_id"
        if "groups" not in branch or not isinstance(branch["groups"], list):
            return False, f"Филиал {branch.get('branch_id')} без групп"
        for group in branch["groups"]:
            if "name" not in group:
                return False, "В группе отсутствует name"
            if "staff_names" not in group or not isinstance(group["staff_names"], list):
                return False, f"Группа {group.get('name')} без staff_names"
    return True, "Конфигурация корректна"


def run_diagnostics(branch_id: int | None = None, day: str | None = None, staff_id: int | None = None) -> dict[str, Any]:
    config_status = {
        "partner_token": _mask_token(settings.yclients_partner_token),
        "user_token": _mask_token(settings.yclients_user_token) if settings.yclients_user_token else "не задан",
        "timezone": settings.timezone,
        "environment": _env_label(),
    }

    log_info = _latest_log_info()
    config_status["last_attempt"] = datetime.utcnow().isoformat()

    tests = []

    # Test 1: Auth check
    auth_resp = _request("GET", "/api/v1/companies")
    tests.append(
        {
            "name": "Доступ по токену",
            "status": _status_label(auth_resp["status_code"]),
            "http_code": auth_resp["status_code"],
            "latency_ms": auth_resp["latency_ms"],
            "message": _status_message(auth_resp["status_code"])
            if not auth_resp["error"]
            else auth_resp["error"],
            "details": auth_resp["json"] or auth_resp["text"],
        }
    )

    # Test 2: Branches + config file
    branch_list = []
    if auth_resp["json"] and isinstance(auth_resp["json"].get("data"), list):
        for item in auth_resp["json"]["data"][:10]:
            branch_list.append({"id": item.get("id"), "title": item.get("title")})
    tests.append(
        {
            "name": "Филиалы доступны",
            "status": _status_label(auth_resp["status_code"]),
            "http_code": auth_resp["status_code"],
            "latency_ms": auth_resp["latency_ms"],
            "message": "Получен список филиалов"
            if auth_resp["status_code"] == 200
            else _status_message(auth_resp["status_code"]),
            "details": {"branches_sample": branch_list},
        }
    )

    cfg_ok, cfg_msg = _validate_config()
    tests.append(
        {
            "name": "Конфигурация групп",
            "status": "ОК" if cfg_ok else "Ошибка",
            "http_code": None,
            "latency_ms": None,
            "message": cfg_msg,
            "details": None,
        }
    )

    if not branch_list and settings.group_config_path.exists():
        try:
            raw = settings.group_config_path.read_text(encoding="utf-8-sig")
            data = json.loads(raw)
            for branch in data.get("branches", [])[:10]:
                branch_list.append(
                    {
                        "id": branch.get("branch_id"),
                        "title": branch.get("display_name") or branch.get("branch_id"),
                    }
                )
        except Exception:  # noqa: BLE001
            pass

    # determine branch
    if branch_id is None and branch_list:
        branch_id = int(branch_list[0]["id"])

    # Test 3: staff list
    if branch_id:
        staff_resp = _request("GET", f"/api/v1/company/{branch_id}/staff/0")
        staff_sample = []
        if staff_resp["json"] and isinstance(staff_resp["json"].get("data"), list):
            for item in staff_resp["json"]["data"][:20]:
                staff_sample.append({"id": item.get("id"), "name": item.get("name")})
        status_label = _status_label(staff_resp["status_code"])
        msg = _status_message(staff_resp["status_code"])
        if staff_resp["status_code"] == 200:
            count = len(staff_resp["json"].get("data") or []) if staff_resp["json"] else 0
            msg = f"Получено сотрудников: {count}"
            if not staff_sample:
                msg = "Список сотрудников пуст"
                status_label = "Предупреждение"
        tests.append(
            {
                "name": "Сотрудники/ресурсы доступны",
                "status": status_label,
                "http_code": staff_resp["status_code"],
                "latency_ms": staff_resp["latency_ms"],
                "message": msg,
                "details": {"branch_id": branch_id, "staff_sample": staff_sample},
            }
        )

        # Test 4: records for day
        if not day:
            day = date.today().isoformat()
        params = {"start_date": day, "end_date": day}
        if staff_id:
            params["staff_id"] = staff_id
        records_resp = _request("GET", f"/api/v1/records/{branch_id}", params=params)
        total = 0
        fact = 0
        samples = []
        if records_resp["json"] and isinstance(records_resp["json"].get("data"), list):
            data_list = records_resp["json"]["data"]
            total = len(data_list)
            for rec in data_list:
                attendance = rec.get("attendance")
                if attendance is None:
                    attendance = rec.get("visit_attendance")
                try:
                    attendance = int(attendance)
                except Exception:  # noqa: BLE001
                    attendance = None
                if attendance in {1, 2}:
                    fact += 1
            for rec in data_list[:5]:
                start_raw = rec.get("datetime") or rec.get("date")
                end_raw = ""
                try:
                    start_dt = parse_datetime(start_raw, settings.timezone)
                    duration = int(rec.get("seance_length") or rec.get("length") or 0)
                    end_dt = start_dt + timedelta(seconds=duration)
                    end_raw = end_dt.isoformat()
                except Exception:  # noqa: BLE001
                    end_raw = ""
                samples.append(
                    {
                        "start": start_raw,
                        "end": end_raw,
                        "staff_id": rec.get("staff_id"),
                        "attendance": rec.get("attendance"),
                    }
                )
        status_label = _status_label(records_resp["status_code"])
        msg = _status_message(records_resp["status_code"])
        if records_resp["status_code"] == 200:
            msg = f"Всего: {total}, фактических: {fact}"
            if total == 0:
                msg = "Записей за день нет"
                status_label = "Предупреждение"
        tests.append(
            {
                "name": "Записи за день",
                "status": status_label,
                "http_code": records_resp["status_code"],
                "latency_ms": records_resp["latency_ms"],
                "message": msg,
                "details": {
                    "branch_id": branch_id,
                    "date": day,
                    "staff_id": staff_id,
                    "total": total,
                    "fact": fact,
                    "samples": samples,
                },
            }
        )

    log_info = _latest_log_info()
    return {"config": config_status, "tests": tests, "log": log_info, "branches": branch_list}
