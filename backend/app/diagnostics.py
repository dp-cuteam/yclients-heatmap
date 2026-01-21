from __future__ import annotations

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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


def _request(
    method: str,
    path: str,
    params: dict[str, Any] | None = None,
    timeout: int | None = None,
) -> dict[str, Any]:
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
            timeout=timeout or settings.yclients_timeout,
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
        if staff_resp["status_code"] in {400, 422}:
            text = staff_resp["text"] or ""
            if "masterId" in text or "staff_id" in text:
                staff_resp = _request("GET", f"/api/v1/staff/{branch_id}")
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


def _mask_token_ascii(token: str | None) -> str:
    if not token:
        return "missing"
    token = token.strip()
    if len(token) <= 8:
        return "****"
    return f"{token[:4]}...{token[-4:]}"


def _masked_headers_for_report() -> dict[str, str]:
    partner = _mask_token_ascii(settings.yclients_partner_token)
    user = _mask_token_ascii(settings.yclients_user_token) if settings.yclients_user_token else "missing"
    auth = ""
    if partner != "missing":
        auth = f"Bearer {partner}"
    if user != "missing":
        auth = f"{auth}, User {user}".strip(", ")
    return {
        "Accept": "application/vnd.yclients.v2+json",
        "Content-Type": "application/json",
        "Authorization": auth or "missing",
    }


def _support_packet_paths() -> tuple[Path, Path]:
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    md_path = settings.data_dir / "SUPPORT_PACKET.md"
    json_path = settings.data_dir / "support_packet_latest.json"
    return md_path, json_path


def latest_support_packet_info() -> dict[str, Any]:
    md_path, json_path = _support_packet_paths()
    return {
        "md_path": str(md_path),
        "json_path": str(json_path),
        "md_exists": md_path.exists(),
        "json_exists": json_path.exists(),
    }


def _load_branch_ids() -> list[int]:
    branch_ids: list[int] = []
    try:
        raw = settings.group_config_path.read_text(encoding="utf-8-sig")
        data = json.loads(raw)
        for item in data.get("branches", []):
            try:
                branch_ids.append(int(item.get("branch_id")))
            except Exception:
                continue
    except Exception:
        branch_ids = []
    if not branch_ids and settings.active_branch_ids:
        branch_ids = list(settings.active_branch_ids)
    return sorted(set(branch_ids))


def _response_excerpt(text: str, limit: int = 20000) -> tuple[str, bool]:
    if not text:
        return "", False
    if len(text) <= limit:
        return text, False
    return text[:limit] + "\n...[truncated]", True


def _request_report(
    name: str,
    method: str,
    path: str,
    params: dict[str, Any] | None = None,
    timeout: int | None = None,
) -> dict[str, Any]:
    resp = _request(method, path, params=params, timeout=timeout)
    response_text, truncated = _response_excerpt(resp["text"] or "")
    return {
        "name": name,
        "method": method,
        "url": f"{settings.yclients_base_url}{path}",
        "headers": _masked_headers_for_report(),
        "params": params or {},
        "status_code": resp["status_code"],
        "latency_ms": resp["latency_ms"],
        "error": resp["error"],
        "response_text": response_text,
        "response_truncated": truncated,
    }


def _infer_data_exchange(tx_resp: dict[str, Any], staff_resp: dict[str, Any]) -> dict[str, str]:
    tx_code = tx_resp.get("status_code")
    staff_code = staff_resp.get("status_code")
    if tx_code == 200:
        return {"status": "likely_on", "reason": "transactions endpoint ok (200)"}
    if tx_code == 401:
        return {"status": "auth_error", "reason": "transactions returned 401 (auth error)"}
    if tx_code == 403 and staff_code == 200:
        return {
            "status": "likely_off",
            "reason": "staff ok (200), transactions=403 → likely no access or data_exchange_gs disabled",
        }
    if tx_code == 404:
        return {"status": "not_found", "reason": "transactions returned 404 (bad endpoint or branch_id)"}
    if tx_code is None:
        return {"status": "request_failed", "reason": "transactions failed (timeout/connection error)"}
    return {"status": "unknown", "reason": f"transactions={tx_code}, staff={staff_code}"}


def _run_branch_support_tests(branch_id: int, day: str, timeout: int) -> dict[str, Any]:
    tx_params = {"page": 1, "count": 5, "start_date": day, "end_date": day}
    tx = _request_report(
        "Test A: transactions",
        "GET",
        f"/api/v1/transactions/{branch_id}",
        params=tx_params,
        timeout=timeout,
    )
    staff = _request_report(
        "Test B: staff list",
        "GET",
        f"/api/v1/company/{branch_id}/staff/0",
        timeout=timeout,
    )
    requests = [tx, staff]
    if staff["status_code"] in {400, 422}:
        text = staff.get("response_text") or ""
        if "masterId" in text or "staff_id" in text:
            fallback = _request_report(
                "Test B (fallback): staff list",
                "GET",
                f"/api/v1/staff/{branch_id}",
                timeout=timeout,
            )
            requests.append(fallback)
            staff = fallback
    data_exchange = _infer_data_exchange(tx, staff)
    return {
        "branch_id": branch_id,
        "requests": requests,
        "data_exchange_gs": data_exchange,
    }


def _render_support_packet_md(packet: dict[str, Any]) -> str:
    meta = packet.get("meta", {})
    lines = []
    lines.append("# SUPPORT_PACKET (YCLIENTS)")
    lines.append("")
    lines.append(f"Generated: {meta.get('generated_at')}")
    lines.append(f"Environment: {meta.get('environment')}")
    lines.append(f"App URL: {meta.get('app_url')}")
    lines.append(f"Base URL: {meta.get('base_url')}")
    lines.append(f"Timeout: {meta.get('timeout_sec')}s")
    lines.append(f"Concurrency: {meta.get('concurrency')}")
    lines.append("")
    lines.append("Tokens (masked):")
    tokens = meta.get("tokens", {})
    lines.append(f"- partner: {tokens.get('partner')}")
    lines.append(f"- user: {tokens.get('user')}")
    lines.append("")
    lines.append("Branch IDs (from config):")
    branch_ids = meta.get("branch_ids") or []
    lines.append(", ".join(str(b) for b in branch_ids) if branch_ids else "none")
    lines.append("")
    lines.append("Notes:")
    lines.append("- Full JSON payload saved to data/support_packet_latest.json")
    lines.append("- This file contains only masked tokens.")
    lines.append("")

    for branch in packet.get("branches", []):
        lines.append(f"## Branch {branch.get('branch_id')}")
        data_exchange = branch.get("data_exchange_gs") or {}
        lines.append(
            f"data_exchange_gs: {data_exchange.get('status')} ({data_exchange.get('reason')})"
        )
        lines.append("")
        for req in branch.get("requests", []):
            lines.append(f"### {req.get('name')}")
            lines.append(f"- Method: {req.get('method')}")
            lines.append(f"- URL: {req.get('url')}")
            lines.append(f"- Headers: {req.get('headers')}")
            lines.append(f"- Params: {json.dumps(req.get('params') or {}, ensure_ascii=True)}")
            lines.append(
                f"- Status: {req.get('status_code')} (latency {req.get('latency_ms')} ms)"
            )
            if req.get("error"):
                lines.append(f"- Error: {req.get('error')}")
            if req.get("response_truncated"):
                lines.append("- Response: [truncated]")
            lines.append("Response body:")
            lines.append("```")
            lines.append(req.get("response_text") or "")
            lines.append("```")
            lines.append("")
    return "\n".join(lines)


def run_support_packet(branch_ids: list[int] | None = None, day: str | None = None) -> dict[str, Any]:
    timeout = min(max(settings.yclients_timeout, 10), 15)
    concurrency = 4
    if not branch_ids:
        branch_ids = _load_branch_ids()
    if not day:
        day = date.today().isoformat()
    meta = {
        "generated_at": datetime.utcnow().isoformat(),
        "environment": _env_label(),
        "app_url": os.getenv("APP_URL", "https://yclients-heatmap.onrender.com"),
        "base_url": settings.yclients_base_url,
        "timeout_sec": timeout,
        "concurrency": concurrency,
        "tokens": {
            "partner": _mask_token_ascii(settings.yclients_partner_token),
            "user": _mask_token_ascii(settings.yclients_user_token),
        },
        "branch_ids": branch_ids,
        "date": day,
        "config_path": str(settings.group_config_path),
    }

    results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {
            executor.submit(_run_branch_support_tests, bid, day, timeout): bid for bid in branch_ids
        }
        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception as exc:  # noqa: BLE001
                bid = futures[future]
                results.append(
                    {
                        "branch_id": bid,
                        "requests": [],
                        "data_exchange_gs": {
                            "status": "request_failed",
                            "reason": f"exception: {exc}",
                        },
                    }
                )

    results.sort(key=lambda x: x.get("branch_id") or 0)
    packet = {"meta": meta, "branches": results}

    md_path, json_path = _support_packet_paths()
    json_path.write_text(json.dumps(packet, ensure_ascii=True, indent=2), encoding="utf-8")
    md_path.write_text(_render_support_packet_md(packet), encoding="utf-8")

    return {"meta": meta, "branches": results, "files": {"md": str(md_path), "json": str(json_path)}}
