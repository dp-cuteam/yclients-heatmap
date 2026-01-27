from __future__ import annotations

import datetime as dt
import os
import subprocess
import sys
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

from .db import get_conn, init_schema, db_source_label, db_target_label, is_postgres
from .settings import settings


SYNC_LOCK = threading.Lock()
SYNC_STATE: Dict[str, Any] = {
    "status": "idle",
    "started_at": None,
    "finished_at": None,
    "last_error": None,
    "last_output": None,
    "last_sheets": [],
    "dry_run": False,
}

IMPORT_LOCK = threading.Lock()
IMPORT_STATE: Dict[str, Any] = {
    "status": "idle",
    "started_at": None,
    "finished_at": None,
    "last_error": None,
    "last_output": None,
}


@dataclass(frozen=True)
class CuteamStatus:
    db_path: str
    db_source: str
    db_exists: bool
    db_size: int | None
    rows: int | None
    date_min: str | None
    date_max: str | None
    updated_at: str | None
    branches: int | None
    env: Dict[str, Any]
    sync: Dict[str, Any]
    imports: Dict[str, Any]


def _db_file_info() -> tuple[bool, int | None]:
    if is_postgres():
        return bool(settings.db_url), None
    db_path = settings.db_path
    if not db_path.exists():
        return False, None
    try:
        return True, db_path.stat().st_size
    except OSError:
        return True, None


def _query_scalar(conn, sql: str, params: tuple = ()) -> Any:
    row = conn.execute(sql, params).fetchone()
    if not row:
        return None
    if isinstance(row, dict):
        return next(iter(row.values()), None)
    return row[0]


def _file_info(path: os.PathLike[str] | str | None) -> Dict[str, Any]:
    if not path:
        return {"exists": False, "path": None, "size": None}
    file_path = Path(path)
    if not file_path.exists():
        return {"exists": False, "path": str(file_path), "size": None}
    try:
        size = file_path.stat().st_size
    except OSError:
        size = None
    return {"exists": True, "path": str(file_path), "size": size}


def get_status() -> CuteamStatus:
    db_exists, db_size = _db_file_info()
    rows = date_min = date_max = updated_at = branches = None
    try:
        init_schema()
        with get_conn() as conn:
            rows = _query_scalar(conn, "SELECT COUNT(*) FROM manual_sheet_daily")
            date_min = _query_scalar(conn, "SELECT MIN(date) FROM manual_sheet_daily")
            date_max = _query_scalar(conn, "SELECT MAX(date) FROM manual_sheet_daily")
            updated_at = _query_scalar(conn, "SELECT MAX(updated_at) FROM manual_sheet_daily")
            branches = _query_scalar(conn, "SELECT COUNT(DISTINCT branch_code) FROM manual_sheet_daily")
    except Exception:
        pass

    env = {
        "sheet_id": os.getenv("SHEET_ID"),
        "sheet_name": os.getenv("SHEET_NAME"),
        "has_sa_json": bool(os.getenv("GOOGLE_SA_JSON")),
        "has_sa_json_b64": bool(os.getenv("GOOGLE_SA_JSON_B64")),
        "db_source": db_source_label(),
        "db_env": settings.db_url_env,
    }
    imports = {
        "state": IMPORT_STATE.copy(),
        "files": {
            "plans": _file_info(settings.plans_2025_path),
            "checks": _file_info(settings.checks_path),
        },
        "sheets": {
            "plans": settings.plans_2025_sheet,
            "checks": settings.checks_sheet,
        },
    }

    return CuteamStatus(
        db_path=db_target_label(),
        db_source=db_source_label(),
        db_exists=db_exists,
        db_size=db_size,
        rows=rows,
        date_min=date_min,
        date_max=date_max,
        updated_at=updated_at,
        branches=branches,
        env=env,
        sync=SYNC_STATE.copy(),
        imports=imports,
    )


def _run_sync(sheet_names: List[str], dry_run: bool) -> None:
    outputs = []
    error = None
    db_target = settings.db_url or str(settings.db_path)
    for sheet in sheet_names:
        cmd = [
            sys.executable,
            "-m",
            "ingest.sync_sheet",
            "--db",
            db_target,
            "--sheet-name",
            sheet,
        ]
        if dry_run:
            cmd.append("--dry-run")
        try:
            result = subprocess.run(
                cmd,
                cwd=str(settings.root_dir / "Показатели"),
                capture_output=True,
                text=True,
                check=False,
                env=os.environ.copy(),
            )
        except Exception as exc:  # noqa: BLE001
            error = str(exc)
            outputs.append(f"[{sheet}] ERROR: {error}")
            break
        outputs.append(f"[{sheet}] stdout:\n{result.stdout.strip()}")
        if result.stderr:
            outputs.append(f"[{sheet}] stderr:\n{result.stderr.strip()}")
        if result.returncode != 0:
            error = f"exit={result.returncode}"
            break

    with SYNC_LOCK:
        SYNC_STATE["status"] = "error" if error else "success"
        SYNC_STATE["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
        SYNC_STATE["last_error"] = error
        combined = "\n\n".join(outputs)
        SYNC_STATE["last_output"] = combined[-8000:] if combined else None


def start_sync(sheet_names: List[str], dry_run: bool = False):
    if not sheet_names:
        default_sheet = os.getenv("SHEET_NAME")
        if default_sheet:
            sheet_names = [default_sheet]
    with SYNC_LOCK:
        if SYNC_STATE.get("status") == "running":
            raise RuntimeError("sync already running")
        if not sheet_names:
            raise RuntimeError("sheet_name is required")
        SYNC_STATE["status"] = "running"
        SYNC_STATE["started_at"] = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
        SYNC_STATE["finished_at"] = None
        SYNC_STATE["last_error"] = None
        SYNC_STATE["last_output"] = None
        SYNC_STATE["last_sheets"] = sheet_names
        SYNC_STATE["dry_run"] = dry_run
    return lambda: _run_sync(sheet_names, dry_run)


def _run_import_plans_checks() -> None:
    outputs = []
    error = None
    cwd = str(settings.root_dir / "Показатели")
    tasks = [
        (
            "plans",
            "ingest.import_plans_2025",
            [str(settings.plans_2025_path), "--sheet", settings.plans_2025_sheet],
        ),
        (
            "checks",
            "ingest.import_checks",
            [str(settings.checks_path), "--sheet", settings.checks_sheet],
        ),
    ]
    for label, module, args in tasks:
        file_path = Path(args[0])
        if not file_path.exists():
            error = f"missing file: {file_path}"
            outputs.append(f"[{label}] ERROR: {error}")
            break
        cmd = [sys.executable, "-m", module] + args
        try:
            result = subprocess.run(
                cmd,
                cwd=cwd,
                capture_output=True,
                text=True,
                check=False,
                env=os.environ.copy(),
            )
        except Exception as exc:  # noqa: BLE001
            error = str(exc)
            outputs.append(f"[{label}] ERROR: {error}")
            break
        outputs.append(f"[{label}] stdout:\n{result.stdout.strip()}")
        if result.stderr:
            outputs.append(f"[{label}] stderr:\n{result.stderr.strip()}")
        if result.returncode != 0:
            error = f"exit={result.returncode}"
            break

    with IMPORT_LOCK:
        IMPORT_STATE["status"] = "error" if error else "success"
        IMPORT_STATE["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
        IMPORT_STATE["last_error"] = error
        combined = "\n\n".join(outputs)
        IMPORT_STATE["last_output"] = combined[-8000:] if combined else None


def start_import_plans_checks():
    with IMPORT_LOCK:
        if IMPORT_STATE.get("status") == "running":
            raise RuntimeError("import already running")
        IMPORT_STATE["status"] = "running"
        IMPORT_STATE["started_at"] = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
        IMPORT_STATE["finished_at"] = None
        IMPORT_STATE["last_error"] = None
        IMPORT_STATE["last_output"] = None
    return _run_import_plans_checks
