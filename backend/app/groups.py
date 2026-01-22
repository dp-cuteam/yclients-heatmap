from __future__ import annotations

import json
import logging
from copy import deepcopy
from pathlib import Path

from .config import settings
from .yclients import YClientsClient, build_client


_branch_names_checked = False


def _load_json(path: Path) -> dict:
    # Always handle possible BOM from Windows-generated UTF-8 files
    raw = path.read_text(encoding="utf-8-sig")
    data = json.loads(raw)
    return data


def load_group_config() -> dict:
    if settings.group_resolved_path.exists():
        data = _load_json(settings.group_resolved_path)
    else:
        data = _load_json(settings.group_config_path)
    return data


def _needs_display_name(branch: dict) -> bool:
    branch_id = branch.get("branch_id")
    display_name = (branch.get("display_name") or "").strip()
    return not display_name or display_name == str(branch_id)


def ensure_branch_names(config: dict) -> dict:
    global _branch_names_checked
    if _branch_names_checked:
        return config
    _branch_names_checked = True
    branches = config.get("branches") or []
    targets = [b for b in branches if _needs_display_name(b)]
    if not targets:
        return config
    log = logging.getLogger("groups")
    try:
        client = build_client()
    except Exception as exc:  # noqa: BLE001
        log.warning("Failed to init YCLIENTS client for branch names: %s", exc)
        return config
    try:
        companies_resp = client.get_companies()
        company_names = {}
        for company in companies_resp.get("data") or []:
            cid = company.get("id")
            title = (company.get("title") or "").strip()
            if cid is not None and title:
                company_names[int(cid)] = title
        updated = False
        for branch in targets:
            branch_id = branch.get("branch_id")
            try:
                branch_id_int = int(branch_id)
            except Exception:
                continue
            title = company_names.get(branch_id_int)
            if title:
                branch["display_name"] = title
                updated = True
        if updated:
            save_group_config(config)
    except Exception as exc:  # noqa: BLE001
        log.warning("Failed to resolve branch display names: %s", exc)
    return config


def save_group_config(config: dict) -> None:
    settings.group_resolved_path.write_text(
        json.dumps(config, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def resolve_staff_ids(config: dict, client: YClientsClient) -> dict:
    resolved = deepcopy(config)
    log = logging.getLogger("groups")
    company_names = {}
    try:
        companies_resp = client.get_companies()
        for company in companies_resp.get("data") or []:
            cid = company.get("id")
            title = (company.get("title") or "").strip()
            if cid is not None and title:
                company_names[int(cid)] = title
    except Exception as exc:  # noqa: BLE001
        log.warning("Failed to load company names: %s", exc)
    for branch in resolved.get("branches", []):
        branch_id = int(branch["branch_id"])
        if branch_id in company_names:
            current = (branch.get("display_name") or "").strip()
            if not current or current == str(branch_id):
                branch["display_name"] = company_names[branch_id]
        staff_resp = client.get_staff(branch_id)
        staff_list = staff_resp.get("data") or []
        by_name = {}
        for staff in staff_list:
            name = (staff.get("name") or "").strip()
            if not name:
                continue
            by_name.setdefault(name, []).append(int(staff.get("id")))
        for group in branch.get("groups", []):
            staff_ids = []
            for staff_name in group.get("staff_names", []):
                matches = by_name.get(staff_name.strip(), [])
                if matches:
                    staff_ids.append(matches[0])
                    if len(matches) > 1:
                        log.warning("Multiple staff matched name '%s' in branch %s", staff_name, branch_id)
                else:
                    log.warning("No staff matched name '%s' in branch %s", staff_name, branch_id)
            group["staff_ids"] = sorted(set(staff_ids))
    return resolved
