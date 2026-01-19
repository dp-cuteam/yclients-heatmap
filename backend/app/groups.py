from __future__ import annotations

import json
import logging
from copy import deepcopy
from pathlib import Path

from .config import settings
from .yclients import YClientsClient


def _load_json(path: Path) -> dict:
    # Always handle possible BOM from Windows-generated UTF-8 files
    raw = path.read_text(encoding="utf-8-sig")
    data = json.loads(raw)
    return data


def load_group_config() -> dict:
    if settings.group_resolved_path.exists():
        return _load_json(settings.group_resolved_path)
    return _load_json(settings.group_config_path)


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
