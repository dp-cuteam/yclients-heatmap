from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Query

from .d1_service import build_d1_payload, build_raw_payload, list_branches, list_months, upsert_plan


router = APIRouter(prefix="/api/cuteam", tags=["cuteam"])


@router.get("/branches")
def api_branches():
    return {"branches": list_branches()}


@router.get("/months")
def api_months(branch_code: str = Query(..., min_length=1)):
    return {"branch_code": branch_code, "months": list_months(branch_code)}


@router.get("/d1")
def api_d1(branch_code: str = Query(..., min_length=1), month: str = Query(..., min_length=7)):
    payload = build_d1_payload(branch_code, month)
    if not payload.get("branch"):
        raise HTTPException(status_code=404, detail="Branch not found")
    return payload


@router.get("/raw")
def api_raw(branch_code: str = Query(..., min_length=1), month: str = Query(..., min_length=7)):
    payload = build_raw_payload(branch_code, month)
    if not payload.get("branch"):
        raise HTTPException(status_code=404, detail="Branch not found")
    return payload


@router.post("/plans")
def api_plan_upsert(
    payload: dict = Body(...),
):
    branch_code = payload.get("branch_code")
    month = payload.get("month")
    metric_code = payload.get("metric_code")
    value = payload.get("value")
    if not branch_code or not month or not metric_code:
        raise HTTPException(status_code=400, detail="branch_code, month, metric_code are required")
    if value is None:
        raise HTTPException(status_code=400, detail="value is required")
    try:
        value = float(value)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="value must be numeric") from exc
    upsert_plan(branch_code, month, metric_code, value)
    return {"status": "ok"}
