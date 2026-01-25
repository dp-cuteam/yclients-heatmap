from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

from .config import settings

_logger = logging.getLogger("yclients_api")


def _log_api_call(
    method: str,
    url: str,
    params: dict | None,
    json_body: dict | None,
    status_code: int,
    response_data: Any,
    error: str | None = None,
) -> None:
    """Log API call to file for debugging."""
    log_path = settings.data_dir / "yclients_api_debug.log"
    entry = {
        "ts": datetime.utcnow().isoformat(),
        "method": method,
        "url": url,
        "params": params,
        "request_body": json_body,
        "status_code": status_code,
        "response": response_data,
        "error": error,
    }
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False, default=str))
            f.write("\n")
    except Exception:  # noqa: BLE001
        _logger.warning("Failed to write API debug log")


@dataclass
class YClientsClient:
    base_url: str
    partner_token: str
    user_token: str | None = None
    timeout: int = 30
    retries: int = 3

    def _headers(self) -> dict[str, str]:
        auth = f"Bearer {self.partner_token}"
        if self.user_token:
            auth = f"{auth}, User {self.user_token}"
        return {
            "Accept": "application/vnd.yclients.v2+json",
            "Content-Type": "application/json",
            "Authorization": auth,
        }

    def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> Any:
        url = f"{self.base_url}{path}"
        last_err = None
        for attempt in range(1, self.retries + 1):
            try:
                resp = requests.request(
                    method,
                    url,
                    headers=self._headers(),
                    params=params,
                    json=json_body,
                    timeout=self.timeout,
                )
                data = resp.json() if resp.text else {}
                
                # Log all PUT/POST requests to visits, goods_transactions, and consumables for debugging
                if method in ("PUT", "POST") and ("/visits/" in path or "/goods_transactions/" in path or "/consumables/" in path):
                    _log_api_call(method, url, params, json_body, resp.status_code, data)
                
                if resp.status_code >= 500:
                    raise RuntimeError(f"Server error {resp.status_code}")
                if data.get("success") is False:
                    _log_api_call(method, url, params, json_body, resp.status_code, data, error="success=false")
                    raise RuntimeError(data.get("meta") or data)
                if resp.status_code >= 400:
                    _log_api_call(method, url, params, json_body, resp.status_code, data, error=f"status={resp.status_code}")
                    raise RuntimeError(data.get("meta") or data)
                return data
            except Exception as exc:  # noqa: BLE001
                last_err = exc
                time.sleep(min(2 ** attempt, 10))
        raise RuntimeError(f"YCLIENTS request failed: {last_err}")

    def get_records(
        self,
        company_id: int,
        start_date: str,
        end_date: str,
        page: int,
        count: int = 50,
    ) -> dict[str, Any]:
        return self._request(
            "GET",
            f"/api/v1/records/{company_id}",
            params={
                "page": page,
                "count": count,
                "start_date": start_date,
                "end_date": end_date,
            },
        )

    def get_staff(self, company_id: int) -> dict[str, Any]:
        # staff_id = 0 means all staff (primary endpoint)
        try:
            return self._request(
                "GET",
                f"/api/v1/company/{company_id}/staff/0",
            )
        except Exception as exc:  # noqa: BLE001
            msg = str(exc)
            # Fallback to deprecated endpoint if API rejects staff_id=0
            if "masterId" in msg or "staff_id" in msg or "400" in msg or "422" in msg:
                return self._request("GET", f"/api/v1/staff/{company_id}")
            raise

    def get_companies(self, my_only: bool = True) -> dict[str, Any]:
        params = {"my": 1} if my_only else None
        return self._request("GET", "/api/v1/companies", params=params)

    def get_company(self, company_id: int, include: str | None = None) -> dict[str, Any]:
        params = {"include": include} if include else None
        return self._request("GET", f"/api/v1/company/{company_id}", params=params)

    def get_record(self, company_id: int, record_id: int, include_consumables: int = 1, include_finance: int = 0) -> dict[str, Any]:
        params = {
            "include_consumables": include_consumables,
            "include_finance_transactions": include_finance,
        }
        return self._request("GET", f"/api/v1/record/{company_id}/{record_id}", params=params)

    def get_record_consumables(self, company_id: int, record_id: int) -> dict[str, Any]:
        return self._request(
            "GET",
            f"/api/v1/technological_cards/record_consumables/{company_id}/{record_id}/",
        )

    def set_record_consumables(
        self,
        company_id: int,
        record_id: int,
        service_id: int,
        consumables: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return self._request(
            "PUT",
            f"/api/v1/technological_cards/record_consumables/consumables/{company_id}/{record_id}/{service_id}/",
            json_body={"consumables": consumables},
        )

    def search_goods(self, company_id: int, term: str, count: int = 30) -> dict[str, Any]:
        params = {
            "term": term,
            "count": count,
            "search_term": term,
            "max_count": count,
        }
        return self._request("GET", f"/api/v1/goods/search/{company_id}", params=params)

    def get_good(self, company_id: int, good_id: int) -> dict[str, Any]:
        return self._request("GET", f"/api/v1/goods/{company_id}/{good_id}")

    def update_visit(self, visit_id: int, record_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request(
            "PUT",
            f"/api/v1/visits/{visit_id}/{record_id}",
            json_body=payload,
        )

    def list_goods(self, company_id: int, page: int = 1, count: int = 200) -> dict[str, Any]:
        return self._request(
            "GET",
            f"/api/v1/goods/{company_id}",
            params={"page": page, "count": count},
        )

    def create_goods_transaction(
        self,
        company_id: int,
        document_id: int,
        good_id: int,
        storage_id: int,
        amount: float,
        cost_per_unit: float,
        cost: float,
        discount: float = 0,
        operation_unit_type: int = 1,
        master_id: int | None = None,
        client_id: int | None = None,
        comment: str = "",
        good_special_number: str = "",
    ) -> dict[str, Any]:
        """Create a goods transaction (sell item to client).
        
        operation_unit_type: 1 = for sale, 2 = for write-off
        """
        payload = {
            "document_id": document_id,
            "good_id": good_id,
            "storage_id": storage_id,
            "amount": amount,
            "cost_per_unit": cost_per_unit,
            "discount": discount,
            "cost": cost,
            "operation_unit_type": operation_unit_type,
            "comment": comment,
        }
        if good_special_number:
            payload["good_special_number"] = good_special_number
        if master_id:
            payload["master_id"] = master_id
        if client_id:
            payload["client_id"] = client_id
        return self._request(
            "POST",
            f"/api/v1/storage_operations/goods_transactions/{company_id}",
            json_body=payload,
        )

    def list_storages(self, company_id: int) -> dict[str, Any]:
        return self._request(
            "GET",
            f"/api/v1/storages/{company_id}",
        )


def build_client() -> YClientsClient:
    if not settings.yclients_partner_token:
        raise RuntimeError("Missing YCLIENTS partner token")
    return YClientsClient(
        base_url=settings.yclients_base_url,
        partner_token=settings.yclients_partner_token,
        user_token=settings.yclients_user_token,
        timeout=settings.yclients_timeout,
        retries=settings.yclients_retries,
    )
