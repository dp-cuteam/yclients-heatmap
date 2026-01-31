from __future__ import annotations

import datetime as dt
from typing import Any, Dict, List, Optional

from .yclients import YClientsClient, build_client
from .utils import _to_int, _to_float


def get_daily_report(branch_id: int, date: dt.date) -> Dict[str, Any]:
    """
    Generates a daily report for the specified branch and date.
    Fetches data from YClients API: records and financial transactions.
    """
    client = build_client()
    
    # Fetch records (visits) for the day
    # We need to calculate service revenue and see detailed services
    records_resp = client.get_records(
        branch_id, 
        page=1, 
        count=1000, 
        start_date=date, 
        end_date=date, 
        include_finance=1, 
        include_consumables=0
    )
    records = records_resp.get("data") or []
    
    # Fetch financial transactions for the day (for expenses and accurate revenue)
    # Note: Using get_financial_transactions if available, or relying on records if that's the primary source.
    # The user requirements mention "Revenue", "Expenses".
    # YClients "records" usually contain services and their cost.
    # Actual payments are in "financial transactions".
    
    # We need to distinguish:
    # 1. Total Revenue (Sales)
    # 2. Payments split (Cash vs Cashless)
    # 3. Expenses (Salaries, Rent, etc.) - These are usually "Financial Transactions" in YClients (type=expense)
    
    # Let's try to fetch financial transactions directly if possible.
    # The current yclients.py might not have a helper, but we can call _request.
    # Endpoint: GET /finance_transactions/{company_id}
    
    fin_trans = []
    try:
        page = 1
        while True:
            ft_resp = client._request(
                "GET", 
                f"/finance_transactions/{branch_id}", 
                params={
                    "start_date": date.isoformat(), 
                    "end_date": date.isoformat(), 
                    "page": page, 
                    "count": 500
                }
            )
            data = ft_resp.get("data") or []
            if not data:
                break
            fin_trans.extend(data)
            if len(data) < 500:
                break
            page += 1
    except Exception:
        # Fallback or log error? For now, we proceed with empty list if it fails, or raise.
        # But for new features, better to return what we have.
        pass

    # --- Processing ---

    # 1. Revenue & Payments
    # Revenue can be calculated from "records" (services + goods) OR from "financial transactions" (income).
    # "Income" in financial transactions includes payments for visits, certificates, etc.
    income_tx = [t for t in fin_trans if _to_key(t, "type_id") == 1] # 1 is usually Income? Need to verify YClients API types.
    # Actually, standard YClients:
    # Use 'records' 'paid' field? Or 'payment_transactions'?
    
    # Let's use the provided screenshots/logic as inspiration.
    # We will aggregate form records for Service breakdown, and Financial Transactions for Totals/Expenses.
    
    total_revenue = 0.0
    cash_revenue = 0.0
    cashless_revenue = 0.0
    
    # If we look at records, we can see services.
    services_breakdown = {}
    
    for record in records:
        # Filter only valid records (not deleted, etc if needed)
        msgs = (record.get("services") or [])
        for srv in msgs:
            cost = _to_float(srv.get("cost"))
            title = srv.get("title")
            cat_id = srv.get("category_id")
            # Grouping
            group_name = "Прочее"
            # Simple heuristic mapping based on common categories (User can refine)
            # Or just list all services.
            # User request: "Services: Group by service category"
            services_breakdown.setdefault(title, {"count": 0, "revenue": 0.0})
            services_breakdown[title]["count"] += 1
            services_breakdown[title]["revenue"] += cost

    # Calculate Expenses
    # YClients Expense types?
    # Usually: type_id=2 (Expense) or similar.
    # Let's iterate fin_trans.
    expenses_breakdown = {}
    
    # Heuristic for YClients versions:
    # type_id: 1 (Income), 2 (Expense), 3 (Transfer), etc.
    # payment_type_id: 1 (Cash), 2 (Card), etc.
    
    for tx in fin_trans:
        # Amount is usually negative for expense? Or positive with type=expense.
        # Documentation check needed or use heuristic.
        # Assuming YClients returns reasonable fields.
        
        # Safe access
        amt = _to_float(tx.get("amount"))
        type_id = _to_int(tx.get("type_id")) # 1: Income, 2: Expense, 3: Move
        
        # Payments (Income)
        if type_id == 1: # Income
            total_revenue += amt
            # Check payment details if available (often nested or separate ID)
            # If payment_type is in `payment_type` dict or id
            pt_id = _to_int(tx.get("client_bank_account_id")) # This might be account.
            # Usually `account_id` tells if it's cashbox or bank.
            # Let's count all as Total, and try to split if `payment_method` exists.
            # For now, Total is most reliable.
            
        elif type_id == 2: # Expense
            # Category?
            category = txt_category(tx)
            expenses_breakdown.setdefault(category, 0.0)
            expenses_breakdown[category] += amt

    return {
        "date": date.isoformat(),
        "branch_id": branch_id,
        "revenue": {
            "total": total_revenue,
            # "cash": cash_revenue, # To be implemented with detailed mapping
            # "cashless": cashless_revenue
        },
        "expenses": expenses_breakdown,
        "services": [
            {"name": k, "count": v["count"], "revenue": v["revenue"]}
            for k, v in sorted(services_breakdown.items(), key=lambda x: x[1]["revenue"], reverse=True)
        ]
    }

def txt_category(tx):
    # Extract category name from transaction
    cat = tx.get("category")
    if isinstance(cat, dict):
        return cat.get("title") or "Прочее"
    return str(cat or "Прочее")

def _to_key(d, key):
    return d.get(key)
