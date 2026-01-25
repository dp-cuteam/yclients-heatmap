"""Fill metric_code and source columns in Google Sheet (safe, idempotent)."""

from __future__ import annotations

import argparse
import base64
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
import sys

sys.path.insert(0, str(ROOT))

from shared import reference  # noqa: E402

DEFAULT_SHEET_NAME = "\u0418\u0422\u041e\u0413\u041e-26"
ARTICLE_HEADER = "\u0441\u0442\u0430\u0442\u044c\u044f"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Auto-fill metric_code and source in Google Sheet.")
    parser.add_argument("--sheet-id", default=os.environ.get("SHEET_ID"))
    parser.add_argument("--sheet-name", default=os.environ.get("SHEET_NAME", DEFAULT_SHEET_NAME))
    parser.add_argument("--service-account-json", default=os.environ.get("GOOGLE_SA_JSON"))
    parser.add_argument("--service-account-b64", default=os.environ.get("GOOGLE_SA_JSON_B64"))
    parser.add_argument("--apply", action="store_true", help="Actually write changes to the sheet")
    return parser.parse_args()


def load_credentials(key_path: Optional[str], key_b64: Optional[str], scopes: List[str]):
    try:
        from google.oauth2 import service_account
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("Missing Google API dependencies. Install: python -m pip install -r requirements.txt") from exc

    if key_b64:
        info = json.loads(base64.b64decode(key_b64).decode("utf-8"))
        return service_account.Credentials.from_service_account_info(info, scopes=scopes)
    if key_path:
        return service_account.Credentials.from_service_account_file(key_path, scopes=scopes)
    raise ValueError("Provide service account via --service-account-json or --service-account-b64")


def build_service(credentials):
    from googleapiclient.discovery import build

    return build("sheets", "v4", credentials=credentials, cache_discovery=False)


def normalize_grid(rows: List[List[Any]]) -> List[List[Any]]:
    if not rows:
        return []
    max_len = max(len(row) for row in rows)
    return [row + [None] * (max_len - len(row)) for row in rows]


def find_header_row(rows: List[List[Any]]) -> int:
    for idx, row in enumerate(rows[:12]):
        for cell in row:
            if isinstance(cell, str) and cell.strip().lower() in {ARTICLE_HEADER, "article"}:
                return idx
    return -1


def find_column(row: List[Any], name: str) -> Optional[int]:
    target = name.strip().lower()
    for i, cell in enumerate(row):
        if isinstance(cell, str) and cell.strip().lower() == target:
            return i
    return None


def col_to_a1(col_idx: int) -> str:
    col = col_idx + 1
    letters = []
    while col:
        col, rem = divmod(col - 1, 26)
        letters.append(chr(65 + rem))
    return "".join(reversed(letters))


def get_sheet_id(service, spreadsheet_id: str, sheet_name: str) -> int:
    meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    for sheet in meta.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("title") == sheet_name:
            return int(props.get("sheetId"))
    raise ValueError(f"Sheet '{sheet_name}' not found in spreadsheet")


def ensure_columns(
    service,
    spreadsheet_id: str,
    sheet_name: str,
    col_article: int,
    col_metric: Optional[int],
    col_source: Optional[int],
) -> None:
    if col_metric is not None and col_source is not None:
        return

    sheet_id = get_sheet_id(service, spreadsheet_id, sheet_name)

    if col_metric is None and col_source is None:
        insert_index = col_article
        insert_count = 2
    elif col_metric is None:
        if col_source is not None and col_source < col_article:
            insert_index = col_source
        else:
            insert_index = col_article
        insert_count = 1
    else:
        insert_index = col_article
        insert_count = 1

    requests = [
        {
            "insertDimension": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": insert_index,
                    "endIndex": insert_index + insert_count,
                },
                "inheritFromBefore": False,
            }
        }
    ]
    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body={"requests": requests}
    ).execute()


def build_label_mapping() -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for label, code in reference.RAW_LABEL_TO_METRIC_CODE.items():
        mapping[label] = code
    for code, meta in reference.EXTRA_INPUT_METRICS.items():
        mapping[meta["label"]] = code
    for code, meta in reference.DERIVED_METRICS.items():
        mapping[meta["label"]] = code
    return mapping


def infer_source(metric_code: Optional[str]) -> Optional[str]:
    if not metric_code:
        return None
    if metric_code in reference.DERIVED_METRICS:
        return "computed"
    return "manual"


def main() -> None:
    args = parse_args()
    if not args.sheet_id:
        raise ValueError("sheet_id is required")

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = load_credentials(args.service_account_json, args.service_account_b64, scopes=scopes)
    service = build_service(creds)

    values_resp = (
        service.spreadsheets()
        .values()
        .get(
            spreadsheetId=args.sheet_id,
            range=args.sheet_name,
            valueRenderOption="UNFORMATTED_VALUE",
            dateTimeRenderOption="FORMATTED_STRING",
        )
        .execute()
    )
    rows = normalize_grid(values_resp.get("values", []))
    if not rows:
        raise RuntimeError("Sheet is empty")

    header_idx = find_header_row(rows)
    if header_idx < 0:
        raise RuntimeError("Header row with 'statya' not found")
    header = rows[header_idx]

    col_article = find_column(header, ARTICLE_HEADER)
    if col_article is None:
        raise RuntimeError("Column 'statya' not found")
    col_metric = find_column(header, "metric_code")
    col_source = find_column(header, "source")

    ensure_columns(service, args.sheet_id, args.sheet_name, col_article, col_metric, col_source)

    values_resp = (
        service.spreadsheets()
        .values()
        .get(
            spreadsheetId=args.sheet_id,
            range=args.sheet_name,
            valueRenderOption="UNFORMATTED_VALUE",
            dateTimeRenderOption="FORMATTED_STRING",
        )
        .execute()
    )
    rows = normalize_grid(values_resp.get("values", []))
    header = rows[header_idx]
    col_article = find_column(header, ARTICLE_HEADER)
    col_metric = find_column(header, "metric_code")
    col_source = find_column(header, "source")
    if col_metric is None or col_source is None:
        raise RuntimeError("Failed to create metric_code/source columns")

    label_to_code = build_label_mapping()
    branch_codes = {b["code"] for b in reference.BRANCHES} | set(reference.IGNORE_BRANCH_CODES)
    ignore_labels = set(reference.IGNORE_LABELS)

    data_start = header_idx + 1
    data_end = len(rows)

    metric_values: List[List[Any]] = []
    source_values: List[List[Any]] = []
    stats = {
        "rows": 0,
        "metric_filled": 0,
        "source_filled": 0,
        "unknown_labels": 0,
        "conflicts": 0,
        "skipped_branch_rows": 0,
        "skipped_ignore": 0,
    }
    unknown_labels: List[str] = []

    for row in rows[data_start:data_end]:
        stats["rows"] += 1
        label = None
        if col_article < len(row):
            cell = row[col_article]
            if isinstance(cell, str):
                label = cell.strip()
            elif cell not in (None, ""):
                label = str(cell).strip()

        if label and label in branch_codes:
            metric_values.append([row[col_metric] if col_metric < len(row) else None])
            source_values.append([row[col_source] if col_source < len(row) else None])
            stats["skipped_branch_rows"] += 1
            continue

        if label in ignore_labels:
            metric_values.append([row[col_metric] if col_metric < len(row) else None])
            source_values.append([row[col_source] if col_source < len(row) else None])
            stats["skipped_ignore"] += 1
            continue

        existing_metric = row[col_metric] if col_metric < len(row) else None
        existing_source = row[col_source] if col_source < len(row) else None

        metric_val = None
        if existing_metric not in (None, ""):
            metric_val = str(existing_metric).strip()
        elif label and label in label_to_code:
            metric_val = label_to_code[label]
            stats["metric_filled"] += 1

        if existing_metric not in (None, "") and label and label in label_to_code:
            if str(existing_metric).strip() != label_to_code[label]:
                stats["conflicts"] += 1

        source_val = None
        if existing_source not in (None, ""):
            source_val = str(existing_source).strip()
        else:
            inferred = infer_source(metric_val)
            if inferred:
                source_val = inferred
                stats["source_filled"] += 1

        if not metric_val and label:
            stats["unknown_labels"] += 1
            if label not in unknown_labels:
                unknown_labels.append(label)

        metric_values.append([metric_val or ""])
        source_values.append([source_val or ""])

    header_updates = []
    if header[col_metric] != "metric_code":
        header_updates.append((col_metric, "metric_code"))
    if header[col_source] != "source":
        header_updates.append((col_source, "source"))

    print(
        "Rows: {rows}, metric filled: {metric_filled}, source filled: {source_filled}, "
        "unknown labels: {unknown_labels}, conflicts: {conflicts}".format(**stats)
    )
    if unknown_labels:
        print("Unknown labels (first 20):")
        for label in unknown_labels[:20]:
            print(f"- {label}")

    if not args.apply:
        print("Dry-run only. Re-run with --apply to write changes.")
        return

    updates = []
    for col_idx, value in header_updates:
        cell = f"{col_to_a1(col_idx)}{header_idx + 1}"
        updates.append({"range": f"{args.sheet_name}!{cell}", "values": [[value]]})

    start_row = header_idx + 2
    end_row = header_idx + 1 + len(metric_values)
    metric_range = f"{args.sheet_name}!{col_to_a1(col_metric)}{start_row}:{col_to_a1(col_metric)}{end_row}"
    source_range = f"{args.sheet_name}!{col_to_a1(col_source)}{start_row}:{col_to_a1(col_source)}{end_row}"
    updates.append({"range": metric_range, "values": metric_values})
    updates.append({"range": source_range, "values": source_values})

    service.spreadsheets().values().batchUpdate(
        spreadsheetId=args.sheet_id,
        body={"valueInputOption": "RAW", "data": updates},
    ).execute()

    print("Sheet updated.")


if __name__ == "__main__":
    main()
