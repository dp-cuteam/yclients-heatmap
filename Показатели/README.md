# Cuteam metrics (MVP)

## Quick start
1) Install deps
```
pip install -r requirements.txt
```

2) Provide Google Sheet access (service account)
- Share the sheet with the service account email.
- Set env vars:
```
setx SHEET_ID "<google_sheet_id>"
setx SHEET_NAME "<sheet_title>"  # e.g. ITOGO-26
setx GOOGLE_SA_JSON "C:\path\to\service_account.json"
```
Or use base64:
```
setx GOOGLE_SA_JSON_B64 "<base64-json>"
```

3) Dry-run parse (no DB writes)
```
python -m ingest.sync_sheet --dry-run
```

4) Sync into SQLite (default: data/cuteam.db)
```
python -m ingest.sync_sheet
```

## Date filtering
```
python -m ingest.sync_sheet --date-from 2026-01-01 --date-to 2026-01-31
```

## Offline parse from .xlsx
```
python -m ingest.sync_sheet --xlsx "C:\path\to\sheet.xlsx" --dry-run
```

## Notes
- Date columns are detected from the header row containing the article label.
- Weekly/monthly totals are ignored because they are not date-typed.
- Empty cells are skipped (NULL). Explicit 0 is stored as 0.
- Branch codes are parsed from block headers. Summary blocks are ignored.
- Reference mappings: `data/metric_mapping.json`, `data/branch_mapping.json`.
