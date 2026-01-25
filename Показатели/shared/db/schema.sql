-- Base schema for SQLite/DuckDB (portable subset; adjust types for Postgres later).

CREATE TABLE IF NOT EXISTS branches (
    code TEXT PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS metrics (
    code TEXT PRIMARY KEY,
    label TEXT NOT NULL,
    unit TEXT,
    group_name TEXT,
    source_labels TEXT,
    is_derived INTEGER NOT NULL DEFAULT 0,
    aggregation TEXT
);

CREATE TABLE IF NOT EXISTS import_runs (
    id INTEGER PRIMARY KEY,
    source_name TEXT NOT NULL,
    file_path TEXT NOT NULL,
    file_hash TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL,
    error TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS import_runs_file_hash_uq
    ON import_runs (file_hash);

CREATE TABLE IF NOT EXISTS facts_daily (
    branch_code TEXT NOT NULL,
    metric_code TEXT NOT NULL,
    date TEXT NOT NULL,
    value REAL NOT NULL,
    import_run_id INTEGER,
    PRIMARY KEY (branch_code, metric_code, date),
    FOREIGN KEY (branch_code) REFERENCES branches (code),
    FOREIGN KEY (metric_code) REFERENCES metrics (code),
    FOREIGN KEY (import_run_id) REFERENCES import_runs (id)
);

CREATE TABLE IF NOT EXISTS derived_daily (
    branch_code TEXT NOT NULL,
    metric_code TEXT NOT NULL,
    date TEXT NOT NULL,
    value REAL NOT NULL,
    PRIMARY KEY (branch_code, metric_code, date),
    FOREIGN KEY (branch_code) REFERENCES branches (code),
    FOREIGN KEY (metric_code) REFERENCES metrics (code)
);

CREATE TABLE IF NOT EXISTS agg_weekly (
    branch_code TEXT NOT NULL,
    metric_code TEXT NOT NULL,
    week_start TEXT NOT NULL,
    week_end TEXT NOT NULL,
    value REAL NOT NULL,
    PRIMARY KEY (branch_code, metric_code, week_start),
    FOREIGN KEY (branch_code) REFERENCES branches (code),
    FOREIGN KEY (metric_code) REFERENCES metrics (code)
);

CREATE TABLE IF NOT EXISTS agg_monthly (
    branch_code TEXT NOT NULL,
    metric_code TEXT NOT NULL,
    month_start TEXT NOT NULL,
    month_end TEXT NOT NULL,
    value REAL NOT NULL,
    PRIMARY KEY (branch_code, metric_code, month_start),
    FOREIGN KEY (branch_code) REFERENCES branches (code),
    FOREIGN KEY (metric_code) REFERENCES metrics (code)
);

CREATE TABLE IF NOT EXISTS plans_monthly (
    branch_code TEXT NOT NULL,
    metric_code TEXT NOT NULL,
    month_start TEXT NOT NULL,
    value REAL NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (branch_code, metric_code, month_start),
    FOREIGN KEY (branch_code) REFERENCES branches (code),
    FOREIGN KEY (metric_code) REFERENCES metrics (code)
);

CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY,
    branch_code TEXT NOT NULL,
    signal_type TEXT NOT NULL,
    metric_code TEXT,
    period_type TEXT NOT NULL,
    period_start TEXT NOT NULL,
    period_end TEXT NOT NULL,
    delta_percent REAL,
    status TEXT NOT NULL,
    details TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (branch_code) REFERENCES branches (code),
    FOREIGN KEY (metric_code) REFERENCES metrics (code)
);

CREATE INDEX IF NOT EXISTS facts_daily_branch_date_idx
    ON facts_daily (branch_code, date);
CREATE INDEX IF NOT EXISTS facts_daily_metric_date_idx
    ON facts_daily (metric_code, date);
CREATE INDEX IF NOT EXISTS derived_daily_branch_date_idx
    ON derived_daily (branch_code, date);
CREATE INDEX IF NOT EXISTS agg_weekly_branch_start_idx
    ON agg_weekly (branch_code, week_start);
CREATE INDEX IF NOT EXISTS agg_monthly_branch_start_idx
    ON agg_monthly (branch_code, month_start);
CREATE INDEX IF NOT EXISTS plans_monthly_branch_start_idx
    ON plans_monthly (branch_code, month_start);
CREATE INDEX IF NOT EXISTS signals_branch_period_idx
    ON signals (branch_code, period_start, period_end);

-- MVP sync tables (manual sheet + Yclients) and merged view.
CREATE TABLE IF NOT EXISTS raw_yclients_daily (
    branch_code TEXT NOT NULL,
    metric_code TEXT NOT NULL,
    date TEXT NOT NULL,
    value REAL NOT NULL,
    source TEXT NOT NULL DEFAULT 'yclients',
    updated_at TEXT NOT NULL,
    PRIMARY KEY (branch_code, metric_code, date),
    FOREIGN KEY (branch_code) REFERENCES branches (code),
    FOREIGN KEY (metric_code) REFERENCES metrics (code)
);

CREATE TABLE IF NOT EXISTS manual_sheet_daily (
    branch_code TEXT NOT NULL,
    metric_code TEXT NOT NULL,
    date TEXT NOT NULL,
    value REAL NOT NULL,
    source TEXT NOT NULL DEFAULT 'manual',
    updated_at TEXT NOT NULL,
    PRIMARY KEY (branch_code, metric_code, date),
    FOREIGN KEY (branch_code) REFERENCES branches (code),
    FOREIGN KEY (metric_code) REFERENCES metrics (code)
);

CREATE VIEW IF NOT EXISTS fact_daily AS
SELECT
    branch_code,
    metric_code,
    date,
    value,
    source,
    updated_at
FROM manual_sheet_daily
UNION ALL
SELECT
    r.branch_code,
    r.metric_code,
    r.date,
    r.value,
    r.source,
    r.updated_at
FROM raw_yclients_daily r
LEFT JOIN manual_sheet_daily m
    ON m.branch_code = r.branch_code
    AND m.metric_code = r.metric_code
    AND m.date = r.date
WHERE m.branch_code IS NULL;

CREATE INDEX IF NOT EXISTS raw_yclients_daily_branch_date_idx
    ON raw_yclients_daily (branch_code, date);
CREATE INDEX IF NOT EXISTS manual_sheet_daily_branch_date_idx
    ON manual_sheet_daily (branch_code, date);
