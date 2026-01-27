from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parents[3]
load_dotenv(ROOT_DIR / ".env")

INDICATORS_DIR_NAME = "\u041f\u043e\u043a\u0430\u0437\u0430\u0442\u0435\u043b\u0438"


def _resolve_path(value: str | Path, base: Path) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return base / path


@dataclass(frozen=True)
class CuteamSettings:
    root_dir: Path
    data_dir: Path
    db_url: str | None
    db_url_env: str | None
    db_path: Path
    heatmap_db_url: str | None
    heatmap_db_path: Path
    heatmap_groups_path: Path
    heatmap_groups_resolved_path: Path
    schema_path: Path
    branch_mapping_path: Path
    metric_mapping_path: Path


def load_settings() -> CuteamSettings:
    data_dir = _resolve_path(os.getenv("DATA_DIR", str(ROOT_DIR / "data")), ROOT_DIR)
    db_url = os.getenv("CUTEAM_DATABASE_URL")
    db_url_env = None
    if db_url:
        db_url_env = "CUTEAM_DATABASE_URL"
    else:
        db_url = os.getenv("DATABASE_URL")
        if db_url:
            db_url_env = "DATABASE_URL"
    db_path = _resolve_path(os.getenv("CUTEAM_DB_PATH", str(data_dir / "cuteam.db")), ROOT_DIR)
    heatmap_db_url = (
        os.getenv("HEATMAP_DATABASE_URL")
        or os.getenv("HEATMAP_DB_URL")
        or os.getenv("DATABASE_URL")
    )
    heatmap_db_path = _resolve_path(
        os.getenv("HEATMAP_DB_PATH", os.getenv("DB_PATH", str(data_dir / "app.db"))),
        ROOT_DIR,
    )
    heatmap_groups_path = _resolve_path(
        os.getenv("HEATMAP_GROUPS_PATH", str(ROOT_DIR / "config" / "groups.json")),
        ROOT_DIR,
    )
    heatmap_groups_resolved_path = _resolve_path(
        os.getenv(
            "HEATMAP_GROUPS_RESOLVED_PATH", str(ROOT_DIR / "config" / "groups_resolved.json")
        ),
        ROOT_DIR,
    )
    indicators_dir = ROOT_DIR / INDICATORS_DIR_NAME
    schema_path = indicators_dir / "shared" / "db" / "schema.sql"
    branch_mapping_path = indicators_dir / "data" / "branch_mapping.json"
    metric_mapping_path = indicators_dir / "data" / "metric_mapping.json"
    return CuteamSettings(
        root_dir=ROOT_DIR,
        data_dir=data_dir,
        db_url=db_url.strip() if db_url else None,
        db_url_env=db_url_env,
        db_path=db_path,
        heatmap_db_url=heatmap_db_url.strip() if heatmap_db_url else None,
        heatmap_db_path=heatmap_db_path,
        heatmap_groups_path=heatmap_groups_path,
        heatmap_groups_resolved_path=heatmap_groups_resolved_path,
        schema_path=schema_path,
        branch_mapping_path=branch_mapping_path,
        metric_mapping_path=metric_mapping_path,
    )


settings = load_settings()
