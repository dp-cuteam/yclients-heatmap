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
    db_path: Path
    schema_path: Path
    branch_mapping_path: Path
    metric_mapping_path: Path


def load_settings() -> CuteamSettings:
    data_dir = _resolve_path(os.getenv("DATA_DIR", str(ROOT_DIR / "data")), ROOT_DIR)
    db_path = _resolve_path(os.getenv("CUTEAM_DB_PATH", str(data_dir / "cuteam.db")), ROOT_DIR)
    indicators_dir = ROOT_DIR / INDICATORS_DIR_NAME
    schema_path = indicators_dir / "shared" / "db" / "schema.sql"
    branch_mapping_path = indicators_dir / "data" / "branch_mapping.json"
    metric_mapping_path = indicators_dir / "data" / "metric_mapping.json"
    return CuteamSettings(
        root_dir=ROOT_DIR,
        data_dir=data_dir,
        db_path=db_path,
        schema_path=schema_path,
        branch_mapping_path=branch_mapping_path,
        metric_mapping_path=metric_mapping_path,
    )


settings = load_settings()
