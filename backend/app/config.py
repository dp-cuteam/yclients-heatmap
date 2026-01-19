from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(BASE_DIR / ".env")


def _read_token_file(path: Path) -> str | None:
    if not path.exists() or not path.is_file():
        return None
    content = path.read_text(encoding="utf-8-sig").strip()
    return content or None


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    db_path: Path
    group_config_path: Path
    group_resolved_path: Path
    yclients_partner_token: str | None
    yclients_user_token: str | None
    yclients_base_url: str
    yclients_timeout: int
    yclients_retries: int
    admin_user: str
    admin_pass: str
    admin2_user: str
    admin2_pass: str
    session_secret: str
    timezone: str


def load_settings() -> Settings:
    data_dir = Path(os.getenv("DATA_DIR", BASE_DIR / "data"))
    db_path = Path(os.getenv("DB_PATH", data_dir / "app.db"))
    group_config_path = Path(
        os.getenv("GROUP_CONFIG_PATH", BASE_DIR / "config" / "groups.json")
    )
    group_resolved_path = Path(
        os.getenv("GROUP_CONFIG_RESOLVED_PATH", BASE_DIR / "config" / "groups_resolved.json")
    )

    token_file_env = os.getenv("YCLIENTS_TOKEN_FILE")
    if token_file_env and token_file_env.strip():
        token_file = Path(token_file_env.strip())
    else:
        token_file = Path(BASE_DIR / "api token.txt")
    partner_token = os.getenv("YCLIENTS_PARTNER_TOKEN") or _read_token_file(token_file)
    user_token = os.getenv("YCLIENTS_USER_TOKEN")

    admin_user = os.getenv("ADMIN_USER", "admin")
    admin_pass = os.getenv("ADMIN_PASS", "")
    admin2_user = os.getenv("ADMIN2_USER", "admin2")
    admin2_pass = os.getenv("ADMIN2_PASS", "")

    session_secret = os.getenv("SESSION_SECRET", "dev-secret-change-me")
    timezone = os.getenv("APP_TIMEZONE", "Europe/Moscow")

    return Settings(
        data_dir=data_dir,
        db_path=db_path,
        group_config_path=group_config_path,
        group_resolved_path=group_resolved_path,
        yclients_partner_token=partner_token,
        yclients_user_token=user_token,
        yclients_base_url=os.getenv("YCLIENTS_BASE_URL", "https://api.yclients.com"),
        yclients_timeout=int(os.getenv("YCLIENTS_TIMEOUT", "30")),
        yclients_retries=int(os.getenv("YCLIENTS_RETRIES", "3")),
        admin_user=admin_user,
        admin_pass=admin_pass,
        admin2_user=admin2_user,
        admin2_pass=admin2_pass,
        session_secret=session_secret,
        timezone=timezone,
    )


settings = load_settings()
