from __future__ import annotations

from fastapi import HTTPException, Request, status

from .config import settings


def authenticate(username: str, password: str) -> bool:
    if username == settings.admin_user and password == settings.admin_pass:
        return True
    if username == settings.admin2_user and password == settings.admin2_pass:
        return True
    return False


def require_admin(request: Request) -> None:
    user = request.session.get("user")
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Не авторизован")
