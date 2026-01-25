from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from .settings import settings


TEMPLATES_DIR = Path(settings.root_dir) / "src" / "features" / "cuteam" / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

router = APIRouter(tags=["cuteam-ui"])


@router.get("/cuteam", response_class=HTMLResponse)
def cuteam_dashboard(request: Request):
    return templates.TemplateResponse("d1.html", {"request": request})
