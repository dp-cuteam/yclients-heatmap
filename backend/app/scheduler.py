from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from .config import settings
from .etl import run_daily
from .yclients import build_client


_scheduler: BackgroundScheduler | None = None


def _daily_job():
    client = build_client()
    # Use yesterday in configured timezone
    tz = ZoneInfo(settings.timezone)
    target = (datetime.now(tz=tz) - timedelta(days=1)).date()
    run_daily(client, target_day=target)


def start_scheduler() -> None:
    global _scheduler
    if _scheduler is not None:
        return
    tz = ZoneInfo(settings.timezone)
    _scheduler = BackgroundScheduler(timezone=tz)
    _scheduler.add_job(_daily_job, CronTrigger(hour=6, minute=0))
    _scheduler.start()


def stop_scheduler() -> None:
    global _scheduler
    if _scheduler:
        _scheduler.shutdown(wait=False)
        _scheduler = None
