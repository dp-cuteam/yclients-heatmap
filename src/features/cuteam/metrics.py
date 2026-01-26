from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MetricDef:
    key: str
    code: str
    label: str
    unit: str
    group: str
    plan: bool = False
    derived: bool = False


GROUP_LABELS = {
    "summary_total": "Выручка",
    "summary_coworking": "Коворкинг",
    "summary_coffee": "Кофейня",
}


D1_METRICS = [
    MetricDef("revenue_total", "revenue_total", "Выручка", "rub", "summary_total", plan=True),
    MetricDef("revenue_open_space", "revenue_open_space", "Аренда", "rub", "summary_coworking", plan=True),
    MetricDef("revenue_cabinets", "revenue_cabinets", "Кабинеты", "rub", "summary_coworking", plan=True),
    MetricDef("revenue_lecture", "revenue_lecture", "Лекторий", "rub", "summary_coworking", plan=True),
    MetricDef("revenue_lab", "revenue_lab", "Лаборатория", "rub", "summary_coworking", plan=True),
    MetricDef("revenue_retail", "revenue_retail", "Ритейл", "rub", "summary_coworking", plan=True),
    MetricDef("revenue_salon", "revenue_salon", "Услуги салона", "rub", "summary_coworking", plan=True),
    MetricDef("coffee_revenue_total", "coffee_revenue_total", "Кофейня", "rub", "summary_coffee", plan=True),
]


PLAN_METRIC_CODES = {metric.code for metric in D1_METRICS if metric.plan}
METRIC_BY_KEY = {metric.key: metric for metric in D1_METRICS}
