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
    "coworking_revenue": "Коворкинг — выручка",
    "load": "Загрузка",
    "coffee_fact": "Кофейня — факт",
    "coffee_categories": "Кофейня — категории",
}


D1_METRICS = [
    MetricDef("revenue_open_space", "revenue_open_space", "Аренда общий зал", "rub", "coworking_revenue", plan=True),
    MetricDef("revenue_cabinets", "revenue_cabinets", "Аренда кабинеты", "rub", "coworking_revenue", plan=True),
    MetricDef("revenue_lab", "revenue_lab", "Лаборатория", "rub", "coworking_revenue", plan=True),
    MetricDef("revenue_retail", "revenue_retail", "Ритейл", "rub", "coworking_revenue", plan=True),
    MetricDef("load_percent", "load_percent", "Загрузка площадки %", "pct", "load"),
    MetricDef("coffee_revenue_total", "coffee_revenue_total", "Кофейня выручка всего", "rub", "coffee_fact", plan=True),
    MetricDef("coffee_checks", "coffee_checks", "Кофейня чеки", "qty", "coffee_fact", plan=False),
    MetricDef("avg_check", "avg_check", "Кофейня средний чек", "rub", "coffee_fact", derived=True),
    MetricDef("written_off_food_total", "written_off_food_total", "Кофейня списание еды", "rub", "coffee_fact"),
    MetricDef("sold_food_total_total", "sold_food_total", "Кофейня продано еды всего", "rub", "coffee_fact"),
    MetricDef("revenue_coffee_hot", "revenue_coffee_hot", "Кофе/горячие", "rub", "coffee_categories"),
    MetricDef("revenue_drinks_cold", "revenue_drinks_cold", "Холод. напитки", "rub", "coffee_categories"),
    MetricDef("revenue_desserts", "revenue_desserts", "Десерты", "rub", "coffee_categories"),
    MetricDef("sold_food_total_food", "sold_food_total", "Еда", "rub", "coffee_categories"),
]


PLAN_METRIC_CODES = {metric.code for metric in D1_METRICS if metric.plan}
METRIC_BY_KEY = {metric.key: metric for metric in D1_METRICS}
