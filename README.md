# CUTEAM Heatmap

Веб‑приложение для отображения недельной теплокарты загрузки ресурсов по фактическим визитам YCLIENTS за 2025 год.

## Быстрый старт (локально)

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

Создайте `.env` или задайте переменные окружения:

```
YCLIENTS_PARTNER_TOKEN=...
YCLIENTS_USER_TOKEN=...          # опционально
SESSION_SECRET=...
ADMIN_USER=admin
ADMIN_PASS=...
ADMIN2_USER=admin2
ADMIN2_PASS=...
APP_TIMEZONE=Europe/Moscow
```

Запуск:

```bash
uvicorn backend.app.main:app --reload
```

## Конфиг групп

Исходный конфиг создаётся из `группировка.xlsx`:

`config/groups.json`

Во время ETL автоматически формируется `config/groups_resolved.json` с найденными `staff_id`.

## ETL

Полная загрузка 2025 запускается из админки:

`POST /api/admin/etl/full_2025/start`

Ежедневный ETL запускается планировщиком в 06:00 (Europe/Moscow).

## Деплой (Render)

Используется `Procfile`:

```
web: uvicorn backend.app.main:app --host 0.0.0.0 --port $PORT
```
"# yclients-heatmap" 
