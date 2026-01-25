# Mini App: Add Goods Fix ✅

## Изменения
Переписана функция `api_mini_add_good` в `backend/app/main.py` согласно рекомендации техподдержки YCLIENTS.

### Было → Стало
| Было | Стало |
|------|-------|
| 4 режима | 1 метод: `update_visit` |
| consumables API | `goods_transactions` |
| `amount: 10` | `amount: -10` |
| `cost_per_unit` | `price` и `cost` |

### Payload формат
```json
{
  "attendance": 1,
  "comment": "",
  "services": [],
  "goods_transactions": [{
    "good_id": 123,
    "storage_id": 456,
    "amount": -10,
    "price": 22,
    "cost": 220,
    "good_special_number": ""
  }]
}
```

## Коммиты
1. `[MINI] Fix add good: use update_visit with goods_transactions per YCLIENTS support`
2. `[MINI] Add walkthrough docs`
3. `[MINI] Remove mode selection UI - backend now uses only update_visit`
