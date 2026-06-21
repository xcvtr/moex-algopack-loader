См. [README.md](README.md)

Содержит загрузчик MOEX AlgoPack fo/. После загрузки данные в ClickHouse `moex.*_fo` — доступны всем проектам.

Не содержит стратегий и аналитики — только data pipeline.

Основные точки входа:
- `scripts/load_algopack_fo.py` — загрузчик (tradestats, obstats, orderstats, hi2, alerts, futoi)
- `run_daily.sh` — cron entrypoint

## Таблицы в CH (10.0.0.60/63, БД moex)

| Таблица | Строк | Диапазон | Описание |
|---------|-------|----------|----------|
| `tradestats_fo` | 21.1M | 2020-2026 | OHLC + OI + агрессивные сделки |
| `obstats_fo` | 46.9M | 2020-2024 | Стакан L1-L20 |
| `futoi` | 1.58M | 2020-2026 | Позиции FIZ/YUR по фьючерсам (78 тикеров) |
| `hi2_fo` | 1.14M | 2020-2026 | HHI-индекс концентрации |
| `alerts_fo` | 331K | 2024-2026 | События 99.9 перцентиля |
| `orderstats_fo` | 0 | — | API пустой |
