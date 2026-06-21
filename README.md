# moex-algopack-loader

MOEX AlgoPack fo/ data loader — общий источник данных AlgoPack для всех TQA-проектов.

## Топология ClickHouse

**Кластер:** 2 ноды, `ReplicatedMergeTree`. Есть VIP для чтения.

| Роль | Хост | Макросы | Порт |
|------|------|---------|------|
| Primary (запись) | `10.0.0.63` | `replica=1, shard=1` | `8123` (HTTP) / `9000` (native) |
| Replica (чтение) | `10.0.0.60` | `replica=2, shard=1` | `8123` (HTTP) / `9000` (native) |
| VIP (replica) | `10.0.0.64` | → `10.0.0.60` | `8123` (HTTP) / `9000` (native) |

**Загрузчик пишет на primary (63).** Репликация — ReplicatedMergeTree, данные синхронизируются автоматически.
Читать можно с любого хоста — все три указывают в БД `moex` без пароля.


## Назначение

Загружает датасеты MOEX AlgoPack fo/ в ClickHouse (10.0.0.63:8123, БД `moex`).
Данные доступны всем проектам: TQA-MOEX-futures, TQA-MOEX-options, option-rf и др.

## Датaсеты

| Датасет | Таблица CH | Статус | Строк | Что содержит |
|---------|-----------|--------|-------|-------------|
| `tradestats` | `moex.tradestats_fo` | ✅ | 21M | OHLC, OI (open/high/low/close), disb, vol_b/s, VWAP |
| `obstats` | `moex.obstats_fo` | 🔄 | 0 | Стакан (спреды, объёмы на L1-L20, micro/mid price) |
| `orderstats` | `moex.orderstats_fo` | ❌ | 0 | Заявки (put/cancel ratio, orders_b/s_put, orders_b/s_cancel, VWAP) — **API отдаёт пусто** |

Период: 2020-01-03 — настоящее время (торговые дни).
Данные лежат в ClickHouse на **10.0.0.63:8123** (без пароля, БД `moex`).

## Схема таблиц

### `tradestats_fo` — 21 млн строк

```sql
tradedate      Date              -- дата торгов
tradetime      String            -- время МСК (HH:MM:SS)
secid          LowCardinality(String)  -- код фьючерса на MOEX (Si, Eu, BR...)
asset_code     LowCardinality(String)  -- код базового актива (USD000UTSTOM, …)
pr_open/high/low/close  Float64  -- OHLC цены
pr_std         Float64           -- стандартное отклонение
vol            Float64           -- объём в контрактах
val            Float64           -- объём в рублях
trades         Int64             -- количество сделок
pr_vwap        Float64           -- VWAP
pr_change      Float64           -- изменение цены за день %
trades_b/s     Int64             -- количество агрессивных покупок/продаж
val_b/s        Float64           -- объём агрессивных покупок/продаж (руб)
vol_b/s        Float64           -- объём агрессивных покупок/продаж (контракты)
disb           Float64           -- дисбаланс агрессивных сделок (vol_b - vol_s) / (vol_b + vol_s)
pr_vwap_b/s    Float64           -- VWAP агрессивных покупок/продаж
im             Float64           -- initial margin
oi_open/high/low/close  Float64  -- OI (open/high/low/close)
sec_pr_open/high/low/close  Float64  -- OHLC базового актива
SYSTIME        DateTime64(6)     -- время загрузки
```

**Пример запроса:**
```sql
SELECT asset_code, secid, tradedate, tradetime, disb, vol_b, vol_s, oi_close
FROM moex.tradestats_fo
WHERE secid = 'Si' AND tradedate >= '2026-01-01'
ORDER BY tradedate, tradetime
LIMIT 10
```

### `obstats_fo` — стакан (0 строк, загружается)

```sql
tradedate      Date              -- дата торгов
tradetime      String            -- время МСК
secid          LowCardinality(String)
asset_code     LowCardinality(String)
mid_price      Nullable(Float64) -- средняя цена (bid+ask)/2
micro_price    Nullable(Float64) -- микро-цена (взвешенная)
spread_l1      Nullable(Float64) -- спред L1
levels_b/s     Nullable(Int32)   -- количество уровней bid/ask
vol_b/s_l1..l20 Nullable(Int64)  -- объём L1-L20 на bid/ask
vwap_b/s_l3    Nullable(Float64) -- VWAP L3
SYSTIME        Nullable(DateTime64(6))
```

### `orderstats_fo` — заявки (0 строк, API пустой)

```sql
tradedate      Date
tradetime      String
secid          LowCardinality(String)
asset_code     LowCardinality(String)
put_cancel_ratio Nullable(Float64) -- отношение отмен к выставлениям
orders_b/s_put   Nullable(Int64)   -- количество выставленных заявок
orders_b/s_cancel Nullable(Int64)  -- количество отменённых заявок
vwap_b/s       Nullable(Float64)   -- VWAP заявок
SYSTIME        Nullable(DateTime64(6))
```

## Для потребителей данных

Таблицы доступны **на чтение без аутентификации** с любого хоста кластера:

```
clickhouse-client --host 10.0.0.63 --port 9000 --database moex   # primary
clickhouse-client --host 10.0.0.60 --port 9000 --database moex   # replica
clickhouse-client --host 10.0.0.64 --port 9000 --database moex   # VIP → replica
```

Или через HTTP API (например, из Python):
```python
import clickhouse_connect
ch = clickhouse_connect.get_client(host='10.0.0.63', port=8123, database='moex')
ch.query('SELECT count() FROM tradestats_fo')
```

Пример для option-rf (часовые агрегаты OI фьючерса):
```sql
SELECT
    toStartOfHour(toDateTime(tradedate, 'UTC') + toIntervalSecond(timeToSec(tradetime))) AS hour,
    argMax(oi_close, tradetime) AS oi,
    argMax(disb, tradetime) AS disb,
    argMax(im, tradetime) AS im
FROM moex.tradestats_fo
WHERE secid = 'Si' AND tradedate >= '2026-01-01'
GROUP BY hour
ORDER BY hour
```

## Использование

```bash
# Инкрементальная загрузка (пропущенные даты за последние 7 дней)
python3 scripts/load_algopack_fo.py

# Полная загрузка всех датасетов за всё время
python3 scripts/load_algopack_fo.py --full

# Конкретный диапазон
python3 scripts/load_algopack_fo.py --start 2025-01-01 --end 2025-06-01

# Только obstats + orderstats
python3 scripts/load_algopack_fo.py --datasets obstats orderstats

# Скрипт для cron
./run_daily.sh
```

## Требования

- Python 3.10+
- pip: `clickhouse-connect`, `requests`
- `.env` с `ALGOPACK_APIKEY=<токен>`

## Токен

Копируется из TQA-MOEX-futures при первом запуске `run_daily.sh`.
Или вручную: `cp /path/to/TQA-MOEX-futures/.env .env`

## Legacy: moex_algopack_v2

Старая БД с Distributed таблицами (предыдущая версия загрузчика).
Содержит ~3.44M строк по tradestats/obstats/orderstats (старые данные, без OI-полей).

**Переезд:** `moex_algopack_v2.tradestats` заменён на VIEW, который читает из `moex.tradestats_fo` (21M строк). Все скрипты, обращающиеся к `moex_algopack_v2.tradestats`, теперь получают актуальные данные.

Обновление скриптов на новую схему:
```python
# Старое (работает через VIEW):
ch.query("SELECT ticker, disb FROM moex_algopack_v2.tradestats")

# Новое (напрямую):
ch.query("SELECT asset_code AS ticker, disb FROM moex.tradestats_fo")
```

`moex_algopack_v2.obstats` и `moex_algopack_v2.orderstats` остаются как есть — их схемы несовместимы с `obstats_fo`/`orderstats_fo`.

## Структура

- `scripts/load_algopack_fo.py` — основной загрузчик
- `run_daily.sh` — cron-обёртка
- `checkpoint/` — чекпойнты
