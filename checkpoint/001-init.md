# 001 — Создание проекта moex-algopack

**Дата:** 2026-06-21
**Статус:** проект создан, загрузка obstats + orderstats запущена

## Что сделано

1. **Создан репозиторий** `moex-algopack` — общий загрузчик MOEX AlgoPack fo/ для всех TQA-проектов.
   - GitHub: https://github.com/xcvtr/moex-algopack
   - Локально: `~/projects/moex-algopack/`

2. **Перенесён загрузчик** `scripts/load_algopack_fo.py` — умеет все 3 датасета:
   - **tradestats** → `moex.tradestats_fo` (21,097,288 rows)
   - **obstats** → `moex.obstats_fo` (0 rows — загружается)
   - **orderstats** → `moex.orderstats_fo` (0 rows — загружается)

3. **Symlink** — старый скрипт в TQA-MOEX-futures заменён на symlink: `scripts/load_algopack_fo.py → moex-algopack/scripts/load_algopack_fo.py`

4. **Настроен cron** — `moex-algopack-daily` (job_id: 1985dbafd127), каждый будний день в 06:00

5. **Запущена историческая загрузка** obstats и orderstats в фоне (каждая — 6 лет, 1686 торговых дней)

## Текущее состояние

| Датасет | Таблица | Строки | Статус |
|---------|---------|--------|--------|
| tradestats | tradestats_fo | 21,097,288 | ✅ Загружено |
| obstats | obstats_fo | 0 → загружается ⏳ | 🔄 В процессе |
| orderstats | orderstats_fo | 0 → загружается ⏳ | 🔄 В процессе |

## Структура проекта

```
moex-algopack/
├── scripts/
│   └── load_algopack_fo.py   # основной загрузчик
├── run_daily.sh               # cron-обёртка
├── .env                       # ALGOPACK_APIKEY
├── README.md                  # документация
├── AGENTS.md                  # инструкция для агентов
└── checkpoint/                # чекпойнты
```

## Что дальше

- Дождаться завершения obstats + orderstats
- Возможно: добавить агрегацию до H1/D1 для option-rf (коллар-стратегия не требует минутных данных)
- Обновить AGENTS.md в TQA-MOEX-futures указав новый проект как источник данных
