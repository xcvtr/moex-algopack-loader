#!/usr/bin/env python3
"""
MOEX AlgoPack fo/ data loader.
Loads tradestats, obstats, orderstats -> ClickHouse moex.*_fo tables.
Shared data source for all TQA projects (futures, options, stocks).

Usage:
  python3 scripts/load_algopack_fo.py                                          # incremental (missed dates)
  python3 scripts/load_algopack_fo.py --start 2020-01-03 --end 2026-06-21      # full range
  python3 scripts/load_algopack_fo.py --datasets tradestats obstats            # specific datasets
"""
import sys, os, time, argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta, datetime
import requests
import clickhouse_connect

CH_HOST = "10.0.0.63"
CH_PORT = 8123
CH_DB = "moex"
API_BASE = "https://apim.moex.com/iss/datashop/algopack/fo"

# read token
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env')
__tok = None
if os.path.exists(env_path):
    with open(env_path) as f:
        for line in f:
            if 'ALGOPACK_APIKEY' in line:
                parts = line.strip().split('=', 1)
                if len(parts) == 2:
                    __tok = parts[1].strip()
                break
if not __tok:
    sys.exit("FATAL: ALGOPACK_APIKEY not found in .env")

HEADERS = {"Authorization": "Bearer " + __tok}

# ── Column definitions ─────────────────────────────────────────────────

TS_COLS_RAW = ["tradedate","tradetime","secid","asset_code",
    "pr_open","pr_high","pr_low","pr_close","pr_std","vol","val",
    "trades","pr_vwap","pr_change","trades_b","trades_s",
    "val_b","val_s","vol_b","vol_s","disb","pr_vwap_b","pr_vwap_s",
    "im","oi_open","oi_high","oi_low","oi_close",
    "sec_pr_open","sec_pr_high","sec_pr_low","sec_pr_close","SYSTIME"]

OBS_COLS_RAW = ["tradedate","tradetime","secid","asset_code",
    "mid_price","micro_price","spread_l1",
    "levels_b","levels_s",
    "vol_b_l1","vol_s_l1","vol_b_l2","vol_s_l2",
    "vol_b_l3","vol_s_l3","vol_b_l5","vol_s_l5",
    "vol_b_l10","vol_s_l10","vol_b_l20","vol_s_l20",
    "vwap_b_l3","vwap_s_l3","SYSTIME"]

# obstats columns with int types (API may return float or empty string)
OBS_INT_COLS = {"levels_b","levels_s","vol_b_l1","vol_s_l1","vol_b_l2","vol_s_l2",
    "vol_b_l3","vol_s_l3","vol_b_l5","vol_s_l5",
    "vol_b_l10","vol_s_l10","vol_b_l20","vol_s_l20"}

ORD_COLS_RAW = ["tradedate","tradetime","secid","asset_code",
    "put_cancel_ratio","orders_b_put","orders_s_put",
    "orders_b_cancel","orders_s_cancel",
    "vwap_b","vwap_s","SYSTIME"]

HI2_COLS_RAW = ["tradedate","tradetime","secid","asset_code",
    "metric","value","reference","SYSTIME"]

ALERT_COLS_RAW = ["tradedate","tradetime","secid","asset_code",
    "alert_type","threshold","value","reference","SYSTIME"]

# ── Type converters ────────────────────────────────────────────────────

def _conv_tradedate(v):
    if v is None or v == '': return None
    return date.fromisoformat(v) if isinstance(v, str) else v

def _conv_systime(v):
    if v is None or v == '': return None
    if isinstance(v, str):
        v = v.replace('T', ' ')
        if '.' in v:
            return datetime.strptime(v, '%Y-%m-%d %H:%M:%S.%f')
        return datetime.strptime(v, '%Y-%m-%d %H:%M:%S')
    return v

def _conv_null(v):
    if v == '' or v is None: return None
    return v

def _conv_date_or_none(v):
    if v is None or v == '':
        return None
    if isinstance(v, str):
        return date.fromisoformat(v)
    return v

TS_CONV = {
    'tradedate': _conv_tradedate,
    'SYSTIME': _conv_systime,
}

FUTOI_COLS_RAW = ["sess_id","seqnum","tradedate","tradetime","ticker",
    "clgroup","pos","pos_long","pos_short",
    "pos_long_num","pos_short_num","systime","trade_session_date"]

FUTOI_CONV = {
    'tradedate': _conv_tradedate,
    'trade_session_date': _conv_date_or_none,
    'systime': _conv_systime,
}

# ── FUTOI: другой endpoint, другой формат JSON ────────────────────────

FUTOI_API = "https://apim.moex.com/iss/analyticalproducts/futoi/securities.json"

def fetch_futoi_date(date_str, cols_raw):
    """Fetch FUTOI — single request, no pagination (API returns all at once)."""
    try:
        params = {"date": date_str, "limit": 10000, "start": 0}
        r = requests.get(FUTOI_API, params=params, headers=HEADERS, timeout=60)
        if r.status_code != 200:
            return []
        rows = r.json().get("futoi", {}).get("data", [])
        if not rows:
            return []
        return [convert_row(cols_raw, row, FUTOI_CONV) for row in rows]
    except Exception as e:
        print(f"  ERR futoi {date_str}: {e}", file=sys.stderr)
        return []

DATASETS = {
    "tradestats": {"cols_raw": TS_COLS_RAW, "table": "tradestats_fo", "conv": TS_CONV},
    "obstats":    {"cols_raw": OBS_COLS_RAW, "table": "obstats_fo",
                   "conv": {'tradedate': _conv_tradedate, 'SYSTIME': _conv_systime},
                   "int_cols": OBS_INT_COLS, "str_cols": {"asset_code"}},
    "orderstats": {"cols_raw": ORD_COLS_RAW, "table": "orderstats_fo",
                   "conv": {'tradedate': _conv_tradedate, 'SYSTIME': _conv_systime}},
    "hi2":       {"cols_raw": HI2_COLS_RAW, "table": "hi2_fo",
                  "conv": {'tradedate': _conv_tradedate, 'SYSTIME': _conv_systime}},
    "alerts":    {"cols_raw": ALERT_COLS_RAW, "table": "alerts_fo",
                  "conv": {'tradedate': _conv_tradedate, 'SYSTIME': _conv_systime}},
    "futoi":     {"cols_raw": FUTOI_COLS_RAW, "table": "futoi",
                  "conv": {'tradedate': _conv_tradedate, 'SYSTIME': _conv_systime},
                  "fetch_fn": fetch_futoi_date},
}
def convert_row(cols_raw, row, conv_map, int_cols=None, str_cols=None):
    converted = []
    for c, v in zip(cols_raw, row):
        if c in conv_map:
            converted.append(conv_map[c](v))
        elif int_cols and c in int_cols:
            # int columns: API may return float or empty string
            if v is None or v == '':
                converted.append(None)
            else:
                converted.append(int(float(v)))
        elif str_cols and c in str_cols:
            # string columns: None -> ''
            if v is None:
                converted.append('')
            else:
                converted.append(_conv_null(v))
        else:
            converted.append(_conv_null(v))
    return converted

def fetch_date_all(dataset, date_str, cols_raw, conv_map, int_cols=None, str_cols=None):
    url = f"{API_BASE}/{dataset}.json"
    all_rows = []
    start = 0
    while True:
        try:
            params = {"date": date_str, "limit": 1000, "start": start}
            r = requests.get(url, params=params, headers=HEADERS, timeout=60)
            if r.status_code != 200:
                break
            rows = r.json().get("data", {}).get("data", [])
            if not rows:
                break
            all_rows.extend(rows)
            if len(rows) < 1000:
                break
            start += 1000
        except Exception as e:
            print(f"  ERR {dataset} {date_str}: {e}", file=sys.stderr)
            time.sleep(5)
            continue
    if not all_rows:
        return []
    return [convert_row(cols_raw, row, conv_map, int_cols, str_cols) for row in all_rows]

def insert_batch(ch, table, rows, cols_raw):
    if not rows:
        return
    try:
        ch.insert(table, rows, column_names=cols_raw)
    except Exception as e:
        print(f"  CH INSERT ERR {table}: {e}", file=sys.stderr)

def generate_dates(start, end):
    d = start
    while d <= end:
        if d.weekday() < 5:
            yield d.strftime("%Y-%m-%d")
        d += timedelta(days=1)

# ── Main ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="MOEX AlgoPack fo/ loader")
    parser.add_argument("--start", default=None)
    parser.add_argument("--end", default=None)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--datasets", nargs="+", default=["tradestats", "obstats", "orderstats"])
    parser.add_argument("--incremental", action="store_true", default=True,
                        help="Only load dates missing from CH tables (default)")
    parser.add_argument("--full", action="store_true",
                        help="Force full reload of all dates")
    args = parser.parse_args()

    # Determine date range
    today = date.today()
    if args.start and args.end:
        start_date = date.fromisoformat(args.start)
        end_date = date.fromisoformat(args.end)
    else:
        # Incremental: last 7 days by default (if tables empty, will grab all)
        start_date = today - timedelta(days=7)
        end_date = today

    # For --full with no explicit dates: use hardcoded origin
    if args.full and not args.start:
        start_date = date(2020, 1, 3)
        end_date = today

    ch = clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT, database=CH_DB)
    ch.command("SET max_partitions_per_insert_block = 0")
    ch.command("SET async_insert = 1")
    ch.command("SET wait_for_async_insert = 0")

    dates = list(generate_dates(start_date, end_date))
    print(f"Dates: {len(dates)} ({dates[0]} .. {dates[-1]})", file=sys.stderr)

    for ds_name in args.datasets:
        if ds_name not in DATASETS:
            print(f"  Unknown dataset: {ds_name}, skipping", file=sys.stderr)
            continue
        ds = DATASETS[ds_name]
        table = ds["table"]
        cols_raw = ds["cols_raw"]
        conv_map = ds["conv"]

        if args.incremental and not args.full:
            existing = set()
            try:
                res = ch.query(f"SELECT DISTINCT tradedate FROM {table}").result_rows
                existing = {str(r[0]) for r in res}
            except Exception:
                pass
            pending = [d for d in dates if d not in existing]
        else:
            pending = dates

        print(f"\n{ds_name} -> {table}: {len(dates)} total, {len(dates)-len(pending)} existing, {len(pending)} pending",
              file=sys.stderr)

        if not pending:
            print(f"  {ds_name}: nothing to load", file=sys.stderr)
            continue

        t0 = time.time()
        total_rows = 0
        done = 0
        batch_buffer = []

        fetch_fn = ds.get("fetch_fn", None)

        if fetch_fn:
            # Кастомный fetch (FUTOI) — однопоточный (нет пагинации)
            for d in pending:
                rows = fetch_fn(d, cols_raw)
                done += 1
                if rows:
                    batch_buffer.extend(rows)
                    total_rows += len(rows)
                    if len(batch_buffer) >= 50000:
                        insert_batch(ch, table, batch_buffer, cols_raw)
                        batch_buffer = []
                if done % 20 == 0 or done == len(pending):
                    elapsed = time.time() - t0
                    rate = done / elapsed if elapsed > 0 else 0
                    eta = (len(pending) - done) / rate if rate > 0 else 0
                    print(f"  {ds_name}: {done}/{len(pending)} days, {total_rows} rows, "
                          f"{rate:.1f}/s, ETA {eta:.0f}s", file=sys.stderr)
            if batch_buffer:
                insert_batch(ch, table, batch_buffer, cols_raw)
        else:
            with ThreadPoolExecutor(max_workers=args.workers) as pool:
                futs = {pool.submit(fetch_date_all, ds_name, d, cols_raw, conv_map,
                                    ds.get("int_cols"), ds.get("str_cols")): d for d in pending}
            for fut in as_completed(futs):
                d = futs[fut]
                rows = fut.result()
                done += 1
                if rows:
                    batch_buffer.extend(rows)
                    total_rows += len(rows)
                    if len(batch_buffer) >= 50000:
                        insert_batch(ch, table, batch_buffer, cols_raw)
                        batch_buffer = []
                elapsed = time.time() - t0
                rate = done / elapsed if elapsed > 0 else 0
                eta = (len(pending) - done) / rate if rate > 0 else 0
                if done % 20 == 0 or done == len(pending):
                    print(f"  {ds_name}: {done}/{len(pending)} days, {total_rows} rows, "
                          f"{rate:.1f}/s, ETA {eta:.0f}s", file=sys.stderr)
            if batch_buffer:
                insert_batch(ch, table, batch_buffer, cols_raw)

        elapsed = time.time() - t0
        print(f"  DONE {ds_name}: {total_rows} rows in {elapsed:.0f}s "
              f"({total_rows/elapsed:.0f} rows/s)", file=sys.stderr)

    ch.close()
    print("\nALL DONE!", file=sys.stderr)

if __name__ == "__main__":
    main()
