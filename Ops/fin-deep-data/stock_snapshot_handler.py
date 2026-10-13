"""cronHandlerv2 -- 10-minute stock/ETF snapshot into TBLSNAPSHOT.

A python3.13 port of ``Ops/fin-cron-data/handler.py`` (``yf_stk_run``, the path
``run()`` takes; the DDS TCP ``stk_run`` is dead and is not ported -- TODOS
4.2). The original stays deployed and untouched; both write the same table, so
only one may be scheduled and this one's schedule ships **disabled** (cutover:
``doc/OPERATIONS.md`` §11.2).

``load_audit.job`` is ``cronHandler``.

**Why the rewrite.** The original made two sequential Yahoo calls --
``Ticker.history(period='1d')`` and ``Ticker.info`` -- for each of ~800
symbols: 517 s on average, 900 s (the timeout) at worst, on a 10-minute
schedule, so runs overlapped and a timed-out run wrote nothing.

Here every column comes from **one** ``Ticker.info`` call per symbol, made on a
pool of ``SNAP_THREADS`` workers: ``info`` already carries the session's
``regularMarketOpen/DayHigh/DayLow/Price/Volume`` next to the quote fields, so
the ``history`` call was redundant. Measured 2026-10-11 on the 856-symbol V3
list: **~85 s**. Yahoo caps the request *rate* at roughly 8-10 per second, so
more threads do not help -- and a batched ``yf.download`` of the same list took
110-155 s at any thread count, which is why it is not used. Each worker stops
starting new symbols once the Lambda is near its timeout.

Kept from the original, so the cutover diff is like for like:

* symbols from ``current_symbols_{SNAP_SYMBOL_PROC_VER}`` (V3 --
  ``load_symbols("system")``);
* ``pclose`` is ``info.previousClose``; ``name`` defaults to the symbol when
  Yahoo has no ``longName`` key; a missing volume is 0;
* a row with any missing value in the 13 original columns is dropped, as the
  original's ``dropna(how='any')`` did;
* ``timestamp`` is ET text, ``%Y/%m/%d-%H:%M:%S``.

New: ``quoteType`` (a table column the original never filled) is written, and
the DELETE and insert are one transaction (``DU.replace_snapshot``) that keeps
the previous snapshot when the fetch comes back empty.

Runs on **python3.13** with the ``finDeepCore`` + ``finDeepYf`` layers.

Local dry run (writes snapshot_yf.csv, touches no table)::

    cd Ops/fin-deep-data
    ../../venv-py313/bin/python stock_snapshot_handler.py
"""

import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import pandas as pd
import pytz
import yfinance as yf
from dotenv import load_dotenv

import dataUtil as DU

logger = logging.getLogger()

load_dotenv()

JOB = "cronHandler"

#: The original's columns -- the dropna(how='any') set.
CORE_COLUMNS = ["Symbol", "open", "high", "low", "last", "volume", "bid", "bidvol",
                "ask", "askvol", "pclose", "name", "timestamp"]
#: snapshot's columns, in table order.
SNAP_COLUMNS = CORE_COLUMNS + ["quoteType"]

#: Ticker.info key -> snapshot column. Several keys for one column are tried in order.
INFO_FIELDS = {"open": ("regularMarketOpen", "open"),
               "high": ("regularMarketDayHigh", "dayHigh"),
               "low": ("regularMarketDayLow", "dayLow"),
               "last": ("regularMarketPrice", "currentPrice"),
               "volume": ("regularMarketVolume", "volume"),
               "bid": ("bid",), "bidvol": ("bidSize",),
               "ask": ("ask",), "askvol": ("askSize",),
               "pclose": ("previousClose", "regularMarketPreviousClose"),
               "quoteType": ("quoteType",)}

FLOAT_COLUMNS = ["open", "high", "low", "last", "volume", "bid", "bidvol", "ask", "askvol", "pclose"]

#: Stop starting new info calls with this much Lambda time left.
RESERVE_MS = 60_000


def _first(info, keys):
    for key in keys:
        value = info.get(key)
        if value is not None:
            return value
    return None


def quote_fields(sym, info):
    """The snapshot's columns, other than Symbol and timestamp, from one Ticker.info. Pure."""
    row = {col: _first(info, keys) for col, keys in INFO_FIELDS.items()}
    row["name"] = info.get("longName", sym)
    if row["volume"] is None or pd.isna(row["volume"]):
        row["volume"] = 0                       # the original's NaN volume -> 0
    return row


def fetch_quotes(symbols, threads, context=None, ticker_factory=yf.Ticker):
    """Ticker.info for each symbol on a thread pool. Returns ({sym: fields}, n_skipped).

    A failed symbol is logged and left out -- its row is then dropped, as the
    original's per-symbol ``except: continue`` did. A worker that finds the
    Lambda within RESERVE_MS of its timeout skips the symbol instead of
    starting a call that could be cut off mid-flight.
    """
    skipped = []

    def _one(sym):
        if not DU.time_left_ok(context, reserve_ms=RESERVE_MS):
            skipped.append(sym)
            return sym, None
        try:
            return sym, quote_fields(sym, ticker_factory(sym).info or {})
        except Exception as e:
            logging.error(f"{sym}: info failed: {type(e).__name__}: {e}")
            return sym, None

    with ThreadPoolExecutor(max_workers=max(int(threads), 1)) as pool:
        results = dict(pool.map(_one, symbols))
    if skipped:
        logging.warning(f"time guard: {len(skipped)} symbol(s) skipped")
    return {s: q for s, q in results.items() if q is not None}, len(skipped)


def build_snapshot(symbols, quotes, stamp):
    """The snapshot frame, in symbol order. Pure.

    Rows with any missing core value are dropped, as the original's
    ``dropna(how='any')`` did; ``quoteType`` may be missing.
    """
    rows = [{"Symbol": sym, **quotes[sym], "timestamp": stamp} for sym in symbols if sym in quotes]
    df = pd.DataFrame(rows, columns=SNAP_COLUMNS).dropna(subset=CORE_COLUMNS, how="any")
    for col in FLOAT_COLUMNS:
        df[col] = df[col].astype(float)
    return df.reset_index(drop=True)


def stk_run(event, context, localrun=False, dbFlag=True):
    ny_time = event.get("NYTIME") or datetime.now(pytz.timezone("US/Eastern"))
    stamp = ny_time.strftime("%Y/%m/%d-%H:%M:%S")
    logger.info(f"stock_snapshot_handler ran at {stamp} ET")
    schema = DU.require_env("DBMKTDATA")
    table = DU.require_env("TBLSNAPSHOT")
    ver = DU.require_env("SNAP_SYMBOL_PROC_VER")
    threads = int(DU.env_or("SNAP_THREADS", "8"))

    symbols = DU.load_symbols_db(ver)
    if not symbols:
        raise RuntimeError(f"symbol list current_symbols_{ver} returned nothing")
    cap = event.get("test")
    if isinstance(cap, int) and not isinstance(cap, bool) and cap > 0:
        symbols = symbols[:cap]

    t0 = time.monotonic()
    quotes, skipped = fetch_quotes(symbols, threads, context)
    snap = build_snapshot(symbols, quotes, stamp)
    logging.info(f"{len(symbols)} symbol(s): {len(quotes)} quote(s) in "
                 f"{time.monotonic() - t0:.1f} s on {threads} thread(s), {len(snap)} row(s) kept")

    if not dbFlag or localrun:
        out = os.path.join(DU.out_dir(localrun), f"{table}_yf.csv")
        snap.to_csv(out, index=False)
        logging.info(f"dry run: wrote {len(snap)} row(s) to {out}")
    else:
        DU.replace_snapshot(snap, schema, table)
    return snap, len(symbols), skipped


def _audit(run_id, started_at, snap, n_expected, skipped=0, status="ok", error=None):
    """One load_audit summary row. Never raises."""
    try:
        n = 0 if snap is None else len(snap)
        if status == "ok" and n == 0:
            status = "empty"
        elif status == "ok" and skipped:
            status = "partial"
        today = DU.utc_now().date()
        DU.audit_run([DU.audit_summary(
            JOB, os.environ.get("TBLSNAPSHOT"), run_id, started_at, status=status, n_rows=n,
            n_ok=n, n_expected=n_expected, date_lo=today, date_hi=today, error=error,
            yf_version=yf.__version__[:16])])
    except Exception:
        logging.error("load_audit summary failed", exc_info=True)


def run(event, context):
    """Lambda entry point.

    Event keys: ``localrun`` / ``dbFlag`` (False = CSV only, no table and no
    audit row), ``test`` (an int caps the symbol count, any value turns on DEBUG
    logging), ``NYTIME`` (injected clock).
    """
    event = dict(event or {})
    logger.setLevel(logging.DEBUG if "test" in event else logging.INFO)
    logging.info(f"** ==> stock_snapshot_handler.run(event: {event}, context: {context})")
    localrun = bool(event.get("localrun", False))
    dbFlag = bool(event.get("dbFlag", True)) and not localrun
    event.setdefault("NYTIME", datetime.now(pytz.timezone("US/Eastern")))
    run_id, started_at = DU.new_run_id(), DU.utc_now()
    try:
        snap, n_expected, skipped = stk_run(event, context, localrun=localrun, dbFlag=dbFlag)
    except Exception as e:
        if dbFlag:
            _audit(run_id, started_at, None, None, status="error", error=f"{type(e).__name__}: {e}")
        raise
    if dbFlag:
        _audit(run_id, started_at, snap, n_expected, skipped)
    return {"run_id": run_id, "rows": len(snap), "symbols": n_expected, "skipped": skipped}


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    # Dry run: reads the V3 symbol list, downloads from Yahoo, writes
    # snapshot_yf.csv, and touches neither snapshot nor load_audit. "test" is
    # a string here, so it only raises the log level -- the full list runs.
    print(run({"localrun": True, "dbFlag": False, "test": "True"}, None))
