"""FXrateHandlerv2 -- hourly FX spot snapshot into TBLFXSNAPSHOT.

A python3.13 port of ``Ops/fin-cron-data/fx_handler.py`` (TODOS 5.10). The
original stays deployed and untouched; its EventBridge rule has been DISABLED
since 2026-10-07 02:00 UTC, so ``FX_snapshot`` has had no writer since. This
one's schedule ships **disabled** like every other in this service.

``load_audit.job`` is ``FXrateHandler``: the data set's job, not the Lambda.

Changed from the original:

* the DELETE and the insert are one transaction (``DU.replace_snapshot``), and
  an empty download keeps the previous snapshot instead of emptying the table;
* the table name is required (``require_env("TBLFXSNAPSHOT")``) rather than
  read with ``environ.get`` -- ``None`` used to reach the SQL string;
* ``localrun`` / ``dbFlag`` come from the event, like every handler here, and
  the no-``NYTIME`` branch no longer calls ``.strftime`` on a string
  (TODOS 4.3);
* ``last_updated`` is written tz-naive **UTC** and ``server_time`` tz-naive
  **ET** -- what PyMySQL already stored for the old tz-aware/str values, now
  explicit;
* the window is the last 5 days, not ``period="1d"``, and ``last_updated`` is
  each pair's own last bar (2026-10-12: the Asian pairs vanished every evening
  between London midnight and the Asia open).

Runs on **python3.13** with the ``finDeepCore`` + ``finDeepYf`` layers.

Local dry run (writes USD_FX.csv, touches no table)::

    cd Ops/fin-deep-data
    ../../venv-py313/bin/python fx_snapshot_handler.py
"""

import ast
import logging
import os
from datetime import datetime

import pandas as pd
import pytz
import yfinance as yf
from dotenv import load_dotenv

import dataUtil as DU

logger = logging.getLogger()

load_dotenv()

JOB = "FXrateHandler"
BASE_CUR = "USD"

#: FX_snapshot's columns, in table order.
FX_SNAP_COLUMNS = ["base_cur", "target_cur", "last_updated", "rate", "server_time"]

#: How far back to look for each pair's last quote. Not "1d": Yahoo's FX day
#: starts at London midnight (23:00 UTC in summer), and CNY/INR/IDR/MYR then
#: have no bar until Asia opens -- the 19:00 and 20:00 ET runs of 2026-10-12
#: dropped all four from the snapshot. 5d also spans a weekend.
FX_PERIOD = "5d"


def fx_tickers():
    """FX_TICKERS, a Python list literal in .env, sorted. Raises when unset or malformed."""
    tickers = ast.literal_eval(DU.require_env("FX_TICKERS"))
    if not isinstance(tickers, (list, tuple)) or not tickers:
        raise ValueError(f"FX_TICKERS must be a non-empty list literal, got {tickers!r}")
    return sorted(tickers)


def latest_rates(close, base, server_time):
    """One row per currency from its last non-NaN Close, plus base/base = 1.

    ``close`` is ``yf.download(...)['Close']``: a frame indexed by bar time with
    one column per ticker. Each row's ``last_updated`` is that ticker's own last
    bar, not the frame's: a thinly quoted pair (CNY, INR, IDR, MYR) can be an
    hour behind EUR, and stamping its rate with EUR's bar time would hide that.
    The base row takes the newest bar. A ticker with no value at all is dropped
    rather than written as NULL. Pure.
    """
    rows = []
    if close is not None and len(close) > 0:
        for ticker, series in close.items():
            when = series.last_valid_index()
            if when is None:
                logging.warning(f"{ticker}: no rate in the last {FX_PERIOD} of 5m bars, not written")
                continue
            rows.append({"base_cur": base, "target_cur": str(ticker).split("=")[0],
                         "last_updated": _utc_naive(when), "rate": float(series[when]),
                         "server_time": server_time})
        rows.append({"base_cur": base, "target_cur": base,
                     "last_updated": _utc_naive(close.index[-1]),
                     "rate": 1.0, "server_time": server_time})
    return pd.DataFrame(rows, columns=FX_SNAP_COLUMNS)


def _utc_naive(ts):
    ts = pd.Timestamp(ts)
    return ts.tz_convert("UTC").tz_localize(None) if ts.tzinfo is not None else ts


def fetch_close(tickers):
    """The last FX_PERIOD of 5-minute Closes per ticker; empty when Yahoo returns nothing."""
    data = yf.download(tickers, period=FX_PERIOD, interval="5m", auto_adjust=False,
                       progress=False)
    if data is None or len(data) == 0 or "Close" not in data:
        return pd.DataFrame()
    close = data["Close"]
    if isinstance(close, pd.Series):          # a single ticker comes back flat
        close = close.to_frame(tickers[0])
    return close


def fx_run(event, context, localrun=False, dbFlag=True):
    ny_time = event.get("NYTIME") or datetime.now(pytz.timezone("US/Eastern"))
    server_time = ny_time.replace(tzinfo=None, microsecond=0)
    logger.info(f"fx_snapshot_handler ran at {server_time:%Y/%m/%d-%H:%M:%S} ET")

    schema = DU.require_env("DBMKTDATA")
    table = DU.require_env("TBLFXSNAPSHOT")
    tickers = fx_tickers()
    logging.debug(tickers)

    fx_df = latest_rates(fetch_close(tickers), BASE_CUR, server_time)
    logging.info(f"{len(fx_df)} FX row(s), last bar {fx_df['last_updated'].max() if len(fx_df) else None}")

    if not dbFlag or localrun:
        out = os.path.join(DU.out_dir(localrun), f"{BASE_CUR}_FX.csv")
        fx_df.to_csv(out, index=False)
        logging.info(f"dry run: wrote {len(fx_df)} row(s) to {out}")
    else:
        DU.replace_snapshot(fx_df, schema, table)
    return fx_df


def _audit(run_id, started_at, fx_df, status="ok", error=None):
    """One load_audit summary row. Never raises."""
    try:
        n = 0 if fx_df is None else len(fx_df)
        day = None if not n else pd.Timestamp(fx_df["last_updated"].max()).date()
        if status == "ok" and n == 0:
            status = "empty"
        DU.audit_run([DU.audit_summary(
            JOB, os.environ.get("TBLFXSNAPSHOT"), run_id, started_at, status=status,
            n_rows=n, date_lo=day, date_hi=day, error=error,
            yf_version=yf.__version__[:16])])
    except Exception:
        logging.error("load_audit summary failed", exc_info=True)


def run(event, context):
    """Lambda entry point.

    Event keys: ``localrun`` / ``dbFlag`` (False = CSV only, no table and no
    audit row), ``test`` (DEBUG logging), ``NYTIME`` (injected clock).
    """
    event = dict(event or {})
    logger.setLevel(logging.DEBUG if "test" in event else logging.INFO)
    logging.info(f"** ==> fx_snapshot_handler.run(event: {event}, context: {context})")
    localrun = bool(event.get("localrun", False))
    dbFlag = bool(event.get("dbFlag", True)) and not localrun
    event.setdefault("NYTIME", datetime.now(pytz.timezone("US/Eastern")))
    run_id, started_at = DU.new_run_id(), DU.utc_now()
    try:
        fx_df = fx_run(event, context, localrun=localrun, dbFlag=dbFlag)
    except Exception as e:
        if dbFlag:
            _audit(run_id, started_at, None, status="error", error=f"{type(e).__name__}: {e}")
        raise
    if dbFlag:
        _audit(run_id, started_at, fx_df)
    return {"run_id": run_id, "rows": len(fx_df)}


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    # Dry run: downloads from Yahoo, writes USD_FX.csv, and touches neither
    # FX_snapshot nor load_audit.
    print(run({"localrun": True, "dbFlag": False, "test": "True"}, None))
