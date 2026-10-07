"""FXHistHandlerv2 -- daily FX EOD bars into TBLHISTFX.

PLAN-SR-UPSTREAM Phase F (U10). A copy of
``Ops/fin-cron-data/fxeod_handler.py`` at fa1d6ac, moved to python3.13 and
extended with one ``load_audit`` summary row per run. The original stays
deployed and untouched; both append to the same table, so only one may be
scheduled and this one's schedule ships **disabled** (see ``doc/OPERATIONS.md``,
cutover runbook).

``load_audit.job`` stays ``FXHistHandler``: it names the data set's job, not the
Lambda, so the status report does not grow a second line at the cutover.

Changed from the original beyond the audit row:

* ``localrun`` is read from the event instead of a module global that only the
  ``__main__`` block could set, so a dry run works through ``run()`` as well;
* the download window is inclusive of ``end_dt`` and incomplete bars are
  dropped (2026-10-06), see ``fetch_exchange_rates``;
* the watermark fallback is logged. ``FIRSTTRAINDTE`` is also eodDaily's first
  date, and flipping it to 2008/01/01 (plan G3) makes a *new* ticker back-fill
  from 2008 here -- accepted in F6, called out here so it is not a surprise.

Runs on **python3.13** with the ``finDeepCore`` + ``finDeepYf`` layers.

Local dry run (writes USD_dailyFX.csv, touches no table)::

    cd Ops/fin-deep-data
    ../../venv-py313/bin/python fxeod_handler.py
"""

import logging
import dataUtil as DU
import pytz

import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd
from dotenv import load_dotenv
import os
import ast

# Create logger
logger = logging.getLogger()

load_dotenv()

JOB = "FXHistHandler"

#: Columns kept per ticker, in the order TBLHISTFX holds them.
FX_COLUMNS = ['base_cur', 'target_cur', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']


def _complete_bars(ddf, end_dt):
    """Drop bars past ``end_dt`` and bars that have no Close yet.

    Two rows have to be kept out of the frame, because either one appends junk
    *and* pushes the watermark past a real bar, which no later run can repair:
    Yahoo returns the bar sitting on ``period2`` as well as the ones before it,
    and a day that is still forming can come back with a NaN Close.
    """
    if ddf is None or len(ddf) == 0:
        return ddf
    keep = pd.DatetimeIndex(ddf.index).date <= end_dt
    if 'Close' in ddf.columns:
        keep = keep & ddf['Close'].notna().to_numpy()
    return ddf[keep].copy()      # a copy: the caller adds columns to it


# Function to fetch the latest exchange rates for multiple tickers
def fetch_exchange_rates(start_dt, end_dt, tickers, base):
    """Daily bars per ticker over ``[start_dt, end_dt]`` -- **both inclusive**.

    ``yf.download``'s ``end`` is exclusive, so the call asks for one day past
    ``end_dt`` and ``_complete_bars`` clamps the result back. Passing ``end_dt``
    straight through is what emptied the 2026-10-06 run: the watermark was the
    previous day, so ``start == end``, and Yahoo answers a zero-width range
    with no rows at all (``doc/OPERATIONS.md`` known incidents).
    """
    cols = FX_COLUMNS
    data = {}
    for ticker in tickers:
        try:
            ddf = yf.download(ticker, start=start_dt, end=end_dt + timedelta(days=1),
                              auto_adjust=False, multi_level_index=False)
            ddf = _complete_bars(ddf, end_dt)
            if len(ddf)>0:
                logging.debug(f'Reshape column of {ticker} to {ddf.head(2)}')
                ddf['base_cur'] = base
                ddf['target_cur'] = ticker.split('=')[0]
                data[ticker] = ddf[cols]
                logging.debug(f'Reshape column of {ticker} to {data[ticker].head(3)}')
        except Exception as e:
            logging.error("Exception occurred at fetch historical FX", exc_info=True)

    # Combine all data into a single DataFrame for easier analysis (optional)
    if len(data)>0:
        combined_data = pd.concat(data.values())
    else:
        combined_data = pd.DataFrame(columns=cols)
    return combined_data


def _output_dir(localrun):
    """CWD locally, /tmp on Lambda where the bundle directory is read-only."""
    return DU.out_dir(localrun)


def fx_run(event, context, localrun=False, dbFlag=True):
    if "NYTIME" in event:
        current_time = event["NYTIME"]
    else:
        current_time = datetime.now()
    mToday = current_time.date()
    today5PM = current_time.replace(hour=17, minute=0, second=0, microsecond=0)
    if current_time < today5PM:
        mToday = mToday - timedelta(days=1)
    current_time = current_time.strftime("%Y/%m/%d-%H:%M:%S")
    logger.info("Your cron function fxeod_handler " + " ran at " + current_time)

    base_cur = "USD"
    # Get the string representation of the list from .env
    my_list_str = os.getenv("FX_TICKERS")
    tickers = ast.literal_eval(my_list_str)
    tickers.sort()
    logging.debug(tickers)
    DBMKTDATA=os.environ.get("DBMKTDATA")
    TBLHISTFX=os.environ.get("TBLHISTFX")
    FIRSTTRAINDTE = datetime.strptime(os.getenv("FIRSTTRAINDTE"), "%Y/%m/%d").date()
    mktdate = DU.get_Max_date(f'{DBMKTDATA}.{TBLHISTFX}')
    if mktdate is None:
        # Whole-table fallback: every ticker back-fills from FIRSTTRAINDTE. Worth
        # a warning, because FIRSTTRAINDTE moves to 2008/01/01 at plan G3 (F6).
        logging.warning(f'{DBMKTDATA}.{TBLHISTFX} is empty: back-filling from FIRSTTRAINDTE {FIRSTTRAINDTE}')
        Sdate = FIRSTTRAINDTE
    else:
        Sdate = mktdate + timedelta(days=1)

    logging.info(f"Start_dt = {Sdate}   ---  end_dt = {mToday} ")
    if pd.Timestamp(Sdate).date() > mToday:
        # Already loaded through mToday -- a same-day re-run. Asking Yahoo for an
        # inverted window is 16 pointless requests that all come back empty.
        logging.info(f'{DBMKTDATA}.{TBLHISTFX} is current through {mktdate}: nothing to fetch')
        fx_df = pd.DataFrame(columns=FX_COLUMNS)
    else:
        fx_df = fetch_exchange_rates(Sdate, mToday, tickers, base_cur)
    fx_df['server_time'] = current_time

    logging.debug(fx_df)

    if not dbFlag or localrun:
        out = os.path.join(_output_dir(localrun), f"{base_cur}_dailyFX.csv")
        fx_df.reset_index().to_csv(out, index=False)
        logging.info(f'dry run: wrote {len(fx_df)} row(s) to {out}')
    elif len(fx_df)>0 :
        DU.StoreEOD(fx_df.reset_index(), DBMKTDATA, TBLHISTFX)
    return fx_df


def _audit(run_id, started_at, fx_df, status="ok", error=None):
    """One load_audit summary row (U10). Never raises."""
    try:
        n = 0 if fx_df is None else len(fx_df)
        dates = [] if not n else list(pd.to_datetime(fx_df.index).date)
        DU.audit_run([DU.audit_summary(
            JOB, os.environ.get("TBLHISTFX"), run_id, started_at,
            status=status, n_rows=n, date_lo=min(dates) if dates else None,
            date_hi=max(dates) if dates else None, error=error,
            yf_version=yf.__version__[:16])])
    except Exception:
        logging.error("load_audit summary failed", exc_info=True)


def run(event, context):
    """Lambda entry point.

    Event keys: ``localrun`` / ``dbFlag`` (False = CSV only, no table and no
    audit row), ``test`` (DEBUG logging), ``NYTIME`` (injected clock).
    """
    event = dict(event or {})
    if ('test' in event):
        logger.setLevel(logging.DEBUG)  # Set the logger to handle DEBUG level messages
    else:
        logger.setLevel(logging.INFO)  # Set the logger to handle DEBUG level messages

    logging.info(f"** ==> fxeod_handler.run(event: {event}, context: {context})")
    localrun = bool(event.get("localrun", False))
    dbFlag = bool(event.get("dbFlag", True)) and not localrun
    # Get the current time in New York
    ny_time = datetime.now().astimezone( pytz.timezone('US/Eastern'))
    logging.info(f"Current NY Time: {ny_time}")
    event.setdefault("NYTIME", ny_time)
    run_id, started_at = DU.new_run_id(), DU.utc_now()
    try:
        fx_df = fx_run(event, context, localrun=localrun, dbFlag=dbFlag)
    except Exception as e:
        if dbFlag:
            _audit(run_id, started_at, None, status="error", error=f"{type(e).__name__}: {e}")
        raise
    if dbFlag:
        _audit(run_id, started_at, fx_df)
    return {"run_id": run_id, "rows": 0 if fx_df is None else len(fx_df)}


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    # Dry run: reads MySQL for the watermark, writes USD_dailyFX.csv, and
    # touches neither FX_histdaily nor load_audit.
    print(run({"localrun": True, "dbFlag": False, "test": "True"}, None))
