"""yfus30minEODv2 / yfasia30minEODv2 -- 15-minute intraday bars into TBLMINUTEPRICE.

PLAN-SR-UPSTREAM Phase F (U10). One module replacing the near-identical pair
``Ops/fin-cron-data/eoddata_minhandler_us.py`` and ``…_asia.py`` at fa1d6ac:
they differed only in the stored procedure that returns the symbol list and in
the audit job name, so the market is a parameter here (:data:`MARKETS`) and the
two Lambdas point at :func:`run_us` and :func:`run_asia`.

Despite the ``30min`` names, the download interval is ``15m`` -- as in the
originals.

The originals stay deployed and untouched. Both they and these append to the
same table, so only one pair may be scheduled: these ship with their schedules
**disabled** (see ``doc/OPERATIONS.md``, cutover runbook).

``load_audit.job`` stays ``yfus30minEOD`` / ``yfasia30minEOD`` -- the data set's
job, not the Lambda -- so the status report does not grow extra lines at the
cutover.

Changed from the originals beyond the audit row and the merge:

* a symbol whose exchange is missing from ``Exchange_timezone.csv`` is skipped
  with a warning. The originals indexed the map directly, so one unmapped
  exchange raised ``KeyError`` and lost the rest of the run's symbols;
* ``run()`` returns a JSON-serialisable summary instead of ``None``;
* ``InitialRun`` is passed down the call chain instead of being a module global
  mutated by ``run()``.

Runs on **python3.13** with the ``finDeepCore`` + ``finDeepYf`` layers.

Local dry run (writes 30min_*.csv, touches no table)::

    cd Ops/fin-deep-data
    ../../venv-py313/bin/python intraday_min_handler.py        # US
    ../../venv-py313/bin/python intraday_min_handler.py asia
"""

import os
import sys
from os import environ
import pandas as pd
import logging
import yfinance as yf
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pytz
import dataUtil as DU

load_dotenv()
DEBUG = environ.get("DEBUG")
if DEBUG == "debug":
    logging.basicConfig(level=logging.DEBUG)
else:
    logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

tformats = '%Y-%m-%d %H:%M:%S'

#: Per-market configuration -- the only thing that differed between the two
#: original handlers. ``proc`` is a server-side procedure that is not in this
#: repo (see CLAUDE.md, *Symbol lists*); ``job`` is the load_audit job name,
#: kept identical to the python3.10 function it replaces.
MARKETS = {
    "us":   {"proc": "GlobalMarketData.get_us_symbol",   "job": "yfus30minEOD"},
    "asia": {"proc": "GlobalMarketData.get_asia_symbol", "job": "yfasia30minEOD"},
}

#: Column order of TBLMINUTEPRICE.
SAVE_COLUMNS = ['Datetime', 'Symbol', 'Exchange', 'Close', 'Open', 'High', 'Low',
                'Volume', 'AdjClose', 'UTCDatetime', 'timezone']

#: yfinance serves at most 60 days of intraday history.
MAX_INTRADAY_DAYS = 59

#: Rows go to the DB one symbol at a time above this count, otherwise they are
#: pooled into one write at the end. Kept from the originals.
PER_SYMBOL_WRITE_MIN = 100


def yf_get_max_datetime(localnow, sym=None):
    DBMKTDATA = environ.get("DBMKTDATA")
    TBLMINUTEPRICE = environ.get("TBLMINUTEPRICE", "minute_price")

    mkt_datetime = DU.get_Max_datetime(f'{DBMKTDATA}.{TBLMINUTEPRICE}', sym)
    logging.info(f"Max date of {sym} at {DBMKTDATA}.{TBLMINUTEPRICE} is {mkt_datetime}")
    if mkt_datetime is None:
        Sdatetime = localnow - timedelta(days=MAX_INTRADAY_DAYS)
    else:
        Sdatetime = mkt_datetime
    logging.debug(f"maxdate : {Sdatetime} for {DBMKTDATA}.{TBLMINUTEPRICE} : {sym}")
    return Sdatetime


def yf_download(sym, sdatetime, edatetime, InitialRun=False):
    if InitialRun:
        logging.debug(f"yf_download({sym}, InitialRun)")
        sDF = yf.download(sym, interval='15m', auto_adjust=False, multi_level_index=False)
    else:
        logging.debug(f"yf_download({sym}, {sdatetime} - {edatetime})")
        sDF = yf.download(sym, start=sdatetime, end=edatetime, interval='15m',
                          auto_adjust=False, multi_level_index=False)
    if len(sDF) > 0:
        sDF = sDF.reset_index()
        if 'Adj Close' in sDF.columns:
            sDF = sDF.rename(columns={'Adj Close': 'AdjClose'})
    return sDF


def check_exchange_from_ticker(ticker):
    st = ticker.split(".")
    if len(st) > 1:
        return st[-1]
    else:
        return None


def yf_exchange_code(exdict, sym):
    if sym in exdict:
        return exdict[sym]
    else:
        return check_exchange_from_ticker(sym)


def load_blacklist(path=None):
    """Symbols known to return junk, subtracted from every list.

    Read from ``DU.list_dir()`` -- the packaged folder -- not from the CWD as the
    originals did, so the Lambda does not depend on its working directory.
    """
    path = path or os.path.join(DU.list_dir(), "intra_blacklist.csv")
    blacklist = pd.read_csv(path, encoding='utf-8')
    return sorted(blacklist.Symbol.unique())


def load_market_symbols(market):
    """The market's symbol list from its stored procedure, minus the blacklist."""
    proc = MARKETS[market]["proc"]
    sql = f"call {proc};"
    logging.info(f"load_market_symbols({sql})")
    df = DU.load_df_SQL(sql)
    if df is None or len(df) == 0:
        logging.error(f"{proc} returned nothing")
        return []
    logging.info("load from intra_blacklist.csv")
    blacklist = load_blacklist()
    symbol_list = sorted(set(df.Symbol.unique()) - set(blacklist))
    logging.debug(f'{symbol_list}')
    return symbol_list


def reshape_bars(sDF, sym, exchange, tz, sdatetime, edatetime):
    """Downloaded bars -> SAVE_COLUMNS, exchange-local ``Datetime`` + ``UTCDatetime``.

    Pure, so the timezone handling is unit-testable. Rows outside
    ``(sdatetime, edatetime]`` -- the watermark window -- are dropped, and
    ``Datetime`` is returned tz-naive in exchange-local time, as the table holds
    it, with the UTC instant kept beside it.
    """
    if sDF.columns.nlevels > 1:
        sDF.columns = [col[0] for col in sDF.columns]
    sDF = sDF.rename(columns={'Datetime': 'UTCDatetime'})
    sDF['Symbol'] = sym
    sDF['Exchange'] = exchange
    # store datetime in local timezone of the exchange
    sDF['Datetime'] = sDF['UTCDatetime'].dt.tz_convert(tz=tz)
    sDF['timezone'] = tz
    logging.info(f'reshape_bars: {sym} from {sDF.Datetime.iloc[0]} to {sDF.Datetime.iloc[-1]}')
    sDF = sDF[(sDF['Datetime'] > sdatetime) & (sDF['Datetime'] <= edatetime)]
    sDF = sDF[SAVE_COLUMNS].copy()
    # Remove timezone information
    sDF['Datetime'] = sDF['Datetime'].dt.tz_localize(None)
    return sDF


def _cap(event):
    """``test: N`` caps the symbol count for a dry run; ``test: true`` only raises logging."""
    value = event.get("test")
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value) if int(value) > 0 else None
    except (TypeError, ValueError):
        return None


def common_fetch_eod(sdatetime, tdatetime, list_name, localrun, dbFlag=True,
                     market="us", InitialRun=False, cap=None):

    logging.info(f'common_fetch_eod handle {sdatetime} UPTO {tdatetime}, '
                 f'market={market}, list_name={list_name}, dbFlag={dbFlag}')

    stored = []   # frames handed to StoreEOD, for the load_audit summary
    symbol_list = load_market_symbols(market)
    if cap:
        symbol_list = symbol_list[:cap]
    if len(symbol_list) <= 0:
        return stored
    exch_dict = DU.load_symbols_dict()
    tzlist = DU.load_exchange_tz()
    TBLMINUTEPRICE = environ.get("TBLMINUTEPRICE", "minute_price")
    datallist = list()
    logging.debug(f"symbol_list: {symbol_list}")

    for sym in symbol_list:
        exchange = yf_exchange_code(exch_dict, sym)
        logging.debug(f"sym: {sym},   Exchange: {exchange}")
        if exchange is None:
            # Skip symbols without exchange info
            logging.warning(f"Exchange not found for symbol {sym}, skipping...")
            continue
        if exchange not in tzlist:
            # The originals indexed tzlist directly and died here, losing every
            # symbol after the unmapped one.
            logging.warning(f"Exchange {exchange} ({sym}) is not in Exchange_timezone.csv, skipping...")
            continue
        tz = tzlist[exchange]
        localnow = tdatetime.astimezone(pytz.timezone(tz))
        sdatetime = yf_get_max_datetime(localnow, sym)
        if sdatetime.tzinfo is None:
            sdatetime = pytz.timezone(tz).localize(sdatetime)
        if localnow >= sdatetime:
            edatetime = localnow + timedelta(minutes=1)
            logging.debug(f'Loading {sym} minute OHLC from Yahoo {sdatetime} to {edatetime}!   localnow={localnow}')

            sDF = yf_download(sym, sdatetime, edatetime, InitialRun=InitialRun)
            if localrun:
                sDF.to_csv(os.path.join(DU.out_dir(localrun), f"30min_{sym}.csv"), index=False)

            logging.info(f"{sym} is downloaded DF : {len(sDF)} records")
            if len(sDF) > 0:
                sDF = reshape_bars(sDF, sym, exchange, tz, sdatetime, edatetime)
            if len(sDF) > 0:
                if len(sDF) > PER_SYMBOL_WRITE_MIN and dbFlag:
                    DU.StoreEOD(sDF, None, TBLMINUTEPRICE)
                    stored.append(sDF)
                else:
                    datallist.append(sDF)

    if len(datallist) > 0:
        totalDF = pd.concat(datallist)
        if localrun:
            totalDF.to_csv(os.path.join(DU.out_dir(localrun), f"30min_{list_name}.csv"),
                           index=False)
        if dbFlag:
            DU.StoreEOD(totalDF, None, TBLMINUTEPRICE)
            stored.append(totalDF)

    logging.info(f'common_fetch_eod finish the handle {list_name} UPTO {tdatetime}')
    return stored


def minute_output_columns():
    return ["Datetime", "Symbol", "Exchange", "garch", "svr", "mlp", "LSTM", "prev_Close", "prediction", "volatility"]


def audit_summary(market, run_id, started_at, stored, status="ok", error=None):
    """One load_audit summary row (U10). Never raises: an audit failure must not fail the load."""
    try:
        n = sum(len(f) for f in stored)
        dates = [d for f in stored for d in pd.to_datetime(f["Datetime"]).dt.date]
        DU.audit_run([DU.audit_summary(
            MARKETS[market]["job"], environ.get("TBLMINUTEPRICE", "minute_price"),
            run_id, started_at,
            status=status, n_rows=n, date_lo=min(dates) if dates else None,
            date_hi=max(dates) if dates else None, error=error,
            yf_version=yf.__version__[:16])])
    except Exception:
        logging.error("load_audit summary failed", exc_info=True)


def run(event, context, market="us"):
    """Shared entry point.

    Event keys: ``localrun`` (per-symbol CSVs), ``dbFlag`` (False = no table and
    no audit row), ``InitialRun`` (full 60-day history instead of the
    watermark), ``test`` (DEBUG logging). Returns a JSON-serialisable summary.
    """
    event = dict(event or {})
    if event.get("test"):
        logging.getLogger().setLevel(logging.DEBUG)
    logging.info(f"** ==> intraday_min_handler.run(market={market}, event: {event})")

    utcNow = datetime.now(pytz.utc)
    Sdatetime = yf_get_max_datetime(utcNow)
    localrun = bool(event.get("localrun", False))
    dbFlag = bool(event.get("dbFlag", True))
    InitialRun = bool(event.get("InitialRun", False))
    cap = _cap(event)
    # list_N is only a CSV-name label: the symbol list always comes from this
    # market's stored procedure, whatever list_name says. So with SYMBOLLIST
    # unset the loop below downloads the SAME symbols four times. Inherited from
    # the originals; keep SYMBOLLIST set (.env.example does) until the lists are
    # actually split per name -- TODOS.md 2.1.
    list_N = ["stock_list", "etf_list", "crypto_list", "us-cn_stock_list"]
    SYMBOLLIST = environ.get("SYMBOLLIST")
    run_id, started_at = DU.new_run_id(), DU.utc_now()
    stored = []
    try:
        if SYMBOLLIST is not None:
            stored += common_fetch_eod(Sdatetime, utcNow, list_name=SYMBOLLIST, localrun=localrun,
                                       dbFlag=dbFlag, market=market, InitialRun=InitialRun,
                                       cap=cap) or []
        else:
            for symN in list_N:
                stored += common_fetch_eod(Sdatetime, utcNow, list_name=symN, localrun=localrun,
                                           dbFlag=dbFlag, market=market, InitialRun=InitialRun,
                                           cap=cap) or []
    except Exception as e:
        if dbFlag:
            audit_summary(market, run_id, started_at, stored, status="error",
                          error=f"{type(e).__name__}: {e}")
        raise
    if dbFlag:
        audit_summary(market, run_id, started_at, stored)
    return {"run_id": run_id, "market": market, "job": MARKETS[market]["job"],
            "rows": sum(len(f) for f in stored)}


def run_us(event, context):
    """Lambda entry point of yfus30minEODv2."""
    return run(event, context, market="us")


def run_asia(event, context):
    """Lambda entry point of yfasia30minEODv2."""
    return run(event, context, market="asia")


if __name__ == '__main__':
    market = sys.argv[1] if len(sys.argv) > 1 else "us"
    # Dry run: reads MySQL for the list and watermarks, writes 30min_*.csv, and
    # touches neither the table nor load_audit.
    print(run({"localrun": True, "dbFlag": False, "test": 5}, None, market=market))
