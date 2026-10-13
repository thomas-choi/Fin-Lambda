"""optHandlerv2 -- 10-minute snapshot of the traded option contracts into TBLOPTSNAPSHOT.

A python3.13 port of ``Ops/fin-cron-data/opt_handler.py`` (TODOS 5.10). The
original stays deployed and untouched; both write the same table, so only one
may be scheduled and this one's schedule ships **disabled** (cutover:
``doc/OPERATIONS.md`` §11.2).

``load_audit.job`` is ``optHandler``.

The contract list is unchanged: every unexpired row of
``CALL Trading.sp_etf_trades_v2`` (strike ``H_Strike``) and
``CALL Trading.sp_stock_trades_V3`` (strike ``Strike``), de-duplicated on
``Symbol-PnC-Strike-Expiration`` with the first occurrence kept. The output is
column-for-column what the original wrote: ``inTheMoney`` as ``'1'``/``'0'``,
``timestamp`` as ET text with microseconds, ``undPrice`` left NULL.

Changed from the original:

* **one ``option_chain()`` per (Symbol, Expiration) and one ``history()`` per
  Symbol**, instead of both -- plus a ``Ticker.options`` call and a 1 s sleep --
  for every contract;
* the DELETE and the insert are one transaction (``DU.replace_snapshot``); an
  empty result keeps the previous snapshot instead of emptying the table;
* a contract with no strike (a NULL ``H_Strike``) is dropped with a warning
  instead of being searched for as ``nan``;
* ``localrun`` / ``dbFlag`` follow the service-wide contract, so a dry run
  never writes.

Runs on **python3.13** with the ``finDeepCore`` + ``finDeepYf`` layers.

Local dry run (writes options_list.csv and options_snapshot.csv)::

    cd Ops/fin-deep-data
    ../../venv-py313/bin/python opt_snapshot_handler.py
"""

import logging
import os
from datetime import date, datetime

import pandas as pd
import pytz
import yfinance as yf
from dotenv import load_dotenv

import dataUtil as DU

logger = logging.getLogger()

load_dotenv()

JOB = "optHandler"

ETF_PROC = "Trading.sp_etf_trades_v2"
STOCK_PROC = "Trading.sp_stock_trades_V3"

CONTRACT_COLUMNS = ["Symbol", "PnC", "Strike", "Expiration"]

#: Fields taken from a yfinance option-chain row, in table order.
CHAIN_FIELDS = ["lastPrice", "bid", "ask", "change", "percentChange", "volume",
                "openInterest", "impliedVolatility", "inTheMoney", "contractSize", "currency"]

#: options_snapshot's written columns (undPrice is left NULL, as before).
SNAP_COLUMNS = CONTRACT_COLUMNS + CHAIN_FIELDS + ["PClose", "timestamp"]

FLOAT_COLUMNS = ["lastPrice", "bid", "ask", "change", "percentChange", "volume",
                 "openInterest", "impliedVolatility", "PClose"]

#: PClose when the underlying's history could not be read -- the original's sentinel.
NO_PCLOSE = -0.001


def contract_key(sym, pnc, strike, expire):
    return f"{sym}-{pnc}-{strike:.2f}-{expire}"


def contract_list(etf_df, stock_df, today):
    """Unexpired, de-duplicated contracts from the two procedures. Pure.

    ETF rows carry the strike in ``H_Strike``, stock rows in ``Strike``.
    ``Expiration`` is returned as ``YYYY-MM-DD`` text, the form
    ``Ticker.option_chain`` takes.
    """
    parts = []
    for df, strike_col in ((etf_df, "H_Strike"), (stock_df, "Strike")):
        if df is None or len(df) == 0:
            continue
        part = pd.DataFrame({"Symbol": df["Symbol"], "PnC": df["PnC"],
                             "Strike": pd.to_numeric(df[strike_col], errors="coerce"),
                             "Expiration": pd.to_datetime(df["Expiration"]).dt.date})
        parts.append(part)
    if not parts:
        return pd.DataFrame(columns=CONTRACT_COLUMNS)
    df = pd.concat(parts, ignore_index=True)

    expired = df["Expiration"] < today
    if expired.any():
        logging.info(f"{int(expired.sum())} expired contract(s) dropped")
    no_strike = df["Strike"].isna()
    for row in df[no_strike & ~expired].itertuples():
        logging.warning(f"{row.Symbol} {row.PnC} {row.Expiration}: no strike, dropped")
    df = df[~expired & ~no_strike].copy()

    df["Expiration"] = df["Expiration"].map(lambda d: d.isoformat())
    keys = [contract_key(*r) for r in df[CONTRACT_COLUMNS].itertuples(index=False)]
    df = df[~pd.Series(keys, index=df.index).duplicated()]
    return df.reset_index(drop=True)[CONTRACT_COLUMNS]


def underlying_pclose(ticker):
    """Last Close of the past month, to 2 dp, or NO_PCLOSE when unavailable."""
    try:
        hist = ticker.history(period="1mo")
        if len(hist) > 0:
            return round(float(hist["Close"].iloc[-1]), 2)
    except Exception:
        logging.error(f"{ticker.ticker}: history failed", exc_info=True)
    return NO_PCLOSE


def match_contract(chain, pnc, strike):
    """The chain row for this side and strike, as a dict of CHAIN_FIELDS, or None."""
    side = chain.puts if pnc == "P" else chain.calls
    hit = side[side["strike"] == strike]
    if len(hit) == 0:
        return None
    row = hit.iloc[0]
    return {f: row.get(f) for f in CHAIN_FIELDS}


def snapshot_contracts(contracts, stamp, ticker_factory=yf.Ticker, context=None):
    """Fetch every contract, one chain per (Symbol, Expiration). Returns (frame, n_missing)."""
    rows, missing = [], 0
    for sym, by_sym in contracts.groupby("Symbol", sort=True):
        if not DU.time_left_ok(context, reserve_ms=30_000):
            logging.warning(f"time guard: stopping before {sym}")
            missing += len(contracts[contracts["Symbol"] >= sym])
            break
        ticker = ticker_factory(sym)
        pclose = underlying_pclose(ticker)
        for expiration, group in by_sym.groupby("Expiration", sort=True):
            try:
                chain = ticker.option_chain(expiration)
            except Exception as e:
                logging.error(f"{sym} {expiration}: option_chain failed: {e}")
                missing += len(group)
                continue
            for c in group.itertuples(index=False):
                fields = match_contract(chain, c.PnC, c.Strike)
                if fields is None:
                    logging.error(f"{sym} {c.Strike}-{c.PnC} {expiration} not found")
                    missing += 1
                    continue
                rows.append({"Symbol": sym, "PnC": c.PnC, "Strike": c.Strike,
                             "Expiration": expiration, **fields,
                             "PClose": pclose, "timestamp": stamp})
    return to_snapshot_frame(rows), missing


def to_snapshot_frame(rows):
    """Rows -> options_snapshot frame with the original's dtypes. Pure."""
    df = pd.DataFrame(rows, columns=SNAP_COLUMNS)
    for col in FLOAT_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
    # yfinance gives a bool; the column is VARCHAR and has always held '1'/'0'.
    df["inTheMoney"] = df["inTheMoney"].map(lambda v: None if pd.isna(v) else str(int(bool(v))))
    df["Expiration"] = pd.to_datetime(df["Expiration"]).dt.date
    return df


def opt_run(event, context, localrun=False, dbFlag=True):
    ny_time = event.get("NYTIME") or datetime.now(pytz.timezone("US/Eastern"))
    stamp = str(ny_time.replace(tzinfo=None))
    logger.info(f"opt_snapshot_handler ran at {stamp} ET")
    schema = DU.require_env("DBMKTDATA")
    table = DU.require_env("TBLOPTSNAPSHOT")

    etf_df = DU.load_df_SQL(f"call {ETF_PROC};")
    stock_df = DU.load_df_SQL(f"call {STOCK_PROC};")
    if etf_df is None or stock_df is None:
        # load_df_SQL swallowed the error; a silent empty list would read as
        # "no open trades" and keep an old snapshot forever.
        raise RuntimeError(f"contract procedure failed: {ETF_PROC if etf_df is None else STOCK_PROC}")
    contracts = contract_list(etf_df, stock_df, ny_time.date())
    cap = event.get("test")
    if isinstance(cap, int) and not isinstance(cap, bool) and cap > 0:
        contracts = contracts.head(cap)
    logging.info(f"{len(contracts)} contract(s) on {contracts['Symbol'].nunique()} underlying(s)")

    outdir = DU.out_dir(localrun)
    if not dbFlag or localrun:
        contracts.to_csv(os.path.join(outdir, "options_list.csv"), index=False)

    snap, missing = snapshot_contracts(contracts, stamp, context=context)
    logging.info(f"{len(snap)} contract(s) priced, {missing} missing")

    if not dbFlag or localrun:
        out = os.path.join(outdir, f"{table}.csv")
        snap.to_csv(out, index=False)
        logging.info(f"dry run: wrote {len(snap)} row(s) to {out}")
    else:
        DU.replace_snapshot(snap, schema, table)
    return snap, len(contracts)


def _audit(run_id, started_at, snap, n_expected, status="ok", error=None):
    """One load_audit summary row. Never raises."""
    try:
        n = 0 if snap is None else len(snap)
        if status == "ok" and n_expected and n < n_expected:
            status = "partial"
        elif status == "ok" and n == 0:
            status = "empty"
        today = DU.utc_now().date()
        DU.audit_run([DU.audit_summary(
            JOB, os.environ.get("TBLOPTSNAPSHOT"), run_id, started_at, status=status,
            n_rows=n, n_ok=n, n_expected=n_expected, date_lo=today, date_hi=today,
            error=error, yf_version=yf.__version__[:16])])
    except Exception:
        logging.error("load_audit summary failed", exc_info=True)


def run(event, context):
    """Lambda entry point.

    Event keys: ``localrun`` / ``dbFlag`` (False = CSV only, no table and no
    audit row), ``test`` (an int caps the contract count, any value turns on
    DEBUG logging), ``NYTIME`` (injected clock).
    """
    event = dict(event or {})
    logger.setLevel(logging.DEBUG if "test" in event else logging.INFO)
    logging.info(f"** ==> opt_snapshot_handler.run(event: {event}, context: {context})")
    localrun = bool(event.get("localrun", False))
    dbFlag = bool(event.get("dbFlag", True)) and not localrun
    event.setdefault("NYTIME", datetime.now(pytz.timezone("US/Eastern")))
    run_id, started_at = DU.new_run_id(), DU.utc_now()
    try:
        snap, n_expected = opt_run(event, context, localrun=localrun, dbFlag=dbFlag)
    except Exception as e:
        if dbFlag:
            _audit(run_id, started_at, None, None, status="error", error=f"{type(e).__name__}: {e}")
        raise
    if dbFlag:
        _audit(run_id, started_at, snap, n_expected)
    return {"run_id": run_id, "rows": len(snap), "contracts": n_expected}


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    # Dry run: reads the two Trading procedures, prices every contract from
    # Yahoo, writes options_list.csv / options_snapshot.csv, and touches
    # neither options_snapshot nor load_audit.
    print(run({"localrun": True, "dbFlag": False, "test": "True"}, None))
