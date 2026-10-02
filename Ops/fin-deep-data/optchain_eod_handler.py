"""optChainEOD -- end-of-day filtered option chains into OptionChains.

Ported from myFinData@19c8509 Ops/optchain_fetch.py (the host-cron job
`optchain_fetch.py -S PM -U -m`), per PLAN-SR-UPSTREAM Phase C. Behaviour is
kept **identical**:

* ``option_chains()`` -- 5 attempts with a 5 s pause; ``UnderlyingPrice`` is
  ``history(period='1d').Close`` taken before the expiries are fetched;
* ``filter_opt_chain()`` -- ``lastPrice > 0.05`` and ``openInterest`` above
  the whole chain's 25th percentile, puts and calls;
* ``contractSize=100``, ``Section='PM'``, ``Date``, ``inTheMoney`` as bool,
  the ``nColumns`` list and order;
* the date rule -- the NY date, minus one day before 09:00.

Changed from production:

* the list is ``current_symbols_{SYMBOL_PROC_VER}`` via ``DU.load_symbols_db``
  (F3), asked for with @type ``'o'`` -- V5 then drops the delisted symbols and
  the ones SymbolMaster says have no options;
* each underlying is written right after it is fetched, with ``INSERT
  IGNORE`` into ``OPT_WRITE_TBL``, then its ``load_audit`` row;
* the raw unfiltered chain goes to R2 at
  ``{OPT_RAW_PREFIX}/{date}/{sym}-PM.csv`` instead of the host disk;
* fan-out: a scheduled ``{"dispatch": true}`` invocation async-invokes this
  function once per shard, and scheduled sweeps retry what is missing.

Runs on **python3.13** with the ``finDeepCore`` + ``finDeepYf`` layers. Its
deploy package carries this module, ``dataUtil.py``, ``eod_daily_handler.py``
(for :func:`exchange_for`) and the two exchange CSVs.

Local dry run -- the full-V4 timing run that sizes OPT_SHARDS (reads MySQL for
the list only; writes CSVs, touches neither the tables nor R2)::

    cd Ops/fin-deep-data
    ../../venv-py313/bin/python optchain_eod_handler.py
"""

import json
import logging
import math
import os
import time
from datetime import date, datetime, timedelta

import pandas as pd
import pytz
import yfinance as yf
from dotenv import load_dotenv

import dataUtil as DU
from eod_daily_handler import exchange_for

logger = logging.getLogger()

load_dotenv()

# --------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------
JOB = "optChainEOD"
# current_symbols_V5 @type: only optionable symbols are worth a chain request.
# Ignored while SYMBOL_PROC_VER names a pre-V5 procedure, which returns the
# unfiltered list -- yfinance then reports an empty chain for the rest.
SYMBOL_TYPE = "o"
SECTION = "PM"

#: ported from myFinData@19c8509 Ops/optchain_fetch.py, where max_retries was 5.
#: Lowered to 2 on 2026-10-01: a yfinance failure for one underlying is almost
#: always a rate-limit that outlives the retry window, so attempts 3-5 spent
#: 15 s of the shard's 540 s budget to fail again. The sweep invocations are the
#: real retry -- they run 60 and 120 min later, when the limit has reset.
max_retries = 2
retry_delay = 5

#: Column order written to OptionChains -- production's nColumns.
N_COLUMNS = ["Date", "Section", "UnderlyingSymbol", "strike", "Expiration", "OptionType",
             "contractSymbol", "lastTradeDate", "lastPrice", "bid", "ask", "change",
             "percentChange", "volume", "openInterest", "impliedVolatility", "inTheMoney",
             "contractSize", "currency", "UnderlyingPrice"]

#: Per-shard time budget used to size OPT_SHARDS: 0.6 x the 900 s Lambda limit.
SHARD_BUDGET_S = 540


# --------------------------------------------------------------------------
# Ported logic
# --------------------------------------------------------------------------
def option_chains(ticker, sleep=None):
    """Download every expiry's chain. Returns ``(chains, error)``.

    ported from myFinData@19c8509 Ops/optchain_fetch.py::option_chains. Same
    retry loop and the same result, except that it makes ``max_retries`` = 2
    attempts rather than production's 5 (see the constant); the only additions
    are the returned error text (for load_audit) and a list-then-concat build,
    which gives the same frame without pandas 2.2's empty-concat FutureWarning.
    """
    sleep = sleep or time.sleep
    error = None
    for retry in range(max_retries):
        try:
            asset = yf.Ticker(ticker)
            histdata = asset.history(period="1d")
            underlyingPrice = histdata["Close"].iloc[-1]

            expirations = asset.options
            logging.debug(f"{ticker} option chain: {expirations}")

            parts = []
            for expiration in expirations:
                # tuple of two dataframes
                opt = asset.option_chain(expiration)

                calls = opt.calls
                calls["OptionType"] = "call"

                puts = opt.puts
                puts["OptionType"] = "put"

                frames = [f for f in (calls, puts) if len(f)]
                if not frames:
                    continue
                chain = pd.concat(frames)
                chain["Expiration"] = pd.to_datetime(expiration)
                parts.append(chain)
            chains = pd.concat(parts) if parts else pd.DataFrame()
            chains["UnderlyingSymbol"] = ticker
            chains["UnderlyingPrice"] = underlyingPrice

            return chains, None
        except Exception as e:
            error = f"{type(e).__name__}: {e}"
            logging.error(f"asset.option({ticker}) error: {e}")
            if retry < max_retries - 1:
                logging.error(f"Retrying in {retry_delay} seconds...")
                sleep(retry_delay)

    return pd.DataFrame(), error


def filter_opt_chain(i_df):
    # ported from myFinData@19c8509 Ops/optchain_fetch.py (unchanged)
    data = i_df[i_df["lastPrice"] > 0.05]
    putdata = data[data["OptionType"] == "put"]
    calldata = data[data["OptionType"] == "call"]
    OI75 = i_df["openInterest"].quantile(0.25)
    put75 = putdata[putdata["openInterest"] > OI75]
    call75 = calldata[calldata["openInterest"] > OI75]
    return put75, call75


def process_chain(options_frame, process_dt, section=SECTION):
    """The post-download half of production's ProcessOptions, returning N_COLUMNS.

    ported from myFinData@19c8509 Ops/optchain_fetch.py::ProcessOptions.
    """
    if options_frame is None or len(options_frame) == 0:
        return pd.DataFrame(columns=N_COLUMNS)
    put_df, call_df = filter_opt_chain(options_frame)
    parts = [f for f in (put_df, call_df) if len(f)]
    if not parts:
        return pd.DataFrame(columns=N_COLUMNS)
    options_frame = pd.concat(parts, axis=0)
    options_frame = options_frame.sort_values(by=["Expiration", "OptionType"])
    options_frame["contractSize"] = 100
    options_frame.insert(0, "Section", section)
    options_frame.insert(0, "Date", process_dt)
    options_frame["inTheMoney"] = options_frame["inTheMoney"].astype("bool")
    return options_frame[N_COLUMNS]


# --------------------------------------------------------------------------
# Pure helpers
# --------------------------------------------------------------------------
def process_date(ny_now):
    """Production's date rule: the NY date, minus one day before 09:00."""
    if ny_now.hour < 9:
        ny_now = ny_now - timedelta(days=1)
    return ny_now.date()


def raw_key(prefix, process_dt, sym, section=SECTION):
    """R2 key of one raw (unfiltered) chain."""
    return f"{prefix.rstrip('/')}/{process_dt}/{sym}-{section}.csv"


def dispatch_events(n, process_dt):
    """The async payloads a dispatcher sends: one per shard, all on the same date."""
    return [{"shard": i, "of": n, "asof": str(process_dt)} for i in range(n)]


def shards_needed(total_seconds, budget=SHARD_BUDGET_S):
    """OPT_SHARDS = ceil(T_total / 540 s), at least 1."""
    return max(1, math.ceil(total_seconds / budget))


def symbol_audit_row(ctx, sym, exchange, status, n_rows, process_dt, started_at, finished_at,
                     error=None):
    return {
        "run_id": ctx["run_id"], "job": JOB, "Symbol": sym, "Exchange": exchange,
        "date_lo": process_dt if n_rows else None, "date_hi": process_dt if n_rows else None,
        "table_name": ctx["table"], "segment": "append", "n_rows": n_rows,
        "status": status, "error": error, "started_at": started_at,
        "finished_at": finished_at, "yf_version": ctx["yf_version"], "host": ctx["host"],
    }


def classify(raw, saved, error, write_failed):
    """Per-underlying status: ok (rows written), empty (no chain / all filtered), error."""
    if write_failed:
        return "error", "write to the options table failed"
    if len(saved):
        return "ok", None
    if error and len(raw) == 0:
        return "error", error
    return "empty", None


# --------------------------------------------------------------------------
# IO wrappers
# --------------------------------------------------------------------------
def _truthy(value, default=False):
    if value is None:
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "y", "on")


def _output_dir(localrun):
    """CWD locally, /tmp on Lambda where the bundle directory is read-only."""
    return DU.out_dir(localrun)


def r2_client():
    """boto3 S3 client for R2 -- the endpoint_url pattern from yf-news-collect.py."""
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=DU.require_env("R2_ENDPOINT"),
        aws_access_key_id=DU.require_env("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=DU.require_env("R2_SECRET_ACCESS_KEY"),
        region_name="auto",
    )


def put_raw(client, bucket, key, frame):
    """Upload one raw chain as CSV. Returns True, or False after logging."""
    try:
        client.put_object(Bucket=bucket, Key=key, Body=frame.to_csv(index=False).encode("utf-8"),
                          ContentType="text/csv")
        return True
    except Exception:
        logging.error(f"R2 put_object failed for {bucket}/{key}", exc_info=True)
        return False


def invoke_shards(events, context):
    """Async-invoke this same function once per event. Returns the count started."""
    import boto3

    name = getattr(context, "invoked_function_arn", None) or DU.require_env("AWS_LAMBDA_FUNCTION_NAME")
    client = boto3.client("lambda")
    started = 0
    for ev in events:
        try:
            client.invoke(FunctionName=name, InvocationType="Event", Payload=json.dumps(ev).encode())
            started += 1
        except Exception:
            logging.error(f"dispatch of {ev} failed", exc_info=True)
    return started


def _cap(event):
    test = event.get("test")
    if isinstance(test, bool) or test is None:
        return None
    try:
        return int(test) if int(test) > 0 else None
    except (TypeError, ValueError):
        return None


def _jsonable(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------
def run(event, context):
    """Lambda entry point.

    Event keys: ``dispatch`` (fan out ``OPT_SHARDS`` async shard invocations
    and return), ``shard``/``of`` (round-robin slice), ``sweep`` (retry every
    underlying with no ok/empty audit row today), ``asof`` (YYYY-MM-DD),
    ``symbols`` (list override), ``localrun`` (CSVs to CWD), ``dbFlag`` (False
    = no DB writes, no R2; CSVs instead), ``test`` (int caps the symbol count;
    any truthy value sets DEBUG logging), ``NYTIME`` (injected clock).

    Returns a JSON-serialisable summary.
    """
    event = dict(event or {})
    logger.setLevel(logging.DEBUG if event.get("test") else logging.INFO)
    logging.info(f"** ==> optchain_eod_handler.run(event: {event})")

    localrun = bool(event.get("localrun", False))
    dbflag = bool(event.get("dbFlag", True))
    sweep = _truthy(event.get("sweep"))

    db = DU.require_env("DBMKTDATA")
    table = DU.require_env("OPT_WRITE_TBL")
    DU.require_env("TBLLOADAUDIT")
    ver = DU.env_or("SYMBOL_PROC_VER", "V5")
    sym_type = event.get("symType", SYMBOL_TYPE)

    ny_now = event.get("NYTIME") or datetime.now(pytz.utc).astimezone(pytz.timezone("America/New_York"))
    todt = (datetime.strptime(event["asof"], "%Y-%m-%d").date() if event.get("asof")
            else process_date(ny_now))

    if _truthy(event.get("dispatch")):
        n = int(DU.env_or("OPT_SHARDS", "3"))
        events = dispatch_events(n, todt)
        started = invoke_shards(events, context) if dbflag else 0
        logging.info(f"dispatch: {started}/{n} shard invocation(s) started for {todt}")
        return {"mode": "dispatch", "asof": str(todt), "shards": n, "started": started, "events": events}

    ctx = {"run_id": DU.new_run_id(), "table": table, "yf_version": yf.__version__[:16],
           "host": DU.run_host()}
    started_at = DU.utc_now()
    mode = "sweep" if sweep else "shard"
    result = {"run_id": ctx["run_id"], "asof": str(todt), "mode": mode, "dbFlag": dbflag,
              "rows_written": 0}
    audit_rows, timings = [], []
    outdir = _output_dir(localrun)

    def finish(error=None):
        dates = [r["date_hi"] for r in audit_rows if r["date_hi"]]
        statuses = [r["status"] for r in audit_rows]
        summary = DU.audit_summary(
            JOB, table, ctx["run_id"], started_at, status="error" if error else "ok",
            n_rows=result["rows_written"], date_lo=min(dates) if dates else None,
            date_hi=max(dates) if dates else None,
            n_ok=sum(s in ("ok", "empty") for s in statuses), n_expected=len(statuses),
            error=error, yf_version=ctx["yf_version"])
        if dbflag:
            DU.audit_run([summary])
        else:
            DU.audit_frame(audit_rows + [summary]).to_csv(
                os.path.join(outdir, f"load_audit_opt_{todt}.csv"), index=False)
            pd.DataFrame(timings).to_csv(os.path.join(outdir, f"optchain_timing_{todt}.csv"), index=False)
        total_s = sum(t["seconds"] for t in timings)
        result.update({
            "status": "error" if error else "ok", "error": error, "n_expected": len(statuses),
            "n_ok": statuses.count("ok"), "n_empty": statuses.count("empty"),
            "n_error": statuses.count("error"), "n_skipped": statuses.count("skipped"),
            "seconds": round(total_s, 1), "opt_shards_needed": shards_needed(total_s),
        })
        logging.info(f"SUMMARY {json.dumps(result, default=_jsonable)}")
        return {k: _jsonable(v) for k, v in result.items()}

    # --- symbol list ------------------------------------------------------
    logging.info(f"symbol list: current_symbols_{ver}({sym_type})")
    symbols = event.get("symbols") or DU.load_symbols_db(ver, sym_type)
    if not symbols:
        return finish(f"symbol list current_symbols_{ver}({sym_type}) returned nothing")
    symbols = sorted(set(symbols))
    if sweep:
        symbols = DU.missing_for_sweep(JOB, todt, symbols, table)
        if symbols is None:
            return finish("sweep: load_audit could not be read")
        logging.info(f"sweep: {len(symbols)} underlying(s) missing for {todt}")
    else:
        symbols = DU.shard_symbols(symbols, int(event.get("shard", 0)), int(event.get("of", 1)))
    cap = _cap(event)
    if cap:
        symbols = symbols[:cap]

    exch_dict = DU.load_symbols_dict() or {}
    bucket = DU.env_or("UPSTREAM_R2_BUCKET", None)
    prefix = DU.env_or("OPT_RAW_PREFIX", "raw/optchain")
    client = None
    if dbflag and bucket:
        try:
            client = r2_client()
        except Exception:
            logging.error("R2 client unavailable; raw chains will not be archived", exc_info=True)
    elif dbflag:
        logging.warning("UPSTREAM_R2_BUCKET is not set; raw chains will not be archived")
    raw_dir = os.path.join(outdir, "OptionsChain")
    if not dbflag:
        os.makedirs(raw_dir, exist_ok=True)

    # --- per underlying ---------------------------------------------------
    for i, sym in enumerate(symbols):
        if not DU.time_left_ok(context):
            now = DU.utc_now()
            rows = [symbol_audit_row(ctx, s, exchange_for(s, exch_dict), "skipped", 0, todt, now, now)
                    for s in symbols[i:]]
            logging.warning(f"time guard: {len(rows)} underlying(s) left unstarted")
            audit_rows.extend(rows)
            if dbflag:
                DU.audit_run(rows)
            break

        t0, sym_started = time.monotonic(), DU.utc_now()
        raw, error = option_chains(sym)
        saved = process_chain(raw, todt)

        write_failed = False
        if len(raw):
            if dbflag and client is not None:
                put_raw(client, bucket, raw_key(prefix, todt, sym), raw)
            elif not dbflag:
                raw.to_csv(os.path.join(raw_dir, f"{sym}_{todt}-{SECTION}.csv"), index=False)
        if len(saved):
            if dbflag:
                inserted = DU.append_ignore(saved, db, table)
                write_failed = inserted is None
                result["rows_written"] += inserted or 0
            else:
                result["rows_written"] += len(saved)
                saved.to_csv(os.path.join(outdir, f"options_eod_{todt}.csv"), index=False,
                             mode="a", header=not os.path.exists(os.path.join(outdir, f"options_eod_{todt}.csv")))

        status, err = classify(raw, saved, error, write_failed)
        row = symbol_audit_row(ctx, sym, exchange_for(sym, exch_dict), status, len(saved), todt,
                               sym_started, DU.utc_now(), err)
        audit_rows.append(row)
        if dbflag:
            DU.audit_run([row])
        timings.append({"Symbol": sym, "seconds": round(time.monotonic() - t0, 3),
                        "n_raw": len(raw), "n_rows": len(saved), "status": status})

    return finish()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # The full-V4 timing run that sizes OPT_SHARDS. dbFlag=False: CSVs only, no
    # table, no R2, no load_audit. Add "test": 10 to cap it while developing.
    print(run({"localrun": True, "dbFlag": True}, None))
