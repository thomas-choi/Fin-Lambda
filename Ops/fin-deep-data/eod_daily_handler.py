"""eodDaily -- daily OHLCV bars into histdailyprice7, plus corporate actions.

Ported from myFinData@19c8509 Ops/eoddata_ext_fetch.py::common_fetch_eod
(the host-cron job `eoddata_ext_fetch.py -m`), per PLAN-SR-UPSTREAM Phase B.
Kept from production:

* the yfinance flags -- ``auto_adjust=False`` (L1), ``Adj Close`` renamed to
  ``AdjClose``, and the ``savColumns`` order;
* the exchange rule: ``stock_exchange.csv``, then ``^HSI`` -> ``HK``, then the
  ticker suffix, else ``""``;
* the watermark: each symbol continues from ``max(Date) + 1``, or from
  ``FIRSTTRAINDTE`` when it has no rows.

Changed from production:

* the list is ``current_symbols_{SYMBOL_PROC_VER}`` via ``DU.load_symbols_db``
  (F3), asked for with @type ``'a'`` -- every symbol V5 does not mark delisted;
* one ``GROUP BY`` watermark query instead of one query per symbol;
* batched downloads grouped by exchange time zone (F7), with a 21-day window
  that doubles as the corporate-action window;
* ``Date == today`` is dropped until 60 min after that exchange's close (L2);
* writes are per-batch ``INSERT IGNORE`` (L3) to ``EOD_WRITE_TBL``, followed by
  one ``load_audit`` row per symbol and a ``'*'`` summary row per table;
* dividends/splits go to ``corp_action_daily`` (first sighting wins);
* the VENDOR=tiingo branch and the host-disk CSV cache are dropped.

Runs on **python3.13** with the ``finDeepCore`` + ``finDeepYf`` layers. Its
deploy package carries this module, ``dataUtil.py``, ``stock_exchange.csv`` and
``Exchange_timezone.csv`` -- nothing else in the folder.

Local dry run (reads MySQL for the list and watermarks, writes only CSVs)::

    cd Ops/fin-deep-data
    ../../venv-py313/bin/python eod_daily_handler.py
"""

import json
import logging
import os
from collections import namedtuple
from datetime import date, datetime, time, timedelta

import pandas as pd
import pytz
import yfinance as yf
from dotenv import load_dotenv

import dataUtil as DU

logger = logging.getLogger()

load_dotenv()

# --------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------
JOB = "eodDaily"
# current_symbols_V5 @type: this job wants every live symbol, not only the
# optionable ones. Ignored while SYMBOL_PROC_VER names a pre-V5 procedure.
SYMBOL_TYPE = "a"

#: Column order of histdailyprice7 -- production's savColumns.
SAVE_COLUMNS = ["Date", "Symbol", "Exchange", "Close", "Open", "High", "Low", "Volume", "AdjClose"]

#: Column order of corp_action_daily.
ACTION_COLUMNS = ["Date", "Symbol", "Exchange", "Dividends", "StockSplits", "first_seen_at", "run_id"]

#: Flags pinned on every download. auto_adjust=False is L1: from yfinance
#: 0.2.51 the default is True, which would silently adjust O/H/L/C and drop
#: 'Adj Close'.
YF_KWARGS = {"auto_adjust": False, "actions": True, "group_by": "ticker", "progress": False}

#: Batched symbols download this many calendar days back (≈14 sessions). The
#: same window is the corporate-action window, so a late-posted dividend or
#: split is still caught.
ACTION_WINDOW_DAYS = 21

#: A bar dated today is kept only once this long has passed since the close.
PARTIAL_BAR_GRACE = timedelta(minutes=60)

#: Time zone used when an exchange is blank or missing from Exchange_timezone.csv
#: (40 of the 857 V4 symbols on 2026-09-25, all US-listed or US funds).
DEFAULT_TZ = "America/New_York"

#: Regular-session close, local time, by exchange time zone.
MARKET_CLOSE = {
    "America/New_York": time(16, 0),
    "America/Mexico_City": time(15, 0),
    "Asia/Hong_Kong": time(16, 10),   # closing auction ends 16:10
    "Asia/Shanghai": time(15, 0),
    "Asia/Seoul": time(15, 30),
    "Asia/Taipei": time(13, 30),
    "Asia/Tokyo": time(15, 30),
    "Asia/Kolkata": time(15, 30),
    "Asia/Jakarta": time(16, 0),
    "Asia/Singapore": time(17, 0),
    "Europe/London": time(16, 30),
    "Europe/Zurich": time(17, 30),
}
DEFAULT_CLOSE = time(16, 0)

#: Exchanges that never close: today's bar is always partial.
NEVER_CLOSES = {"CRYPTO"}

#: One planned yf.download call.
Download = namedtuple("Download", "symbols start end kind")


# --------------------------------------------------------------------------
# Pure helpers
# --------------------------------------------------------------------------
def session_date(ny_now):
    """Production's date rule: the NY date, minus one day before 09:00."""
    if ny_now.hour < 9:
        ny_now = ny_now - timedelta(days=1)
    return ny_now.date()


def check_exchange_from_ticker(ticker):
    # ported from myFinData@19c8509 Ops/eoddata_ext_fetch.py
    st = ticker.split(".")
    if len(st) > 1:
        return st[-1]
    return ""


def exchange_for(sym, exch_dict):
    """stock_exchange.csv first, then ^HSI -> HK, then the suffix, else ''."""
    if exch_dict and sym in exch_dict:
        return exch_dict[sym]
    if sym == "^HSI":
        return "HK"
    return check_exchange_from_ticker(sym)


def tz_for(exchange, tz_map):
    """Exchange -> IANA zone, DEFAULT_TZ when unknown."""
    return (tz_map or {}).get(exchange) or DEFAULT_TZ


def start_dates(symbols, watermarks, first_date):
    """``{sym: (start, segment)}``: max(Date)+1 and 'append', or FIRSTTRAINDTE and 'first'."""
    out = {}
    for sym in symbols:
        last = watermarks.get(sym)
        if last is None or pd.isna(last):
            out[sym] = (first_date, "first")
        else:
            out[sym] = (pd.Timestamp(last).date() + timedelta(days=1), "append")
    return out


def _batches(symbols, tz_of, batch_size):
    """Split symbols into same-time-zone batches of at most batch_size (F7)."""
    groups = {}
    for sym in sorted(symbols):
        groups.setdefault(tz_of(sym), []).append(sym)
    for tz in sorted(groups):
        syms = groups[tz]
        for i in range(0, len(syms), batch_size):
            yield syms[i:i + batch_size]


def plan_downloads(starts, asof, tz_of, batch_size, window_days=ACTION_WINDOW_DAYS):
    """Plan the yf.download calls for a normal (append) run.

    Symbols whose start is within the window share batched calls over the
    common window ``[asof - window_days, asof]``; that includes symbols that
    are already up to date, so their recent actions are still seen. Older
    starts -- first loads and gaps -- are downloaded one symbol at a time.
    ``end`` is exclusive, as yfinance expects.
    """
    window_start = asof - timedelta(days=window_days)
    recent = [s for s, (start, _) in starts.items() if start >= window_start]
    old = sorted(s for s, (start, _) in starts.items() if start < window_start)

    plan = [Download(tuple(b), window_start, asof + timedelta(days=1), "batch")
            for b in _batches(recent, tz_of, batch_size)]
    plan += [Download((s,), starts[s][0], asof + timedelta(days=1), "single") for s in old]
    return plan


def prepend_ranges(min_dates, first_date):
    """``{sym: (first_date, MIN(Date) - 1)}`` for every symbol with a gap before it.

    Symbols with no rows at all are left to the normal run's first load.
    """
    out = {}
    for sym, lo in min_dates.items():
        if lo is None or pd.isna(lo):
            continue
        lo = pd.Timestamp(lo).date()
        if lo > first_date:
            out[sym] = (first_date, lo - timedelta(days=1))
    return out


def plan_prepend(ranges, tz_of, batch_size):
    """Batched prepend downloads: each batch spans its members' widest range."""
    plan = []
    for batch in _batches(ranges.keys(), tz_of, batch_size):
        lo = min(ranges[s][0] for s in batch)
        hi = max(ranges[s][1] for s in batch)
        plan.append(Download(tuple(batch), lo, hi + timedelta(days=1), "prepend"))
    return plan


def _symbol_frame(raw, sym):
    """One symbol's OHLCV frame out of a yf.download result, or None."""
    if raw is None or len(raw) == 0:
        return None
    if isinstance(raw.columns, pd.MultiIndex):
        if sym not in raw.columns.get_level_values(0):
            return None
        sub = raw[sym].copy()
    else:
        sub = raw.copy()
    sub.columns = [str(c) for c in sub.columns]
    sub = sub.reset_index()
    first = sub.columns[0]
    sub = sub.rename(columns={first: "Date", "Adj Close": "AdjClose", "Stock Splits": "StockSplits"})
    dates = pd.to_datetime(sub["Date"])
    if getattr(dates.dt, "tz", None) is not None:
        dates = dates.dt.tz_localize(None)
    sub["Date"] = dates.dt.date
    return sub


def reshape_batch(raw, symbols, windows, exch_of):
    """Long bars frame in SAVE_COLUMNS order.

    ``windows`` is ``{sym: (lo, hi)}``; rows outside it are dropped, as are
    rows with a NaN Close (a symbol with no data that day, F7).
    """
    frames = []
    for sym in symbols:
        sub = _symbol_frame(raw, sym)
        if sub is None or "Close" not in sub.columns:
            continue
        lo, hi = windows[sym]
        sub = sub[sub["Close"].notna() & (sub["Date"] >= lo) & (sub["Date"] <= hi)].copy()
        if len(sub) == 0:
            continue
        if "AdjClose" not in sub.columns:
            sub["AdjClose"] = float("nan")
        sub["Symbol"] = sym
        sub["Exchange"] = exch_of(sym)
        frames.append(sub[SAVE_COLUMNS])
    return _concat(frames, SAVE_COLUMNS)


def drop_partial_bar(df, now_utc, exch_tz):
    """Drop bars dated the exchange's local today until 60 min after its close (L2).

    ``now_utc`` is tz-naive UTC; ``exch_tz`` maps Exchange -> IANA zone.
    """
    if df is None or len(df) == 0:
        return df
    now = pytz.utc.localize(now_utc) if now_utc.tzinfo is None else now_utc
    keep = pd.Series(True, index=df.index)
    for exchange in df["Exchange"].unique():
        tz = pytz.timezone(exch_tz(exchange))
        local = now.astimezone(tz)
        today = local.date()
        if exchange in NEVER_CLOSES:
            final = False
        else:
            close = MARKET_CLOSE.get(tz.zone, DEFAULT_CLOSE)
            close_at = tz.localize(datetime.combine(today, close))
            final = local >= close_at + PARTIAL_BAR_GRACE
        if not final:
            keep &= ~((df["Exchange"] == exchange) & (df["Date"] == today))
    dropped = int((~keep).sum())
    if dropped:
        logging.info(f"drop_partial_bar: dropped {dropped} bar(s) dated the exchange's today")
    return df[keep].reset_index(drop=True)


def extract_actions(raw, symbols, since, exch_of, seen_at, run_id):
    """Non-zero Dividends / Stock Splits dated >= since, as corp_action_daily rows."""
    rows = []
    for sym in symbols:
        sub = _symbol_frame(raw, sym)
        if sub is None:
            continue
        div = sub["Dividends"] if "Dividends" in sub.columns else pd.Series(0.0, index=sub.index)
        spl = sub["StockSplits"] if "StockSplits" in sub.columns else pd.Series(0.0, index=sub.index)
        hit = (div.fillna(0) != 0) | (spl.fillna(0) != 0)
        hit &= sub["Date"] >= since
        for idx in sub.index[hit]:
            rows.append({
                "Date": sub.at[idx, "Date"], "Symbol": sym, "Exchange": exch_of(sym),
                "Dividends": float(div.at[idx]) if pd.notna(div.at[idx]) else None,
                "StockSplits": float(spl.at[idx]) if pd.notna(spl.at[idx]) else None,
                "first_seen_at": seen_at, "run_id": run_id,
            })
    return pd.DataFrame(rows, columns=ACTION_COLUMNS)


def symbol_audit_rows(symbols, bars, errors, segment_of, exch_of, ctx, started_at, finished_at,
                      write_failed=False, status_override=None):
    """One load_audit row per symbol for a finished (or skipped) download."""
    rows = []
    for sym in symbols:
        sub = bars[bars["Symbol"] == sym] if bars is not None and len(bars) else None
        n = 0 if sub is None else len(sub)
        error = None
        if status_override:
            status = status_override
        elif n > 0 and write_failed:
            status, error = "error", "write to the bars table failed"
        elif n > 0:
            status = "ok"
        elif sym in errors:
            status, error = "error", str(errors[sym])
        else:
            status = "empty"
        rows.append({
            "run_id": ctx["run_id"], "job": JOB, "Symbol": sym, "Exchange": exch_of(sym),
            "date_lo": sub["Date"].min() if n else None,
            "date_hi": sub["Date"].max() if n else None,
            "table_name": ctx["table"], "segment": segment_of(sym), "n_rows": n,
            "status": status, "error": error, "started_at": started_at,
            "finished_at": finished_at, "yf_version": ctx["yf_version"], "host": ctx["host"],
        })
    return rows


def summary_row(ctx, table_name, rows_written, dates, symbol_rows, started_at, error=None):
    """The '*' summary row for one table.

    n_ok counts 'ok' and 'empty' symbols -- both are a successful check. With
    ``symbol_rows=None`` (corp_action_daily) n_ok / n_expected stay NULL.
    """
    statuses = None if symbol_rows is None else [r["status"] for r in symbol_rows]
    return DU.audit_summary(
        JOB, table_name, ctx["run_id"], started_at,
        status="error" if error else "ok",
        n_rows=rows_written,
        date_lo=min(dates) if dates else None,
        date_hi=max(dates) if dates else None,
        n_ok=None if statuses is None else sum(s in ("ok", "empty") for s in statuses),
        n_expected=None if statuses is None else len(statuses),
        error=error, yf_version=ctx["yf_version"],
    )


# --------------------------------------------------------------------------
# Environment / IO wrappers
# --------------------------------------------------------------------------
def _truthy(value, default=False):
    if value is None:
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "y", "on")


def _output_dir(localrun):
    """CWD locally, /tmp on Lambda where the bundle directory is read-only."""
    return DU.out_dir(localrun)


def _in_list(symbols):
    return ", ".join("'" + s.replace("'", "''") + "'" for s in symbols)


def load_watermarks(db, table, symbols, agg="MAX"):
    """``{Symbol: agg(Date)}`` in one query, or None when the table cannot be read."""
    if not symbols:
        return {}
    df = DU.load_df_SQL(
        f"SELECT Symbol, {agg}(Date) AS d FROM {db}.{table} "
        f"WHERE Symbol IN ({_in_list(symbols)}) GROUP BY Symbol;"
    )
    if df is None:
        return None
    return {row.Symbol: row.d for row in df.itertuples(index=False)}


def download(symbols, start, end):
    """One pinned yf.download call. Returns ``(frame, {symbol: error})``."""
    raw = yf.download(list(symbols), start=start, end=end, **YF_KWARGS)
    errors = dict(getattr(getattr(yf, "shared", None), "_ERRORS", {}) or {})
    return raw, errors


def _concat(frames, columns):
    """concat without the empty frames (pandas 2.2 FutureWarning), typed-empty if none."""
    frames = [f for f in frames if f is not None and len(f)]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=columns)


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

    Event keys: ``localrun`` (CSVs to CWD), ``dbFlag`` (False = no DB writes,
    CSVs instead), ``shard``/``of`` (round-robin slice), ``sweep`` (retry every
    symbol with no ok/empty audit row today), ``prepend`` (U2b: FIRSTTRAINDTE
    to MIN(Date)-1), ``asof`` (YYYY-MM-DD session date), ``symbols`` (list
    override), ``test`` (int caps the symbol count; any truthy value sets DEBUG
    logging), ``NYTIME`` (injected clock, for tests).

    Returns a JSON-serialisable summary.
    """
    event = dict(event or {})
    logger.setLevel(logging.DEBUG if event.get("test") else logging.INFO)
    logging.info(f"** ==> eod_daily_handler.run(event: {event})")

    localrun = bool(event.get("localrun", False))
    dbflag = bool(event.get("dbFlag", True))
    prepend = _truthy(event.get("prepend"))
    sweep = _truthy(event.get("sweep"))

    db = DU.require_env("DBMKTDATA")
    table = DU.require_env("EOD_WRITE_TBL")
    DU.require_env("TBLLOADAUDIT")
    corp_table = DU.require_env("TBLCORPACTION")
    first_date = datetime.strptime(DU.require_env("FIRSTTRAINDTE"), "%Y/%m/%d").date()
    ver = DU.env_or("SYMBOL_PROC_VER", "V5")
    sym_type = event.get("symType", SYMBOL_TYPE)
    batch_size = int(DU.env_or("EOD_BATCH", "200"))

    ny_now = event.get("NYTIME") or datetime.now(pytz.utc).astimezone(pytz.timezone("America/New_York"))
    asof = (datetime.strptime(event["asof"], "%Y-%m-%d").date() if event.get("asof")
            else session_date(ny_now))

    ctx = {"run_id": DU.new_run_id(), "table": table, "yf_version": yf.__version__[:16],
           "host": DU.run_host()}
    started_at = DU.utc_now()
    mode = "prepend" if prepend else ("sweep" if sweep else "append")
    result = {"run_id": ctx["run_id"], "asof": asof.isoformat(), "mode": mode, "dbFlag": dbflag}
    audit_rows, summary_rows = [], []
    all_bars, all_actions = [], []

    def finish(error=None):
        bar_dates = [d for r in audit_rows if r["date_hi"] for d in (r["date_lo"], r["date_hi"])]
        written = result.get("rows_written", 0)
        summary_rows.append(summary_row(ctx, table, written, bar_dates, audit_rows, started_at, error))
        if not prepend:
            act = _concat(all_actions, ACTION_COLUMNS)
            summary_rows.append(summary_row(
                ctx, corp_table, result.get("actions_written", 0), list(act["Date"]),
                None, started_at, error))
        if dbflag:
            DU.audit_run(summary_rows)
        else:
            outdir = _output_dir(localrun)
            bars = _concat(all_bars, SAVE_COLUMNS)
            acts = _concat(all_actions, ACTION_COLUMNS)
            bars.to_csv(os.path.join(outdir, f"eod_daily_{asof}.csv"), index=False)
            acts.to_csv(os.path.join(outdir, f"corp_action_{asof}.csv"), index=False)
            DU.audit_frame(audit_rows + summary_rows).to_csv(
                os.path.join(outdir, f"load_audit_{asof}.csv"), index=False)
        statuses = [r["status"] for r in audit_rows]
        result.update({
            "status": "error" if error else "ok", "error": error,
            "n_expected": len(statuses),
            "n_ok": statuses.count("ok"), "n_empty": statuses.count("empty"),
            "n_error": statuses.count("error"), "n_skipped": statuses.count("skipped"),
        })
        logging.info(f"SUMMARY {json.dumps(result, default=_jsonable)}")
        return {k: _jsonable(v) for k, v in result.items()}

    # --- symbol list ------------------------------------------------------
    symbols = event.get("symbols") or DU.load_symbols_db(ver, sym_type)
    if not symbols:
        return finish(f"symbol list current_symbols_{ver}({sym_type}) returned nothing")
    symbols = sorted(set(symbols))
    if sweep:
        symbols = DU.missing_for_sweep(JOB, asof, symbols, table)
        if symbols is None:
            return finish("sweep: load_audit could not be read")
        logging.info(f"sweep: {len(symbols)} symbol(s) missing for {asof}")
    else:
        symbols = DU.shard_symbols(symbols, int(event.get("shard", 0)), int(event.get("of", 1)))
    cap = _cap(event)
    if cap:
        symbols = symbols[:cap]

    exch_dict = DU.load_symbols_dict() or {}
    tz_map = DU.load_exchange_tz() or {}
    exch_of = lambda s: exchange_for(s, exch_dict)
    tz_of = lambda s: tz_for(exch_of(s), tz_map)

    # --- plan -------------------------------------------------------------
    if prepend:
        mins = load_watermarks(db, table, symbols, agg="MIN")
        if mins is None:
            return finish(f"could not read MIN(Date) from {db}.{table}")
        ranges = prepend_ranges(mins, first_date)
        windows = ranges
        segment_of = lambda s: "prepend"
        plan = plan_prepend(ranges, tz_of, batch_size)
    else:
        marks = load_watermarks(db, table, symbols)
        if marks is None:
            # Never fall back to FIRSTTRAINDTE for everything: that would
            # re-download 18 years for every symbol.
            return finish(f"could not read the watermark from {db}.{table}")
        starts = start_dates(symbols, marks, first_date)
        windows = {s: (start, asof) for s, (start, _) in starts.items()}
        segment_of = lambda s: starts[s][1]
        plan = plan_downloads(starts, asof, tz_of, batch_size)
    logging.info(f"{mode}: {len(symbols)} symbols in {len(plan)} download(s)")

    # --- download / write -------------------------------------------------
    for i, dl in enumerate(plan):
        if not DU.time_left_ok(context):
            remaining = [s for d in plan[i:] for s in d.symbols]
            logging.warning(f"time guard: {len(remaining)} symbol(s) left unstarted")
            now = DU.utc_now()
            audit_rows += symbol_audit_rows(remaining, None, {}, segment_of, exch_of, ctx,
                                            now, now, status_override="skipped")
            if dbflag:
                DU.audit_run(audit_rows[-len(remaining):])
            break

        dl_started = DU.utc_now()
        try:
            raw, errors = download(dl.symbols, dl.start, dl.end)
        except Exception as e:
            logging.error(f"download failed for {len(dl.symbols)} symbol(s)", exc_info=True)
            raw, errors = None, {s: repr(e) for s in dl.symbols}

        bars = reshape_batch(raw, dl.symbols, windows, exch_of)
        bars = drop_partial_bar(bars, DU.utc_now(), lambda ex: tz_for(ex, tz_map))
        actions = (extract_actions(raw, dl.symbols, asof - timedelta(days=ACTION_WINDOW_DAYS),
                                   exch_of, DU.utc_now(), ctx["run_id"])
                   if not prepend and raw is not None else pd.DataFrame(columns=ACTION_COLUMNS))

        write_failed = False
        if dbflag and len(bars):
            inserted = DU.append_ignore(bars, db, table)
            write_failed = inserted is None
            result["rows_written"] = result.get("rows_written", 0) + (inserted or 0)
        if dbflag and len(actions):
            inserted = DU.append_ignore(actions, db, corp_table)
            result["actions_written"] = result.get("actions_written", 0) + (inserted or 0)
        if not dbflag:
            result["rows_written"] = result.get("rows_written", 0) + len(bars)
            result["actions_written"] = result.get("actions_written", 0) + len(actions)
            all_bars.append(bars)
        all_actions.append(actions)

        rows = symbol_audit_rows(dl.symbols, bars, errors, segment_of, exch_of, ctx,
                                 dl_started, DU.utc_now(), write_failed=write_failed)
        audit_rows += rows
        if dbflag:
            DU.audit_run(rows)

    return finish()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # dbFlag=False: writes eod_daily_*.csv / corp_action_*.csv / load_audit_*.csv
    # into the CWD and touches no table. test=25 caps the symbol count.
    print(run({"localrun": True, "dbFlag": False, "test": 25}, None))
