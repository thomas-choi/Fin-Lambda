"""Unit tests for ``eod_daily_handler`` (eodDaily, PLAN-SR-UPSTREAM Phase B).

python3.13 only (``venv-py313``): the handler runs on the finDeepCore +
finDeepYf layers.
Every yfinance and MySQL touch is mocked; the multi-ticker frame is a real
``yf.download(["AAPL","MSFT","ZZZZNOTREAL"], auto_adjust=False, actions=True,
group_by="ticker")`` saved to ``tests/fixtures/yf_batch_daily.csv`` on
2026-09-25. It carries AAPL's 2026-08-10 and MSFT's 2026-08-20 dividends and an
all-NaN column block for the 404 ticker.
"""

import os
from datetime import date, datetime, timedelta
from types import SimpleNamespace

import pandas as pd
import pytest

import dataUtil as DU
import eod_daily_handler as H

pytestmark = pytest.mark.unit

US = "America/New_York"
HK = "Asia/Hong_Kong"


@pytest.fixture(scope="module")
def batch_raw(request):
    path = os.path.join(os.path.dirname(__file__), "..", "fixtures", "yf_batch_daily.csv")
    return pd.read_csv(path, header=[0, 1], index_col=0, parse_dates=True)


@pytest.fixture
def eod_env(env, monkeypatch):
    for k, v in {"EOD_WRITE_TBL": "histdailyprice7_shadow", "TBLLOADAUDIT": "load_audit",
                 "TBLCORPACTION": "corp_action_daily", "SYMBOL_PROC_VER": "V4",
                 "EOD_BATCH": "200"}.items():
        monkeypatch.setenv(k, v)
    return env


EXCH = {"AAPL": "NASDAQ", "MSFT": "NASDAQ", "KO": "NYSE", "BTC-USD": "CRYPTO"}
TZ = {"NASDAQ": US, "NYSE": US, "HK": HK, "CRYPTO": US}


# --------------------------------------------------------------------------
# E1 exchange / date rules
# --------------------------------------------------------------------------
@pytest.mark.parametrize("sym,expected", [
    ("AAPL", "NASDAQ"),        # stock_exchange.csv wins
    ("^HSI", "HK"),            # special case
    ("0700.HK", "HK"),         # suffix
    ("RELIANCE.NS", "NS"),
    ("BRK-B", ""),             # nothing -> blank, as production
])
def test_exchange_for(sym, expected):
    assert H.exchange_for(sym, {"AAPL": "NASDAQ"}) == expected


def test_exchange_for_csv_beats_hsi_rule():
    assert H.exchange_for("^HSI", {"^HSI": "HKEX"}) == "HKEX"


def test_tz_for_defaults_to_new_york():
    assert H.tz_for("", TZ) == US
    assert H.tz_for("HK", TZ) == HK
    assert H.tz_for("XX", None) == US


@pytest.mark.parametrize("hour,expected", [(8, date(2026, 9, 24)), (9, date(2026, 9, 25)), (18, date(2026, 9, 25))])
def test_session_date(hour, expected):
    assert H.session_date(datetime(2026, 9, 25, hour, 30)) == expected


def test_start_dates():
    out = H.start_dates(["A", "B"], {"A": date(2026, 9, 10), "B": None}, date(2008, 1, 1))
    assert out == {"A": (date(2026, 9, 11), "append"), "B": (date(2008, 1, 1), "first")}


# --------------------------------------------------------------------------
# E2 plan_downloads
# --------------------------------------------------------------------------
def test_plan_downloads_batches_recent_and_singles_old():
    asof = date(2026, 9, 25)
    starts = {"AAPL": (date(2026, 9, 25), "append"), "0700.HK": (date(2026, 9, 20), "append"),
              "KO": (date(2008, 1, 1), "first"), "MSFT": (date(2026, 9, 26), "append")}
    tz_of = lambda s: HK if s.endswith(".HK") else US
    plan = H.plan_downloads(starts, asof, tz_of, batch_size=200)

    batches = [d for d in plan if d.kind == "batch"]
    singles = [d for d in plan if d.kind == "single"]
    # up-to-date MSFT is still batched: its recent actions must be seen
    assert sorted(s for d in batches for s in d.symbols) == ["0700.HK", "AAPL", "MSFT"]
    # never mix time zones in one batch (F7)
    assert {tuple(sorted({tz_of(s) for s in d.symbols})) for d in batches} == {(US,), (HK,)}
    assert all(d.start == asof - timedelta(days=21) and d.end == asof + timedelta(days=1) for d in batches)
    assert singles == [H.Download(("KO",), date(2008, 1, 1), asof + timedelta(days=1), "single")]


def test_plan_downloads_respects_batch_size():
    starts = {f"S{i:03d}": (date(2026, 9, 25), "append") for i in range(450)}
    plan = H.plan_downloads(starts, date(2026, 9, 25), lambda s: US, batch_size=200)
    assert [len(d.symbols) for d in plan] == [200, 200, 50]


def test_plan_downloads_window_boundary():
    asof = date(2026, 9, 25)
    edge = asof - timedelta(days=21)
    plan = H.plan_downloads({"IN": (edge, "append"), "OUT": (edge - timedelta(days=1), "append")},
                            asof, lambda s: US, 200)
    assert {d.kind: d.symbols for d in plan} == {"batch": ("IN",), "single": ("OUT",)}


# --------------------------------------------------------------------------
# E3 prepend
# --------------------------------------------------------------------------
def test_prepend_ranges():
    first = date(2008, 1, 1)
    out = H.prepend_ranges({"A": date(2010, 1, 4), "B": date(2008, 1, 1), "C": None,
                            "D": pd.Timestamp("2015-06-01")}, first)
    assert out == {"A": (first, date(2010, 1, 3)), "D": (first, date(2015, 5, 31))}


def test_plan_prepend_spans_widest_range_per_tz_batch():
    first = date(2008, 1, 1)
    ranges = {"A": (first, date(2010, 1, 3)), "B": (first, date(2012, 5, 1)), "0005.HK": (first, date(2010, 1, 3))}
    plan = H.plan_prepend(ranges, lambda s: HK if s.endswith(".HK") else US, 200)
    by_syms = {d.symbols: d for d in plan}
    assert by_syms[("A", "B")].end == date(2012, 5, 2)
    assert by_syms[("0005.HK",)].end == date(2010, 1, 4)
    assert all(d.kind == "prepend" and d.start == first for d in plan)


# --------------------------------------------------------------------------
# E4 reshape_batch
# --------------------------------------------------------------------------
def _windows(lo, hi, syms=("AAPL", "MSFT", "ZZZZNOTREAL")):
    return {s: (lo, hi) for s in syms}


def test_reshape_batch_columns_and_nan_drop(batch_raw):
    out = H.reshape_batch(batch_raw, ("AAPL", "MSFT", "ZZZZNOTREAL"),
                          _windows(date(2026, 8, 1), date(2026, 9, 30)), lambda s: EXCH.get(s, ""))
    assert list(out.columns) == H.SAVE_COLUMNS
    assert set(out.Symbol) == {"AAPL", "MSFT"}          # all-NaN 404 ticker dropped
    assert out.Close.notna().all()
    assert (out.Exchange == "NASDAQ").all()
    assert isinstance(out.Date.iloc[0], date)
    # Adj Close renamed and kept distinct from Close (auto_adjust=False, L1)
    aapl = out[out.Symbol == "AAPL"].set_index("Date")
    assert aapl.loc[date(2026, 8, 3), "AdjClose"] < aapl.loc[date(2026, 8, 3), "Close"]


def test_reshape_batch_window_filter(batch_raw):
    out = H.reshape_batch(batch_raw, ("AAPL",), _windows(date(2026, 9, 21), date(2026, 9, 23)),
                          lambda s: "NASDAQ")
    assert sorted(out.Date) == [date(2026, 9, 21), date(2026, 9, 22), date(2026, 9, 23)]


def test_reshape_batch_symbol_absent_or_empty_raw(batch_raw):
    assert len(H.reshape_batch(batch_raw, ("NOPE",), {"NOPE": (date(2026, 1, 1), date(2026, 12, 1))}, str)) == 0
    assert list(H.reshape_batch(None, ("A",), {}, str).columns) == H.SAVE_COLUMNS


def test_reshape_batch_single_level_columns(batch_raw):
    single = batch_raw["AAPL"]
    out = H.reshape_batch(single, ("AAPL",), _windows(date(2026, 9, 24), date(2026, 9, 24)), lambda s: "NASDAQ")
    assert len(out) == 1 and out.Symbol.iloc[0] == "AAPL"


def test_reshape_batch_drops_nan_close_today_bar(batch_raw):
    """Observed 2026-09-25 20:06 ET: Yahoo serves today's bar with Close=NaN."""
    raw = batch_raw.copy()
    raw.loc[pd.Timestamp("2026-09-25"), ("AAPL", "Close")] = float("nan")
    out = H.reshape_batch(raw, ("AAPL",), _windows(date(2026, 9, 24), date(2026, 9, 25)), lambda s: "NASDAQ")
    assert list(out.Date) == [date(2026, 9, 24)]


# --------------------------------------------------------------------------
# E5 drop_partial_bar (L2)
# --------------------------------------------------------------------------
def _bars(exchange, day):
    return pd.DataFrame({"Date": [day - timedelta(days=1), day], "Exchange": [exchange] * 2})


@pytest.mark.parametrize("minute,kept", [(59, 1), (60, 2)])
def test_drop_partial_bar_ny_boundary(minute, kept):
    # 2026-09-25 is EDT: close 16:00 local = 20:00 UTC; +60 min = 21:00 UTC
    now_utc = datetime(2026, 9, 25, 20, minute) if minute < 60 else datetime(2026, 9, 25, 21, 0)
    out = H.drop_partial_bar(_bars("NASDAQ", date(2026, 9, 25)), now_utc, lambda e: TZ.get(e, US))
    assert len(out) == kept


@pytest.mark.parametrize("now_utc,kept", [(datetime(2026, 9, 25, 9, 9), 1), (datetime(2026, 9, 25, 9, 10), 2)])
def test_drop_partial_bar_hk_boundary(now_utc, kept):
    # HK close 16:10 HKT = 08:10 UTC; +60 min = 09:10 UTC
    out = H.drop_partial_bar(_bars("HK", date(2026, 9, 25)), now_utc, lambda e: TZ.get(e, US))
    assert len(out) == kept


def test_drop_partial_bar_winter_ny():
    # EST: close 16:00 local = 21:00 UTC. The old 21:10 UTC cron (16:10 ET) is inside the grace.
    out = H.drop_partial_bar(_bars("NYSE", date(2026, 12, 1)), datetime(2026, 12, 1, 21, 10), lambda e: US)
    assert len(out) == 1


def test_drop_partial_bar_hk_after_us_close_keeps_everything():
    # 18:30 ET is 06:30 HKT the next day, so HK's "today" has no bar yet
    out = H.drop_partial_bar(_bars("HK", date(2026, 9, 25)), datetime(2026, 9, 25, 22, 30), lambda e: TZ.get(e, US))
    assert len(out) == 2


def test_drop_partial_bar_crypto_never_final():
    out = H.drop_partial_bar(_bars("CRYPTO", date(2026, 9, 25)), datetime(2026, 9, 25, 23, 59), lambda e: US)
    assert len(out) == 1


# --------------------------------------------------------------------------
# E6 extract_actions
# --------------------------------------------------------------------------
def test_extract_actions_from_fixture(batch_raw):
    seen = datetime(2026, 9, 25, 22, 31)
    out = H.extract_actions(batch_raw, ("AAPL", "MSFT", "ZZZZNOTREAL"), date(2026, 8, 1),
                            lambda s: EXCH.get(s, ""), seen, "RUN")
    assert list(out.columns) == H.ACTION_COLUMNS
    got = {(r.Symbol, r.Date): r.Dividends for r in out.itertuples()}
    assert got == {("AAPL", date(2026, 8, 10)): 0.27, ("MSFT", date(2026, 8, 20)): 0.91}
    assert (out.first_seen_at == seen).all() and (out.run_id == "RUN").all()


def test_extract_actions_window_and_split(batch_raw):
    raw = batch_raw.copy()
    raw.loc[pd.Timestamp("2026-09-22"), ("AAPL", "Stock Splits")] = 4.0
    out = H.extract_actions(raw, ("AAPL",), date(2026, 9, 1), lambda s: "NASDAQ", datetime(2026, 9, 25), "R")
    assert len(out) == 1  # the 08-10 dividend is outside the window
    row = out.iloc[0]
    assert row.Date == date(2026, 9, 22) and row.StockSplits == 4.0 and row.Dividends == 0.0


# --------------------------------------------------------------------------
# E7 audit rows
# --------------------------------------------------------------------------
CTX = {"run_id": "RUN", "table": "histdailyprice7", "yf_version": "0.2.58", "host": "h"}


def test_symbol_audit_rows_statuses():
    bars = pd.DataFrame({"Symbol": ["A", "A"], "Date": [date(2026, 9, 24), date(2026, 9, 25)]})
    rows = H.symbol_audit_rows(["A", "B", "C"], bars, {"C": "HTTP 404"}, lambda s: "append",
                               lambda s: "NYSE", CTX, datetime(2026, 1, 1), datetime(2026, 1, 1))
    by = {r["Symbol"]: r for r in rows}
    assert by["A"]["status"] == "ok" and by["A"]["n_rows"] == 2
    assert (by["A"]["date_lo"], by["A"]["date_hi"]) == (date(2026, 9, 24), date(2026, 9, 25))
    assert by["B"]["status"] == "empty" and by["B"]["date_hi"] is None
    assert by["C"]["status"] == "error" and "404" in by["C"]["error"]
    assert all(r["yf_version"] == "0.2.58" and r["segment"] == "append" for r in rows)


def test_symbol_audit_rows_write_failure_and_skip():
    bars = pd.DataFrame({"Symbol": ["A"], "Date": [date(2026, 9, 25)]})
    rows = H.symbol_audit_rows(["A"], bars, {}, lambda s: "first", str, CTX,
                               datetime(2026, 1, 1), datetime(2026, 1, 1), write_failed=True)
    assert rows[0]["status"] == "error"
    rows = H.symbol_audit_rows(["A"], None, {}, lambda s: "first", str, CTX,
                               datetime(2026, 1, 1), datetime(2026, 1, 1), status_override="skipped")
    assert rows[0]["status"] == "skipped" and rows[0]["n_rows"] == 0


def test_summary_row_counts_empty_as_ok():
    sym_rows = [{"status": s} for s in ("ok", "empty", "error", "skipped")]
    row = H.summary_row(CTX, "histdailyprice7", 10, [date(2026, 9, 24), date(2026, 9, 25)],
                        sym_rows, datetime(2026, 1, 1))
    assert (row["Symbol"], row["segment"], row["n_ok"], row["n_expected"]) == ("*", "summary", 2, 4)
    assert (row["date_lo"], row["date_hi"], row["status"]) == (date(2026, 9, 24), date(2026, 9, 25), "ok")
    corp = H.summary_row(CTX, "corp_action_daily", 1, [], None, datetime(2026, 1, 1))
    assert corp["n_ok"] is None and corp["n_expected"] is None


# --------------------------------------------------------------------------
# E8 download flags pinned (L1)
# --------------------------------------------------------------------------
def test_download_pins_flags(monkeypatch):
    seen = {}

    def fake(tickers, **kw):
        seen.update(kw, tickers=tickers)
        return pd.DataFrame()

    monkeypatch.setattr(H.yf, "download", fake)
    H.download(("AAPL",), date(2026, 9, 4), date(2026, 9, 26))
    assert seen["auto_adjust"] is False and seen["actions"] is True
    assert seen["group_by"] == "ticker" and seen["progress"] is False
    assert seen["tickers"] == ["AAPL"]  # always a list -> always (Ticker, Price) columns


# --------------------------------------------------------------------------
# E9 run() -- env, dry run, writes, guards
# --------------------------------------------------------------------------
@pytest.fixture
def wired(eod_env, monkeypatch, batch_raw, tmp_path):
    """run() with every IO edge replaced; records what would have been written."""
    calls = {"downloads": [], "append": [], "audit": [], "symbol_proc": []}
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(DU, "load_symbols_db", lambda ver, sym_type=None:
                        calls["symbol_proc"].append((ver, sym_type))
                        or ["AAPL", "MSFT", "ZZZZNOTREAL"])
    monkeypatch.setattr(DU, "load_symbols_dict", lambda: dict(EXCH))
    monkeypatch.setattr(DU, "load_exchange_tz", lambda: dict(TZ))
    monkeypatch.setattr(H, "load_watermarks",
                        lambda db, t, syms, agg="MAX": {"AAPL": date(2026, 9, 22), "MSFT": date(2026, 9, 22)})
    monkeypatch.setattr(DU, "utc_now", lambda: datetime(2026, 9, 25, 22, 30))

    def fake_download(symbols, start, end):
        calls["downloads"].append((tuple(symbols), start, end))
        return batch_raw, {"ZZZZNOTREAL": "HTTPError('HTTP Error 404: ')"}

    monkeypatch.setattr(H, "download", fake_download)
    monkeypatch.setattr(DU, "append_ignore", lambda df, s, t, chunk=500: calls["append"].append((t, df.copy())) or len(df))
    monkeypatch.setattr(DU, "audit_run", lambda rows: calls["audit"].extend(rows) or len(rows))
    return calls


EV = {"asof": "2026-09-25"}


@pytest.mark.parametrize("missing", ["EOD_WRITE_TBL", "TBLLOADAUDIT", "TBLCORPACTION"])
def test_run_refuses_without_required_env(wired, unset_env, missing):
    unset_env(missing)
    with pytest.raises(RuntimeError, match=missing):
        H.run(dict(EV), None)
    assert wired["downloads"] == []


def test_run_dry_run_writes_csvs_not_db(wired, tmp_path):
    out = H.run(dict(EV, dbFlag=False, localrun=True), None)
    assert wired["append"] == [] and wired["audit"] == []
    bars = pd.read_csv(tmp_path / "eod_daily_2026-09-25.csv")
    assert list(bars.columns) == H.SAVE_COLUMNS
    assert sorted(bars.Date.unique()) == ["2026-09-23", "2026-09-24", "2026-09-25"]
    audit = pd.read_csv(tmp_path / "load_audit_2026-09-25.csv")
    assert list(audit.columns) == DU.AUDIT_COLUMNS
    assert sorted(audit[audit.Symbol == "*"].table_name) == ["corp_action_daily", "histdailyprice7_shadow"]
    assert (tmp_path / "corp_action_2026-09-25.csv").exists()
    assert out["status"] == "ok" and (out["n_ok"], out["n_error"]) == (2, 1)
    import json
    json.dumps(out)  # JSON-safe for Lambda


def test_run_writes_bars_actions_and_audit(wired, monkeypatch, batch_raw):
    raw = batch_raw.copy()
    raw.loc[pd.Timestamp("2026-09-15"), ("MSFT", "Dividends")] = 0.91
    monkeypatch.setattr(H, "download", lambda syms, s, e: wired["downloads"].append(syms) or (raw, {}))
    out = H.run(dict(EV), None)
    tables = [t for t, _ in wired["append"]]
    assert tables == ["histdailyprice7_shadow", "corp_action_daily"]
    bars = wired["append"][0][1]
    assert bars.Date.min() == date(2026, 9, 23)            # watermark + 1
    actions = wired["append"][1][1]
    # only the injected 09-15 dividend is inside asof-21d; the fixture's 08-10 / 08-20 are not
    assert [(r.Symbol, r.Date) for r in actions.itertuples()] == [("MSFT", date(2026, 9, 15))]
    sym_rows = [r for r in wired["audit"] if r["Symbol"] != "*"]
    assert {r["Symbol"]: r["status"] for r in sym_rows} == {"AAPL": "ok", "MSFT": "ok", "ZZZZNOTREAL": "empty"}
    assert {r["segment"] for r in sym_rows} == {"append", "first"}
    summaries = [r for r in wired["audit"] if r["Symbol"] == "*"]
    assert len(summaries) == 2 and all(r["run_id"] == out["run_id"] for r in summaries)


def test_run_partial_bar_dropped_before_grace(wired, monkeypatch):
    monkeypatch.setattr(DU, "utc_now", lambda: datetime(2026, 9, 25, 20, 30))  # 16:30 EDT
    H.run(dict(EV), None)
    assert wired["append"][0][1].Date.max() == date(2026, 9, 24)


def test_run_watermark_failure_is_fatal_not_a_full_reload(wired, monkeypatch):
    monkeypatch.setattr(H, "load_watermarks", lambda *a, **k: None)
    out = H.run(dict(EV), None)
    assert out["status"] == "error" and "watermark" in out["error"]
    assert wired["downloads"] == []
    assert [r["status"] for r in wired["audit"]] == ["error", "error"]


def test_run_asks_the_symbol_procedure_for_every_live_symbol(wired):
    """@type 'a': eodDaily covers more than the optionable names (V5)."""
    H.run(dict(EV), None)
    assert wired["symbol_proc"] == [("V4", "a")]


def test_run_symbol_type_is_overridable_by_the_event(wired):
    H.run(dict(EV) | {"symType": "o"}, None)
    assert wired["symbol_proc"] == [("V4", "o")]


def test_run_empty_symbol_list_is_an_error(wired, monkeypatch):
    monkeypatch.setattr(DU, "load_symbols_db", lambda ver, sym_type=None: None)
    assert H.run(dict(EV), None)["status"] == "error"
    assert wired["downloads"] == []


def test_run_time_guard_marks_skipped(wired, monkeypatch):
    monkeypatch.setattr(H, "load_watermarks", lambda *a, **k: {"AAPL": date(2026, 9, 22)})
    ctx = SimpleNamespace(get_remaining_time_in_millis=lambda: 60_000)
    out = H.run(dict(EV), ctx)
    assert wired["downloads"] == []
    assert out["n_skipped"] == 3
    assert {r["status"] for r in wired["audit"] if r["Symbol"] != "*"} == {"skipped"}


def test_run_shard(wired):
    H.run(dict(EV, shard=1, of=2), None)
    assert [s for d in wired["downloads"] for s in d[0]] == ["MSFT"]


def test_run_sweep_takes_only_missing(wired, monkeypatch):
    seen = {}

    def fake_missing(job, day, expected, table):
        seen.update(job=job, day=day, expected=expected, table=table)
        return ["MSFT"]

    monkeypatch.setattr(DU, "missing_for_sweep", fake_missing)
    out = H.run(dict(EV, sweep=True, shard=1, of=2), None)
    assert seen == {"job": "eodDaily", "day": date(2026, 9, 25),
                    "expected": ["AAPL", "MSFT", "ZZZZNOTREAL"], "table": "histdailyprice7_shadow"}
    assert [s for d in wired["downloads"] for s in d[0]] == ["MSFT"]
    assert out["mode"] == "sweep"


def test_run_sweep_unreadable_audit_is_fatal(wired, monkeypatch):
    monkeypatch.setattr(DU, "missing_for_sweep", lambda *a: None)
    assert H.run(dict(EV, sweep=True), None)["status"] == "error"
    assert wired["downloads"] == []


def test_run_prepend(wired, monkeypatch, eod_env):
    monkeypatch.setenv("FIRSTTRAINDTE", "2026/08/01")
    monkeypatch.setattr(H, "load_watermarks",
                        lambda db, t, syms, agg="MAX": {"AAPL": date(2026, 8, 12), "MSFT": date(2026, 8, 1)})
    out = H.run(dict(EV, prepend=True), None)
    assert out["mode"] == "prepend"
    assert wired["downloads"] == [(("AAPL",), date(2026, 8, 1), date(2026, 8, 12))]
    bars = wired["append"][0][1]
    assert bars.Date.min() == date(2026, 8, 3) and bars.Date.max() == date(2026, 8, 11)
    assert [t for t, _ in wired["append"]] == ["histdailyprice7_shadow"]  # no actions on prepend
    sym_rows = [r for r in wired["audit"] if r["Symbol"] != "*"]
    assert [(r["Symbol"], r["segment"]) for r in sym_rows] == [("AAPL", "prepend")]
    assert [r["table_name"] for r in wired["audit"] if r["Symbol"] == "*"] == ["histdailyprice7_shadow"]


def test_run_test_cap_and_symbols_override(wired):
    H.run(dict(EV, symbols=["MSFT", "AAPL"], test=1), None)
    assert [s for d in wired["downloads"] for s in d[0]] == ["AAPL"]


def test_run_write_failure_marks_symbols_error(wired, monkeypatch):
    monkeypatch.setattr(DU, "append_ignore", lambda *a, **k: None)
    out = H.run(dict(EV), None)
    assert out["n_error"] == 3
