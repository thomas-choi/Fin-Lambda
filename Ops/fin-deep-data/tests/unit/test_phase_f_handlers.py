"""load_audit summary rows in the v2 copies of the older handlers (Phase F / U10).

Covers ``usrate_handler`` (usrateHandlerv2), ``fxeod_handler`` (FXHistHandlerv2)
and ``intraday_min_handler`` (yfus30minEODv2 / yfasia30minEODv2).
``port_assets_handler``'s audit row is covered in
``test_port_assets_handler.py``.

python3.13 only. The engine is the ``mock_engine`` MagicMock; every data source
and write is patched, so nothing here touches Yahoo, the Fed or MySQL.
"""

from datetime import date, datetime, timedelta

import pandas as pd
import pytest
import pytz

import dataUtil as DU

pytestmark = pytest.mark.unit


@pytest.fixture
def audit(env, mock_engine, monkeypatch):
    """Capture audit rows and stored frames instead of writing them."""
    rows, stored = [], []
    monkeypatch.setattr(DU, "audit_run", lambda r: rows.extend(r) or len(r))
    monkeypatch.setattr(DU, "StoreEOD", lambda df, db, tbl: stored.append((df.copy(), db, tbl)))
    return {"rows": rows, "stored": stored}


class _no_warning:
    """Fail if a FutureWarning/DeprecationWarning comes out of our own call."""

    def __enter__(self):
        import warnings

        self._ctx = warnings.catch_warnings()
        self._ctx.__enter__()
        warnings.simplefilter("error", FutureWarning)
        warnings.simplefilter("error", DeprecationWarning)
        return self

    def __exit__(self, *exc):
        return self._ctx.__exit__(*exc)


def _is_summary(row, job, table):
    return (row["job"], row["table_name"], row["Symbol"], row["segment"], row["Exchange"]) == \
        (job, table, "*", "summary", "") and len(row["run_id"]) == 26


# --------------------------------------------------------------------------
# usrateHandlerv2
# --------------------------------------------------------------------------
def _h15_frame(days):
    import usrate_handler as UR

    data = {"Instruments": list(UR.nInstruments)}
    for i, d in enumerate(days):
        data[d] = [f"{3.5 + i / 100:.2f}"] * len(UR.nInstruments)
    return pd.DataFrame(data)


def test_usrate_writes_one_summary(audit, monkeypatch):
    import usrate_handler as UR

    monkeypatch.setattr(UR, "HTML2DataFrame", lambda url: _h15_frame(["2026-09-22", "2026-09-23", "2026-09-24"]))
    monkeypatch.setattr(DU, "get_Max_date", lambda tbl, sym=None: date(2026, 9, 22))

    out = UR.run({}, None)

    [row] = audit["rows"]
    assert _is_summary(row, "usrateHandler", "USRates")      # the data set, not the Lambda
    assert (row["status"], row["n_rows"]) == ("ok", 2)
    assert (row["date_lo"], row["date_hi"]) == (date(2026, 9, 23), date(2026, 9, 24))
    assert row["yf_version"] == ""                           # no yfinance in this handler
    assert out["rows"] == 2 and len(audit["stored"]) == 1


def test_usrate_reshape_drops_group_rows_and_casts(audit, monkeypatch):
    """`n.a.` becomes NaN, group header rows are dropped, rates are floats."""
    import usrate_handler as UR

    frame = _h15_frame(["2026-09-24"])
    frame.loc[0, "2026-09-24"] = "n.a."
    out = UR.reshape_rates(frame)

    assert list(out.columns)[0] == "Date"
    assert len(out.columns) == 1 + len(UR.nInstruments) - len(UR.GROUP_ROWS)
    assert not any(c in out.columns for c in UR.GROUP_ROWS)
    assert out["Federal_funds"].isna().all()
    assert out.drop(columns=["Date"]).dtypes.eq(float).all()


def test_usrate_empty_table_fallback_no_longer_raises(audit, monkeypatch):
    """Regression: `datetime(1800,1,1)` raised NameError (the module imports dt)."""
    import usrate_handler as UR

    monkeypatch.setattr(UR, "HTML2DataFrame", lambda url: _h15_frame(["2026-09-24"]))
    monkeypatch.setattr(DU, "get_Max_date", lambda tbl, sym=None: None)
    UR.run({}, None)
    assert audit["rows"][0]["n_rows"] == 1


def test_usrate_dry_run_writes_csv_and_no_audit(audit, monkeypatch, tmp_path):
    import usrate_handler as UR

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(UR, "HTML2DataFrame", lambda url: _h15_frame(["2026-09-24"]))
    monkeypatch.setattr(DU, "get_Max_date", lambda tbl, sym=None: date(2026, 9, 22))

    UR.run({"localrun": True, "dbFlag": False}, None)

    assert audit["rows"] == [] and audit["stored"] == []
    [csv] = list(tmp_path.glob("USrates_*.csv"))
    assert len(pd.read_csv(csv)) == 1


def test_usrate_read_html_accepts_a_literal_table(monkeypatch):
    """StringIO wrapping: a bare HTML string is deprecated in pandas 2.1+."""
    import usrate_handler as UR

    html = "<table id='h15table'><tr><th>Instruments</th><th>2026-09-24</th></tr>" \
           "<tr><td>Federal funds</td><td>4.33</td></tr></table>"

    class _Resp:
        text = html

    monkeypatch.setattr(UR.requests, "get", lambda url, **kw: _Resp())
    with _no_warning():
        out = UR.HTML2DataFrame("http://example.invalid")
    assert list(out.columns) == ["Instruments", "2026-09-24"]


def test_usrate_failure_is_audited_and_reraised(audit, monkeypatch):
    import usrate_handler as UR

    monkeypatch.setattr(DU, "get_Max_date", lambda tbl, sym=None: date(2026, 9, 22))
    monkeypatch.setattr(UR, "HTML2DataFrame", lambda url: (_ for _ in ()).throw(ValueError("h15table missing")))
    with pytest.raises(ValueError):
        UR.run({}, None)
    [row] = audit["rows"]
    assert row["status"] == "error" and "h15table missing" in row["error"]


def test_usrate_audit_failure_does_not_fail_the_load(audit, monkeypatch):
    import usrate_handler as UR

    monkeypatch.setattr(UR, "HTML2DataFrame", lambda url: _h15_frame(["2026-09-24"]))
    monkeypatch.setattr(DU, "get_Max_date", lambda tbl, sym=None: date(2026, 9, 22))
    monkeypatch.setattr(DU, "audit_run", lambda rows: (_ for _ in ()).throw(RuntimeError("boom")))
    assert UR.run({}, None)["rows"] == 1


# --------------------------------------------------------------------------
# FXHistHandlerv2
# --------------------------------------------------------------------------
def _fx_download(ticker, start=None, end=None, **kw):
    idx = pd.DatetimeIndex(pd.to_datetime(["2026-09-23", "2026-09-24"]), name="Date")
    return pd.DataFrame({"Open": [7.8, 7.8], "High": [7.9, 7.9], "Low": [7.7, 7.7],
                         "Close": [7.8, 7.8], "Adj Close": [7.8, 7.8], "Volume": [0, 0]}, index=idx)


@pytest.fixture
def fx(audit, monkeypatch):
    import fxeod_handler as FX

    monkeypatch.setenv("FX_TICKERS", '["HKD=X", "JPY=X"]')
    monkeypatch.setattr(FX.yf, "download", _fx_download)
    monkeypatch.setattr(DU, "get_Max_date", lambda tbl, sym=None: date(2026, 9, 22))
    return FX


def test_fxeod_writes_one_summary(fx, audit):
    out = fx.run({}, None)
    [row] = audit["rows"]
    assert _is_summary(row, "FXHistHandler", "FX_histdaily")
    assert (row["status"], row["n_rows"]) == ("ok", 4)    # 2 tickers x 2 days
    assert (row["date_lo"], row["date_hi"]) == (date(2026, 9, 23), date(2026, 9, 24))
    assert row["yf_version"] == fx.yf.__version__
    assert out["rows"] == 4


def test_fxeod_local_run_writes_csv_and_no_audit(fx, audit, monkeypatch, tmp_path):
    """localrun comes from the event now, not a module global only __main__ could set."""
    monkeypatch.chdir(tmp_path)
    fx.run({"localrun": True}, None)
    assert audit["rows"] == [] and audit["stored"] == []
    assert len(pd.read_csv(tmp_path / "USD_dailyFX.csv")) == 4


def test_fxeod_empty_result_is_audited_as_zero_rows(fx, audit, monkeypatch):
    monkeypatch.setattr(fx.yf, "download", lambda *a, **k: pd.DataFrame())
    out = fx.run({}, None)
    [row] = audit["rows"]
    assert (row["status"], row["n_rows"], out["rows"]) == ("ok", 0, 0)
    assert row["date_lo"] is None and row["date_hi"] is None


def test_fxeod_failure_is_audited_and_reraised(fx, audit, unset_env):
    unset_env("FX_TICKERS")                                  # ast.literal_eval(None) -> ValueError
    with pytest.raises(ValueError):
        fx.run({}, None)
    assert audit["rows"][0]["status"] == "error"


# --------------------------------------------------------------------------
# yfus30minEODv2 / yfasia30minEODv2  (intraday_min_handler)
# --------------------------------------------------------------------------
def _bars(now_utc, n=3):
    times = [now_utc - timedelta(minutes=15 * (n - i)) for i in range(n)]
    return pd.DataFrame({
        "Datetime": pd.DatetimeIndex(times).tz_convert("UTC"),
        "Open": 1.0, "High": 1.0, "Low": 1.0, "Close": 1.0, "Volume": 10, "AdjClose": 1.0,
    })


MARKET_CASES = [
    ("us", "yfus30minEOD", "AAPL", "NASDAQ", "America/New_York"),
    ("asia", "yfasia30minEOD", "0700.HK", "HK", "Asia/Hong_Kong"),
]


@pytest.fixture
def intraday(audit, monkeypatch):
    import intraday_min_handler as M

    monkeypatch.setattr(M, "load_blacklist", lambda path=None: [])
    return M


@pytest.mark.parametrize("market,job,sym,exch,tz", MARKET_CASES)
def test_intraday_writes_one_summary(intraday, audit, monkeypatch, market, job, sym, exch, tz):
    M = intraday
    now_utc = datetime.now(pytz.utc).replace(second=0, microsecond=0)
    local_mark = now_utc.astimezone(pytz.timezone(tz)).replace(tzinfo=None) - timedelta(hours=2)

    monkeypatch.setattr(DU, "load_df_SQL", lambda sql: pd.DataFrame({"Symbol": [sym]}))
    monkeypatch.setattr(DU, "load_symbols_dict", lambda: {sym: exch})
    monkeypatch.setattr(DU, "load_exchange_tz", lambda: {exch: tz})
    monkeypatch.setattr(DU, "get_Max_datetime", lambda tbl, s=None: local_mark)
    monkeypatch.setattr(M, "yf_download", lambda s, a, b, InitialRun=False: _bars(now_utc))

    out = M.run({"dbFlag": True}, None, market=market)

    [row] = audit["rows"]
    assert _is_summary(row, job, "histminprice")
    assert (row["status"], row["n_rows"]) == ("ok", 3) and out["rows"] == 3
    assert out["job"] == job
    # bars are stored in exchange-local time, so the dates are local dates
    assert row["date_hi"] == (now_utc - timedelta(minutes=15)).astimezone(pytz.timezone(tz)).date()
    assert row["yf_version"] == M.yf.__version__
    assert len(audit["stored"]) == 1


def test_intraday_entry_points_pick_the_market(intraday, monkeypatch):
    seen = []
    monkeypatch.setattr(intraday, "run", lambda e, c, market="us": seen.append(market))
    intraday.run_us({}, None)
    intraday.run_asia({}, None)
    assert seen == ["us", "asia"]


@pytest.mark.parametrize("market,_job,_sym,_exch,_tz", MARKET_CASES)
def test_intraday_queries_its_own_procedure(intraday, monkeypatch, market, _job, _sym, _exch, _tz):
    seen = []
    monkeypatch.setattr(DU, "load_df_SQL", lambda sql: seen.append(sql) or pd.DataFrame())
    intraday.load_market_symbols(market)
    assert seen == [f"call {intraday.MARKETS[market]['proc']};"]


def test_intraday_skips_symbol_with_unmapped_exchange(intraday, audit, monkeypatch):
    """Regression: the originals raised KeyError and lost the rest of the run."""
    M = intraday
    now_utc = datetime.now(pytz.utc).replace(second=0, microsecond=0)
    local_mark = now_utc.astimezone(pytz.timezone("America/New_York")).replace(tzinfo=None) - timedelta(hours=2)

    monkeypatch.setattr(DU, "load_df_SQL", lambda sql: pd.DataFrame({"Symbol": ["AAPL", "XYZ.ZZ"]}))
    monkeypatch.setattr(DU, "load_symbols_dict", lambda: {"AAPL": "NASDAQ"})
    monkeypatch.setattr(DU, "load_exchange_tz", lambda: {"NASDAQ": "America/New_York"})
    monkeypatch.setattr(DU, "get_Max_datetime", lambda tbl, s=None: local_mark)
    downloaded = []
    monkeypatch.setattr(M, "yf_download",
                        lambda s, a, b, InitialRun=False: downloaded.append(s) or _bars(now_utc))

    out = M.run({"dbFlag": True}, None, market="us")

    assert downloaded == ["AAPL"]                 # ZZ has no time zone, so it is skipped
    assert out["rows"] == 3 and audit["rows"][0]["status"] == "ok"


def test_intraday_reshape_bars_is_local_naive_with_utc_kept(intraday):
    tz = "Asia/Hong_Kong"
    now_utc = pd.Timestamp("2026-09-25 06:00", tz="UTC")
    raw = _bars(now_utc, n=2)
    lo = (now_utc - timedelta(hours=3)).tz_convert(tz)
    hi = (now_utc + timedelta(minutes=1)).tz_convert(tz)

    out = intraday.reshape_bars(raw, "0700.HK", "HK", tz, lo, hi)

    assert list(out.columns) == intraday.SAVE_COLUMNS
    assert out["Datetime"].dt.tz is None
    assert str(out["UTCDatetime"].dt.tz) == "UTC"
    assert (out["timezone"] == tz).all() and (out["Exchange"] == "HK").all()
    # the last bar is 05:45 UTC (_bars ends one interval before now), 13:45 in Hong Kong
    assert (out["Datetime"].iloc[-1].hour, out["Datetime"].iloc[-1].minute) == (13, 45)


def test_intraday_reshape_bars_drops_rows_outside_the_window(intraday):
    tz = "America/New_York"
    now_utc = pd.Timestamp("2026-09-25 19:00", tz="UTC")
    raw = _bars(now_utc, n=4)
    lo = (now_utc - timedelta(minutes=31)).tz_convert(tz)      # keeps the last two bars
    hi = (now_utc + timedelta(minutes=1)).tz_convert(tz)
    assert len(intraday.reshape_bars(raw, "AAPL", "NASDAQ", tz, lo, hi)) == 2


def test_intraday_dry_run_writes_no_audit(intraday, audit, monkeypatch):
    monkeypatch.setattr(DU, "load_df_SQL", lambda sql: pd.DataFrame())
    monkeypatch.setattr(DU, "get_Max_datetime", lambda tbl, s=None: None)
    intraday.run({"dbFlag": False}, None, market="us")
    assert audit["rows"] == []


def test_intraday_failure_is_audited_and_reraised(intraday, audit, monkeypatch):
    monkeypatch.setattr(DU, "get_Max_datetime", lambda tbl, s=None: None)
    monkeypatch.setattr(intraday, "load_market_symbols",
                        lambda market: (_ for _ in ()).throw(KeyError("Symbol")))
    with pytest.raises(KeyError):
        intraday.run({"dbFlag": True}, None, market="us")
    assert audit["rows"][0]["status"] == "error"


def test_intraday_blacklist_is_read_from_the_packaged_folder(monkeypatch):
    """load_blacklist() must not depend on the CWD (it did in the originals)."""
    import intraday_min_handler as M

    monkeypatch.chdir("/")
    assert "Symbol" not in M.load_blacklist()          # returns symbols, not the header


# --------------------------------------------------------------------------
# Every handler's _output_dir resolves to /tmp on Lambda
#
# One test per handler rather than one for DU.out_dir alone: the 2026-10-01
# failure was six independent copies of the same wrong condition, so what needs
# guarding is that each handler still delegates (doc/OPERATIONS.md 8.6).
# --------------------------------------------------------------------------
@pytest.mark.parametrize("module_name", [
    "eod_daily_handler", "fxeod_handler", "optchain_eod_handler",
    "usrate_handler", "port_assets_handler",
])
def test_handler_output_dir_is_tmp_on_lambda(monkeypatch, module_name):
    import importlib
    mod = importlib.import_module(module_name)
    monkeypatch.setenv("AWS_LAMBDA_FUNCTION_NAME", f"fin-deep-data-dev-{module_name}")
    monkeypatch.setenv("PORT_OUTPUT_DIR", "/tmp")
    assert mod._output_dir(True).startswith("/tmp"), \
        f"{module_name}._output_dir(localrun=True) must not return a read-only path on Lambda"
    assert mod._output_dir(False).startswith("/tmp")


@pytest.mark.parametrize("module_name", [
    "eod_daily_handler", "fxeod_handler", "optchain_eod_handler",
    "usrate_handler", "port_assets_handler",
])
def test_handler_output_dir_is_cwd_locally(monkeypatch, module_name):
    import importlib
    mod = importlib.import_module(module_name)
    monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)
    monkeypatch.delenv("PORT_OUTPUT_DIR", raising=False)
    assert mod._output_dir(True) == "."
