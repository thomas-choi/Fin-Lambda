"""The three snapshot ports of 2026-10-11 (TODOS 5.10).

Covers ``fx_snapshot_handler`` (FXrateHandlerv2), ``opt_snapshot_handler``
(optHandlerv2) and ``stock_snapshot_handler`` (cronHandlerv2).

python3.13 only. yfinance, the stored procedures and every write are patched,
so nothing here touches Yahoo or MySQL.
"""

from datetime import date, datetime

import numpy as np
import pandas as pd
import pytest
import pytz

import dataUtil as DU

pytestmark = pytest.mark.unit

NY = pytz.timezone("US/Eastern")
NYTIME = NY.localize(datetime(2026, 10, 9, 14, 30, 5))


@pytest.fixture
def writes(env, monkeypatch):
    """Capture snapshot writes and audit rows instead of performing them."""
    out = {"snap": [], "audit": [], "store": []}
    monkeypatch.setattr(DU, "replace_snapshot",
                        lambda df, schema, table: out["snap"].append((df.copy(), schema, table)) or len(df))
    monkeypatch.setattr(DU, "audit_run", lambda rows: out["audit"].extend(rows) or len(rows))
    monkeypatch.setattr(DU, "StoreEOD", lambda *a: out["store"].append(a))
    return out


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


def _summary(rows, job):
    assert len(rows) == 1, rows
    row = rows[0]
    assert (row["job"], row["Symbol"], row["segment"]) == (job, "*", "summary")
    return row


# --------------------------------------------------------------------------
# FXrateHandlerv2
# --------------------------------------------------------------------------
def _fx_close():
    idx = pd.DatetimeIndex(["2026-10-09 18:00", "2026-10-09 18:05", "2026-10-09 18:10"], tz="UTC")
    return pd.DataFrame({"EUR=X": [0.90, 0.91, np.nan],     # last bar NaN -> 0.91 at 18:05
                         "HKD=X": [7.80, 7.81, 7.82],
                         "JPY=X": [np.nan, np.nan, np.nan]},  # nothing to fill -> dropped
                        index=idx)


def test_fx_latest_rates_ffill_base_row_and_dropped_nan():
    import fx_snapshot_handler as H

    st = datetime(2026, 10, 9, 14, 10)
    with _no_warning():
        df = H.latest_rates(_fx_close(), "USD", st)
    assert list(df.columns) == H.FX_SNAP_COLUMNS
    rates = dict(zip(df.target_cur, df.rate))
    assert rates == {"EUR": 0.91, "HKD": 7.82, "USD": 1.0}
    # each pair's own last bar, tz-naive UTC; the base row takes the newest
    when = dict(zip(df.target_cur, df.last_updated))
    assert when == {"EUR": pd.Timestamp("2026-10-09 18:05"),
                    "HKD": pd.Timestamp("2026-10-09 18:10"),
                    "USD": pd.Timestamp("2026-10-09 18:10")}
    assert df.last_updated.dt.tz is None
    assert (df.server_time == st).all()


def test_fx_latest_rates_keeps_pair_quiet_since_london_midnight():
    """2026-10-12 23:50 UTC: CNY's last bar was 22:35, before Yahoo's FX day rolled."""
    import fx_snapshot_handler as H

    idx = pd.DatetimeIndex(["2026-10-12 22:35", "2026-10-12 23:45", "2026-10-12 23:50"], tz="UTC")
    close = pd.DataFrame({"CNY=X": [7.12, np.nan, np.nan], "EUR=X": [0.86, 0.87, 0.88]}, index=idx)
    df = H.latest_rates(close, "USD", datetime(2026, 10, 12, 19, 50))
    cny = df[df.target_cur == "CNY"].iloc[0]
    assert (cny.rate, cny.last_updated) == (7.12, pd.Timestamp("2026-10-12 22:35"))
    assert H.FX_PERIOD != "1d"


def test_fx_latest_rates_empty_download_gives_empty_frame():
    import fx_snapshot_handler as H

    df = H.latest_rates(pd.DataFrame(), "USD", datetime(2026, 10, 9))
    assert len(df) == 0 and list(df.columns) == H.FX_SNAP_COLUMNS


def test_fx_run_writes_snapshot_and_audit(writes, monkeypatch):
    import fx_snapshot_handler as H

    monkeypatch.setattr(H, "fetch_close", lambda t: _fx_close())
    result = H.run({"NYTIME": NYTIME}, None)
    assert result["rows"] == 3
    (df, schema, table), = writes["snap"]
    assert (schema, table) == ("GlobalMarketData", "FX_snapshot")
    assert (df.server_time == datetime(2026, 10, 9, 14, 30, 5)).all()
    row = _summary(writes["audit"], "FXrateHandler")
    assert (row["status"], row["n_rows"], row["table_name"]) == ("ok", 3, "FX_snapshot")


def test_fx_dry_run_writes_csv_only(writes, monkeypatch, tmp_path):
    import fx_snapshot_handler as H

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(H, "fetch_close", lambda t: _fx_close())
    H.run({"NYTIME": NYTIME, "localrun": True, "dbFlag": False}, None)
    assert writes["snap"] == [] and writes["audit"] == []
    assert len(pd.read_csv(tmp_path / "USD_FX.csv")) == 3


def test_fx_empty_download_audits_empty(writes, monkeypatch):
    import fx_snapshot_handler as H

    monkeypatch.setattr(H, "fetch_close", lambda t: pd.DataFrame())
    H.run({"NYTIME": NYTIME}, None)
    assert writes["snap"][0][0].empty            # replace_snapshot keeps the old rows
    assert _summary(writes["audit"], "FXrateHandler")["status"] == "empty"


def test_fx_error_is_audited_and_reraised(writes, monkeypatch):
    import fx_snapshot_handler as H

    monkeypatch.setattr(H, "fetch_close", lambda t: (_ for _ in ()).throw(RuntimeError("yahoo down")))
    with pytest.raises(RuntimeError):
        H.run({"NYTIME": NYTIME}, None)
    row = _summary(writes["audit"], "FXrateHandler")
    assert row["status"] == "error" and "yahoo down" in row["error"]


@pytest.mark.parametrize("missing", ["TBLFXSNAPSHOT", "FX_TICKERS"])
def test_fx_missing_env_raises_before_any_download(writes, monkeypatch, unset_env, missing):
    import fx_snapshot_handler as H

    unset_env(missing)
    called = []
    monkeypatch.setattr(H, "fetch_close", lambda t: called.append(t))
    with pytest.raises(RuntimeError, match=missing):
        H.run({"NYTIME": NYTIME}, None)
    assert called == [] and writes["snap"] == []


def test_fx_tickers_rejects_non_list(monkeypatch):
    import fx_snapshot_handler as H

    monkeypatch.setenv("FX_TICKERS", "'EUR=X'")
    with pytest.raises(ValueError):
        H.fx_tickers()


# --------------------------------------------------------------------------
# optHandlerv2
# --------------------------------------------------------------------------
def _etf_df():
    return pd.DataFrame({
        "Symbol": ["HYG", "HYG", "LQD", "OLD"],
        "PnC": ["P", "P", "P", "C"],
        "H_Strike": [78.0, 78.0, np.nan, 10.0],          # dup, no strike, expired
        "Expiration": [date(2026, 12, 18), date(2026, 12, 18), date(2026, 12, 18), date(2026, 1, 16)],
    })


def _stock_df():
    return pd.DataFrame({
        "Symbol": ["AVGO", "AVGO", "AVGO", "HYG"],
        "PnC": ["P", "C", "P", "P"],
        "Strike": [350.0, 400.0, 340.0, 78.0],            # last one dups the ETF row
        "Expiration": [date(2026, 10, 16), date(2026, 10, 16), date(2026, 11, 20), date(2026, 12, 18)],
    })


def test_contract_list_drops_expired_nostrike_and_duplicates():
    import opt_snapshot_handler as H

    df = H.contract_list(_etf_df(), _stock_df(), date(2026, 10, 9))
    assert list(df.columns) == H.CONTRACT_COLUMNS
    assert df.values.tolist() == [
        ["HYG", "P", 78.0, "2026-12-18"],
        ["AVGO", "P", 350.0, "2026-10-16"],
        ["AVGO", "C", 400.0, "2026-10-16"],
        ["AVGO", "P", 340.0, "2026-11-20"],
    ]


def test_contract_list_keeps_contract_expiring_today():
    import opt_snapshot_handler as H

    df = H.contract_list(_etf_df(), _stock_df(), date(2026, 10, 16))
    assert ["AVGO", "P", 350.0, "2026-10-16"] in df.values.tolist()


def _chain_side(strikes, itm=False):
    return pd.DataFrame({"strike": strikes, "lastPrice": [1.5] * len(strikes),
                         "bid": [1.4] * len(strikes), "ask": [1.6] * len(strikes),
                         "change": [0.1] * len(strikes), "percentChange": [7.1] * len(strikes),
                         "volume": [10] * len(strikes), "openInterest": [100] * len(strikes),
                         "impliedVolatility": [0.3] * len(strikes), "inTheMoney": [itm] * len(strikes),
                         "contractSize": ["REGULAR"] * len(strikes), "currency": ["USD"] * len(strikes)})


class _FakeTicker:
    calls = []

    def __init__(self, sym):
        self.ticker = sym

    def history(self, period):
        _FakeTicker.calls.append(("history", self.ticker))
        return pd.DataFrame({"Close": [100.0, 361.537]})

    def option_chain(self, expiration):
        _FakeTicker.calls.append(("chain", self.ticker, expiration))
        from types import SimpleNamespace
        return SimpleNamespace(puts=_chain_side([340.0, 350.0, 78.0], itm=True),
                               calls=_chain_side([400.0]))


@pytest.fixture
def fake_ticker():
    _FakeTicker.calls = []
    return _FakeTicker


def test_snapshot_contracts_one_chain_per_symbol_expiration(fake_ticker):
    import opt_snapshot_handler as H

    contracts = H.contract_list(_etf_df(), _stock_df(), date(2026, 10, 9))
    with _no_warning():
        snap, missing = H.snapshot_contracts(contracts, "stamp", ticker_factory=fake_ticker)
    assert missing == 0 and len(snap) == 4
    assert sorted(c for c in fake_ticker.calls if c[0] == "chain") == [
        ("chain", "AVGO", "2026-10-16"), ("chain", "AVGO", "2026-11-20"), ("chain", "HYG", "2026-12-18")]
    assert sorted(c for c in fake_ticker.calls if c[0] == "history") == [
        ("history", "AVGO"), ("history", "HYG")]


def test_snapshot_frame_columns_and_dtypes(fake_ticker):
    import opt_snapshot_handler as H

    contracts = H.contract_list(_etf_df(), _stock_df(), date(2026, 10, 9))
    snap, _ = H.snapshot_contracts(contracts, "2026-10-09 14:30:05", ticker_factory=fake_ticker)
    assert list(snap.columns) == H.SNAP_COLUMNS
    for col in H.FLOAT_COLUMNS:
        assert snap[col].dtype == float, col
    row = snap[(snap.Symbol == "AVGO") & (snap.PnC == "P") & (snap.Strike == 350.0)].iloc[0]
    assert (row.PClose, row.inTheMoney, row.Expiration) == (361.54, "1", date(2026, 10, 16))
    assert snap[snap.PnC == "C"].inTheMoney.tolist() == ["0"]


def test_snapshot_missing_strike_and_failed_chain_are_counted(fake_ticker, monkeypatch):
    import opt_snapshot_handler as H

    contracts = pd.DataFrame([["AVGO", "P", 999.0, "2026-10-16"], ["BAD", "P", 1.0, "2026-10-16"]],
                             columns=H.CONTRACT_COLUMNS)

    class _Bad(fake_ticker):
        def option_chain(self, expiration):
            if self.ticker == "BAD":
                raise ValueError("no expirations")
            return super().option_chain(expiration)

    snap, missing = H.snapshot_contracts(contracts, "s", ticker_factory=_Bad)
    assert len(snap) == 0 and missing == 2


def test_underlying_pclose_sentinel_on_failure():
    import opt_snapshot_handler as H

    class _T:
        ticker = "X"

        def history(self, period):
            raise RuntimeError("boom")

    assert H.underlying_pclose(_T()) == H.NO_PCLOSE


def _opt_procs(monkeypatch):
    monkeypatch.setattr(DU, "load_df_SQL", lambda sql: _etf_df() if "etf" in sql else _stock_df())


def test_opt_run_writes_snapshot_and_partial_audit(writes, monkeypatch, fake_ticker):
    import opt_snapshot_handler as H

    _opt_procs(monkeypatch)
    real = H.snapshot_contracts
    monkeypatch.setattr(H, "snapshot_contracts",
                        lambda c, s, context=None: real(c, s, ticker_factory=fake_ticker, context=context))
    result = H.run({"NYTIME": NYTIME}, None)
    assert result["contracts"] == 4 and result["rows"] == 4
    (df, schema, table), = writes["snap"]
    assert (schema, table) == ("GlobalMarketData", "options_snapshot")
    assert (df.timestamp == "2026-10-09 14:30:05").all()
    row = _summary(writes["audit"], "optHandler")
    assert (row["status"], row["n_ok"], row["n_expected"]) == ("ok", 4, 4)


def test_opt_run_failed_procedure_is_an_error(writes, monkeypatch):
    """load_df_SQL returns None on failure -- not an empty trade list."""
    import opt_snapshot_handler as H

    monkeypatch.setattr(DU, "load_df_SQL", lambda sql: None)
    with pytest.raises(RuntimeError, match="procedure failed"):
        H.run({"NYTIME": NYTIME}, None)
    assert writes["snap"] == []
    assert _summary(writes["audit"], "optHandler")["status"] == "error"


def test_opt_dry_run_writes_csvs_only(writes, monkeypatch, fake_ticker, tmp_path):
    import opt_snapshot_handler as H

    monkeypatch.chdir(tmp_path)
    _opt_procs(monkeypatch)
    real = H.snapshot_contracts
    monkeypatch.setattr(H, "snapshot_contracts",
                        lambda c, s, context=None: real(c, s, ticker_factory=fake_ticker, context=context))
    H.run({"NYTIME": NYTIME, "localrun": True, "dbFlag": False, "test": 2}, None)
    assert writes["snap"] == [] and writes["audit"] == []
    assert len(pd.read_csv(tmp_path / "options_list.csv")) == 2       # test=2 caps it
    assert len(pd.read_csv(tmp_path / "options_snapshot.csv")) == 2


def test_opt_missing_table_env_raises_before_procedures(writes, monkeypatch, unset_env):
    import opt_snapshot_handler as H

    unset_env("TBLOPTSNAPSHOT")
    called = []
    monkeypatch.setattr(DU, "load_df_SQL", lambda sql: called.append(sql))
    with pytest.raises(RuntimeError, match="TBLOPTSNAPSHOT"):
        H.run({"NYTIME": NYTIME}, None)
    assert called == []


# --------------------------------------------------------------------------
# cronHandlerv2
# --------------------------------------------------------------------------
def _info(price=10.0, **over):
    info = {"regularMarketOpen": 9.0, "regularMarketDayHigh": 11.0, "regularMarketDayLow": 8.5,
            "regularMarketPrice": price, "regularMarketVolume": 1000, "previousClose": 9.5,
            "bid": 9.9, "bidSize": 3, "ask": 10.1, "askSize": 4, "longName": "Acme Inc",
            "quoteType": "EQUITY"}
    info.update(over)
    return {k: v for k, v in info.items() if v is not ...}


def test_quote_fields_maps_info_and_defaults():
    import stock_snapshot_handler as H

    row = H.quote_fields("ACME", _info(regularMarketVolume=None, longName=...))
    assert row == {"open": 9.0, "high": 11.0, "low": 8.5, "last": 10.0, "volume": 0,
                   "bid": 9.9, "bidvol": 3, "ask": 10.1, "askvol": 4, "pclose": 9.5,
                   "quoteType": "EQUITY", "name": "ACME"}      # no longName key -> symbol


def test_quote_fields_falls_back_to_secondary_keys():
    import stock_snapshot_handler as H

    row = H.quote_fields("X", _info(regularMarketPrice=..., currentPrice=12.0,
                                    previousClose=..., regularMarketPreviousClose=11.0))
    assert (row["last"], row["pclose"]) == (12.0, 11.0)


def test_build_snapshot_drops_incomplete_rows_keeps_missing_quotetype():
    import stock_snapshot_handler as H

    quotes = {"A": H.quote_fields("A", _info()),
              "B": H.quote_fields("B", _info(bid=None)),            # dropped, as before
              "C": H.quote_fields("C", _info(quoteType=None))}       # kept: not a core column
    with _no_warning():
        df = H.build_snapshot(["A", "B", "C", "D"], quotes, "2026/10/09-14:30:05")
    assert list(df.columns) == H.SNAP_COLUMNS
    assert df.Symbol.tolist() == ["A", "C"]
    assert df.loc[1, "quoteType"] is None or pd.isna(df.loc[1, "quoteType"])
    for col in H.FLOAT_COLUMNS:
        assert df[col].dtype == float, col


class _InfoTicker:
    infos = {}

    def __init__(self, sym):
        self.sym = sym

    @property
    def info(self):
        value = _InfoTicker.infos[self.sym]
        if isinstance(value, Exception):
            raise value
        return value


def test_fetch_quotes_skips_failures(monkeypatch):
    import stock_snapshot_handler as H

    _InfoTicker.infos = {"A": _info(), "B": RuntimeError("404"), "C": _info(5.0)}
    quotes, skipped = H.fetch_quotes(["A", "B", "C"], 2, ticker_factory=_InfoTicker)
    assert sorted(quotes) == ["A", "C"] and skipped == 0
    assert quotes["C"]["last"] == 5.0


def test_fetch_quotes_time_guard_skips_without_calling(monkeypatch):
    import stock_snapshot_handler as H

    _InfoTicker.infos = {}                       # any call would KeyError
    monkeypatch.setattr(DU, "time_left_ok", lambda ctx, reserve_ms=0: False)
    quotes, skipped = H.fetch_quotes(["A", "B"], 2, context=object(), ticker_factory=_InfoTicker)
    assert quotes == {} and skipped == 2


def _stock_env(monkeypatch, symbols, infos):
    import stock_snapshot_handler as H

    monkeypatch.setattr(DU, "load_symbols_db", lambda ver, sym_type=None: list(symbols))
    _InfoTicker.infos = infos
    real = H.fetch_quotes
    monkeypatch.setattr(H, "fetch_quotes",
                        lambda s, t, c=None: real(s, t, c, ticker_factory=_InfoTicker))
    return H


def test_stock_run_writes_snapshot_and_audit(writes, monkeypatch):
    H = _stock_env(monkeypatch, ["A", "B"], {"A": _info(), "B": _info(bid=None)})
    result = H.run({"NYTIME": NYTIME}, None)
    assert result == {"run_id": result["run_id"], "rows": 1, "symbols": 2, "skipped": 0}
    (df, schema, table), = writes["snap"]
    assert (schema, table) == ("GlobalMarketData", "snapshot")
    assert df.timestamp.tolist() == ["2026/10/09-14:30:05"]
    row = _summary(writes["audit"], "cronHandler")
    assert (row["status"], row["n_rows"], row["n_expected"]) == ("ok", 1, 2)


def test_stock_run_uses_the_snapshot_proc_version(writes, monkeypatch):
    H = _stock_env(monkeypatch, ["A"], {"A": _info()})
    seen = []
    monkeypatch.setattr(DU, "load_symbols_db", lambda ver, sym_type=None: seen.append(ver) or ["A"])
    H.run({"NYTIME": NYTIME}, None)
    assert seen == ["V3"]


def test_stock_run_empty_symbol_list_is_an_error(writes, monkeypatch):
    H = _stock_env(monkeypatch, [], {})
    with pytest.raises(RuntimeError, match="returned nothing"):
        H.run({"NYTIME": NYTIME}, None)
    assert writes["snap"] == []
    assert _summary(writes["audit"], "cronHandler")["status"] == "error"


def test_stock_dry_run_csv_only_and_test_cap(writes, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    H = _stock_env(monkeypatch, ["A", "B", "C"], {"A": _info(), "B": _info()})
    H.run({"NYTIME": NYTIME, "localrun": True, "dbFlag": False, "test": 2}, None)
    assert writes["snap"] == [] and writes["audit"] == []
    assert pd.read_csv(tmp_path / "snapshot_yf.csv").Symbol.tolist() == ["A", "B"]


@pytest.mark.parametrize("missing", ["TBLSNAPSHOT", "SNAP_SYMBOL_PROC_VER"])
def test_stock_missing_env_raises_before_symbol_list(writes, monkeypatch, unset_env, missing):
    H = _stock_env(monkeypatch, ["A"], {"A": _info()})
    unset_env(missing)
    called = []
    monkeypatch.setattr(DU, "load_symbols_db", lambda ver, sym_type=None: called.append(ver))
    with pytest.raises(RuntimeError, match=missing):
        H.run({"NYTIME": NYTIME}, None)
    assert called == []
