"""Unit tests for ``optchain_eod_handler`` (optChainEOD, PLAN-SR-UPSTREAM Phase C).

python3.13 only (``venv-py313``). ``yf.Ticker`` is replaced by a fake whose
chain frames carry yfinance 0.2.58's real column set and dtypes (read off a
live ``Ticker("SPY").option_chain()`` on 2026-09-25), so no test touches the
network, MySQL, R2 or the Lambda API.
"""

import json
from datetime import date, datetime
from types import SimpleNamespace

import pandas as pd
import pytest

import dataUtil as DU
import optchain_eod_handler as H

pytestmark = pytest.mark.unit


def _side(kind, strikes, last, oi, itm):
    n = len(strikes)
    return pd.DataFrame({
        "contractSymbol": [f"X{kind[0].upper()}{s}" for s in strikes],
        "lastTradeDate": pd.to_datetime(["2026-09-25 19:16:11"] * n).tz_localize("UTC"),
        "strike": [float(s) for s in strikes],
        "lastPrice": last, "bid": [1.0] * n, "ask": [1.1] * n, "change": [0.1] * n,
        "percentChange": [1.0] * n, "volume": [5.0] * n, "openInterest": oi,
        "impliedVolatility": [0.3] * n, "inTheMoney": itm,
        "contractSize": ["REGULAR"] * n, "currency": ["USD"] * n,
    })


class FakeTicker:
    """Two expiries; a known mix of rows the filter must keep and drop."""

    fail_times = 0          # raise this many times before succeeding
    expiries = ("2026-10-02", "2026-10-16")
    calls = 0

    def __init__(self, ticker):
        self.ticker = ticker

    def history(self, period):
        FakeTicker.calls += 1
        if FakeTicker.calls <= FakeTicker.fail_times:
            raise ConnectionError("Too Many Requests")
        return pd.DataFrame({"Close": [99.0, 100.5]})

    @property
    def options(self):
        return FakeTicker.expiries

    def option_chain(self, expiration):
        calls = _side("call", [90, 100, 110], [10.0, 0.05, 0.5], [50, 500, 1], [True, False, False])
        puts = _side("put", [90, 100, 110], [0.04, 2.0, 9.0], [300, 200, 100], [False, True, True])
        return SimpleNamespace(calls=calls, puts=puts)


@pytest.fixture
def fake_yf(monkeypatch):
    FakeTicker.calls = 0
    FakeTicker.fail_times = 0
    FakeTicker.expiries = ("2026-10-02", "2026-10-16")
    monkeypatch.setattr(H.yf, "Ticker", FakeTicker)
    return FakeTicker


@pytest.fixture
def opt_env(env, monkeypatch):
    for k, v in {"OPT_WRITE_TBL": "OptionChains_shadow", "TBLLOADAUDIT": "load_audit",
                 "SYMBOL_PROC_VER": "V4", "OPT_SHARDS": "3",
                 "UPSTREAM_R2_BUCKET": "fin-upstream", "OPT_RAW_PREFIX": "raw/optchain"}.items():
        monkeypatch.setenv(k, v)
    return env


NOSLEEP = lambda s: None


# --------------------------------------------------------------------------
# O1 option_chains -- ported retry loop
# --------------------------------------------------------------------------
def test_option_chains_shape(fake_yf):
    raw, err = H.option_chains("SPY", sleep=NOSLEEP)
    assert err is None
    assert len(raw) == 12                                   # 2 expiries x (3 calls + 3 puts)
    assert set(raw.OptionType) == {"call", "put"}
    assert (raw.UnderlyingSymbol == "SPY").all()
    assert (raw.UnderlyingPrice == 100.5).all()             # last history() close
    assert sorted(raw.Expiration.unique()) == list(pd.to_datetime(["2026-10-02", "2026-10-16"]))


def test_option_chains_retries_then_succeeds(fake_yf):
    fake_yf.fail_times = 1
    slept = []
    raw, err = H.option_chains("SPY", sleep=slept.append)
    assert len(raw) == 12 and err is None
    assert slept == [5]


def test_option_chains_gives_up_after_max_retries(fake_yf):
    '''max_retries is 2 (was 5): one retry, then the sweeps are the real retry.'''
    fake_yf.fail_times = 99
    slept = []
    raw, err = H.option_chains("SPY", sleep=slept.append)
    assert len(raw) == 0 and "Too Many Requests" in err
    assert H.max_retries == 2
    assert fake_yf.calls == 2 and slept == [5]      # no sleep after the last try


def test_option_chains_no_expiries_is_empty_not_error(fake_yf):
    fake_yf.expiries = ()
    raw, err = H.option_chains("0700.HK", sleep=NOSLEEP)
    assert len(raw) == 0 and err is None


# --------------------------------------------------------------------------
# O2 filter_opt_chain / process_chain
# --------------------------------------------------------------------------
def test_filter_opt_chain_edges(fake_yf):
    raw, _ = H.option_chains("SPY", sleep=NOSLEEP)
    # OI quantile(0.25) over the WHOLE chain, before the price filter:
    # OI [1,1,50,50,100,100,200,200,300,300,500,500] -> linear q25 = 50
    assert raw.openInterest.quantile(0.25) == 50
    puts, calls = H.filter_opt_chain(raw)
    # calls: 90 (10.0, OI 50) fails OI > 50 (strict); 100 (0.05) fails lastPrice > 0.05 (strict); 110 (OI 1) fails OI
    assert len(calls) == 0
    # puts: 90 (0.04) fails price; 100 (2.0, OI 200) and 110 (9.0, OI 100) pass
    assert sorted(puts.strike.unique()) == [100.0, 110.0] and len(puts) == 4


def test_process_chain_columns_and_dtypes(fake_yf):
    raw, _ = H.option_chains("SPY", sleep=NOSLEEP)
    out = H.process_chain(raw, date(2026, 9, 25))
    assert list(out.columns) == H.N_COLUMNS
    assert (out.Section == "PM").all() and (out.Date == date(2026, 9, 25)).all()
    assert (out.contractSize == 100).all()                  # forced, replaces 'REGULAR'
    assert out.inTheMoney.dtype == bool
    assert list(out.Expiration) == sorted(out.Expiration)   # sorted by Expiration, OptionType


def test_process_chain_empty_inputs():
    assert list(H.process_chain(pd.DataFrame(), date(2026, 9, 25)).columns) == H.N_COLUMNS
    assert list(H.process_chain(None, date(2026, 9, 25)).columns) == H.N_COLUMNS


def test_process_chain_all_filtered(fake_yf):
    raw, _ = H.option_chains("SPY", sleep=NOSLEEP)
    raw["lastPrice"] = 0.01
    assert len(H.process_chain(raw, date(2026, 9, 25))) == 0


# --------------------------------------------------------------------------
# O3 pure helpers
# --------------------------------------------------------------------------
@pytest.mark.parametrize("hour,expected", [(8, date(2026, 9, 24)), (17, date(2026, 9, 25))])
def test_process_date(hour, expected):
    assert H.process_date(datetime(2026, 9, 25, hour, 40)) == expected


def test_raw_key():
    assert H.raw_key("raw/optchain/", date(2026, 9, 25), "SPY") == "raw/optchain/2026-09-25/SPY-PM.csv"


def test_dispatch_events():
    ev = H.dispatch_events(3, date(2026, 9, 25))
    assert ev == [{"shard": i, "of": 3, "asof": "2026-09-25"} for i in range(3)]


@pytest.mark.parametrize("secs,n", [(0, 1), (540, 1), (541, 2), (2300, 5)])
def test_shards_needed(secs, n):
    assert H.shards_needed(secs) == n


@pytest.mark.parametrize("raw_n,saved_n,err,wf,expected", [
    (10, 5, None, False, "ok"),
    (10, 0, None, False, "empty"),      # all filtered out
    (0, 0, None, False, "empty"),       # no listed options
    (0, 0, "HTTPError", False, "error"),
    (10, 5, None, True, "error"),
])
def test_classify(raw_n, saved_n, err, wf, expected):
    status, _ = H.classify(pd.DataFrame(index=range(raw_n)), pd.DataFrame(index=range(saved_n)), err, wf)
    assert status == expected


# --------------------------------------------------------------------------
# O4 run()
# --------------------------------------------------------------------------
@pytest.fixture
def wired(opt_env, fake_yf, monkeypatch, tmp_path):
    calls = {"append": [], "audit": [], "r2": [], "invoke": [], "symbol_proc": []}
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(H.time, "sleep", NOSLEEP)
    monkeypatch.setattr(DU, "load_symbols_db", lambda ver, sym_type=None:
                        calls["symbol_proc"].append((ver, sym_type))
                        or ["SPY", "QQQ", "0700.HK"])
    monkeypatch.setattr(DU, "load_symbols_dict", lambda: {"SPY": "AMEX"})
    monkeypatch.setattr(DU, "append_ignore", lambda df, s, t, chunk=500: calls["append"].append((t, df.copy())) or len(df))
    monkeypatch.setattr(DU, "audit_run", lambda rows: calls["audit"].extend(rows) or len(rows))
    monkeypatch.setattr(H, "r2_client", lambda: "client")
    monkeypatch.setattr(H, "put_raw", lambda c, b, k, f: calls["r2"].append((b, k, len(f))) or True)
    monkeypatch.setattr(H, "invoke_shards", lambda events, ctx: calls["invoke"].extend(events) or len(events))
    return calls


EV = {"asof": "2026-09-25"}


@pytest.mark.parametrize("missing", ["OPT_WRITE_TBL", "TBLLOADAUDIT"])
def test_run_refuses_without_required_env(wired, unset_env, missing):
    unset_env(missing)
    with pytest.raises(RuntimeError, match=missing):
        H.run(dict(EV), None)
    assert wired["append"] == []


def test_run_writes_per_underlying_then_audits(wired):
    out = H.run(dict(EV), None)
    assert [t for t, _ in wired["append"]] == ["OptionChains_shadow"] * 3
    sym_rows = [r for r in wired["audit"] if r["Symbol"] != "*"]
    assert [r["Symbol"] for r in sym_rows] == ["0700.HK", "QQQ", "SPY"]
    assert all(r["status"] == "ok" and r["date_hi"] == date(2026, 9, 25) for r in sym_rows)
    assert {r["Symbol"]: r["Exchange"] for r in sym_rows}["SPY"] == "AMEX"
    summary = [r for r in wired["audit"] if r["Symbol"] == "*"][0]
    assert (summary["n_ok"], summary["n_expected"], summary["n_rows"]) == (3, 3, 12)
    assert ("fin-upstream", "raw/optchain/2026-09-25/SPY-PM.csv", 12) in wired["r2"]
    json.dumps(out)


def test_run_asks_the_symbol_procedure_for_optionable_symbols_only(wired):
    """@type 'o': V5 drops the delisted and the non-optionable names (TODOS 2.13)."""
    H.run(dict(EV), None)
    assert wired["symbol_proc"] == [("V4", "o")]


def test_run_symbol_type_is_overridable_by_the_event(wired):
    H.run(dict(EV) | {"symType": "a"}, None)
    assert wired["symbol_proc"] == [("V4", "a")]


def test_run_event_symbols_skip_the_procedure(wired):
    H.run(dict(EV, symbols=["SPY"]), None)
    assert wired["symbol_proc"] == []


def test_run_empty_chain_is_audited_empty(wired, fake_yf):
    fake_yf.expiries = ()
    H.run(dict(EV, symbols=["0700.HK"]), None)
    assert wired["append"] == [] and wired["r2"] == []
    assert [r["status"] for r in wired["audit"]] == ["empty", "ok"]   # symbol row, summary row


def test_run_retries_exhausted_is_error(wired, fake_yf):
    fake_yf.fail_times = 99
    out = H.run(dict(EV, symbols=["SPY"]), None)
    assert out["n_error"] == 1 and wired["append"] == []
    assert "Too Many Requests" in wired["audit"][0]["error"]


def test_run_dry_run_writes_csvs_only(wired, tmp_path):
    out = H.run(dict(EV, dbFlag=False, localrun=True), None)
    assert wired["append"] == [] and wired["audit"] == [] and wired["r2"] == []
    saved = pd.read_csv(tmp_path / "options_eod_2026-09-25.csv")
    assert list(saved.columns) == H.N_COLUMNS and len(saved) == 12
    assert (tmp_path / "OptionsChain" / "SPY_2026-09-25-PM.csv").exists()
    timing = pd.read_csv(tmp_path / "optchain_timing_2026-09-25.csv")
    assert list(timing.columns) == ["Symbol", "seconds", "n_raw", "n_rows", "status"]
    assert out["opt_shards_needed"] == 1


def test_run_time_guard_marks_skipped(wired):
    ctx = SimpleNamespace(get_remaining_time_in_millis=lambda: 1_000)
    out = H.run(dict(EV), ctx)
    assert out["n_skipped"] == 3 and wired["append"] == []


def test_run_shard_slice(wired):
    H.run(dict(EV, shard=0, of=3), None)
    assert [r["Symbol"] for r in wired["audit"] if r["Symbol"] != "*"] == ["0700.HK"]


def test_run_sweep_takes_only_missing(wired, monkeypatch):
    seen = {}
    monkeypatch.setattr(DU, "missing_for_sweep",
                        lambda job, d, exp, t: seen.update(job=job, d=d, exp=exp, t=t) or ["QQQ"])
    out = H.run(dict(EV, sweep=True), None)
    assert seen == {"job": "optChainEOD", "d": date(2026, 9, 25),
                    "exp": ["0700.HK", "QQQ", "SPY"], "t": "OptionChains_shadow"}
    assert [r["Symbol"] for r in wired["audit"] if r["Symbol"] != "*"] == ["QQQ"]
    assert out["mode"] == "sweep"


def test_run_dispatch_emits_n_shard_events(wired):
    out = H.run(dict(EV, dispatch=True), None)
    assert wired["invoke"] == [{"shard": i, "of": 3, "asof": "2026-09-25"} for i in range(3)]
    assert out["started"] == 3 and wired["append"] == [] and wired["audit"] == []


def test_run_dispatch_dry_run_invokes_nothing(wired):
    out = H.run(dict(EV, dispatch=True, dbFlag=False), None)
    assert wired["invoke"] == [] and out["shards"] == 3


def test_run_without_bucket_still_writes_table(wired, unset_env):
    unset_env("UPSTREAM_R2_BUCKET")
    H.run(dict(EV, symbols=["SPY"]), None)
    assert wired["r2"] == [] and len(wired["append"]) == 1


def test_run_date_rule_from_clock(wired):
    import pytz
    ny = pytz.timezone("America/New_York").localize(datetime(2026, 9, 26, 8, 0))
    assert H.run({"NYTIME": ny, "symbols": ["SPY"]}, None)["asof"] == "2026-09-25"


def test_put_raw_swallows_errors():
    class Boom:
        def put_object(self, **kw):
            raise OSError("r2 down")
    assert H.put_raw(Boom(), "b", "k", pd.DataFrame({"a": [1]})) is False


def test_invoke_shards_uses_async_event_invocation(monkeypatch):
    import boto3
    sent = []

    class FakeLambda:
        def invoke(self, **kw):
            sent.append(kw)

    monkeypatch.setattr(boto3, "client", lambda name: FakeLambda())
    ctx = SimpleNamespace(invoked_function_arn="arn:aws:lambda:us-east-2:1:function:optChainEOD")
    assert H.invoke_shards(H.dispatch_events(2, date(2026, 9, 25)), ctx) == 2
    assert all(k["InvocationType"] == "Event" and k["FunctionName"] == ctx.invoked_function_arn for k in sent)
    assert json.loads(sent[1]["Payload"]) == {"shard": 1, "of": 2, "asof": "2026-09-25"}
