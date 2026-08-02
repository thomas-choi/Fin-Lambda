"""Unit tests for ``port_assets_handler`` (portAssetsHandler).

All fixture-driven. No test touches the network, MySQL or S3 -- the autouse
``no_network`` fixture in ``conftest.py`` fails any that tries.

The saved payloads under ``tests/fixtures/`` are the real responses captured on
2026-08-01; the Wikipedia ones are trimmed to their ``<table>`` elements with
attributes stripped, which keeps the table-selection logic under test while
keeping the repo small.
"""

import json
from datetime import date, datetime

import pandas as pd
import pytest

import dataUtil as DU
import port_assets_handler as PAH

pytestmark = pytest.mark.unit


# --------------------------------------------------------------------------
# T1 -- S&P 500 Wikipedia parse
# --------------------------------------------------------------------------
def test_parse_sp500_wikipedia_shape(sp500_wikipedia_html):
    df = PAH.parse_sp500_wikipedia(sp500_wikipedia_html)

    assert len(df) == 503
    assert list(df.columns) == ["Symbol", "Sector"]
    assert df.Symbol.is_unique
    assert not df.Symbol.isna().any()
    assert not df.Sector.isna().any()


def test_parse_sp500_uses_gics_sector_not_sub_industry(sp500_wikipedia_html):
    """Sector must be the GICS *Sector*, the coarse taxonomy already in the table."""
    df = PAH.parse_sp500_wikipedia(sp500_wikipedia_html)
    sectors = set(df.Sector)

    assert "Information Technology" in sectors
    assert "Health Care" in sectors
    # 11 GICS sectors; sub-industries would give ~160 distinct values
    assert len(sectors) <= 15, f"looks like sub-industry leaked in: {len(sectors)} values"


def test_parse_sp500_emits_dashed_class_shares(sp500_wikipedia_html):
    """A2: symbols are stored in the dashed yfinance form, not Wikipedia's dotted form.

    Wikipedia publishes ``BRK.B``/``BF.B``; every price table in this database
    is yfinance-fed and uses ``BRK-B``. Storing dashed is what makes the join
    work without a per-symbol fixup.
    """
    df = PAH.parse_sp500_wikipedia(sp500_wikipedia_html)
    symbols = set(df.Symbol)

    assert "BRK-B" in symbols
    assert "BF-B" in symbols
    assert "BRK.B" not in symbols
    assert "BF.B" not in symbols
    assert not any("." in s for s in symbols), "a dotted symbol survived normalization"


def test_parse_sp500_picks_the_table_by_content_not_index(sp500_wikipedia_html):
    """The constituent table must be found by its columns, not by ``[0]``.

    The NASDAQ-100 list has already moved pages once; index-based selection is
    exactly the fragility that caused it.
    """
    tables = pd.read_html(__import__("io").BytesIO(sp500_wikipedia_html))
    shuffled = list(reversed(tables))
    picked = PAH._pick_table(shuffled, ["Symbol", "GICS Sector"])

    assert len(picked) == 503


def test_pick_table_raises_when_no_table_matches():
    empty = [pd.DataFrame({"Nope": [1]})]
    with pytest.raises(ValueError, match="structure has probably changed"):
        PAH._pick_table(empty, ["Symbol", "GICS Sector"])


# --------------------------------------------------------------------------
# T2 -- NASDAQ-100 official API parse
# --------------------------------------------------------------------------
def test_parse_ndx_nasdaq_api_shape(ndx100_nasdaq_json):
    df = PAH.parse_ndx_nasdaq_api(ndx100_nasdaq_json)

    assert len(df) == 103
    assert list(df.columns) == ["Symbol", "Sector"]
    assert "AAPL" in set(df.Symbol)
    assert df.Symbol.is_unique


def test_parse_ndx_nasdaq_api_follows_the_doubled_data_path(ndx100_nasdaq_json):
    """Rows live at ``data.data.rows``, not ``data.rows``.

    A payload shaped like the shallower path must raise rather than yield an
    empty set that would then trip the sanity gate for the wrong reason.
    """
    shallow = {"data": {"rows": ndx100_nasdaq_json["data"]["data"]["rows"]}}
    with pytest.raises(ValueError, match="data.data.rows"):
        PAH.parse_ndx_nasdaq_api(shallow)


def test_parse_ndx_nasdaq_api_accepts_raw_json_text(ndx100_nasdaq_json):
    df = PAH.parse_ndx_nasdaq_api(json.dumps(ndx100_nasdaq_json))
    assert len(df) == 103


def test_parse_ndx_nasdaq_asof(ndx100_nasdaq_json):
    """``data.date`` is 'Jul 30, 2026' and is used as the NDX effective date."""
    assert PAH.parse_ndx_nasdaq_asof(ndx100_nasdaq_json) == date(2026, 7, 30)


def test_parse_ndx_nasdaq_asof_returns_none_when_unparseable():
    assert PAH.parse_ndx_nasdaq_asof({"data": {"date": "not a date"}}) is None
    assert PAH.parse_ndx_nasdaq_asof({"data": {}}) is None


# --------------------------------------------------------------------------
# T3 -- NASDAQ-100 Wikipedia fallback
# --------------------------------------------------------------------------
def test_parse_ndx_wikipedia_shape(ndx100_wikipedia_html):
    df = PAH.parse_ndx_wikipedia(ndx100_wikipedia_html)

    assert len(df) == 103
    assert list(df.columns) == ["Symbol", "Sector"]
    assert "ADBE" in set(df.Symbol)


def test_parse_ndx_wikipedia_discards_icb_taxonomy(ndx100_wikipedia_html):
    """Membership only. The page's ICB sectors must never reach the output.

    Mixing ICB (this page) with GICS (the S&P page) in one column would make
    ``Sector`` incomparable across ``Port_name``.
    """
    df = PAH.parse_ndx_wikipedia(ndx100_wikipedia_html)

    assert set(df.Sector) == {PAH.NO_SECTOR}
    assert "Software" not in set(df.Sector)
    assert "Semiconductors" not in set(df.Sector)
    assert not any("ICB" in str(c) for c in df.columns)


# --------------------------------------------------------------------------
# T4 -- SPY holdings cross-check workbook
# --------------------------------------------------------------------------
def test_parse_spy_holdings_locates_header_and_asof(spy_holdings_xlsx):
    df, as_of = PAH.parse_spy_holdings(spy_holdings_xlsx)

    assert as_of == date(2026, 7, 30)
    assert list(df.columns) == ["Symbol"]
    assert 490 <= len(df) <= 520, f"got {len(df)} rows"
    assert "AAPL" in set(df.Symbol)


def test_parse_spy_holdings_drops_metadata_and_footer_rows(spy_holdings_xlsx):
    """The 3 leading metadata rows and the trailing legal footer must not appear."""
    df, _ = PAH.parse_spy_holdings(spy_holdings_xlsx)
    symbols = set(df.Symbol)

    assert "TICKER SYMBOL:" not in symbols
    assert "FUND NAME:" not in symbols
    assert not any(len(s) > 10 for s in symbols), "a footer paragraph leaked in"
    assert not any(" " in s for s in symbols)


def test_parse_spy_holdings_raises_when_header_missing():
    blank = pd.DataFrame({0: ["junk"], 1: ["junk"]})
    import io

    buf = io.BytesIO()
    blank.to_excel(buf, index=False, header=False)
    buf.seek(0)
    with pytest.raises(ValueError, match="no Name/Ticker header row"):
        PAH.parse_spy_holdings(buf.getvalue())


# --------------------------------------------------------------------------
# T5 -- NDX sector sentinel
# --------------------------------------------------------------------------
def test_ndx_sector_is_the_general_sentinel(ndx100_nasdaq_json):
    """Every NDX row carries the literal sentinel -- not None, NaN or ''.

    'General' is the convention DJI and HSI already use in this table for
    index membership with no sector.
    """
    df = PAH.parse_ndx_nasdaq_api(ndx100_nasdaq_json)

    assert set(df.Sector) == {"General"}
    assert PAH.NO_SECTOR == "General"
    assert not df.Sector.isna().any()
    assert not (df.Sector == "").any()
    assert df.Sector.map(lambda v: isinstance(v, str)).all()


def test_ndx_sector_sentinel_survives_a_future_api_that_supplies_sectors(
    ndx100_nasdaq_json,
):
    """Pinned deliberately: if Nasdaq starts returning sectors, they are still
    ignored. Their taxonomy is not GICS, and silently mixing it into the column
    would make ``Sector`` incomparable across ``Port_name``. Adopting it must be
    a conscious change, which this test forces.
    """
    payload = json.loads(json.dumps(ndx100_nasdaq_json))
    for row in payload["data"]["data"]["rows"]:
        row["sector"] = "Technology"

    df = PAH.parse_ndx_nasdaq_api(payload)
    assert set(df.Sector) == {"General"}


# --------------------------------------------------------------------------
# T6 -- build_frame
# --------------------------------------------------------------------------
@pytest.fixture
def members():
    return pd.DataFrame(
        {"Symbol": ["MSFT", "AAPL", "BRK-B"], "Sector": ["Tech", "Tech", "Financials"]}
    )


def test_build_frame_produces_the_ddl_columns_in_order(members):
    frame = PAH.build_frame(members, "SP500", date(2026, 8, 3), "Equity", "Stock")

    assert list(frame.columns) == [
        "Date", "Port_name", "Symbol", "Class", "Sector", "Type", "Currency", "Rate",
    ]
    assert list(frame.columns) == PAH.TABLE_COLUMNS


def test_build_frame_constants(members):
    frame = PAH.build_frame(members, "SP500", date(2026, 8, 3), "Equity", "Stock")

    assert (frame.Class == "Equity").all()
    assert (frame.Type == "Stock").all()
    assert (frame.Currency == "USD").all()
    assert (frame.Rate == 1.0).all()
    assert (frame.Port_name == "SP500").all()


def test_build_frame_date_is_a_date_not_a_timestamp(members):
    """The column is DATE in MySQL; a Timestamp would round-trip a time component."""
    frame = PAH.build_frame(members, "SP500", date(2026, 8, 3), "Equity", "Stock")
    value = frame.Date.iloc[0]

    assert isinstance(value, date)
    assert not isinstance(value, (datetime, pd.Timestamp))
    assert value == date(2026, 8, 3)


def test_build_frame_normalises_datetime_input_to_date(members):
    frame = PAH.build_frame(
        members, "SP500", datetime(2026, 8, 3, 18, 30), "Equity", "Stock"
    )
    assert frame.Date.iloc[0] == date(2026, 8, 3)
    assert not isinstance(frame.Date.iloc[0], datetime)


def test_build_frame_sorts_by_symbol(members):
    frame = PAH.build_frame(members, "SP500", date(2026, 8, 3), "Equity", "Stock")
    assert list(frame.Symbol) == ["AAPL", "BRK-B", "MSFT"]


def test_build_frame_falls_back_to_sentinel_for_missing_sector():
    members = pd.DataFrame({"Symbol": ["AAPL", "MSFT"], "Sector": [None, ""]})
    frame = PAH.build_frame(members, "NDX100", date(2026, 8, 3), "Equity", "Stock")
    assert set(frame.Sector) == {PAH.NO_SECTOR}


def test_build_frame_honours_overridden_class_and_type(members):
    frame = PAH.build_frame(members, "SP500", date(2026, 8, 3), "Fund", "ETF")
    assert (frame.Class == "Fund").all()
    assert (frame.Type == "ETF").all()


# --------------------------------------------------------------------------
# T7 -- sanity gate
# --------------------------------------------------------------------------
@pytest.mark.parametrize(
    "count,band,expected",
    [
        (503, PAH.SP500_BAND, True),
        (490, PAH.SP500_BAND, True),   # inclusive lower bound
        (520, PAH.SP500_BAND, True),   # inclusive upper bound
        (489, PAH.SP500_BAND, False),
        (521, PAH.SP500_BAND, False),
        (3, PAH.SP500_BAND, False),    # the broken-parse case
        (0, PAH.SP500_BAND, False),
        (103, PAH.NDX100_BAND, True),
        (95, PAH.NDX100_BAND, True),
        (110, PAH.NDX100_BAND, True),
        (94, PAH.NDX100_BAND, False),
        (111, PAH.NDX100_BAND, False),
    ],
)
def test_sanity_gate_bands(count, band, expected):
    df = pd.DataFrame({"Symbol": [f"S{i}" for i in range(count)]})
    assert PAH.sanity_gate(df, band[0], band[1], "test") is expected


def test_sanity_gate_rejects_none():
    assert PAH.sanity_gate(None, 490, 520, "test") is False


# --------------------------------------------------------------------------
# T8 -- change detection
# --------------------------------------------------------------------------
def _set(symbols, sectors=None, rate=1.0):
    sectors = sectors or ["Tech"] * len(symbols)
    return pd.DataFrame(
        {
            "Symbol": symbols,
            "Class": "Equity",
            "Sector": sectors,
            "Type": "Stock",
            "Currency": "USD",
            "Rate": rate,
        }
    )


def test_has_changed_false_for_identical_sets():
    a = _set(["AAPL", "MSFT"])
    assert PAH.has_changed(a, a.copy()) is False


def test_has_changed_true_when_symbol_added():
    assert PAH.has_changed(_set(["AAPL", "MSFT", "NVDA"]), _set(["AAPL", "MSFT"])) is True


def test_has_changed_true_when_symbol_removed():
    assert PAH.has_changed(_set(["AAPL"]), _set(["AAPL", "MSFT"])) is True


def test_has_changed_true_for_sector_only_change():
    """A reclassification with identical membership is still a new dated set."""
    old = _set(["AAPL", "MSFT"], ["Tech", "Tech"])
    new = _set(["AAPL", "MSFT"], ["Information Technology", "Tech"])
    assert PAH.has_changed(new, old) is True


def test_has_changed_false_when_only_row_order_differs():
    old = _set(["AAPL", "MSFT", "NVDA"])
    new = _set(["NVDA", "AAPL", "MSFT"])
    assert PAH.has_changed(new, old) is False


def test_has_changed_false_for_nan_versus_none_sector():
    """Sector can legitimately be missing; the two spellings must compare equal."""
    old = _set(["AAPL", "MSFT"], [None, "Tech"])
    new = _set(["AAPL", "MSFT"], [float("nan"), "Tech"])
    assert PAH.has_changed(new, old) is False


def test_has_changed_true_when_nothing_is_stored():
    assert PAH.has_changed(_set(["AAPL"]), None) is True
    assert PAH.has_changed(_set(["AAPL"]), pd.DataFrame()) is True


def test_has_changed_true_for_rate_change():
    assert PAH.has_changed(_set(["AAPL"], rate=0.98), _set(["AAPL"], rate=1.0)) is True


def test_has_changed_ignores_float_noise_below_six_decimals():
    assert PAH.has_changed(_set(["AAPL"], rate=1.0000000001), _set(["AAPL"], rate=1.0)) is False


# --------------------------------------------------------------------------
# T9 -- effective-date resolution (all five rows of the design table)
# --------------------------------------------------------------------------
def test_resolve_writes_when_nothing_is_stored():
    d = PAH.resolve_effective_date(date(2026, 8, 3), None, changed=True)
    assert d.action == "write"
    assert d.write_date == date(2026, 8, 3)


def test_resolve_writes_when_newer_and_changed():
    d = PAH.resolve_effective_date(date(2026, 8, 3), date(2026, 7, 30), changed=True)
    assert d.action == "write"
    assert d.write_date == date(2026, 8, 3)


def test_resolve_replaces_when_same_date_and_changed():
    """Same-day re-run is idempotent and a mid-day correction can land."""
    d = PAH.resolve_effective_date(date(2026, 8, 3), date(2026, 8, 3), changed=True)
    assert d.action == "replace"
    assert d.write_date == date(2026, 8, 3)


def test_resolve_skips_when_source_is_stale():
    d = PAH.resolve_effective_date(date(2026, 7, 1), date(2026, 8, 3), changed=True)
    assert d.action == "skip"
    assert d.write_date is None
    assert "older than" in d.reason


def test_resolve_skips_when_unchanged():
    """The expected outcome on the vast majority of runs."""
    d = PAH.resolve_effective_date(date(2026, 8, 3), date(2026, 7, 30), changed=False)
    assert d.action == "skip"
    assert "unchanged" in d.reason


def test_resolve_force_overrides_the_unchanged_skip():
    d = PAH.resolve_effective_date(
        date(2026, 8, 3), date(2026, 7, 30), changed=False, force=True
    )
    assert d.action == "write"


def test_resolve_force_does_not_override_the_stale_source_guard():
    """force is for the only-on-change check, not for writing out-of-order sets."""
    d = PAH.resolve_effective_date(
        date(2026, 7, 1), date(2026, 8, 3), changed=True, force=True
    )
    assert d.action == "skip"


def test_resolve_accepts_datetimes():
    d = PAH.resolve_effective_date(
        datetime(2026, 8, 3, 18, 30), datetime(2026, 7, 30, 9, 0), changed=True
    )
    assert d.write_date == date(2026, 8, 3)


def test_pick_effective_date_prefers_the_source_asof(frozen_ny_time):
    assert PAH.pick_effective_date(date(2026, 7, 30), frozen_ny_time) == date(2026, 7, 30)


def test_pick_effective_date_falls_back_to_ny_date(frozen_ny_time):
    assert PAH.pick_effective_date(None, frozen_ny_time) == date(2026, 8, 3)


# --------------------------------------------------------------------------
# Symbol normalization
# --------------------------------------------------------------------------
@pytest.mark.parametrize(
    "raw,expected",
    [
        ("BRK.B", "BRK-B"),
        ("BF.B", "BF-B"),
        ("AAPL", "AAPL"),
        ("  msft  ", "MSFT"),
        ("BRK.B ", "BRK-B"),  # Wikipedia emits non-breaking spaces
        (None, None),
    ],
)
def test_normalize_us_symbol(raw, expected):
    assert PAH.normalize_us_symbol(raw) == expected


# --------------------------------------------------------------------------
# T10 / T11 -- environment contract
# --------------------------------------------------------------------------
def test_run_raises_named_error_when_dbtrading_is_missing(env, unset_env):
    """A missing var must fail before any SQL is built.

    ``environ.get()`` returning None silently interpolates the string 'None'
    into SQL, which only surfaces later as a confusing database error.
    """
    unset_env("DBTRADING")
    with pytest.raises(RuntimeError, match="DBTRADING"):
        PAH.run({"localrun": True, "dbFlag": False}, None)


def test_run_raises_named_error_when_tblportassets_is_missing(env, unset_env):
    unset_env("TBLPORTASSETS")
    with pytest.raises(RuntimeError, match="TBLPORTASSETS"):
        PAH.run({"localrun": True, "dbFlag": False}, None)


def test_require_env_rejects_blank_values(monkeypatch):
    monkeypatch.setenv("DBTRADING", "   ")
    with pytest.raises(RuntimeError, match="DBTRADING"):
        PAH._require_env("DBTRADING")


def test_env_defaults_are_used_when_unset(env, unset_env):
    """T11: the documented defaults apply and the run proceeds."""
    unset_env("SP500_PORT_NAME", "NDX100_PORT_NAME", "PORT_ASSET_CLASS", "PORT_ASSET_TYPE")

    assert PAH._env("SP500_PORT_NAME", "SP500") == "SP500"
    assert PAH._env("NDX100_PORT_NAME", "NDX100") == "NDX100"
    assert PAH._env("PORT_ASSET_CLASS", "Equity") == "Equity"
    assert PAH._env("PORT_ASSET_TYPE", "Stock") == "Stock"


@pytest.mark.parametrize(
    "raw,expected",
    [("True", True), ("true", True), ("1", True), ("yes", True),
     ("False", False), ("false", False), ("0", False), ("no", False)],
)
def test_truthy_parsing(raw, expected):
    assert PAH._truthy(raw) is expected


def test_truthy_defaults_when_absent():
    assert PAH._truthy(None, default=True) is True
    assert PAH._truthy(None, default=False) is False


# --------------------------------------------------------------------------
# T12 / T13 -- end-to-end run() behaviour, network and DB stubbed
# --------------------------------------------------------------------------
@pytest.fixture
def stub_sources(monkeypatch, sp500_wikipedia_html, ndx100_nasdaq_json, spy_holdings_xlsx):
    """Replace every network call with the saved fixtures."""
    sp500 = PAH.parse_sp500_wikipedia(sp500_wikipedia_html)
    ndx = PAH.parse_ndx_nasdaq_api(ndx100_nasdaq_json)
    spy, spy_asof = PAH.parse_spy_holdings(spy_holdings_xlsx)

    monkeypatch.setattr(PAH, "fetch_sp500", lambda: (sp500, "fixture:sp500"))
    monkeypatch.setattr(
        PAH, "fetch_ndx100", lambda: (ndx, "nasdaq-api:fixture", date(2026, 7, 30))
    )
    monkeypatch.setattr(PAH, "fetch_spy_holdings", lambda: (spy, spy_asof))
    monkeypatch.setattr(PAH, "fetch_ndx100_crosscheck", lambda: ndx)
    return {"sp500": sp500, "ndx": ndx}


@pytest.fixture
def spy_db(monkeypatch):
    """Record every DB call instead of making one."""
    calls = {"StoreEOD": [], "ExecSQL": [], "load_df_SQL": []}

    def _store(df, db, tbl):
        calls["StoreEOD"].append((df.copy(), db, tbl))

    def _exec(query):
        calls["ExecSQL"].append(query)
        return 0

    def _load(query):
        calls["load_df_SQL"].append(query)
        return None

    monkeypatch.setattr(DU, "StoreEOD", _store)
    monkeypatch.setattr(DU, "ExecSQL", _exec)
    monkeypatch.setattr(DU, "load_df_SQL", _load)
    return calls


def test_dbflag_false_writes_csvs_and_touches_no_database(
    env, stub_sources, spy_db, frozen_ny_time, tmp_path
):
    """T12: the dry-run switch. CSVs still land; nothing reaches MySQL."""
    summaries = PAH.run(
        {"dbFlag": False, "test": True, "NYTIME": frozen_ny_time}, None
    )

    assert calls_empty(spy_db)

    written = {p.name for p in tmp_path.glob("*.csv")}
    assert "portfolio_assets_info_SP500.csv" in written
    assert "portfolio_assets_info_NDX100.csv" in written
    assert "portfolio_assets_info.csv" in written

    assert {s["port_name"] for s in summaries} == {"SP500", "NDX100"}
    assert all(s["action"] == "skip" for s in summaries)
    assert all("dbFlag=False" in s["reason"] for s in summaries)


def calls_empty(calls):
    return not calls["StoreEOD"] and not calls["ExecSQL"] and not calls["load_df_SQL"]


def test_dry_run_csv_has_the_table_columns_and_expected_counts(
    env, stub_sources, spy_db, frozen_ny_time, tmp_path
):
    PAH.run({"dbFlag": False, "NYTIME": frozen_ny_time}, None)

    combined = pd.read_csv(tmp_path / "portfolio_assets_info.csv")
    assert list(combined.columns) == PAH.TABLE_COLUMNS

    by_port = combined.groupby("Port_name").size().to_dict()
    assert by_port["SP500"] == 503
    assert by_port["NDX100"] == 103

    ndx_rows = combined[combined.Port_name == "NDX100"]
    assert set(ndx_rows.Sector) == {"General"}
    assert (combined.Rate == 1.0).all()
    assert (combined.Currency == "USD").all()


def test_sp500_uses_ny_date_and_ndx_uses_the_source_asof(
    env, stub_sources, spy_db, frozen_ny_time, tmp_path
):
    """SP500's primary source carries no as-of date, so it uses the run date.

    NDX100's primary source (the Nasdaq API) supplies its own, which is
    preferred. The SPY workbook's as-of is logged but deliberately not used --
    it belongs to a different product and is behind an optional flag, so
    deriving a primary-key column from it would make the date move whenever
    INDEX_CROSSCHECK is toggled.
    """
    PAH.run({"dbFlag": False, "NYTIME": frozen_ny_time}, None)
    combined = pd.read_csv(tmp_path / "portfolio_assets_info.csv")

    sp_dates = set(combined[combined.Port_name == "SP500"].Date)
    ndx_dates = set(combined[combined.Port_name == "NDX100"].Date)

    assert sp_dates == {"2026-08-03"}
    assert ndx_dates == {"2026-07-30"}


def test_unchanged_set_is_not_rewritten(
    env, stub_sources, monkeypatch, frozen_ny_time, tmp_path
):
    """The expected no-op path: identical set already stored for the same date."""
    stored = {}

    def _load(query):
        if "max(Date)" in query:
            port = "SP500" if "SP500" in query else "NDX100"
            return pd.DataFrame(
                {"maxdate": [date(2026, 8, 3) if port == "SP500" else date(2026, 7, 30)]}
            )
        if query.strip().startswith("SELECT Symbol"):
            port = "SP500" if "SP500" in query else "NDX100"
            return stored[port]
        return None

    # seed 'stored' with exactly what this run will produce
    for port, members, d in [
        ("SP500", stub_sources["sp500"], date(2026, 8, 3)),
        ("NDX100", stub_sources["ndx"], date(2026, 7, 30)),
    ]:
        stored[port] = PAH.build_frame(members, port, d, "Equity", "Stock")[
            PAH.COMPARE_COLUMNS
        ]

    monkeypatch.setattr(DU, "load_df_SQL", _load)
    store_calls, exec_calls = [], []
    monkeypatch.setattr(DU, "StoreEOD", lambda *a: store_calls.append(a))
    monkeypatch.setattr(DU, "ExecSQL", lambda q: exec_calls.append(q))

    summaries = PAH.run({"NYTIME": frozen_ny_time}, None)

    assert store_calls == []
    assert exec_calls == []
    assert all(s["action"] == "skip" for s in summaries)
    assert all(s["changed"] is False for s in summaries)


def test_force_writes_even_when_unchanged(
    env, stub_sources, monkeypatch, frozen_ny_time, tmp_path
):
    """T13: force bypasses the only-on-change check."""
    stored = {}
    for port, members, d in [
        ("SP500", stub_sources["sp500"], date(2026, 8, 3)),
        ("NDX100", stub_sources["ndx"], date(2026, 7, 30)),
    ]:
        stored[port] = PAH.build_frame(members, port, d, "Equity", "Stock")[
            PAH.COMPARE_COLUMNS
        ]

    def _load(query):
        port = "SP500" if "SP500" in query else "NDX100"
        if "max(Date)" in query:
            return pd.DataFrame(
                {"maxdate": [date(2026, 8, 3) if port == "SP500" else date(2026, 7, 30)]}
            )
        if "count(*)" in query:
            return pd.DataFrame({"n": [len(stored[port])]})
        return stored[port]

    store_calls, exec_calls = [], []
    monkeypatch.setattr(DU, "load_df_SQL", _load)
    monkeypatch.setattr(DU, "StoreEOD", lambda df, db, tbl: store_calls.append((df, db, tbl)))
    monkeypatch.setattr(DU, "ExecSQL", lambda q: (exec_calls.append(q), 503)[1])

    summaries = PAH.run({"force": True, "NYTIME": frozen_ny_time}, None)

    assert len(store_calls) == 2, "both sets should have been written"
    assert all(s["action"] == "replace" for s in summaries)
    # same date as stored -> DELETE that (Date, Port_name) first, for idempotency
    assert len(exec_calls) == 2
    assert all(q.strip().upper().startswith("DELETE") for q in exec_calls)
    assert all("Port_name" in q and "Date" in q for q in exec_calls)


def test_sanity_gate_failure_blocks_the_write(
    env, stub_sources, spy_db, monkeypatch, frozen_ny_time, tmp_path
):
    """A broken parse must not wipe a good stored membership set."""
    monkeypatch.setattr(
        PAH, "fetch_sp500", lambda: (pd.DataFrame({"Symbol": ["AAPL", "MSFT"]}), "broken")
    )

    summaries = PAH.run({"NYTIME": frozen_ny_time}, None)
    sp = next(s for s in summaries if s["port_name"] == "SP500")

    assert sp["action"] == "skip"
    assert "sanity gate" in sp["reason"]

    # SP500 must not be written -- but NDX100 is a separate index and is
    # unaffected, so assert on the SP500 rows specifically rather than on
    # "nothing was written at all".
    written_ports = {df.Port_name.iloc[0] for df, _, _ in spy_db["StoreEOD"]}
    assert "SP500" not in written_ports
    assert not any("SP500" in q for q in spy_db["ExecSQL"])


def test_one_index_failing_does_not_stop_the_other(
    env, stub_sources, spy_db, monkeypatch, frozen_ny_time
):
    def _boom():
        raise RuntimeError("nasdaq is down and wikipedia is down too")

    monkeypatch.setattr(PAH, "fetch_ndx100", _boom)
    summaries = PAH.run({"dbFlag": False, "NYTIME": frozen_ny_time}, None)

    sp = next(s for s in summaries if s["port_name"] == "SP500")
    ndx = next(s for s in summaries if s["port_name"] == "NDX100")
    assert sp["rows"] == 503
    assert ndx["action"] == "failed"


def test_write_verification_flags_a_swallowed_store_failure(env, monkeypatch):
    """``StoreEOD`` logs and swallows write errors; the recount is what catches it."""
    monkeypatch.setattr(DU, "StoreEOD", lambda df, db, tbl: None)  # silently writes nothing
    monkeypatch.setattr(DU, "ExecSQL", lambda q: 0)
    monkeypatch.setattr(DU, "load_df_SQL", lambda q: pd.DataFrame({"n": [0]}))

    frame = PAH.build_frame(
        pd.DataFrame({"Symbol": ["AAPL"]}), "SP500", date(2026, 8, 3), "Equity", "Stock"
    )
    ok = PAH.write_set("Trading", "portfolio_assets_info", frame, "SP500",
                       date(2026, 8, 3), replace=False)
    assert ok is False


def test_log_symmetric_difference_is_non_fatal_with_a_missing_source():
    PAH.log_symmetric_difference("SP500", pd.DataFrame({"Symbol": ["AAPL"]}), None)
    PAH.log_symmetric_difference("SP500", None, None)


def test_cross_check_agreement_between_wikipedia_and_spy(
    sp500_wikipedia_html, spy_holdings_xlsx
):
    """The two independent SP500 sources should broadly agree on membership.

    Not an equality assertion -- SPY holds a few non-index lines and the two
    snapshots are taken at different moments -- but a wide divergence means one
    of the parsers has broken.
    """
    wiki = PAH.parse_sp500_wikipedia(sp500_wikipedia_html)
    spy, _ = PAH.parse_spy_holdings(spy_holdings_xlsx)

    overlap = set(wiki.Symbol) & set(spy.Symbol)
    assert len(overlap) >= 480, f"only {len(overlap)} symbols in common"


# --------------------------------------------------------------------------
# T15 -- the return value must survive Lambda's JSON marshalling
#
# Regression: the 2026-08-02 09:06 UTC production run did all its work, then
# died with ``Runtime.MarshalError: Object of type DataFrame is not JSON
# serializable`` because run() returned the internal summaries, which carry the
# built frame under 'frame' and a datetime.date under 'date'.
# --------------------------------------------------------------------------
def test_run_result_is_json_serialisable_on_a_dry_run(
    env, stub_sources, spy_db, frozen_ny_time, tmp_path
):
    summaries = PAH.run({"dbFlag": False, "NYTIME": frozen_ny_time}, None)
    json.dumps(summaries)  # exactly what the Lambda runtime does with it


def test_run_result_carries_no_dataframe(
    env, stub_sources, spy_db, frozen_ny_time, tmp_path
):
    assert all(
        "frame" not in s
        for s in PAH.run({"dbFlag": False, "NYTIME": frozen_ny_time}, None)
    )


def test_run_result_is_json_serialisable_when_rows_are_written(
    env, stub_sources, monkeypatch, frozen_ny_time, tmp_path
):
    """The write path is where 'date' is populated -- and a date is not JSON either."""
    stored = {
        port: PAH.build_frame(members, port, d, "Equity", "Stock")[PAH.COMPARE_COLUMNS]
        for port, members, d in [
            ("SP500", stub_sources["sp500"], date(2026, 8, 3)),
            ("NDX100", stub_sources["ndx"], date(2026, 7, 30)),
        ]
    }

    def _load(query):
        port = "SP500" if "SP500" in query else "NDX100"
        if "max(Date)" in query:
            return pd.DataFrame(
                {"maxdate": [date(2026, 8, 3) if port == "SP500" else date(2026, 7, 30)]}
            )
        if "count(*)" in query:
            return pd.DataFrame({"n": [len(stored[port])]})
        return stored[port]

    monkeypatch.setattr(DU, "load_df_SQL", _load)
    monkeypatch.setattr(DU, "StoreEOD", lambda df, db, tbl: None)
    monkeypatch.setattr(DU, "ExecSQL", lambda q: 503)

    summaries = PAH.run({"force": True, "NYTIME": frozen_ny_time}, None)
    by_port = {s["port_name"]: s for s in summaries}

    assert {s["action"] for s in summaries} == {"replace"}
    assert by_port["SP500"]["date"] == "2026-08-03"
    assert by_port["NDX100"]["date"] == "2026-07-30"
    json.dumps(summaries)


def test_run_result_is_json_serialisable_when_an_index_fails(
    env, stub_sources, spy_db, monkeypatch, frozen_ny_time, tmp_path
):
    def _boom():
        raise RuntimeError("nasdaq is down and wikipedia is down too")

    monkeypatch.setattr(PAH, "fetch_ndx100", _boom)
    json.dumps(PAH.run({"dbFlag": False, "NYTIME": frozen_ny_time}, None))


def test_jsonable_summary_normalises_a_timestamp_date():
    out = PAH._jsonable_summary(
        {"port_name": "SP500", "date": pd.Timestamp("2026-08-03 14:30"), "frame": pd.DataFrame()}
    )
    assert out == {"port_name": "SP500", "date": "2026-08-03"}


def test_jsonable_summary_leaves_a_missing_date_alone():
    assert PAH._jsonable_summary({"action": "skip", "date": None})["date"] is None


# --------------------------------------------------------------------------
# T14 -- network policy
# --------------------------------------------------------------------------
def test_unit_tests_cannot_open_a_real_socket():
    """The autouse ``no_network`` guard must actually be armed."""
    import socket

    from conftest import NetworkAccessAttempted

    with pytest.raises(NetworkAccessAttempted):
        socket.create_connection(("example.com", 80), timeout=1)

    with pytest.raises(NetworkAccessAttempted):
        socket.socket().connect(("example.com", 80))


def test_fetch_sp500_would_hit_the_network_and_is_therefore_blocked():
    """Proof the guard covers the handler's own fetchers, not just raw sockets."""
    with pytest.raises(Exception):
        PAH.fetch_sp500()
