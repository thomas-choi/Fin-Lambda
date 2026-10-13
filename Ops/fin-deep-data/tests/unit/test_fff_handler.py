"""fffHandlerv2 -- fff_handler.py (2026-10-11, TODOS 5.10 and 4.1).

The fixture ``ff3_factors_sample.csv`` is a trimmed copy of the real
F-F_Research_Data_Factors.csv (2026-10-11 download): preamble, header, a few
monthly rows, the annual block and the copyright line. Nothing here touches
the network or MySQL.
"""

from datetime import date

import pandas as pd
import pytest

import dataUtil as DU

pytestmark = pytest.mark.unit


@pytest.fixture
def ff3_text(fixture_dir):
    return (fixture_dir / "ff3_factors_sample.csv").read_text(encoding="latin-1")


@pytest.fixture
def writes(env, monkeypatch):
    out = {"append": [], "audit": []}
    monkeypatch.setattr(DU, "append_ignore",
                        lambda df, schema, table: out["append"].append((df.copy(), schema, table)) or len(df))
    monkeypatch.setattr(DU, "audit_run", lambda rows: out["audit"].extend(rows) or len(rows))
    return out


def test_parse_ff3_keeps_only_the_monthly_block(ff3_text):
    import fff_handler as H

    df = H.parse_ff3(ff3_text)
    assert list(df.columns) == H.FF3_COLUMNS
    # 6 monthly rows; none of the annual 1927/1928/2025 rows leak in
    assert df["Date"].tolist() == [date(1926, 7, 31), date(2023, 9, 30), date(2023, 10, 31),
                                   date(2023, 11, 30), date(2023, 12, 31), date(2026, 8, 31)]


def test_parse_ff3_converts_percent_to_fraction(ff3_text):
    """famaFrench stores fractions: 2023-10's -3.18 % is -0.0318 in the table."""
    import fff_handler as H

    row = H.parse_ff3(ff3_text).set_index("Date").loc[date(2023, 12, 31)]
    assert row.to_dict() == {"Mkt-RF": 0.0485, "SMB": 0.0628, "HML": 0.0487, "RF": 0.0043}


def test_parse_ff3_month_end_handles_february():
    import fff_handler as H

    text = ",Mkt-RF,SMB,HML,RF\n202402,  5.07, -0.78, -3.49,  0.42\n"
    assert H.parse_ff3(text)["Date"].tolist() == [date(2024, 2, 29)]


def test_parse_ff3_raises_on_unrecognised_layout():
    import fff_handler as H

    with pytest.raises(ValueError, match="layout"):
        H.parse_ff3("Date,A,B,C,D\n202401,1,2,3,4\n")


def test_new_rows_filters_strictly_after_max_date(ff3_text):
    import fff_handler as H

    df = H.parse_ff3(ff3_text)
    assert H.new_rows(df, date(2023, 10, 31))["Date"].min() == date(2023, 11, 30)
    assert len(H.new_rows(df, date(2026, 8, 31))) == 0
    assert len(H.new_rows(df, None)) == len(df)


def test_run_appends_new_months_and_audits(writes, monkeypatch, ff3_text):
    import fff_handler as H

    monkeypatch.setattr(H, "fetch_ff3_text", lambda: ff3_text)
    monkeypatch.setattr(DU, "get_Max_date", lambda t: date(2023, 10, 31))
    result = H.run({}, None)
    (df, schema, table), = writes["append"]
    assert (schema, table) == ("GlobalMarketData", "famaFrench")
    assert df["Date"].tolist() == [date(2023, 11, 30), date(2023, 12, 31), date(2026, 8, 31)]
    assert result == {"run_id": result["run_id"], "rows": 3, "date_hi": "2026-08-31"}
    (row,) = writes["audit"]
    assert (row["job"], row["status"], row["n_rows"], row["date_lo"], row["date_hi"]) == \
        ("fffHandler", "ok", 3, date(2023, 11, 30), date(2026, 8, 31))


def test_run_nothing_new_writes_nothing(writes, monkeypatch, ff3_text):
    import fff_handler as H

    monkeypatch.setattr(H, "fetch_ff3_text", lambda: ff3_text)
    monkeypatch.setattr(DU, "get_Max_date", lambda t: date(2026, 8, 31))
    H.run({}, None)
    assert writes["append"] == []
    assert writes["audit"][0]["n_rows"] == 0


def test_run_failed_append_is_an_error(writes, monkeypatch, ff3_text):
    """append_ignore returns None on failure; that must not be reported as ok."""
    import fff_handler as H

    monkeypatch.setattr(H, "fetch_ff3_text", lambda: ff3_text)
    monkeypatch.setattr(DU, "get_Max_date", lambda t: date(2023, 10, 31))
    monkeypatch.setattr(DU, "append_ignore", lambda df, s, t: None)
    with pytest.raises(RuntimeError, match="append"):
        H.run({}, None)
    assert writes["audit"][0]["status"] == "error"


def test_dry_run_writes_csv_only(writes, monkeypatch, ff3_text, tmp_path):
    import fff_handler as H

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(H, "fetch_ff3_text", lambda: ff3_text)
    monkeypatch.setattr(DU, "get_Max_date", lambda t: None)
    H.run({"localrun": True, "dbFlag": False}, None)
    assert writes["append"] == [] and writes["audit"] == []
    assert len(pd.read_csv(tmp_path / "famaFrench.csv")) == 6


def test_missing_table_env_raises_before_download(writes, monkeypatch, unset_env):
    import fff_handler as H

    unset_env("TBLFFF")
    called = []
    monkeypatch.setattr(H, "fetch_ff3_text", lambda: called.append(1))
    with pytest.raises(RuntimeError, match="TBLFFF"):
        H.run({}, None)
    assert called == []


def test_fetch_ff3_text_unzips_the_csv(monkeypatch):
    import io
    import zipfile

    import fff_handler as H

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("F-F_Research_Data_Factors.CSV", "hello")

    class _Resp:
        content = buf.getvalue()

        def raise_for_status(self):
            pass

    monkeypatch.setattr(H.requests, "get", lambda url, timeout: _Resp())
    assert H.fetch_ff3_text() == "hello"
