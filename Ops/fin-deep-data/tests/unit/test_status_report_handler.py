"""Unit tests for ``status_report_handler`` (statusReport, PLAN-SR-UPSTREAM Phase D).

python3.13 only (``venv-py313``). The view / audit reads go through a sqlite
mirror of ``load_audit`` or a patched ``load_df_SQL``; SNS is stubbed.
"""

from datetime import date, datetime

import pandas as pd
import pytest

import dataUtil as DU
import status_report_handler as H

pytestmark = pytest.mark.unit


def _summary(**kw):
    row = {"started_at": datetime(2026, 9, 24, 22, 31), "finished_at": datetime(2026, 9, 24, 22, 44),
           "status": "ok", "n_ok": 10, "n_expected": 10, "n_rows": 10,
           "date_hi": date(2026, 9, 24), "error": None}
    row.update(kw)
    return row


# --------------------------------------------------------------------------
# S1 status rules
# --------------------------------------------------------------------------
@pytest.mark.parametrize("today,prev", [
    (date(2026, 9, 28), date(2026, 9, 25)),   # Monday -> Friday
    (date(2026, 9, 25), date(2026, 9, 24)),
    (date(2026, 9, 27), date(2026, 9, 25)),   # Sunday -> Friday
])
def test_previous_weekday(today, prev):
    assert H.previous_weekday(today) == prev


def _line(**kw):
    line = {"last_data_date": date(2026, 9, 25), "failed": False, "n_ok": 10, "n_expected": 10,
            "inferred": False}
    line.update(kw)
    return line


def test_status_ok_partial_error():
    today = date(2026, 9, 25)
    assert H.status_for(_line(), today) == "ok"
    assert H.status_for(_line(n_ok=9), today) == "partial"
    assert H.status_for(_line(failed=True), today) == "error"
    assert H.status_for(_line(n_ok=None, n_expected=None), today) == "ok"


def test_status_stale_boundary():
    today = date(2026, 9, 25)  # Friday: previous weekday Thursday 24th
    assert H.status_for(_line(last_data_date=date(2026, 9, 24)), today) == "ok"
    assert H.status_for(_line(last_data_date=date(2026, 9, 23)), today) == "stale"
    assert H.status_for(_line(last_data_date=None), today) == "stale"


def test_status_stale_on_monday():
    monday = date(2026, 9, 28)
    assert H.status_for(_line(last_data_date=date(2026, 9, 25)), monday) == "ok"      # Friday's data
    assert H.status_for(_line(last_data_date=date(2026, 9, 24)), monday) == "stale"   # Thursday's


def test_status_error_beats_stale_and_partial():
    assert H.status_for(_line(failed=True, last_data_date=date(2026, 1, 1), n_ok=0), date(2026, 9, 25)) == "error"


def test_status_inferred():
    today = date(2026, 9, 25)
    assert H.status_for({"inferred": True, "last_data_date": date(2026, 9, 25)}, today) == "—"
    assert H.status_for({"inferred": True, "last_data_date": date(2026, 9, 1)}, today) == "stale"


def test_status_event_table_judged_by_last_run():
    """corp_action_daily's last ex-date is naturally old; the run date is what matters."""
    today, events = date(2026, 9, 25), {"corp_action_daily", "portfolio_assets_info"}
    ran_today = _line(table_name="corp_action_daily", last_data_date=date(2026, 9, 15),
                      finished_at=datetime(2026, 9, 25, 22, 44))
    assert H.status_for(ran_today, today, events) == "ok"
    ran_long_ago = dict(ran_today, finished_at=datetime(2026, 9, 20, 22, 44))
    assert H.status_for(ran_long_ago, today, events) == "stale"
    inferred = {"table_name": "portfolio_assets_info", "inferred": True, "last_data_date": date(2026, 8, 2)}
    assert H.status_for(inferred, today, events) == "—"
    assert H.status_for(inferred, today) == "stale"   # without the event rule it would alarm


# --------------------------------------------------------------------------
# S2 aggregate_shards
# --------------------------------------------------------------------------
def test_aggregate_shards_spans_all_shards_and_sweep():
    rows = [
        _summary(started_at=datetime(2026, 9, 24, 21, 40), finished_at=datetime(2026, 9, 24, 21, 49), n_rows=100),
        _summary(started_at=datetime(2026, 9, 24, 21, 40), finished_at=datetime(2026, 9, 24, 21, 52), n_rows=200),
        _summary(started_at=datetime(2026, 9, 24, 22, 40), finished_at=datetime(2026, 9, 24, 22, 41), n_rows=5,
                 date_hi=None),  # sweep that recovered little
    ]
    agg = H.aggregate_shards(rows, symbol_counts=(857, 850))
    assert agg["started_at"] == datetime(2026, 9, 24, 21, 40)
    assert agg["finished_at"] == datetime(2026, 9, 24, 22, 41)
    assert agg["n_rows"] == 305 and agg["n_runs"] == 3
    assert (agg["n_ok"], agg["n_expected"]) == (850, 857)
    assert agg["last_data_date"] == date(2026, 9, 24) and agg["failed"] is False


def test_aggregate_shards_without_symbol_rows_sums_summaries():
    rows = [_summary(n_ok=None, n_expected=None, n_rows=1)]
    agg = H.aggregate_shards(rows, symbol_counts=(0, 0))
    assert agg["n_ok"] is None and agg["n_expected"] is None and agg["n_rows"] == 1
    rows = [_summary(n_ok=2, n_expected=2), _summary(n_ok=1, n_expected=2)]
    agg = H.aggregate_shards(rows)
    assert (agg["n_ok"], agg["n_expected"]) == (3, 4)


def test_aggregate_shards_flags_a_failed_shard():
    agg = H.aggregate_shards([_summary(), _summary(status="error", error="watermark unreadable")])
    assert agg["failed"] is True and agg["error"] == "watermark unreadable"


def test_aggregate_shards_tolerates_nan_from_pandas():
    rows = pd.DataFrame([_summary(n_ok=float("nan"), n_expected=float("nan"), date_hi=None)]).to_dict("records")
    agg = H.aggregate_shards(rows)
    assert agg["n_ok"] is None and agg["last_data_date"] is None


# --------------------------------------------------------------------------
# S3 formatting
# --------------------------------------------------------------------------
def test_run_span_in_et():
    # 22:31 / 22:44 UTC on 2026-09-24 are 18:31 / 18:44 EDT
    assert H.run_span(datetime(2026, 9, 24, 22, 31), datetime(2026, 9, 24, 22, 44)) == "2026-09-24 18:31–18:44"
    assert H.run_span(datetime(2026, 9, 24, 22, 31), datetime(2026, 9, 25, 4, 1)).endswith("2026-09-25 00:01")
    assert H.run_span(None, None) == "—"


def test_format_report_matches_sr_layout():
    lines = [
        {"table_name": "histdailyprice7", "job": "eodDaily", "last_data_date": date(2026, 9, 24),
         "started_at": datetime(2026, 9, 24, 22, 31), "finished_at": datetime(2026, 9, 24, 22, 44),
         "status": "ok", "n_ok": 1212, "n_expected": 1212, "n_rows": 1212, "inferred": False},
        {"table_name": "corp_action_daily", "job": "eodDaily", "last_data_date": date(2026, 9, 24),
         "started_at": datetime(2026, 9, 24, 22, 31), "finished_at": datetime(2026, 9, 24, 22, 44),
         "status": "ok", "n_ok": None, "n_expected": None, "n_rows": 7, "inferred": False},
        {"table_name": "FX_histdaily", "job": None, "last_data_date": date(2026, 9, 24),
         "status": "—", "inferred": True},
    ]
    text = H.format_report(lines, date(2026, 9, 24)).splitlines()
    assert text[2].startswith("DataName (table)     Job           Last data   Last run (ET)")
    assert text[3] == ("histdailyprice7      eodDaily      2026-09-24  2026-09-24 18:31–18:44    "
                       "ok       1212/1212    1212")
    assert text[4].split() == ["corp_action_daily", "eodDaily", "2026-09-24", "2026-09-24",
                               "18:31–18:44", "ok", "—", "7"]
    assert text[5].split() == ["FX_histdaily", "(inferred)", "2026-09-24", "—", "—", "—", "—"]


def test_format_report_lists_errors_and_subject():
    lines = [{"table_name": "OptionChains_shadow", "job": "optChainEOD", "status": "error",
              "error": "sweep: load_audit could not be read", "last_data_date": None, "n_rows": 0}]
    assert "sweep: load_audit could not be read" in H.format_report(lines, date(2026, 9, 25))
    assert H.subject_for(lines, date(2026, 9, 25)) == "Fin-Lambda status 2026-09-25: 1 issue(s)"
    assert H.subject_for([], date(2026, 9, 25)).endswith("all ok")


# --------------------------------------------------------------------------
# S4 run() against a sqlite mirror
# --------------------------------------------------------------------------
@pytest.fixture
def status_db(env, sqlite_engine, monkeypatch):
    from test_dataUtil import LOAD_AUDIT_SQLITE
    from sqlalchemy import text

    view = """CREATE VIEW v_load_status AS
      SELECT table_name, job, date_hi AS last_data_date, started_at, finished_at,
             status, n_ok, n_expected, n_rows, error
      FROM (SELECT a.*, ROW_NUMBER() OVER (PARTITION BY table_name, job ORDER BY finished_at DESC) AS rn
            FROM load_audit a WHERE a.segment = 'summary') s WHERE rn = 1"""
    with sqlite_engine.begin() as conn:
        conn.execute(text(LOAD_AUDIT_SQLITE))
        conn.execute(text(view))
        conn.execute(text("CREATE TABLE histdailyprice7 (Date DATE)"))
        conn.execute(text("INSERT INTO histdailyprice7 VALUES ('2026-09-24')"))
        conn.execute(text("CREATE TABLE histminprice (Datetime DATETIME)"))
        conn.execute(text("INSERT INTO histminprice VALUES ('2026-09-24 15:45:00')"))
    monkeypatch.setenv("TBLLOADAUDIT", "load_audit")

    def strip_schema(q):
        for schema in ("GlobalMarketData.", "Trading."):
            q = q.replace(schema, "")
        try:
            return pd.read_sql(q, sqlite_engine)
        except Exception:
            return None  # a table the mirror does not have -> unreadable, as in production

    monkeypatch.setattr(DU, "load_df_SQL", strip_schema)
    real = DU.append_ignore
    monkeypatch.setattr(DU, "append_ignore", lambda df, s, t, chunk=500: real(df, None, t, chunk))
    return sqlite_engine


def _write_run(job, table, run_id, started, finished, statuses, date_hi, summary_status="ok"):
    rows = [{"run_id": run_id, "job": job, "Symbol": sym, "Exchange": "", "table_name": table,
             "segment": "append", "n_rows": 1 if st == "ok" else 0, "status": st,
             "date_hi": date_hi if st == "ok" else None,
             "started_at": started, "finished_at": finished} for sym, st in statuses.items()]
    rows.append(DU.audit_summary(job, table, run_id, started, status=summary_status,
                                 n_rows=sum(r["n_rows"] for r in rows), date_hi=date_hi,
                                 n_ok=sum(s in ("ok", "empty") for s in statuses.values()),
                                 n_expected=len(statuses), finished_at=finished))
    DU.audit_run(rows)


def test_run_combines_shards_and_sweep(status_db, capsys, monkeypatch):
    d = date(2026, 9, 25)
    _write_run("eodDaily", "histdailyprice7_shadow", "R1", datetime(2026, 9, 25, 22, 30),
               datetime(2026, 9, 25, 22, 35), {"A": "ok", "B": "skipped"}, d)
    _write_run("eodDaily", "histdailyprice7_shadow", "R2", datetime(2026, 9, 25, 22, 30),
               datetime(2026, 9, 25, 22, 36), {"C": "ok", "D": "empty"}, d)
    _write_run("eodDaily", "histdailyprice7_shadow", "R3", datetime(2026, 9, 25, 23, 0),
               datetime(2026, 9, 25, 23, 1), {"B": "ok"}, d)            # sweep recovers B
    _write_run("usrateHandler", "USRates", "R4", datetime(2026, 9, 25, 21, 1),
               datetime(2026, 9, 25, 21, 1), {}, date(2026, 9, 24))
    # yesterday's run must not leak into today's line
    _write_run("eodDaily", "histdailyprice7_shadow", "R0", datetime(2026, 9, 24, 22, 30),
               datetime(2026, 9, 24, 22, 35), {"A": "ok", "Z": "error"}, date(2026, 9, 24))

    out = H.run({"localrun": True, "asof": "2026-09-25"}, None)
    text = capsys.readouterr().out
    eod = next(l for l in text.splitlines() if l.startswith("histdailyprice7_shadow"))
    assert eod.split() == ["histdailyprice7_shadow", "eodDaily", "2026-09-25", "2026-09-25",
                           "18:30–19:01", "ok", "4/4", "3"]
    assert "USRates" in text and "(inferred)" in text
    inferred = [l for l in text.splitlines() if "(inferred)" in l]
    assert any(l.startswith("histdailyprice7 ") and "2026-09-24" in l for l in inferred)
    assert any(l.startswith("histminprice") and "2026-09-24" in l for l in inferred)
    assert not any(l.startswith("USRates") for l in inferred)   # audited now, not inferred
    assert out["sns"] is False and "r2" not in out


def test_run_partial_after_sweep(status_db, capsys):
    _write_run("optChainEOD", "OptionChains_shadow", "R1", datetime(2026, 9, 25, 21, 40),
               datetime(2026, 9, 25, 21, 50), {"SPY": "ok", "QQQ": "skipped"}, date(2026, 9, 25))
    H.run({"localrun": True, "asof": "2026-09-25"}, None)
    line = next(l for l in capsys.readouterr().out.splitlines() if l.startswith("OptionChains_shadow"))
    assert "partial" in line and "1/2" in line


def test_run_sends_sns(status_db, monkeypatch):
    sent = {}
    monkeypatch.setenv("STATUS_TOPIC_ARN", "arn:aws:sns:us-east-2:1:finStatus")
    monkeypatch.setattr(H, "publish_sns", lambda arn, subj, body: sent.update(arn=arn, subj=subj, body=body))
    out = H.run({"asof": "2026-09-25"}, None)
    assert out["sns"] is True and "r2" not in out
    assert sent["arn"].endswith("finStatus") and sent["subj"].startswith("Fin-Lambda status 2026-09-25")
    assert sent["body"].startswith("Fin-Lambda load status for 2026-09-25 (ET)")


def test_run_has_no_r2_upload_path(status_db, monkeypatch):
    """U9 report delivers e-mail only: load_audit is the durable copy."""
    monkeypatch.setenv("STATUS_TOPIC_ARN", "arn:aws:sns:us-east-2:1:finStatus")
    monkeypatch.setattr(H, "publish_sns", lambda *a: None)
    assert not hasattr(H, "put_r2_json") and not hasattr(H, "to_json")
    assert "r2" not in H.run({"asof": "2026-09-25"}, None)


def test_run_delivery_failures_are_logged_not_raised(status_db, monkeypatch):
    monkeypatch.setenv("STATUS_TOPIC_ARN", "arn")
    monkeypatch.setattr(H, "publish_sns", lambda *a: (_ for _ in ()).throw(OSError("sns down")))
    out = H.run({"asof": "2026-09-25"}, None)
    assert out["sns"] is False


def test_run_unreadable_view_still_reports_inferred(env, monkeypatch, capsys):
    monkeypatch.setenv("TBLLOADAUDIT", "load_audit")
    monkeypatch.setattr(DU, "load_df_SQL",
                        lambda q: None if "v_load_status" in q else pd.DataFrame({"d": [date(2026, 9, 25)]}))
    out = H.run({"localrun": True, "asof": "2026-09-25"}, None)
    assert "v_load_status" in out["error"] and "view unreadable" in out["subject"]
    assert out["lines"] == len(H.INFERRED_TABLES)


def test_run_requires_audit_table_env(env, unset_env):
    unset_env("TBLLOADAUDIT")
    with pytest.raises(RuntimeError, match="TBLLOADAUDIT"):
        H.run({"localrun": True}, None)
