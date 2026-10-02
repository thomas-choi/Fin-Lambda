"""Unit tests for the shared ``dataUtil`` module.

Two halves. ``ExecSQL`` is covered because ``dataUtil`` is zipped into every
function's deployment package and the commit semantics must not regress: a
``DELETE`` that silently stopped committing would duplicate a snapshot table on
every run. The rest covers the PLAN-SR-UPSTREAM Phase A helpers that the
handlers in this service are built on.

This file tests ``Ops/fin-deep-data/dataUtil.py`` -- the python3.13 fork. The
python3.10 original has its own copy of these tests at the repo root.
"""

import logging
import warnings

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

import dataUtil as DU

pytestmark = pytest.mark.unit


@pytest.fixture
def seeded(sqlite_engine):
    """An in-memory table with three rows, reachable through ``dataUtil``."""
    with sqlite_engine.begin() as conn:
        conn.execute(text("CREATE TABLE widget (id INTEGER, name TEXT)"))
        conn.execute(text("INSERT INTO widget VALUES (1,'a'),(2,'b'),(3,'c')"))
    return sqlite_engine


def _count(engine, table="widget"):
    with engine.connect() as conn:
        return conn.execute(text(f"SELECT count(*) FROM {table}")).scalar()


# D1 -- ExecSQL actually executes
def test_exec_sql_create_insert_delete_take_effect(sqlite_engine):
    """CREATE / INSERT / DELETE all reach the database."""
    DU.ExecSQL("CREATE TABLE gadget (id INTEGER, name TEXT)")
    DU.ExecSQL("INSERT INTO gadget VALUES (1,'x'),(2,'y')")
    assert _count(sqlite_engine, "gadget") == 2

    DU.ExecSQL("DELETE FROM gadget WHERE id = 1")
    assert _count(sqlite_engine, "gadget") == 1


# D2 -- returns rowcount
def test_exec_sql_returns_rowcount(seeded):
    """A DELETE affecting 2 rows returns 2. Previously always None."""
    assert DU.ExecSQL("DELETE FROM widget WHERE id IN (1,2)") == 2


def test_exec_sql_returns_zero_when_nothing_matched(seeded):
    """Zero is a real answer and must not be conflated with the None failure."""
    assert DU.ExecSQL("DELETE FROM widget WHERE id = 999") == 0


# D3 -- commits
def test_exec_sql_commits_visible_to_independent_connection(seeded):
    """``Engine.begin()`` commits where 1.4's legacy autocommit did implicitly.

    This is the specific equivalence the snapshot handlers depend on: they
    DELETE, then append via ``StoreEOD`` on a *separate* connection. If the
    DELETE did not commit, the append would land on top of the old snapshot.
    """
    DU.ExecSQL("DELETE FROM widget WHERE id = 1")

    with seeded.connect() as independent:
        remaining = independent.execute(
            text("SELECT id FROM widget ORDER BY id")
        ).fetchall()

    assert [row[0] for row in remaining] == [2, 3]


def test_exec_sql_delete_then_append_cycle_leaves_only_new_rows(seeded):
    """The full snapshot pattern used by cronHandler / optHandler / FXrateHandler."""
    DU.ExecSQL("DELETE FROM widget")
    assert _count(seeded) == 0

    with seeded.begin() as conn:
        conn.execute(text("INSERT INTO widget VALUES (9,'fresh')"))

    assert _count(seeded) == 1


# D4 -- error contract preserved
def test_exec_sql_swallows_errors_and_returns_none(sqlite_engine, caplog):
    """Invalid SQL logs at ERROR and returns None -- it must not raise.

    The module-wide error contract: callers all over this repo rely on it and
    almost none of them check the return value.
    """
    with caplog.at_level(logging.ERROR):
        result = DU.ExecSQL("DELETE FROM table_that_does_not_exist")

    assert result is None
    assert any(
        "ExecSQL" in record.message and record.levelno == logging.ERROR
        for record in caplog.records
    )


def test_exec_sql_does_not_raise_when_engine_is_unreachable(monkeypatch):
    """A dead engine is logged, not raised -- same contract as bad SQL."""
    monkeypatch.setattr(
        DU, "get_DBengine", lambda: (_ for _ in ()).throw(OSError("no route to host"))
    )
    assert DU.ExecSQL("DELETE FROM widget") is None


# D5 -- no deprecated API
def test_exec_sql_emits_no_removed_in_20_warning(seeded):
    """Proves the 2.0-readiness claim rather than asserting it.

    Before the change, ``get_DBengine().execute(...)`` emitted
    ``RemovedIn20Warning`` on the pinned SQLAlchemy 1.4.46.
    """
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        DU.ExecSQL("DELETE FROM widget WHERE id = 1")

    offenders = [
        str(w.message)
        for w in caught
        if "RemovedIn20Warning" in type(w.message).__name__
        or "SQLAlchemy 2.0" in str(w.message)
    ]
    assert not offenders, f"deprecated SQLAlchemy API still in use: {offenders}"


def test_exec_sql_engine_execute_would_have_warned(seeded):
    """Control for the test above: confirm the old idiom really did warn.

    Without this, D5 could pass simply because the installed SQLAlchemy no
    longer emits the warning at all, making it a vacuous assertion.
    """
    import sqlalchemy

    if not sqlalchemy.__version__.startswith("1.4"):
        pytest.skip("Engine.execute() no longer exists on SQLAlchemy 2.x")

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        seeded.execute("SELECT 1")

    assert any(
        "RemovedIn20Warning" in type(w.message).__name__ for w in caught
    ), "expected the legacy Engine.execute() to warn on 1.4"


# Guard on the harness itself
def test_reset_dbconn_isolates_the_engine_singleton():
    """The autouse ``reset_dbconn`` fixture must leave ``dbconn`` unset."""
    assert DU.dbconn is None


# ==========================================================================
# PLAN-SR-UPSTREAM Phase A helpers. python3.13 / SQLAlchemy 2.0 / pandas 2.2 --
# the only stack this service deploys. The helpers deliberately use only APIs
# that also exist on SQLAlchemy 1.4, so they could be back-ported to
# fin-cron-data's copy unchanged, but that is not tested here.
# ==========================================================================
import re
from datetime import date, datetime
from types import SimpleNamespace

import pandas as pd

#: sqlite mirror of sql/upstream_tables.sql load_audit -- same columns and PK.
LOAD_AUDIT_SQLITE = """
CREATE TABLE load_audit (
  run_id TEXT NOT NULL, job TEXT NOT NULL, Symbol TEXT NOT NULL,
  Exchange TEXT NOT NULL, date_lo DATE, date_hi DATE, table_name TEXT NOT NULL,
  segment TEXT NOT NULL, n_rows INTEGER NOT NULL, n_ok INTEGER, n_expected INTEGER,
  status TEXT NOT NULL, error TEXT, started_at DATETIME NOT NULL,
  finished_at DATETIME NOT NULL, yf_version TEXT NOT NULL, host TEXT NOT NULL,
  PRIMARY KEY (run_id, table_name, Symbol, Exchange))
"""


@pytest.fixture
def price_table(sqlite_engine):
    with sqlite_engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE px (Date DATE NOT NULL, Symbol TEXT NOT NULL, "
            "Close REAL, PRIMARY KEY (Date, Symbol))"))
    return sqlite_engine


@pytest.fixture
def audit_table(sqlite_engine, monkeypatch):
    with sqlite_engine.begin() as conn:
        conn.execute(text(LOAD_AUDIT_SQLITE))
    # sqlite has no schemas; audit_run passes DBMKTDATA as the schema, so
    # route the append to the default schema for these tests.
    real = DU.append_ignore
    monkeypatch.setattr(DU, "append_ignore", lambda df, schema, table, chunk=500: real(df, None, table, chunk))
    monkeypatch.setenv("TBLLOADAUDIT", "load_audit")
    monkeypatch.setenv("DBMKTDATA", "GlobalMarketData")
    return sqlite_engine


def _px(rows):
    return pd.DataFrame(rows, columns=["Date", "Symbol", "Close"])


# A1 -- append_ignore
def test_append_ignore_inserts_and_counts(price_table):
    n = DU.append_ignore(_px([(date(2026, 9, 1), "A", 1.0), (date(2026, 9, 2), "A", 2.0)]), None, "px")
    assert n == 2
    assert _count(price_table, "px") == 2


def test_append_ignore_keeps_first_value_and_counts_only_new(price_table):
    DU.append_ignore(_px([(date(2026, 9, 1), "A", 1.0)]), None, "px")
    n = DU.append_ignore(_px([(date(2026, 9, 1), "A", 99.0), (date(2026, 9, 2), "A", 2.0)]), None, "px")
    assert n == 1
    with price_table.connect() as conn:
        first = conn.execute(text("SELECT Close FROM px WHERE Date = '2026-09-01'")).scalar()
    assert first == 1.0


def test_append_ignore_chunks(price_table):
    rows = [(date(2026, 1, 1) + pd.Timedelta(days=i), "A", float(i)) for i in range(25)]
    assert DU.append_ignore(_px(rows), None, "px", chunk=7) == 25


def test_append_ignore_empty_frame_returns_zero(price_table):
    assert DU.append_ignore(_px([]), None, "px") == 0
    assert DU.append_ignore(None, None, "px") == 0


def test_append_ignore_swallows_errors(sqlite_engine, caplog):
    with caplog.at_level(logging.ERROR):
        result = DU.append_ignore(_px([(date(2026, 9, 1), "A", 1.0)]), "no_such_schema", "px")
    assert result is None
    assert any("append_ignore" in r.message for r in caplog.records)


def test_append_ignore_uses_mysql_verb(monkeypatch):
    """The MySQL dialect gets INSERT IGNORE, not sqlite's INSERT OR IGNORE."""
    from sqlalchemy import Column, Integer, MetaData, Table
    from sqlalchemy.dialects import mysql

    tbl = Table("t", MetaData(), Column("a", Integer, primary_key=True))
    captured = {}

    class _Conn:
        def execute(self, stmt, rows):
            captured["sql"] = str(stmt.compile(dialect=mysql.dialect()))
            return SimpleNamespace(rowcount=len(rows))

    method = DU._insert_ignore_method("IGNORE")
    assert method(SimpleNamespace(table=tbl), _Conn(), ["a"], iter([(1,), (2,)])) == 2
    assert captured["sql"].startswith("INSERT IGNORE INTO t")


# A1 -- run id / host
def test_new_run_id_is_a_ulid():
    rid = DU.new_run_id()
    assert re.fullmatch(r"[0-9A-HJKMNP-TV-Z]{26}", rid)


def test_new_run_id_sorts_by_time_and_is_unique():
    a = DU.new_run_id(now_ms=1_700_000_000_000)
    b = DU.new_run_id(now_ms=1_700_000_000_001)
    assert a < b
    assert len({DU.new_run_id() for _ in range(200)}) == 200


def test_run_host_on_lambda(monkeypatch):
    monkeypatch.setenv("AWS_LAMBDA_FUNCTION_NAME", "fin-deep-data-dev-eodDaily")
    assert DU.run_host() == "lambda:fin-deep-data-dev-eodDaily"


def test_run_host_locally(monkeypatch):
    monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)
    assert DU.run_host() and len(DU.run_host()) <= 64


# A1 -- audit
def _audit_row(**kw):
    row = DU.audit_summary("eodDaily", "histdailyprice7", "01TESTRUN0000000000000000A",
                           datetime(2026, 9, 25, 22, 30), n_rows=5)
    row.update(kw)
    return row


def test_audit_frame_shape_and_defaults():
    df = DU.audit_frame([{"run_id": "R", "job": "j", "Symbol": "A", "table_name": "t",
                          "segment": "append", "n_rows": 3, "status": "ok",
                          "started_at": datetime(2026, 1, 1), "finished_at": datetime(2026, 1, 1)}])
    assert list(df.columns) == DU.AUDIT_COLUMNS
    row = df.iloc[0]
    assert row.Exchange == "" and row.yf_version == "" and row.host
    assert row.n_ok is None and row.error is None and row.n_rows == 3


def test_audit_frame_truncates_error_to_512():
    df = DU.audit_frame([_audit_row(error="x" * 600, status="error")])
    assert len(df.iloc[0].error) == 512


def test_audit_frame_keeps_ints_as_ints():
    """857, not 857.0 -- with a None elsewhere the column would otherwise go float."""
    df = DU.audit_frame([_audit_row(n_ok=857, n_expected=857), _audit_row(n_ok=None)])
    assert df.n_ok.tolist() == [857, None]
    assert not isinstance(df.n_ok.iloc[0], float)


def test_audit_run_writes_rows(audit_table):
    rows = [_audit_row(), _audit_row(table_name="corp_action_daily", n_rows=1)]
    assert DU.audit_run(rows) == 2
    assert _count(audit_table, "load_audit") == 2


def test_audit_run_two_summary_rows_same_run_survive(audit_table):
    """The PK deviation from SR: one run, two tables, two '*' rows -- both kept."""
    DU.audit_run([_audit_row()])
    DU.audit_run([_audit_row(table_name="corp_action_daily")])
    DU.audit_run([_audit_row()])  # a true duplicate is ignored
    assert _count(audit_table, "load_audit") == 2


def test_audit_run_swallows_its_own_failure(monkeypatch, caplog):
    monkeypatch.setenv("TBLLOADAUDIT", "load_audit")
    monkeypatch.setenv("DBMKTDATA", "GlobalMarketData")
    monkeypatch.setattr(DU, "get_DBengine", lambda: (_ for _ in ()).throw(OSError("db down")))
    with caplog.at_level(logging.ERROR):
        assert DU.audit_run([_audit_row()]) is None


def test_audit_run_without_table_env_is_logged_not_raised(monkeypatch, caplog):
    """TBLLOADAUDIT absent: environ.get() would give None -> 'GlobalMarketData.None'."""
    monkeypatch.delenv("TBLLOADAUDIT", raising=False)
    with caplog.at_level(logging.ERROR):
        assert DU.audit_run([_audit_row()]) is None
    assert any("audit_run" in r.message for r in caplog.records)


# A1 -- shard / time guard
def test_shard_symbols_round_robin_over_sorted_unique():
    syms = ["D", "B", "A", "C", "E", "A"]
    assert DU.shard_symbols(syms, 0, 2) == ["A", "C", "E"]
    assert DU.shard_symbols(syms, 1, 2) == ["B", "D"]


def test_shard_symbols_partition_is_complete_and_disjoint():
    syms = [f"S{i:03d}" for i in range(857)]
    parts = [DU.shard_symbols(syms, i, 5) for i in range(5)]
    assert sorted(sum(parts, [])) == sorted(syms)
    assert max(map(len, parts)) - min(map(len, parts)) <= 1


def test_shard_symbols_rejects_bad_index():
    with pytest.raises(ValueError):
        DU.shard_symbols(["A"], 3, 3)


def test_time_left_ok():
    ctx = lambda ms: SimpleNamespace(get_remaining_time_in_millis=lambda: ms)
    assert DU.time_left_ok(None) is True
    assert DU.time_left_ok(ctx(120_001)) is True
    assert DU.time_left_ok(ctx(120_000)) is False
    assert DU.time_left_ok(ctx(5_000), reserve_ms=1_000) is True


def test_day_start_utc_follows_dst():
    assert DU.day_start_utc(date(2026, 9, 25)) == datetime(2026, 9, 25, 4, 0)
    assert DU.day_start_utc(date(2026, 12, 1)) == datetime(2026, 12, 1, 5, 0)


# A1 -- sweep
def test_missing_for_sweep(audit_table, monkeypatch):
    monkeypatch.setattr(DU, "load_df_SQL", lambda q: pd.read_sql(q.replace("GlobalMarketData.", ""), audit_table))
    base = dict(run_id="R1", job="eodDaily", Exchange="NYSE", table_name="histdailyprice7",
                segment="append", n_rows=1, started_at=datetime(2026, 9, 25, 22, 30))
    DU.audit_run([
        dict(base, Symbol="A", status="ok", finished_at=datetime(2026, 9, 25, 22, 31)),
        dict(base, Symbol="B", status="empty", finished_at=datetime(2026, 9, 25, 22, 31)),
        dict(base, Symbol="C", status="skipped", finished_at=datetime(2026, 9, 25, 22, 31)),
        dict(base, Symbol="D", status="ok", finished_at=datetime(2026, 9, 24, 22, 31)),  # yesterday
        dict(base, Symbol="E", status="ok", job="optChainEOD", finished_at=datetime(2026, 9, 25, 22, 31)),
    ])
    missing = DU.missing_for_sweep("eodDaily", date(2026, 9, 25), ["A", "B", "C", "D", "E", "F"], "histdailyprice7")
    assert missing == ["C", "D", "E", "F"]


def test_missing_for_sweep_returns_none_when_unreadable(monkeypatch):
    monkeypatch.setenv("TBLLOADAUDIT", "load_audit")
    monkeypatch.setattr(DU, "load_df_SQL", lambda q: None)
    assert DU.missing_for_sweep("eodDaily", date(2026, 9, 25), ["A"], "histdailyprice7") is None


# A1 -- env helpers / symbol procedure
def test_require_env(monkeypatch):
    monkeypatch.setenv("X_REQ", " v ")
    assert DU.require_env("X_REQ") == "v"
    monkeypatch.setenv("X_REQ", "  ")
    with pytest.raises(RuntimeError, match="X_REQ"):
        DU.require_env("X_REQ")


def test_env_or(monkeypatch):
    monkeypatch.delenv("X_OPT", raising=False)
    assert DU.env_or("X_OPT", "d") == "d"
    monkeypatch.setenv("X_OPT", "v")
    assert DU.env_or("X_OPT", "d") == "v"


@pytest.fixture
def seen_sql(monkeypatch):
    """Records the SQL load_symbols_db sends, and answers with three symbols."""
    seen = {}

    def fake(q):
        seen["q"] = q
        return pd.DataFrame({"Symbol": ["MSFT", "AAPL", "MSFT"]})

    monkeypatch.setattr(DU, "load_df_SQL", fake)
    return seen


def test_load_symbols_db_calls_versioned_procedure(seen_sql):
    assert DU.load_symbols_db("V4") == ["AAPL", "MSFT"]
    assert seen_sql["q"] == "call GlobalMarketData.current_symbols_V4"


# --------------------------------------------------------------------------
# current_symbols_V5's @type argument
#
# V1-V4 take no argument, so passing one to them is a SQL error; V5 requires
# one, because MySQL has no default argument values. load_symbols_db hides the
# difference, which is what lets a handler pass its type unconditionally while
# SYMBOL_PROC_VER still names V4. sql/current_symbols_V5.sql.
# --------------------------------------------------------------------------
@pytest.mark.parametrize("sym_type, expected", [
    ("a", "a"), ("o", "o"), ("O", "o"), (" a ", "a"),
    (None, "a"), ("", "a"), ("x", "a"), ("a'; DROP", "a"),
])
def test_symbol_proc_type_defaults_to_all(sym_type, expected):
    assert DU.symbol_proc_type(sym_type) == expected


@pytest.mark.parametrize("sym_type, arg", [("a", "'a'"), ("o", "'o'"), (None, "'a'")])
def test_load_symbols_db_sends_the_type_to_v5(seen_sql, sym_type, arg):
    assert DU.load_symbols_db("V5", sym_type) == ["AAPL", "MSFT"]
    assert seen_sql["q"] == f"call GlobalMarketData.current_symbols_V5({arg})"


@pytest.mark.parametrize("ver", ["V3", "V4", "v4"])
def test_load_symbols_db_withholds_the_type_from_older_procedures(seen_sql, ver):
    assert DU.load_symbols_db(ver, "o") == ["AAPL", "MSFT"]
    assert seen_sql["q"] == f"call GlobalMarketData.current_symbols_{ver}"


def test_load_symbols_db_assumes_a_future_version_takes_the_type(seen_sql):
    """A V6 must get the argument without a change here -- fail forward, not silent."""
    DU.load_symbols_db("V6", "o")
    assert seen_sql["q"] == "call GlobalMarketData.current_symbols_V6('o')"


def test_load_symbols_system_list_defaults_to_all(seen_sql):
    """load_symbols("system") passes no type, so V5 gets the 'a' default."""
    assert list(DU.load_symbols("system", "V5")) == ["AAPL", "MSFT"]
    assert seen_sql["q"] == "call GlobalMarketData.current_symbols_V5('a')"


# --------------------------------------------------------------------------
# out_dir -- the writable-directory rule
#
# Regression tests for the 2026-10-01 failure (doc/OPERATIONS.md 8.6): every
# handler decided this itself as `if localrun or not on Lambda: return "."`, so
# a `localrun` invoke *on Lambda* tried to write into the read-only /var/task
# and died with OSError Errno 30. The invariant that matters is the last test:
# on Lambda the answer is always under /tmp, whatever else is asked for.
# --------------------------------------------------------------------------
LAMBDA_VAR = "AWS_LAMBDA_FUNCTION_NAME"


def test_out_dir_local_localrun_is_cwd(monkeypatch):
    monkeypatch.delenv(LAMBDA_VAR, raising=False)
    assert DU.out_dir(True) == "."


def test_out_dir_local_without_localrun_is_cwd(monkeypatch):
    monkeypatch.delenv(LAMBDA_VAR, raising=False)
    assert DU.out_dir(False) == "."


def test_out_dir_local_honours_configured_dir(monkeypatch, tmp_path):
    monkeypatch.delenv(LAMBDA_VAR, raising=False)
    monkeypatch.setenv("PORT_OUTPUT_DIR", str(tmp_path))
    assert DU.out_dir(False, "PORT_OUTPUT_DIR") == str(tmp_path)


def test_out_dir_local_localrun_beats_configured_dir(monkeypatch, tmp_path):
    """A local dry run writes where the operator is standing, as documented."""
    monkeypatch.delenv(LAMBDA_VAR, raising=False)
    monkeypatch.setenv("PORT_OUTPUT_DIR", str(tmp_path))
    assert DU.out_dir(True, "PORT_OUTPUT_DIR") == "."


def test_out_dir_on_lambda_ignores_localrun(monkeypatch):
    """The bug: localrun must not win on Lambda -- /var/task is read-only."""
    monkeypatch.setenv(LAMBDA_VAR, "fin-deep-data-dev-eodDaily")
    assert DU.out_dir(True) == "/tmp"


def test_out_dir_on_lambda_allows_a_subdir_of_tmp(monkeypatch):
    monkeypatch.setenv(LAMBDA_VAR, "fin-deep-data-dev-portAssetsHandlerv2")
    monkeypatch.setenv("PORT_OUTPUT_DIR", "/tmp/port")
    assert DU.out_dir(False, "PORT_OUTPUT_DIR") == "/tmp/port"
    import os
    assert os.path.isdir("/tmp/port")


@pytest.mark.parametrize("configured", [".", "/var/task", "output", ""])
def test_out_dir_on_lambda_refuses_unwritable_configured_dir(monkeypatch, configured):
    monkeypatch.setenv(LAMBDA_VAR, "fin-deep-data-dev-portAssetsHandlerv2")
    monkeypatch.setenv("PORT_OUTPUT_DIR", configured)
    assert DU.out_dir(False, "PORT_OUTPUT_DIR") == "/tmp"


def test_on_lambda_flag(monkeypatch):
    monkeypatch.delenv(LAMBDA_VAR, raising=False)
    assert DU.on_lambda() is False
    monkeypatch.setenv(LAMBDA_VAR, "x")
    assert DU.on_lambda() is True
