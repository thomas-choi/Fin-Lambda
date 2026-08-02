"""Unit tests for the shared ``dataUtil`` module.

Scope is deliberately narrow: only ``ExecSQL``, which was changed to be
SQLAlchemy 1.4/2.0-agnostic. ``dataUtil`` is zipped into the deployment package
of every function in this service and three *live* handlers call ``ExecSQL``
(``handler.py``, ``opt_handler.py``, ``fx_handler.py``), all of them for the
delete-then-append snapshot pattern. A ``DELETE`` that silently stopped
committing would duplicate a snapshot table on every run, so the commit
semantics are pinned here rather than argued about.

The wider ``dataUtil`` backlog stays open — see ``TODOS.md`` section 1.
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

    ``TODOS.md`` section 1.7: callers all over this repo rely on this and
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
