"""Shared pytest fixtures for the Fin-Lambda suite.

This is the first slice of the harness described in ``TODOS.md`` section 0. It
exists to work around the four documented obstacles that kept a test suite from
existing in this repo:

* **Module-level side effects** — every handler calls ``load_dotenv()`` at
  import and several read ``environ`` into module constants at import time.
  The ``env`` fixture therefore sets variables *before* the handler is
  imported; tests that need a re-read use ``importlib.reload``.
* **Flat imports** — handlers do ``import dataUtil as DU`` with no package.
  Handled by ``pythonpath = Ops/fin-cron-data`` in ``pytest.ini``.
* **Relative CSV reads** — some handlers read CSVs from the CWD. Use the
  ``chdir_handler_dir`` fixture.
* **Global engine singleton** — ``dataUtil.dbconn`` persists across tests, so a
  mocked engine leaks into the next test. ``reset_dbconn`` is autouse.
"""

import os
import socket
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import pytz

REPO_ROOT = Path(__file__).resolve().parents[1]
HANDLER_DIR = REPO_ROOT / "Ops" / "fin-cron-data"
FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures"


# --------------------------------------------------------------------------
# Paths / payloads
# --------------------------------------------------------------------------
@pytest.fixture(scope="session")
def fixture_dir() -> Path:
    """Directory holding the saved source payloads."""
    return FIXTURE_DIR


@pytest.fixture(scope="session")
def sp500_wikipedia_html() -> bytes:
    """Saved *List of S&P 500 companies* page, trimmed to its table elements."""
    return (FIXTURE_DIR / "sp500_wikipedia.html").read_bytes()


@pytest.fixture(scope="session")
def ndx100_wikipedia_html() -> bytes:
    """Saved *List of NASDAQ-100 companies* page, trimmed to its tables."""
    return (FIXTURE_DIR / "ndx100_wikipedia.html").read_bytes()


@pytest.fixture(scope="session")
def ndx100_nasdaq_json() -> dict:
    """Saved ``api.nasdaq.com/api/quote/list-type/nasdaq100`` response."""
    import json

    return json.loads((FIXTURE_DIR / "ndx100_nasdaq_api.json").read_text())


@pytest.fixture(scope="session")
def spy_holdings_xlsx() -> bytes:
    """Saved SSGA SPY daily-holdings workbook."""
    return (FIXTURE_DIR / "spy_holdings.xlsx").read_bytes()


# --------------------------------------------------------------------------
# Environment
# --------------------------------------------------------------------------
#: The full env surface the handlers read. Values are obviously fake so that a
#: test which accidentally reaches a real service fails loudly rather than
#: touching production.
FAKE_ENV = {
    # DB connection
    "DBHOST": "test-host.invalid",
    "DBPORT": "3306",
    "DBUSER": "testuser",
    "DBPWD": "testpwd",
    "DBMKTDATA": "GlobalMarketData",
    "DBTRADING": "Trading",
    "DBPREDICT": "Stk_Predict",
    "DBWEB": "dcWebUsers",
    # Tables
    "TBLDLYPRICE": "histdailyprice7",
    "TBLMINUTEPRICE": "histminprice",
    "TBLSNAPSHOOT": "snapshot",
    "TBLFXSNAPSHOT": "FX_snapshot",
    "TBLHISTFX": "FX_histdaily",
    "TBLUSRATES": "USRates",
    "TBLOPTCHAIN": "OptionChains",
    "TBLPORTASSETS": "portfolio_assets_info",
    # Lists
    "PROD_LIST_DIR": str(HANDLER_DIR),
    "SYMBOLLIST": "system",
    "FIRSTTRAINDTE": "2010/01/01",
    # portAssetsHandler
    "SP500_PORT_NAME": "SP500",
    "NDX100_PORT_NAME": "NDX100",
    "PORT_ASSET_CLASS": "Equity",
    "PORT_ASSET_TYPE": "Stock",
    "INDEX_CROSSCHECK": "True",
}


@pytest.fixture
def env(monkeypatch, tmp_path):
    """Monkeypatch the whole env surface to safe fake values.

    Returns the applied mapping so a test can assert against it. Set
    ``PORT_OUTPUT_DIR`` to a tmp dir so no test writes into the repo.
    """
    applied = dict(FAKE_ENV)
    applied["PORT_OUTPUT_DIR"] = str(tmp_path)
    for key, value in applied.items():
        monkeypatch.setenv(key, value)
    return applied


@pytest.fixture
def unset_env(monkeypatch):
    """Return a callable that deletes env vars, for missing-variable tests."""

    def _unset(*names):
        for name in names:
            monkeypatch.delenv(name, raising=False)

    return _unset


# --------------------------------------------------------------------------
# Database
# --------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def reset_dbconn():
    """Clear ``dataUtil``'s module-global engine before and after every test.

    Without this a mocked engine set by one test leaks into the next, because
    ``get_DBengine()`` caches into a module global.
    """
    import dataUtil

    dataUtil.dbconn = None
    yield
    dataUtil.dbconn = None


@pytest.fixture
def sqlite_engine(monkeypatch):
    """A real in-memory SQLite engine installed as ``dataUtil.dbconn``.

    Lets the SQL-executing paths be exercised for real — statements actually
    run and commits are observable — with no MySQL and no network. Uses a
    shared-cache URI so a second, independent connection can verify commits.
    """
    from sqlalchemy import create_engine
    from sqlalchemy.pool import StaticPool

    import dataUtil

    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    monkeypatch.setattr(dataUtil, "dbconn", engine)
    yield engine
    engine.dispose()


@pytest.fixture
def mock_engine(monkeypatch):
    """A ``MagicMock`` standing in for the SQLAlchemy engine.

    For tests that only care *that* a query was issued, not what it returned.
    """
    import dataUtil

    engine = MagicMock(name="mock_engine")
    monkeypatch.setattr(dataUtil, "dbconn", engine)
    return engine


# --------------------------------------------------------------------------
# Time / CWD
# --------------------------------------------------------------------------
@pytest.fixture
def chdir_handler_dir(monkeypatch):
    """Run the test with the CWD set to ``Ops/fin-cron-data``.

    Handlers read CSVs by relative path, so they only work from their own
    directory (see ``CLAUDE.md``, *Local runs*).
    """
    monkeypatch.chdir(HANDLER_DIR)
    return HANDLER_DIR


@pytest.fixture
def frozen_ny_time():
    """A fixed tz-aware US/Eastern datetime: 2026-08-03 22:30 UTC equivalent.

    2026-08-03 is a Monday, so it is a valid ``MON-FRI`` schedule slot, and
    18:30 EDT is after the US close — the window ``portAssetsHandler`` runs in.
    """
    return pytz.timezone("US/Eastern").localize(datetime(2026, 8, 3, 18, 30, 0))


# --------------------------------------------------------------------------
# Network policy (TODOS.md 0.6)
# --------------------------------------------------------------------------
class NetworkAccessAttempted(RuntimeError):
    """Raised when a unit test tries to open a real socket."""


@pytest.fixture(autouse=True)
def no_network(request, monkeypatch):
    """Fail any test that opens a real network socket.

    Unit tests must never reach yfinance, MySQL, S3/R2, the Nasdaq API,
    Wikipedia or the DDS socket. Tests marked ``integration`` are exempt —
    reaching a live source is the whole point of those.
    """
    if request.node.get_closest_marker("integration"):
        return

    def _blocked(self, address, *args, **kwargs):
        raise NetworkAccessAttempted(
            f"unit test attempted a network connection to {address!r}. "
            "Use a saved fixture, or mark the test @pytest.mark.integration."
        )

    monkeypatch.setattr(socket.socket, "connect", _blocked)
    monkeypatch.setattr(socket.socket, "connect_ex", _blocked)
    monkeypatch.setattr(
        socket, "create_connection", lambda address, *a, **k: _blocked(None, address)
    )
