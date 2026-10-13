"""yfNewshandlerv2 -- news_handler.py (2026-10-11, TODOS 5.10 / 2.8).

python3.13 only. Yahoo, the article pages and R2 are all stubbed: the stores
under test are ``LocalStore`` on a tmp dir and ``R2Store`` over a dict-backed
fake client, never a live bucket.
"""

from datetime import date

import pytest

import dataUtil as DU

pytestmark = pytest.mark.unit


class _ClientError(Exception):
    def __init__(self, code):
        self.response = {"Error": {"Code": code}}


class _FakeR2:
    """head/get/put_object over a dict -- the subset R2Store uses."""

    def __init__(self):
        self.objects = {}

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise _ClientError("404")
        return {}

    def put_object(self, Bucket, Key, Body, ContentType):
        self.objects[Key] = Body

    def get_object(self, Bucket, Key):
        import io
        return {"Body": io.BytesIO(self.objects[Key])}


def _items(ticker, n):
    return [{"uuid": f"{ticker}-{k}", "title": f"t{k}", "link": f"https://x/{ticker}/{k}",
             "publisher": "P", "providerPublishTime": 1, "relatedTickers": [ticker]}
            for k in range(n)]


@pytest.fixture
def yahoo(monkeypatch):
    """Stub search + page fetch; records every page fetched."""
    import news_handler as H

    state = {"pages": [], "search": [], "fail": set(), "n": 2}

    def _search(session, ticker):
        state["search"].append(ticker)
        if ticker in state["fail"]:
            raise RuntimeError("HTTP 500")
        return _items(ticker, state["n"])

    def _page(session, url):
        state["pages"].append(url)
        return "body"

    monkeypatch.setattr(H, "search_news", _search)
    monkeypatch.setattr(H, "fetch_full_text", _page)
    monkeypatch.setattr(H.time, "sleep", lambda s: None)
    monkeypatch.setattr(H, "fetch_financials", lambda t: {"income_statement_quarterly": {"x": {}}})
    monkeypatch.setattr(H, "fetch_statistics", lambda t, d: {"ticker": t, "date": d})
    return state


@pytest.fixture
def audit(env, monkeypatch):
    rows = []
    monkeypatch.setattr(DU, "audit_run", lambda r: rows.extend(r) or len(r))
    return rows


# --------------------------------------------------------------------------
# Pure helpers
# --------------------------------------------------------------------------
def test_keys_keep_the_original_layout():
    import news_handler as H

    assert H.news_key("2026-10-09", "AAPL", "u1") == "NEWS/yfinance/2026-10-09/AAPL/u1.json"
    assert H.financials_key("2026-10-09", "AAPL") == "FINANCIALS/yfinance/2026-10-09/AAPL/financials.json"
    assert H.statistics_key("2026-10-09", "AAPL") == "STATISTICS/yfinance/2026-10-09/AAPL/statistics.json"
    assert H.progress_key("2026-10-09", 2, 8) == "PROGRESS/yfinance/2026-10-09/worker-2-of-8.json"


def test_shards_cover_the_list_without_overlap():
    syms = [f"S{i:03d}" for i in range(838)]
    shards = [DU.shard_symbols(syms, i, 8) for i in range(8)]
    flat = [s for sh in shards for s in sh]
    assert sorted(flat) == syms and len(flat) == len(set(flat))


@pytest.mark.parametrize("start, expected", [(0, [1, 2, 3]), (1, [2, 3, 1]), (4, [2, 3, 1])])
def test_rotate_wraps(start, expected):
    import news_handler as H

    assert H.rotate([1, 2, 3], start) == expected
    assert H.rotate([], 3) == []


@pytest.mark.parametrize("day, expected", [(date(2026, 10, 1), True), (date(2026, 1, 1), True),
                                           (date(2026, 10, 2), False), (date(2026, 3, 1), False)])
def test_is_quarter_start(day, expected):
    import news_handler as H

    assert H.is_quarter_start(day) is expected


def test_article_record_shape():
    import news_handler as H

    rec = H.article_record(_items("AAPL", 1)[0], "AAPL", "text")
    assert rec == {"id": "AAPL-0", "ticker": "AAPL", "title": "t0", "link": "https://x/AAPL/0",
                   "publisher": "P", "providerPublishTime": 1, "full_text": "text", "tags": ["AAPL"]}


def test_df_to_serializable_dates_and_nan():
    import pandas as pd

    import news_handler as H

    df = pd.DataFrame({pd.Timestamp("2026-06-30"): [1.0, float("nan")]}, index=["Revenue", "EBIT"])
    assert H.df_to_serializable(df) == {"06/30/2026": {"Revenue": 1.0, "EBIT": None}}
    assert H.df_to_serializable(None) == {}


# --------------------------------------------------------------------------
# Stores
# --------------------------------------------------------------------------
def test_r2_store_roundtrip():
    import news_handler as H

    store = H.R2Store(_FakeR2(), "bucket")
    assert store.get_json("a/b.json") is None and not store.exists("a/b.json")
    store.put_json("a/b.json", {"x": 1})
    assert store.exists("a/b.json") and store.get_json("a/b.json") == {"x": 1}


def test_local_store_roundtrip(tmp_path):
    import news_handler as H

    store = H.LocalStore(str(tmp_path))
    store.put_json("NEWS/yfinance/d/T/u.json", {"x": 1})
    assert (tmp_path / "NEWS" / "yfinance" / "d" / "T" / "u.json").exists()
    assert store.get_json("NEWS/yfinance/d/T/u.json") == {"x": 1}


# --------------------------------------------------------------------------
# Work
# --------------------------------------------------------------------------
def test_existing_article_skips_the_page_fetch(yahoo, tmp_path):
    import news_handler as H

    store = H.LocalStore(str(tmp_path))
    store.put_json(H.news_key("2026-10-09", "AAPL", "AAPL-0"), {"old": True})
    tally = H.Tally()
    H.process_ticker(None, store, "AAPL", "2026-10-09", False, tally)
    assert yahoo["pages"] == ["https://x/AAPL/1"]            # AAPL-0 was not re-fetched
    assert tally.articles == 1
    assert store.get_json(H.news_key("2026-10-09", "AAPL", "AAPL-0")) == {"old": True}


def test_quarter_start_writes_fundamentals_once(yahoo, tmp_path):
    import news_handler as H

    store, tally = H.LocalStore(str(tmp_path)), H.Tally()
    H.process_ticker(None, store, "AAPL", "2026-10-01", True, tally)
    assert store.exists(H.financials_key("2026-10-01", "AAPL"))
    assert store.exists(H.statistics_key("2026-10-01", "AAPL"))
    H.process_ticker(None, store, "MSFT", "2026-10-01", False, tally)
    assert not store.exists(H.financials_key("2026-10-01", "MSFT"))


def test_worker_saves_checkpoint_and_next_run_resumes(yahoo, tmp_path, monkeypatch):
    import news_handler as H

    store = H.LocalStore(str(tmp_path))
    budget = iter([True, True, False])            # time for two tickers, then stop
    monkeypatch.setattr(DU, "time_left_ok", lambda ctx, reserve_ms=0: next(budget))
    tally = H.Tally()
    H.run_worker(0, 1, ["A", "B", "C", "D"], store, "2026-10-09", False, object(), tally)
    assert yahoo["search"] == ["A", "B"] and tally.skipped == 2
    assert store.get_json(H.progress_key("2026-10-09", 0, 1))["next"] == 2

    monkeypatch.setattr(DU, "time_left_ok", lambda ctx, reserve_ms=0: True)
    yahoo["search"].clear()
    H.run_worker(0, 1, ["A", "B", "C", "D"], store, "2026-10-09", False, object(), H.Tally())
    assert yahoo["search"] == ["C", "D", "A", "B"]                # resumed, then wrapped
    assert store.get_json(H.progress_key("2026-10-09", 0, 1))["next"] == 0


def test_worker_counts_a_failed_ticker_and_continues(yahoo, tmp_path):
    import news_handler as H

    yahoo["fail"].add("B")
    tally = H.Tally()
    H.run_worker(0, 1, ["A", "B", "C"], H.LocalStore(str(tmp_path)), "2026-10-09", False, None, tally)
    assert (tally.done, tally.errors, tally.articles) == (2, 1, 4)


# --------------------------------------------------------------------------
# run()
# --------------------------------------------------------------------------
def test_run_uses_v5_all_list_and_writes_r2_and_audit(yahoo, audit, monkeypatch):
    import news_handler as H

    client = _FakeR2()
    monkeypatch.setattr(DU, "r2_client", lambda: client)
    seen = []
    monkeypatch.setattr(DU, "load_symbols_db",
                        lambda ver, sym_type=None: seen.append((ver, sym_type)) or ["A", "B", "C", "D"])
    result = H.run({"quarter_start": False}, None)
    assert seen == [("V4", "a")]                      # SYMBOL_PROC_VER from the env fixture
    assert (result["tickers"], result["done"], result["articles"]) == (4, 4, 8)
    assert sum(k.startswith("NEWS/yfinance/") for k in client.objects) == 8
    (row,) = audit
    assert (row["job"], row["table_name"], row["status"], row["n_rows"], row["n_ok"], row["n_expected"]) == \
        ("yfNewshandler", "r2:news", "ok", 8, 4, 4)


def test_run_partial_when_a_ticker_fails(yahoo, audit, monkeypatch):
    import news_handler as H

    monkeypatch.setattr(DU, "r2_client", lambda: _FakeR2())
    yahoo["fail"].add("B")
    H.run({"tickers": ["a", "b"], "quarter_start": False}, None)    # also upper-cased
    assert audit[0]["status"] == "partial"


def test_dry_run_writes_local_files_only(yahoo, audit, monkeypatch, tmp_path):
    import news_handler as H

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(DU, "r2_client", lambda: pytest.fail("dry run must not open R2"))
    H.run({"localrun": True, "dbFlag": False, "tickers": ["AAPL"], "quarter_start": False}, None)
    assert audit == []
    assert len(list((tmp_path / "DATA" / "NEWS").rglob("*.json"))) == 2


def test_missing_bucket_env_raises_and_audits(yahoo, audit, monkeypatch, unset_env):
    import news_handler as H

    monkeypatch.setattr(DU, "r2_client", lambda: _FakeR2())
    unset_env("NEWS_R2_BUCKET")
    with pytest.raises(RuntimeError, match="NEWS_R2_BUCKET"):
        H.run({"tickers": ["A"]}, None)
    assert yahoo["search"] == [] and audit[0]["status"] == "error"


def test_empty_symbol_list_is_an_error(yahoo, audit, monkeypatch):
    import news_handler as H

    monkeypatch.setattr(DU, "r2_client", lambda: _FakeR2())
    monkeypatch.setattr(DU, "load_symbols_db", lambda ver, sym_type=None: [])
    with pytest.raises(RuntimeError, match="returned nothing"):
        H.run({}, None)
