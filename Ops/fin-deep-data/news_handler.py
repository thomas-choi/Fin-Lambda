"""yfNewshandlerv2 -- hourly Yahoo news (and quarterly fundamentals) into R2.

A python3.13 port of ``Ops/fin-cron-data/yf-news-collect.py`` (TODOS 5.10),
which fails on every invocation as deployed: ``DEFAULT_TICKERS`` names
``stock_list_global.csv``, which exists nowhere, and ``LOCALRUN`` defaults to
True so it would have written to the read-only bundle anyway. The original
stays deployed and untouched; this one's schedule ships **disabled**.

``load_audit.job`` is ``yfNewshandler``; ``table_name`` is ``r2:news``.

What changed:

* **symbols** come from ``current_symbols_{SYMBOL_PROC_VER}('a')`` -- the same
  list ``eodDaily`` collects -- instead of a CSV;
* **one full pass per run, on a thread pool.** The list is split into
  ``NEWS_THREADS`` disjoint sub-lists (``DU.shard_symbols``) and each worker
  thread walks its own. The original did 25 tickers per invocation;
* **the existence check comes before the full-text fetch.** The original
  downloaded the article page of every one of up to 50 search results per
  ticker and only then skipped the ones already stored; an hourly re-scan now
  costs one search call per ticker plus one HEAD per result;
* **timeout resume.** A worker near the Lambda timeout stores its position in
  ``PROGRESS/yfinance/{date}/worker-{i}-of-{n}.json``; the next run starts each
  worker there and wraps around, so a long pass still covers every ticker;
* ``localrun`` / ``dbFlag`` follow the service-wide contract: ``dbFlag=False``
  writes JSON under ``DU.out_dir(localrun)/DATA`` and nothing to R2 or
  ``load_audit``. ``LOCALRUN``, ``DEFAULT_TICKERS``, ``S3_BUCKET`` and
  ``BATCH_SIZE`` are no longer read.

R2 key layout, unchanged from the original (bucket ``NEWS_R2_BUCKET``)::

    NEWS/yfinance/{YYYY-MM-DD}/{TICKER}/{uuid}.json
    FINANCIALS/yfinance/{YYYY-MM-DD}/{TICKER}/financials.json    (quarter start)
    STATISTICS/yfinance/{YYYY-MM-DD}/{TICKER}/statistics.json    (quarter start)

``{YYYY-MM-DD}`` is the UTC run date, so an article first seen on a later day
is stored again under that day -- as before.

Runs on **python3.13** with the ``finDeepCore`` + ``finDeepYf`` layers
(``bs4`` ships in ``finDeepYf``; ``boto3`` in the Lambda runtime).

Local dry run (six tickers, files under ./DATA, no R2, no audit row)::

    cd Ops/fin-deep-data
    ../../venv-py313/bin/python news_handler.py
"""

import json
import logging
import os
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import pandas as pd
import requests
import yfinance as yf
from bs4 import BeautifulSoup
from dotenv import load_dotenv

import dataUtil as DU

logger = logging.getLogger()

load_dotenv()

JOB = "yfNewshandler"
AUDIT_TABLE = "r2:news"
SOURCE = "yfinance"

SEARCH_URL = "https://query1.finance.yahoo.com/v1/finance/search"
NEWS_COUNT = 50
HTTP_TIMEOUT = 10
#: Stop starting new tickers with this much Lambda time left.
RESERVE_MS = 90_000

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"),
    "Accept-Language": "en-US,en;q=0.9",
}

QUARTER_STARTS = {(1, 1), (4, 1), (7, 1), (10, 1)}

STAT_MAP = {
    'Market Cap': 'marketCap',
    'Enterprise Value': 'enterpriseValue',
    'Trailing P/E': 'trailingPE',
    'Forward P/E': 'forwardPE',
    'PEG Ratio': 'pegRatio',
    'Price/Sales (ttm)': 'priceToSalesTrailing12Months',
    'Price/Book': 'priceToBook',
    'Enterprise/Revenue': 'enterpriseToRevenue',
    'Enterprise/EBITDA': 'enterpriseToEbitda',
    'EBITDA': 'ebitda',
}


# --------------------------------------------------------------------------
# Keys
# --------------------------------------------------------------------------
def news_key(run_date, ticker, uid):
    return f"NEWS/{SOURCE}/{run_date}/{ticker}/{uid}.json"


def financials_key(run_date, ticker):
    return f"FINANCIALS/{SOURCE}/{run_date}/{ticker}/financials.json"


def statistics_key(run_date, ticker):
    return f"STATISTICS/{SOURCE}/{run_date}/{ticker}/statistics.json"


def progress_key(run_date, worker, n_workers):
    """Per-worker checkpoint. ``n_workers`` is in the name, so changing
    NEWS_THREADS between runs starts fresh instead of reusing a position that
    indexes a different sub-list."""
    return f"PROGRESS/{SOURCE}/{run_date}/worker-{worker}-of-{n_workers}.json"


# --------------------------------------------------------------------------
# Stores -- R2 in production, a local folder for a dry run. Same interface.
# --------------------------------------------------------------------------
class R2Store:
    def __init__(self, client, bucket):
        self.client, self.bucket = client, bucket

    def exists(self, key):
        return DU.r2_exists(self.client, self.bucket, key)

    def put_json(self, key, obj):
        DU.r2_put_json(self.client, self.bucket, key, obj)

    def get_json(self, key):
        """The object as JSON, or None when it does not exist."""
        if not self.exists(key):
            return None
        body = self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()
        return json.loads(body.decode("utf-8"))


class LocalStore:
    def __init__(self, root):
        self.root = root

    def _path(self, key):
        return os.path.join(self.root, *key.split("/"))

    def exists(self, key):
        return os.path.exists(self._path(key))

    def put_json(self, key, obj):
        path = self._path(key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2, default=str)

    def get_json(self, key):
        if not self.exists(key):
            return None
        with open(self._path(key), encoding="utf-8") as f:
            return json.load(f)


# --------------------------------------------------------------------------
# Yahoo
# --------------------------------------------------------------------------
def new_session():
    """One requests.Session per worker thread: Session is not thread-safe."""
    session = requests.Session()
    session.headers.update(HEADERS)
    return session


def search_news(session, ticker):
    """Raw news items from Yahoo's search endpoint. Raises on HTTP failure."""
    resp = session.get(SEARCH_URL, params={"q": ticker, "newsCount": NEWS_COUNT, "quotesCount": 0},
                       timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    return resp.json().get("news", []) or []


def fetch_full_text(session, url):
    """Paragraph text of an article page; '' when it cannot be read."""
    try:
        resp = session.get(url, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        container = soup.find("article") or soup
        return "\n\n".join(p.get_text(strip=True) for p in container.find_all("p"))
    except Exception as e:
        logging.debug(f"Could not fetch full text from {url}: {e}")
        return ""


def article_record(item, ticker, full_text):
    """The stored JSON for one search result -- the original's shape. Pure."""
    return {
        "id": item.get("uuid"),
        "ticker": ticker,
        "title": item.get("title"),
        "link": item.get("link"),
        "publisher": item.get("publisher"),
        "providerPublishTime": item.get("providerPublishTime"),
        "full_text": full_text,
        "tags": item.get("relatedTickers", []),
    }


def df_to_serializable(df):
    """yfinance statement frame -> {MM/DD/YYYY: {line item: value}}. Pure."""
    out = {}
    if df is None:
        return out
    for col, inner in df.to_dict().items():
        date_key = col.strftime("%m/%d/%Y") if hasattr(col, "strftime") else str(col)
        out[date_key] = {str(k): (None if pd.isna(v) else v) for k, v in inner.items()}
    return out


def fetch_financials(ticker):
    tkr = yf.Ticker(ticker)
    return {
        "income_statement_quarterly": df_to_serializable(tkr.quarterly_financials),
        "balance_sheet_quarterly": df_to_serializable(tkr.quarterly_balance_sheet),
        "cash_flow_quarterly": df_to_serializable(tkr.quarterly_cashflow),
    }


def fetch_statistics(ticker, run_date):
    info = yf.Ticker(ticker).info or {}
    rec = {"ticker": ticker, "date": run_date}
    for label, key in STAT_MAP.items():
        rec[label] = info.get(key)
    return rec


def is_quarter_start(day):
    return (day.month, day.day) in QUARTER_STARTS


# --------------------------------------------------------------------------
# Work
# --------------------------------------------------------------------------
class Tally:
    """Thread-safe counters for the run summary."""

    def __init__(self):
        self._lock = threading.Lock()
        self.done = self.articles = self.errors = self.skipped = 0

    def add(self, **counts):
        with self._lock:
            for name, n in counts.items():
                setattr(self, name, getattr(self, name) + n)


def process_ticker(session, store, ticker, run_date, quarter, tally):
    """Store the ticker's new articles (and fundamentals on a quarter start)."""
    items = search_news(session, ticker)
    new = 0
    for item in items:
        uid = item.get("uuid")
        if not uid:
            continue
        key = news_key(run_date, ticker, uid)
        if store.exists(key):          # before the page fetch -- the expensive part
            continue
        link = item.get("link")
        store.put_json(key, article_record(item, ticker, fetch_full_text(session, link) if link else ""))
        new += 1
        time.sleep(random.uniform(0.01, 0.3))
    if quarter:
        key = financials_key(run_date, ticker)
        if not store.exists(key):
            data = fetch_financials(ticker)
            if any(data.values()):
                store.put_json(key, data)
        key = statistics_key(run_date, ticker)
        if not store.exists(key):
            store.put_json(key, fetch_statistics(ticker, run_date))
    tally.add(articles=new)
    logging.info(f"{ticker}: {len(items)} item(s), {new} new")


def rotate(items, start):
    """items from position start, wrapping around. Pure."""
    if not items:
        return []
    start = start % len(items)
    return items[start:] + items[:start]


def run_worker(i, n, sub_list, store, run_date, quarter, context, tally):
    """Walk one sub-list from its checkpoint; save a checkpoint if time runs out."""
    session = new_session()
    pkey = progress_key(run_date, i, n)
    try:
        start = int((store.get_json(pkey) or {}).get("next", 0))
    except Exception:
        logging.error(f"worker {i}: progress unreadable, starting at 0", exc_info=True)
        start = 0
    order = rotate(sub_list, start)
    for k, ticker in enumerate(order):
        if not DU.time_left_ok(context, reserve_ms=RESERVE_MS):
            remaining = len(order) - k
            tally.add(skipped=remaining)
            nxt = (start + k) % len(sub_list)
            store.put_json(pkey, {"next": nxt, "total": len(sub_list), "run_date": run_date})
            logging.warning(f"worker {i}: time guard, {remaining} ticker(s) left, resume at {nxt}")
            return
        try:
            process_ticker(session, store, ticker, run_date, quarter, tally)
            tally.add(done=1)
        except Exception as e:
            tally.add(errors=1)
            logging.error(f"worker {i}: {ticker} failed: {type(e).__name__}: {e}")
    if start:
        store.put_json(pkey, {"next": 0, "total": len(sub_list), "run_date": run_date})


def news_run(event, context, localrun=False, dbFlag=True):
    run_date = datetime.now(timezone.utc).date()
    threads = int(DU.env_or("NEWS_THREADS", "8"))
    if dbFlag:
        store = R2Store(DU.r2_client(), DU.require_env("NEWS_R2_BUCKET"))
    else:
        store = LocalStore(os.path.join(DU.out_dir(localrun), "DATA"))

    if event.get("tickers"):
        symbols = sorted({str(t).strip().upper() for t in event["tickers"] if str(t).strip()})
    else:
        ver = DU.require_env("SYMBOL_PROC_VER")
        symbols = DU.load_symbols_db(ver, "a")
        if not symbols:
            raise RuntimeError(f"symbol list current_symbols_{ver} returned nothing")
    cap = event.get("test")
    if isinstance(cap, int) and not isinstance(cap, bool) and cap > 0:
        symbols = symbols[:cap]

    flag = event.get("quarter_start")
    quarter = is_quarter_start(run_date) if flag is None else str(flag).lower() in ("true", "1")
    n = max(1, min(threads, len(symbols)))
    logging.info(f"{len(symbols)} ticker(s) on {n} worker(s), run date {run_date}, quarter_start={quarter}")

    tally = Tally()
    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=n) as pool:
        futures = [pool.submit(run_worker, i, n, DU.shard_symbols(symbols, i, n), store,
                               run_date.isoformat(), quarter, context, tally) for i in range(n)]
        for f in futures:
            f.result()
    logging.info(f"{tally.done}/{len(symbols)} ticker(s) in {time.monotonic() - t0:.1f} s: "
                 f"{tally.articles} new article(s), {tally.errors} error(s), {tally.skipped} left for next run")
    return {"date": run_date.isoformat(), "tickers": len(symbols), "done": tally.done,
            "articles": tally.articles, "errors": tally.errors, "skipped": tally.skipped,
            "quarter_start": quarter}


def _audit(run_id, started_at, result, status="ok", error=None):
    """One load_audit summary row. Never raises."""
    try:
        if result is not None and status == "ok" and (result["errors"] or result["skipped"]):
            status = "partial"
        day = DU.utc_now().date()
        DU.audit_run([DU.audit_summary(
            JOB, AUDIT_TABLE, run_id, started_at, status=status,
            n_rows=0 if result is None else result["articles"],
            n_ok=None if result is None else result["done"],
            n_expected=None if result is None else result["tickers"],
            date_lo=day, date_hi=day, error=error, yf_version=yf.__version__[:16])])
    except Exception:
        logging.error("load_audit summary failed", exc_info=True)


def run(event, context):
    """Lambda entry point.

    Event keys: ``localrun`` / ``dbFlag`` (False = local JSON only, no R2 and no
    audit row), ``test`` (an int caps the ticker count; any value turns on DEBUG
    logging), ``tickers`` (explicit list instead of the symbol procedure),
    ``quarter_start`` (force the fundamentals on or off).
    """
    event = dict(event or {})
    logger.setLevel(logging.DEBUG if "test" in event else logging.INFO)
    logging.info(f"** ==> news_handler.run(event: {event}, context: {context})")
    localrun = bool(event.get("localrun", False))
    dbFlag = bool(event.get("dbFlag", True)) and not localrun
    run_id, started_at = DU.new_run_id(), DU.utc_now()
    try:
        result = news_run(event, context, localrun=localrun, dbFlag=dbFlag)
    except Exception as e:
        if dbFlag:
            _audit(run_id, started_at, None, status="error", error=f"{type(e).__name__}: {e}")
        raise
    if dbFlag:
        _audit(run_id, started_at, result)
    return {"run_id": run_id, **result}


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    # Dry run: searches Yahoo for six tickers, writes JSON under ./DATA, and
    # touches neither R2 nor load_audit. An explicit list, because the first
    # tickers of the sorted V5 list are Asian listings Yahoo's search returns
    # no news for. Drop "tickers" to exercise the symbol procedure instead.
    print(run({"localrun": True, "dbFlag": False, "test": True,
               "tickers": ["AAPL", "MSFT", "NVDA", "SPY", "XOM", "0700.HK"]}, None))
