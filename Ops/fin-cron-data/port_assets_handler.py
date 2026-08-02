"""portAssetsHandler -- S&P 500 / NASDAQ-100 index membership.

Downloads the current constituent list of the S&P 500 and the NASDAQ-100,
writes each to a CSV for eyeball verification, and appends the membership set
to ``Trading.portfolio_assets_info`` -- but only when the set has actually
changed since the last stored one.

Runs on **python3.13** with its own layer (``finPort313``), unlike the nine
python3.10 functions in this service. ``dataUtil`` is shared with them and is
SQLAlchemy 1.4/2.0-agnostic, so it works on both stacks unmodified.

Local dry run (writes CSVs, touches no database)::

    cd Ops/fin-cron-data          # required: flat imports
    python port_assets_handler.py

Everything that parses a payload or decides what to write is a pure, importable
function -- that is the unit-test surface in
``tests/unit/test_port_assets_handler.py``. Only thin wrappers touch the
network or the database.
"""

import json
import logging
import os
from collections import namedtuple
from datetime import date, datetime
from io import BytesIO
from os import environ

import pandas as pd
import pytz
import requests
from dotenv import load_dotenv

import dataUtil as DU

logger = logging.getLogger()

load_dotenv()

# --------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------

#: Column order of Trading.portfolio_assets_info, as created in the database.
TABLE_COLUMNS = [
    "Date",
    "Port_name",
    "Symbol",
    "Class",
    "Sector",
    "Type",
    "Currency",
    "Rate",
]

#: Columns that define whether a membership set has changed. Deliberately the
#: full attribute tuple, not just Symbol -- a GICS reclassification with no
#: membership change is still a new point-in-time set.
COMPARE_COLUMNS = ["Symbol", "Class", "Sector", "Type", "Currency", "Rate"]

#: Both indices are 100% USD, so Rate -- the FX rate to USD -- is always 1.0.
BASE_CURRENCY = "USD"
BASE_RATE = 1.0

#: The Nasdaq API returns an empty string for every sector, so NDX100 rows get
#: a sentinel. 'General' is what DJI and HSI already use in this table for
#: exactly this case; reusing it keeps Sector comparable across Port_name.
#: Mixing Wikipedia's ICB taxonomy for NDX with the S&P page's GICS would make
#: the column meaningless (AAPL would read 'Information Technology' under
#: SP500 and 'Technology' under NDX100).
NO_SECTOR = "General"

#: Row-count sanity bands. Outside these, the parse is assumed broken and
#: nothing is written -- the guard against a page restructure silently wiping
#: a membership set down to three rows.
SP500_BAND = (490, 520)
NDX100_BAND = (95, 110)

SP500_WIKIPEDIA_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
NDX100_NASDAQ_API_URL = "https://api.nasdaq.com/api/quote/list-type/nasdaq100"
NDX100_WIKIPEDIA_URL = "https://en.wikipedia.org/wiki/List_of_NASDAQ-100_companies"
SPY_HOLDINGS_URL = (
    "https://www.ssga.com/us/en/intermediary/library-content/products/"
    "fund-data/etfs/us/holdings-daily-us-en-spy.xlsx"
)

#: Wikimedia's bot policy requires a descriptive User-Agent; the default
#: python-requests one gets blocked.
WIKI_HEADERS = {
    "User-Agent": "Fin-Lambda/1.0 (portAssetsHandler; twmchoi2010@gmail.com) python-requests"
}
#: api.nasdaq.com and ssga.com both reject non-browser agents.
BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
}

HTTP_TIMEOUT = 60

#: What run() decided to do with one index.
WriteDecision = namedtuple("WriteDecision", "action write_date reason")


# --------------------------------------------------------------------------
# Pure helpers
# --------------------------------------------------------------------------
def normalize_us_symbol(symbol):
    """Return a US ticker in the dashed form used by yfinance-fed tables.

    Wikipedia publishes class shares dotted (``BRK.B``, ``BF.B``); yfinance --
    and therefore every price table in this database -- uses ``BRK-B``. Storing
    the dashed form is what lets ``portfolio_assets_info`` be joined to price
    history without a per-symbol fixup.

    Scoped to US symbols on purpose: a blanket '.'->'-' would corrupt foreign
    listings like ``0700.HK``, and neither index contains any.
    """
    if symbol is None:
        return None
    cleaned = str(symbol).replace(" ", " ").strip().upper()
    return cleaned.replace(".", "-")


def _pick_table(tables, required_columns):
    """Return the first parsed table containing every required column.

    Selecting by content rather than by list index. ``read_html()[0]`` happens
    to be right today, but the NASDAQ-100 constituent table has already moved
    once (off ``/wiki/Nasdaq-100`` onto ``/wiki/List_of_NASDAQ-100_companies``),
    and the NDX page alone parses to 30+ tables.
    """
    for table in tables:
        columns = {str(c) for c in table.columns}
        if all(
            any(str(req).lower() in col.lower() for col in columns)
            for req in required_columns
        ):
            return table
    raise ValueError(
        f"no parsed table carried all of {required_columns}; "
        f"the source page structure has probably changed"
    )


def parse_sp500_wikipedia(html):
    """Parse *List of S&P 500 companies* into ``DataFrame[Symbol, Sector]``.

    Sector is the page's GICS Sector, which is the taxonomy the SP500 rows in
    ``portfolio_assets_info`` carry.
    """
    tables = pd.read_html(BytesIO(html) if isinstance(html, bytes) else html)
    table = _pick_table(tables, ["Symbol", "GICS Sector"])

    sector_col = next(c for c in table.columns if "GICS Sector" in str(c))
    out = pd.DataFrame(
        {
            "Symbol": table["Symbol"].map(normalize_us_symbol),
            "Sector": table[sector_col].astype(str).str.strip(),
        }
    )
    return out.dropna(subset=["Symbol"]).drop_duplicates("Symbol").reset_index(drop=True)


def parse_ndx_nasdaq_api(payload):
    """Parse the official Nasdaq API payload into ``DataFrame[Symbol, Sector]``.

    The rows live at ``data.data.rows`` -- note the doubled ``data`` -- not at
    ``data.rows``. Sector is forced to :data:`NO_SECTOR`: the API's own
    ``sector`` field is an empty string on every row, and pinning the constant
    here means a future API change cannot silently start mixing a second
    taxonomy into the column.
    """
    if isinstance(payload, (bytes, str)):
        payload = json.loads(payload)

    rows = payload.get("data", {}).get("data", {}).get("rows")
    if not rows:
        raise ValueError(
            "Nasdaq API payload had no data.data.rows; the response shape changed"
        )

    symbols = [normalize_us_symbol(r.get("symbol")) for r in rows]
    out = pd.DataFrame({"Symbol": symbols, "Sector": NO_SECTOR})
    return out.dropna(subset=["Symbol"]).drop_duplicates("Symbol").reset_index(drop=True)


def parse_ndx_nasdaq_asof(payload):
    """Return the Nasdaq API's own as-of date, or None if unparseable."""
    if isinstance(payload, (bytes, str)):
        payload = json.loads(payload)
    raw = payload.get("data", {}).get("date")
    if not raw:
        return None
    try:
        return datetime.strptime(str(raw).strip(), "%b %d, %Y").date()
    except ValueError:
        logging.warning(f"could not parse Nasdaq API as-of date {raw!r}")
        return None


def parse_ndx_wikipedia(html):
    """Parse *List of NASDAQ-100 companies* into ``DataFrame[Symbol, Sector]``.

    Fallback source, used for **membership only**. The page classifies under
    ICB, so its sector columns are deliberately discarded and Sector is set to
    :data:`NO_SECTOR`, exactly as the primary Nasdaq path does.
    """
    tables = pd.read_html(BytesIO(html) if isinstance(html, bytes) else html)
    table = _pick_table(tables, ["Ticker", "Company"])

    out = pd.DataFrame(
        {"Symbol": table["Ticker"].map(normalize_us_symbol), "Sector": NO_SECTOR}
    )
    return out.dropna(subset=["Symbol"]).drop_duplicates("Symbol").reset_index(drop=True)


def parse_spy_holdings(content):
    """Parse the SSGA SPY daily-holdings workbook.

    Returns ``(DataFrame[Symbol], as_of_date_or_None)``. Membership cross-check
    only -- the workbook's ``Sector`` column is ``-`` on every row, so it
    carries no usable sector.

    The real header sits on sheet row 4 under three metadata rows
    (``Fund Name:``, ``Ticker Symbol:``, ``Holdings: As of 30-Jul-2026``), and
    the sheet ends with blank rows plus a legal footer.
    """
    raw = pd.read_excel(
        BytesIO(content) if isinstance(content, bytes) else content, header=None
    )

    as_of = None
    header_row = None
    for idx in range(min(15, len(raw))):
        cells = [str(v) for v in raw.iloc[idx].tolist() if pd.notna(v)]
        joined = " ".join(cells)
        if as_of is None and "As of" in joined:
            token = joined.split("As of", 1)[1].strip().split()[0].strip(" ,")
            try:
                as_of = datetime.strptime(token, "%d-%b-%Y").date()
            except ValueError:
                logging.warning(f"could not parse SPY as-of token {token!r}")
        if header_row is None and "Ticker" in cells and "Name" in cells:
            header_row = idx

    if header_row is None:
        raise ValueError("SPY holdings sheet had no Name/Ticker header row")

    table = raw.iloc[header_row + 1 :].copy()
    table.columns = [str(c).strip() for c in raw.iloc[header_row].tolist()]

    symbols = (
        table["Ticker"]
        .dropna()
        .map(normalize_us_symbol)
        # drop the trailing legal footer and any cash/placeholder lines
        .loc[lambda s: s.str.len().between(1, 10) & ~s.str.contains(r"\s", regex=True)]
    )
    out = pd.DataFrame({"Symbol": symbols}).drop_duplicates("Symbol")
    return out.reset_index(drop=True), as_of


def sanity_gate(df, low, high, label=""):
    """True when the row count is inside the plausible band for this index.

    The guard that stops a broken parse from overwriting a good membership set.
    """
    count = 0 if df is None else len(df)
    if low <= count <= high:
        logging.info(f"sanity_gate[{label}]: {count} rows, inside [{low}, {high}]")
        return True
    logging.error(
        f"sanity_gate[{label}]: {count} rows is outside [{low}, {high}] -- "
        f"refusing to write. The source layout has probably changed."
    )
    return False


def build_frame(
    members,
    port_name,
    effective_date,
    asset_class,
    asset_type,
    currency=BASE_CURRENCY,
    rate=BASE_RATE,
):
    """Build the exact eight table columns, in DDL order.

    ``members`` is a frame carrying at least ``Symbol``; ``Sector`` is used
    when present and falls back to :data:`NO_SECTOR`.
    """
    if isinstance(effective_date, datetime):
        effective_date = effective_date.date()
    elif isinstance(effective_date, pd.Timestamp):
        effective_date = effective_date.date()

    sector = (
        members["Sector"].fillna(NO_SECTOR).replace("", NO_SECTOR)
        if "Sector" in members.columns
        else NO_SECTOR
    )

    frame = pd.DataFrame(
        {
            "Date": effective_date,
            "Port_name": port_name,
            "Symbol": members["Symbol"].values,
            "Class": asset_class,
            "Sector": sector.values if hasattr(sector, "values") else sector,
            "Type": asset_type,
            "Currency": currency,
            "Rate": float(rate),
        },
        columns=TABLE_COLUMNS,
    )
    return frame.sort_values("Symbol").reset_index(drop=True)[TABLE_COLUMNS]


def _canonical(df):
    """Normalise a set for comparison: sorted, NaN-safe, stable dtypes."""
    if df is None or len(df) == 0:
        return None
    out = df.loc[:, COMPARE_COLUMNS].copy()
    for col in ["Symbol", "Class", "Sector", "Type", "Currency"]:
        out[col] = out[col].fillna("").astype(str).str.strip()
    out["Rate"] = pd.to_numeric(out["Rate"], errors="coerce").fillna(0.0).round(6)
    return out.sort_values("Symbol").reset_index(drop=True)


def has_changed(new_df, old_df):
    """True when the membership set differs from the stored one.

    Compares the full row tuple, not just the symbol set, so a sector
    reclassification counts as a change. Row order is irrelevant and NaN and
    None sectors compare equal, since ``Sector`` can legitimately be missing.
    """
    old = _canonical(old_df)
    if old is None:
        return True
    new = _canonical(new_df)
    if new is None:
        return True
    return not new.equals(old)


def resolve_effective_date(new_date, stored_max, changed, force=False):
    """Decide whether -- and under which date -- to write this set.

    Returns a :data:`WriteDecision` whose ``action`` is one of ``write``,
    ``replace`` (delete that date first, making a same-day re-run idempotent)
    or ``skip``.
    """
    if isinstance(new_date, datetime):
        new_date = new_date.date()
    if isinstance(stored_max, datetime):
        stored_max = stored_max.date()

    if stored_max is None:
        return WriteDecision("write", new_date, "no rows stored yet; seeding the table")

    if new_date < stored_max:
        return WriteDecision(
            "skip",
            None,
            f"source date {new_date} is older than the stored max {stored_max}; "
            "writing would create an out-of-order set and break as-of queries",
        )

    if not changed and not force:
        return WriteDecision(
            "skip", None, "membership set is unchanged since the last stored set"
        )

    reason = "forced" if (not changed and force) else "membership set changed"
    if new_date == stored_max:
        return WriteDecision(
            "replace", new_date, f"{reason}; replacing the set already stored for {new_date}"
        )
    return WriteDecision("write", new_date, reason)


def pick_effective_date(source_asof, ny_now):
    """Prefer the source's own as-of date, else the NY-local run date."""
    if source_asof is not None:
        return source_asof
    if isinstance(ny_now, datetime):
        return ny_now.date()
    return ny_now


# --------------------------------------------------------------------------
# Environment
# --------------------------------------------------------------------------
def _require_env(name):
    """Read a required variable, failing loudly rather than silently None.

    ``environ.get()`` returning None is an established failure class in this
    repo: it interpolates the string 'None' into SQL and only surfaces much
    later as a confusing database error.
    """
    value = environ.get(name)
    if value is None or str(value).strip() == "":
        raise RuntimeError(
            f"required environment variable {name} is not set. "
            f"Add it to Ops/fin-cron-data/.env (see .env.example)."
        )
    return value.strip()


def _env(name, default):
    value = environ.get(name)
    return default if value is None or str(value).strip() == "" else value.strip()


def _truthy(value, default=True):
    if value is None:
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "y", "on")


def _output_dir(localrun):
    """Where the verification CSVs go.

    CWD for a local run, ``PORT_OUTPUT_DIR`` otherwise, defaulting to ``/tmp``
    on Lambda where the bundle directory is read-only.
    """
    if localrun:
        return "."
    on_lambda = environ.get("AWS_LAMBDA_FUNCTION_NAME") is not None
    return _env("PORT_OUTPUT_DIR", "/tmp" if on_lambda else ".")


# --------------------------------------------------------------------------
# Network wrappers -- thin on purpose, the parsing above is what gets tested
# --------------------------------------------------------------------------
def fetch_sp500():
    """Fetch S&P 500 membership from Wikipedia. Returns ``(df, source_name)``."""
    response = requests.get(SP500_WIKIPEDIA_URL, headers=WIKI_HEADERS, timeout=HTTP_TIMEOUT)
    response.raise_for_status()
    return parse_sp500_wikipedia(response.content), "wikipedia:List_of_S&P_500_companies"


def fetch_spy_holdings():
    """Fetch the SSGA SPY holdings cross-check. Returns ``(df, as_of)`` or ``(None, None)``."""
    try:
        response = requests.get(
            SPY_HOLDINGS_URL, headers=BROWSER_HEADERS, timeout=HTTP_TIMEOUT
        )
        response.raise_for_status()
        return parse_spy_holdings(response.content)
    except Exception:
        logging.warning("SPY holdings cross-check unavailable", exc_info=True)
        return None, None


def fetch_ndx100():
    """Fetch NASDAQ-100 membership. Returns ``(df, source_name, as_of)``.

    The official Nasdaq API is primary; Wikipedia is the automatic fallback,
    since AWS egress IPs are filtered harder than residential ones and the API
    is the more likely of the two to start refusing us.
    """
    try:
        response = requests.get(
            NDX100_NASDAQ_API_URL, headers=BROWSER_HEADERS, timeout=HTTP_TIMEOUT
        )
        response.raise_for_status()
        payload = response.json()
        return (
            parse_ndx_nasdaq_api(payload),
            "nasdaq-api:list-type/nasdaq100",
            parse_ndx_nasdaq_asof(payload),
        )
    except Exception:
        logging.warning(
            "Nasdaq API unavailable, falling back to Wikipedia", exc_info=True
        )

    response = requests.get(
        NDX100_WIKIPEDIA_URL, headers=WIKI_HEADERS, timeout=HTTP_TIMEOUT
    )
    response.raise_for_status()
    return (
        parse_ndx_wikipedia(response.content),
        "wikipedia:List_of_NASDAQ-100_companies",
        None,
    )


def fetch_ndx100_crosscheck():
    """Wikipedia NDX membership, used only to log a symmetric difference."""
    try:
        response = requests.get(
            NDX100_WIKIPEDIA_URL, headers=WIKI_HEADERS, timeout=HTTP_TIMEOUT
        )
        response.raise_for_status()
        return parse_ndx_wikipedia(response.content)
    except Exception:
        logging.warning("NDX Wikipedia cross-check unavailable", exc_info=True)
        return None


def log_symmetric_difference(label, primary, secondary):
    """Log how two sources disagree. Never fatal -- both sources are scrapes."""
    if primary is None or secondary is None:
        logging.info(f"cross-check[{label}]: skipped, second source unavailable")
        return
    a, b = set(primary["Symbol"]), set(secondary["Symbol"])
    only_primary, only_secondary = sorted(a - b), sorted(b - a)
    if not only_primary and not only_secondary:
        logging.info(f"cross-check[{label}]: both sources agree on {len(a)} symbols")
        return
    logging.warning(
        f"cross-check[{label}]: sources disagree -- "
        f"{len(only_primary)} only in primary {only_primary[:15]}, "
        f"{len(only_secondary)} only in secondary {only_secondary[:15]}"
    )


# --------------------------------------------------------------------------
# Database wrappers
# --------------------------------------------------------------------------
def get_stored_max_date(db, table, port_name):
    """Newest Date stored for this Port_name, or None if the set is unseeded."""
    df = DU.load_df_SQL(
        f"SELECT max(Date) as maxdate FROM {db}.{table} "
        f"WHERE Port_name = '{port_name}';"
    )
    if df is None or len(df) == 0 or pd.isna(df.maxdate.iloc[0]):
        return None
    value = df.maxdate.iloc[0]
    return value.date() if isinstance(value, (datetime, pd.Timestamp)) else value


def load_stored_set(db, table, port_name, on_date):
    """The stored membership set for a Port_name on a given date."""
    if on_date is None:
        return None
    return DU.load_df_SQL(
        f"SELECT {', '.join(COMPARE_COLUMNS)} FROM {db}.{table} "
        f"WHERE Port_name = '{port_name}' AND Date = '{on_date}';"
    )


def count_stored_rows(db, table, port_name, on_date):
    df = DU.load_df_SQL(
        f"SELECT count(*) as n FROM {db}.{table} "
        f"WHERE Port_name = '{port_name}' AND Date = '{on_date}';"
    )
    return None if df is None or len(df) == 0 else int(df.n.iloc[0])


def write_set(db, table, frame, port_name, write_date, replace):
    """Write one dated membership set and verify the row count afterwards.

    ``StoreEOD`` wraps ``to_sql`` in try/except and only *logs* on failure, so
    a duplicate-key IntegrityError would otherwise be swallowed and the run
    would report success having written nothing. Hence the explicit recount.
    """
    if replace:
        deleted = DU.ExecSQL(
            f"DELETE FROM {db}.{table} "
            f"WHERE Port_name = '{port_name}' AND Date = '{write_date}';"
        )
        logging.info(f"[{port_name}] deleted {deleted} existing rows for {write_date}")

    DU.StoreEOD(frame, db, table)

    stored = count_stored_rows(db, table, port_name, write_date)
    if stored != len(frame):
        logging.error(
            f"[{port_name}] WRITE VERIFICATION FAILED: expected {len(frame)} rows for "
            f"{write_date} but the table holds {stored}. StoreEOD swallows write "
            f"errors -- check the log above for an IntegrityError."
        )
        return False
    logging.info(f"[{port_name}] verified {stored} rows stored for {write_date}")
    return True


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------
def process_index(
    label,
    members,
    source_name,
    source_asof,
    port_name,
    band,
    cfg,
    ny_now,
    outdir,
    dbflag,
    force,
):
    """Gate, build, dump to CSV and conditionally write one index. Returns a summary."""
    summary = {
        "port_name": port_name,
        "source": source_name,
        "rows": 0 if members is None else len(members),
        "changed": None,
        "action": "skip",
        "date": None,
        "reason": "",
    }

    if not sanity_gate(members, band[0], band[1], label):
        summary["reason"] = "failed the row-count sanity gate"
        return summary

    effective_date = pick_effective_date(source_asof, ny_now)
    frame = build_frame(
        members,
        port_name,
        effective_date,
        cfg["asset_class"],
        cfg["asset_type"],
    )

    # CSV first, always -- it is the verification artefact and must exist even
    # when the DB write is skipped or fails.
    csv_path = os.path.join(outdir, f"portfolio_assets_info_{port_name}.csv")
    frame.to_csv(csv_path, index=False)
    logging.info(f"[{port_name}] wrote {len(frame)} rows to {csv_path}")
    summary["csv"] = csv_path
    summary["frame"] = frame

    if not dbflag:
        summary["reason"] = "dbFlag=False, database writes suppressed"
        return summary

    stored_max = get_stored_max_date(cfg["db"], cfg["table"], port_name)
    stored_set = load_stored_set(cfg["db"], cfg["table"], port_name, stored_max)
    changed = has_changed(frame, stored_set)
    summary["changed"] = changed

    decision = resolve_effective_date(effective_date, stored_max, changed, force)
    summary["action"] = decision.action
    summary["date"] = decision.write_date
    summary["reason"] = decision.reason

    if decision.action == "skip":
        level = logging.ERROR if "older than" in decision.reason else logging.INFO
        logging.log(level, f"[{port_name}] skipping: {decision.reason}")
        return summary

    frame = frame.assign(Date=decision.write_date)
    ok = write_set(
        cfg["db"],
        cfg["table"],
        frame,
        port_name,
        decision.write_date,
        replace=(decision.action == "replace"),
    )
    if not ok:
        summary["action"] = "failed"
    return summary


def _jsonable_summary(summary):
    """Return one summary with everything Lambda cannot JSON-encode removed.

    Lambda marshals whatever ``run()`` returns. ``frame`` is a DataFrame and
    ``date`` a ``datetime.date``; both raise ``Runtime.MarshalError`` *after*
    the CSVs are written and the DB work is done, turning a healthy run into a
    failed invocation. ``frame`` stays on the in-process summary because the
    combined golden CSV is built from it -- it is dropped only on the way out.
    """
    out = {k: v for k, v in summary.items() if k != "frame"}
    value = out.get("date")
    if isinstance(value, pd.Timestamp):
        value = value.date()
    if isinstance(value, (datetime, date)):
        out["date"] = value.isoformat()
    return out


def run(event, context):
    """Lambda entry point.

    Event keys: ``localrun`` (CSVs to CWD), ``dbFlag`` (False suppresses all DB
    writes -- the dry-run switch), ``force`` (write even when unchanged),
    ``test`` (DEBUG logging), ``NYTIME`` (injected clock, for tests).

    Returns one JSON-serialisable summary dict per index -- Lambda marshals the
    return value, so nothing that ``json`` cannot encode may appear in it.
    """
    event = dict(event or {})
    logger.setLevel(logging.DEBUG if event.get("test") else logging.INFO)
    logging.info(f"** ==> port_assets_handler.run(event: {event})")

    localrun = bool(event.get("localrun", False))
    dbflag = bool(event.get("dbFlag", True))
    force = bool(event.get("force", False))

    cfg = {
        "db": _require_env("DBTRADING"),
        "table": _require_env("TBLPORTASSETS"),
        "asset_class": _env("PORT_ASSET_CLASS", "Equity"),
        "asset_type": _env("PORT_ASSET_TYPE", "Stock"),
    }
    sp500_port = _env("SP500_PORT_NAME", "SP500")
    ndx100_port = _env("NDX100_PORT_NAME", "NDX100")
    crosscheck = _truthy(environ.get("INDEX_CROSSCHECK"), default=True)

    ny_now = event.get("NYTIME") or datetime.now().astimezone(pytz.timezone("US/Eastern"))
    logging.info(f"Current NY Time: {ny_now}")

    outdir = _output_dir(localrun)
    os.makedirs(outdir, exist_ok=True)

    summaries = []

    # --- S&P 500 -----------------------------------------------------------
    # Effective date is the NY-local run date, not the SPY workbook's as-of:
    # Wikipedia (the primary source) carries no as-of date, and deriving a
    # primary-key column from an optional cross-check would make the date move
    # whenever INDEX_CROSSCHECK is toggled.
    try:
        sp500, sp500_source = fetch_sp500()
        if crosscheck:
            spy, spy_asof = fetch_spy_holdings()
            logging.info(f"SPY holdings as of {spy_asof} (logged, not used as the Date)")
            log_symmetric_difference("SP500", sp500, spy)
        summaries.append(
            process_index(
                "SP500", sp500, sp500_source, None, sp500_port, SP500_BAND,
                cfg, ny_now, outdir, dbflag, force,
            )
        )
    except Exception:
        logging.error("SP500 collection failed", exc_info=True)
        summaries.append({"port_name": sp500_port, "action": "failed", "reason": "exception"})

    # --- NASDAQ-100 --------------------------------------------------------
    try:
        ndx, ndx_source, ndx_asof = fetch_ndx100()
        if crosscheck and ndx_source.startswith("nasdaq-api"):
            log_symmetric_difference("NDX100", ndx, fetch_ndx100_crosscheck())
        summaries.append(
            process_index(
                "NDX100", ndx, ndx_source, ndx_asof, ndx100_port, NDX100_BAND,
                cfg, ny_now, outdir, dbflag, force,
            )
        )
    except Exception:
        logging.error("NDX100 collection failed", exc_info=True)
        summaries.append({"port_name": ndx100_port, "action": "failed", "reason": "exception"})

    # --- Combined golden CSV ----------------------------------------------
    frames = [s["frame"] for s in summaries if s.get("frame") is not None]
    if frames:
        combined = pd.concat(frames, ignore_index=True)[TABLE_COLUMNS]
        combined_path = os.path.join(outdir, "portfolio_assets_info.csv")
        combined.to_csv(combined_path, index=False)
        logging.info(f"wrote combined {len(combined)} rows to {combined_path}")

    for s in summaries:
        logging.info(
            f"SUMMARY [{s.get('port_name')}] source={s.get('source')} "
            f"rows={s.get('rows')} changed={s.get('changed')} "
            f"action={s.get('action')} date={s.get('date')} reason={s.get('reason')}"
        )
    return [_jsonable_summary(s) for s in summaries]


if __name__ == "__main__":
    run({"localrun": True, "dbFlag": True, "test": True}, None)
