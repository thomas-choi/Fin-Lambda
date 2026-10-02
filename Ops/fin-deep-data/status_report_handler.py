"""statusReport -- one status line per data set, e-mailed every weekday evening.

PLAN-SR-UPSTREAM Phase D (U9). Reads ``GlobalMarketData.v_load_status`` for
every (table, job) that writes ``load_audit``, combines all of that day's
shard and sweep runs, and adds an ``(inferred)`` line -- ``MAX(Date)`` only --
for each known table that has no audit rows yet. The format is Support-
Resistance-Agent tech doc §4.5.7's::

    DataName (table)     Job           Last data   Last run (ET)             Status   Symbols      Rows
    histdailyprice7      eodDaily      2026-09-24  2026-09-24 18:31–18:44    ok       1212/1212    1212

Status is ``error`` (a run failed), ``stale`` (last data older than the
previous weekday; for the on-change sets in ``EVENT_TABLES``, last *run*), ``partial`` (some symbols not ok/empty after the sweep) or
``ok``. Holidays are not modelled: the SR watchdog applies the exchange
calendar, so this report can say ``stale`` the day after a US holiday.

Outputs: SNS e-mail (``STATUS_TOPIC_ARN``), R2 JSON (``STATUS_R2_KEY`` in
``UPSTREAM_R2_BUCKET``) and stdout. ``dbFlag=False`` / ``localrun`` prints only.

Runs on **python3.13** with the ``finDeepCore`` layer -- no yfinance, no
scraping. Its deploy package carries this module and ``dataUtil.py`` only.

Local run (reads MySQL, prints the report, sends nothing)::

    cd Ops/fin-deep-data
    ../../venv-py313/bin/python status_report_handler.py
"""

import json
import logging
from datetime import date, datetime, timedelta

import pandas as pd
import pytz
from dotenv import load_dotenv

import dataUtil as DU

logger = logging.getLogger()

load_dotenv()

NY = pytz.timezone("America/New_York")
VIEW = "v_load_status"
DASH = "—"

#: Tables reported with MAX(Date) when no load_audit row exists for them:
#: (schema env var, table env var, default table, date column). Includes the
#: production bars/options tables, which myFinData's cron writes until U6.
INFERRED_TABLES = [
    ("DBMKTDATA", "TBLDLYPRICE", "histdailyprice7", "Date"),
    ("DBMKTDATA", "TBLOPTCHAIN", "OptionChains", "Date"),
    ("DBMKTDATA", "TBLUSRATES", "USRates", "Date"),
    ("DBMKTDATA", "TBLHISTFX", "FX_histdaily", "Date"),
    ("DBMKTDATA", "TBLMINUTEPRICE", "histminprice", "Datetime"),
    ("DBTRADING", "TBLPORTASSETS", "portfolio_assets_info", "Date"),
]

#: On-change data sets: a quiet day writes nothing, so their last data date
#: says nothing about freshness. They are judged by their last run instead,
#: and an inferred line for one never goes stale. Env var, default.
EVENT_TABLES = [("TBLCORPACTION", "corp_action_daily"), ("TBLPORTASSETS", "portfolio_assets_info")]

#: Column widths of the report, SR tech doc §4.5.7.
HEADER = ("DataName (table)", "Job", "Last data", "Last run (ET)", "Status", "Symbols", "Rows")
WIDTHS = (21, 14, 12, 26, 9, 13)


# --------------------------------------------------------------------------
# Pure helpers
# --------------------------------------------------------------------------
def previous_weekday(day):
    """The weekday before ``day`` (Monday -> Friday)."""
    day = day - timedelta(days=1)
    while day.weekday() >= 5:
        day -= timedelta(days=1)
    return day


def _as_date(value):
    if value is None or (not isinstance(value, (date, datetime)) and pd.isna(value)):
        return None
    if isinstance(value, pd.Timestamp):
        return value.date()
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return pd.Timestamp(value).date()


def _none_if_nan(value):
    return None if value is None or (not isinstance(value, str) and pd.isna(value)) else value


def aggregate_shards(summaries, symbol_counts=None):
    """Combine one day's summary rows (all shards + sweeps) into one line.

    ``summaries`` are load_audit summary rows as dicts. ``symbol_counts`` is
    ``(n_symbols, n_good)`` from the per-symbol rows of the same day, where a
    symbol is good when any of its rows is ok/empty -- so a symbol skipped by
    a shard and recovered by the sweep counts once, as good. Without per-
    symbol rows (the U10 handlers) the summaries' own n_ok/n_expected are summed.
    """
    starts = [s["started_at"] for s in summaries if _none_if_nan(s.get("started_at")) is not None]
    ends = [s["finished_at"] for s in summaries if _none_if_nan(s.get("finished_at")) is not None]
    last = [_as_date(s.get("date_hi")) for s in summaries]
    last = [d for d in last if d is not None]
    errors = [s.get("error") for s in summaries if s.get("status") == "error"]

    n_ok = n_expected = None
    if symbol_counts and symbol_counts[0]:
        n_expected, n_ok = int(symbol_counts[0]), int(symbol_counts[1])
    else:
        oks = [_none_if_nan(s.get("n_ok")) for s in summaries]
        exps = [_none_if_nan(s.get("n_expected")) for s in summaries]
        if any(v is not None for v in exps):
            n_ok = int(sum(v or 0 for v in oks))
            n_expected = int(sum(v or 0 for v in exps))

    return {
        "started_at": min(starts) if starts else None,
        "finished_at": max(ends) if ends else None,
        "last_data_date": max(last) if last else None,
        "n_rows": int(sum(_none_if_nan(s.get("n_rows")) or 0 for s in summaries)),
        "n_ok": n_ok, "n_expected": n_expected,
        "n_runs": len(summaries), "failed": bool(errors),
        "error": next((e for e in errors if e), None),
    }


def event_tables():
    return {DU.env_or(env, default) for env, default in EVENT_TABLES}


def status_for(line, today, events=frozenset()):
    """error > stale > partial > ok. Inferred lines are 'stale' or '—'.

    Stale means the last data date -- or, for a table in ``events``, the ET
    date of the last run -- is older than the previous weekday.
    """
    is_event = line.get("table_name") in events
    if is_event:
        if line.get("inferred"):
            return DASH
        ran = _et(line.get("finished_at"))
        last = ran.date() if ran is not None else None
    else:
        last = _as_date(line.get("last_data_date"))
    stale = last is None or last < previous_weekday(today)
    if line.get("inferred"):
        return "stale" if stale else DASH
    if line.get("failed"):
        return "error"
    if stale:
        return "stale"
    n_ok, n_exp = line.get("n_ok"), line.get("n_expected")
    if n_exp and n_ok is not None and n_ok < n_exp:
        return "partial"
    return "ok"


def _et(value):
    if value is None:
        return None
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert(NY)


def run_span(started_at, finished_at):
    """'2026-09-24 18:31–18:44' in ET; the end carries its date if it differs."""
    s, f = _et(started_at), _et(finished_at)
    if s is None or f is None:
        return DASH
    end = f.strftime("%H:%M") if f.date() == s.date() else f.strftime("%Y-%m-%d %H:%M")
    return f"{s:%Y-%m-%d %H:%M}–{end}"


def format_line(line):
    symbols = (f"{line['n_ok']}/{line['n_expected']}"
               if line.get("n_expected") is not None and line.get("n_ok") is not None else DASH)
    last = _as_date(line.get("last_data_date"))
    cells = (
        line["table_name"],
        "(inferred)" if line.get("inferred") else line["job"],
        str(last) if last else DASH,
        DASH if line.get("inferred") else run_span(line.get("started_at"), line.get("finished_at")),
        line["status"],
        symbols,
    )
    rows = DASH if line.get("inferred") else str(line.get("n_rows", 0))
    # (c + " ") keeps a separator when a name overflows its column
    return "".join((str(c) + " ").ljust(w) for c, w in zip(cells, WIDTHS)) + rows


def format_report(lines, today):
    out = [f"Fin-Lambda load status for {today} (ET)", "",
           "".join(h.ljust(w) for h, w in zip(HEADER, WIDTHS)) + HEADER[-1]]
    out += [format_line(line) for line in lines]
    issues = [l for l in lines if l["status"] in ("error", "stale", "partial")]
    for line in issues:
        if line.get("error"):
            out.append(f"  {line['table_name']}/{line.get('job')}: {line['error']}")
    return "\n".join(out)


def subject_for(lines, today):
    bad = sum(l["status"] in ("error", "stale", "partial") for l in lines)
    return f"Fin-Lambda status {today}: " + ("all ok" if bad == 0 else f"{bad} issue(s)")


def to_json(lines, today, generated_at):
    def enc(v):
        if isinstance(v, (datetime, date, pd.Timestamp)):
            return v.isoformat()
        return _none_if_nan(v)
    return json.dumps({"asof": str(today), "generated_at": enc(generated_at),
                       "rows": [{k: enc(v) for k, v in l.items()} for l in lines]}, default=str)


# --------------------------------------------------------------------------
# Database wrappers
# --------------------------------------------------------------------------
def _schema_for(table):
    """Schema of a table named in load_audit (portfolio_assets_info lives in Trading)."""
    if table == DU.env_or("TBLPORTASSETS", "portfolio_assets_info"):
        return DU.env_or("DBTRADING", "Trading")
    return DU.require_env("DBMKTDATA")


def _date_column(table):
    return "Datetime" if table == DU.env_or("TBLMINUTEPRICE", "histminprice") else "Date"


def table_max_date(schema, table, column="Date"):
    df = DU.load_df_SQL(f"SELECT MAX({column}) AS d FROM {schema}.{table};")
    if df is None or len(df) == 0:
        return None
    return _as_date(df.d.iloc[0])


def audited_lines(db, audit_tbl):
    """One aggregated line per (table_name, job) in v_load_status, or None if unreadable."""
    view = DU.load_df_SQL(f"SELECT * FROM {db}.{VIEW};")
    if view is None:
        return None
    lines = []
    for v in view.to_dict("records"):
        table, job = v["table_name"], v["job"]
        day = _et(v["finished_at"]).date()
        since = DU.day_start_utc(day)
        where = (f"table_name = '{table}' AND job = '{job}' "
                 f"AND finished_at >= '{since:%Y-%m-%d %H:%M:%S}'")
        summaries = DU.load_df_SQL(
            f"SELECT started_at, finished_at, status, n_ok, n_expected, n_rows, date_hi, error "
            f"FROM {db}.{audit_tbl} WHERE {where} AND segment = 'summary';")
        counts = DU.load_df_SQL(
            f"SELECT COUNT(DISTINCT Symbol) AS n_sym, "
            f"COUNT(DISTINCT CASE WHEN status IN ('ok', 'empty') THEN Symbol END) AS n_good "
            f"FROM {db}.{audit_tbl} WHERE {where} AND segment <> 'summary';")
        rows = summaries.to_dict("records") if summaries is not None and len(summaries) else [v]
        sym = (int(counts.n_sym.iloc[0]), int(counts.n_good.iloc[0])) if counts is not None and len(counts) else None
        line = aggregate_shards(rows, sym)
        if line["last_data_date"] is None:
            line["last_data_date"] = table_max_date(_schema_for(table), table, _date_column(table))
        line.update(table_name=table, job=job, inferred=False)
        lines.append(line)
    return sorted(lines, key=lambda l: (l["table_name"], l["job"]))


def inferred_lines(audited_tables):
    lines = []
    for schema_env, table_env, default, column in INFERRED_TABLES:
        table = DU.env_or(table_env, default)
        if table in audited_tables:
            continue
        schema = DU.env_or(schema_env, "Trading" if schema_env == "DBTRADING" else "GlobalMarketData")
        lines.append({"table_name": table, "job": None, "inferred": True,
                      "last_data_date": table_max_date(schema, table, column)})
    return lines


# --------------------------------------------------------------------------
# Delivery
# --------------------------------------------------------------------------
def publish_sns(topic_arn, subject, body):
    import boto3

    boto3.client("sns").publish(TopicArn=topic_arn, Subject=subject[:100], Message=body)


def put_r2_json(payload):
    import boto3

    bucket = DU.require_env("UPSTREAM_R2_BUCKET")
    key = DU.env_or("STATUS_R2_KEY", "status/latest.json")
    client = boto3.client("s3", endpoint_url=DU.require_env("R2_ENDPOINT"),
                          aws_access_key_id=DU.require_env("R2_ACCESS_KEY_ID"),
                          aws_secret_access_key=DU.require_env("R2_SECRET_ACCESS_KEY"),
                          region_name="auto")
    client.put_object(Bucket=bucket, Key=key, Body=payload.encode("utf-8"),
                      ContentType="application/json")
    return f"{bucket}/{key}"


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------
def run(event, context):
    """Lambda entry point.

    Event keys: ``localrun`` / ``dbFlag=False`` (print only: no SNS, no R2),
    ``asof`` (YYYY-MM-DD report date), ``NYTIME`` (injected clock), ``test``
    (DEBUG logging). Returns a JSON-serialisable summary.
    """
    event = dict(event or {})
    logger.setLevel(logging.DEBUG if event.get("test") else logging.INFO)
    localrun = bool(event.get("localrun", False))
    send = bool(event.get("dbFlag", True)) and not localrun

    db = DU.require_env("DBMKTDATA")
    audit_tbl = DU.require_env("TBLLOADAUDIT")
    ny_now = event.get("NYTIME") or datetime.now(pytz.utc).astimezone(NY)
    today = datetime.strptime(event["asof"], "%Y-%m-%d").date() if event.get("asof") else ny_now.date()

    lines = audited_lines(db, audit_tbl)
    error = None
    if lines is None:
        error = f"could not read {db}.{VIEW}"
        lines = []
    lines += inferred_lines({l["table_name"] for l in lines})
    events = event_tables()
    for line in lines:
        line["status"] = status_for(line, today, events)

    body = format_report(lines, today)
    if error:
        body += f"\n\nERROR: {error}"
    subject = subject_for(lines, today) + (" (view unreadable)" if error else "")
    print(body)

    delivered = {"sns": False, "r2": None}
    if send:
        topic = DU.env_or("STATUS_TOPIC_ARN", None)
        if topic:
            try:
                publish_sns(topic, subject, body)
                delivered["sns"] = True
            except Exception:
                logging.error("SNS publish failed", exc_info=True)
        else:
            logging.warning("STATUS_TOPIC_ARN is not set; no e-mail sent")
        try:
            delivered["r2"] = put_r2_json(to_json(lines, today, DU.utc_now()))
        except Exception:
            logging.error("R2 status upload failed", exc_info=True)

    counts = pd.Series([l["status"] for l in lines]).value_counts().to_dict() if lines else {}
    return {"asof": str(today), "subject": subject, "lines": len(lines),
            "statuses": {str(k): int(v) for k, v in counts.items()}, "error": error, **delivered}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run({"localrun": True}, None)
