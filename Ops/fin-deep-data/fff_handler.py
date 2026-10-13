"""fffHandlerv2 -- monthly Fama-French 3 factors into TBLFFF.

A python3.13 port of ``Ops/fin-cron-data/fff_handler.py`` (TODOS 5.10), which
has never run in its deployed form: it imports ``getFamaFrenchFactors``, which
is in no layer, so every monthly invocation fails with
``Runtime.ImportModuleError`` -- and past that, it calls ``datetime.now()`` and
``pytz`` without importing either (TODOS 4.1). ``famaFrench`` stops at
2023-10-31. This one's schedule ships **disabled**.

The factors now come straight from Ken French's data library -- the same file
``getFamaFrenchFactors`` wrapped -- with ``requests`` and ``zipfile``, so the
function needs only the ``finDeepCore`` layer and no new dependency.

What is written, matching the existing rows:

* ``Date`` -- the **month-end** date of a ``YYYYMM`` row (``famaFrench``'s PK);
* ``Mkt-RF``, ``SMB``, ``HML``, ``RF`` -- as **fractions**: the file is in
  percent, the table holds ``-0.0318`` for 2023-10's ``-3.18``.

Only months after the table's max ``Date`` are appended, through
``append_ignore`` so a re-run is harmless. Ken French revises history slightly
between releases (2023-10 ``Mkt-RF`` reads ``-3.15`` today against ``-3.18``
stored); stored months are deliberately **not** rewritten.

``load_audit.job`` is ``fffHandler``.

Local dry run (writes famaFrench.csv, touches no table)::

    cd Ops/fin-deep-data
    ../../venv-py313/bin/python fff_handler.py
"""

import io
import logging
import os
import zipfile
from datetime import datetime

import pandas as pd
import pytz
import requests
from dotenv import load_dotenv

import dataUtil as DU

logger = logging.getLogger()

load_dotenv()

JOB = "fffHandler"

FF3_URL = ("https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/"
           "F-F_Research_Data_Factors_CSV.zip")

#: famaFrench's columns, in table order.
FF3_COLUMNS = ["Date", "Mkt-RF", "SMB", "HML", "RF"]


def parse_ff3(text):
    """The monthly block of F-F_Research_Data_Factors.csv as a famaFrench frame.

    The file is a preamble, a ``,Mkt-RF,SMB,HML,RF`` header, monthly rows keyed
    ``YYYYMM``, a blank line, then an ``Annual Factors`` block keyed ``YYYY``
    and a copyright line. Only six-digit keys after the first header and before
    the annual block are monthly rows. Pure.
    """
    rows, in_monthly = [], False
    for line in text.splitlines():
        cells = [c.strip() for c in line.split(",")]
        if not in_monthly:
            in_monthly = cells[1:5] == FF3_COLUMNS[1:]
            continue
        if "Annual" in line:
            break
        if len(cells) >= 5 and len(cells[0]) == 6 and cells[0].isdigit():
            rows.append(cells[:5])
    if not rows:
        raise ValueError("no monthly rows found -- the Fama-French file layout changed")
    df = pd.DataFrame(rows, columns=FF3_COLUMNS)
    df["Date"] = (pd.to_datetime(df["Date"], format="%Y%m") + pd.offsets.MonthEnd(0)).dt.date
    for col in FF3_COLUMNS[1:]:
        # round: 4.85 / 100 is 0.048499999999999995 in binary floating point
        df[col] = (pd.to_numeric(df[col]) / 100.0).round(6)
    return df


def fetch_ff3_text(url=FF3_URL, timeout=60):
    """Download the zip and return its one CSV as text."""
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        name = next(n for n in zf.namelist() if n.lower().endswith(".csv"))
        return zf.read(name).decode("latin-1")


def new_rows(df, max_date):
    """Rows strictly after max_date; all of them when the table is empty. Pure."""
    if max_date is None:
        return df.reset_index(drop=True)
    return df[df["Date"] > pd.Timestamp(max_date).date()].reset_index(drop=True)


def fff_run(event, context, localrun=False, dbFlag=True):
    schema = DU.require_env("DBMKTDATA")
    table = DU.require_env("TBLFFF")

    ff3 = parse_ff3(fetch_ff3_text())
    max_date = DU.get_Max_date(f"{schema}.{table}")
    logging.info(f"{schema}.{table} max Date {max_date}; file runs to {ff3['Date'].max()}")
    fresh = new_rows(ff3, max_date)
    logging.info(f"{len(fresh)} new month(s) for {table}")

    if not dbFlag or localrun:
        out = os.path.join(DU.out_dir(localrun), f"{table}.csv")
        fresh.to_csv(out, index=False)
        logging.info(f"dry run: wrote {len(fresh)} row(s) to {out}")
    elif len(fresh) > 0:
        inserted = DU.append_ignore(fresh, schema, table)
        if inserted is None:
            raise RuntimeError(f"append to {schema}.{table} failed (see the logged traceback)")
    return fresh


def _audit(run_id, started_at, df, status="ok", error=None):
    """One load_audit summary row. Never raises."""
    try:
        n = 0 if df is None else len(df)
        DU.audit_run([DU.audit_summary(
            JOB, os.environ.get("TBLFFF"), run_id, started_at, status=status, n_rows=n,
            date_lo=df["Date"].min() if n else None,
            date_hi=df["Date"].max() if n else None, error=error)])
    except Exception:
        logging.error("load_audit summary failed", exc_info=True)


def run(event, context):
    """Lambda entry point.

    Event keys: ``localrun`` / ``dbFlag`` (False = CSV only, no table and no
    audit row), ``test`` (DEBUG logging).
    """
    event = dict(event or {})
    logger.setLevel(logging.DEBUG if "test" in event else logging.INFO)
    logging.info(f"** ==> fff_handler.run(event: {event}, context: {context})")
    logging.info(f"Current NY Time: {datetime.now(pytz.timezone('US/Eastern'))}")
    localrun = bool(event.get("localrun", False))
    dbFlag = bool(event.get("dbFlag", True)) and not localrun
    run_id, started_at = DU.new_run_id(), DU.utc_now()
    try:
        df = fff_run(event, context, localrun=localrun, dbFlag=dbFlag)
    except Exception as e:
        if dbFlag:
            _audit(run_id, started_at, None, status="error", error=f"{type(e).__name__}: {e}")
        raise
    if dbFlag:
        _audit(run_id, started_at, df)
    return {"run_id": run_id, "rows": len(df),
            "date_hi": None if df.empty else str(df["Date"].max())}


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    # Dry run: downloads the factor file, reads famaFrench's max Date, writes
    # famaFrench.csv, and touches neither famaFrench nor load_audit.
    print(run({"localrun": True, "dbFlag": False, "test": "True"}, None))
