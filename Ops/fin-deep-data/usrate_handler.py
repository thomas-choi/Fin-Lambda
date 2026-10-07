"""usrateHandlerv2 -- US interest rates from the Fed H.15 release.

PLAN-SR-UPSTREAM Phase F (U10). A copy of
``Ops/fin-cron-data/usrate_handler.py`` at fa1d6ac, moved to python3.13 and
extended with one ``load_audit`` summary row per run. The original stays
deployed and untouched; only one of the two may be scheduled, because both
append to the same table, so this one's schedule ships **disabled** (see
``doc/OPERATIONS.md``, cutover runbook).

``load_audit.job`` stays ``usrateHandler`` -- it names the data set's job, not
the Lambda -- so the status report does not grow a second line at the cutover.

Changed from the original beyond the audit row:

* the H.15 table is handed to ``pandas.read_html`` wrapped in ``StringIO``;
  passing a literal HTML string is deprecated in pandas 2.1+ and raises in 3.0;
* ``maxdate`` falls back to ``dt.datetime(1800, 1, 1)``. The original wrote
  ``datetime(1800,1,1)``, a ``NameError`` -- the module imports ``datetime as
  dt`` -- so an empty table crashed the run instead of back-filling it;
* a real dry run: ``{"dbFlag": false}`` writes ``USrates_<asof>.csv`` and
  touches neither the table nor ``load_audit``. The original's ``__main__``
  wrote straight to production;
* ``{"localrun": true}`` also prints the rates to stdout, one row per
  instrument, so a dry run does not need the CSV opened to be read. When the
  table is already current it prints the latest H.15 date instead of nothing.

Runs on **python3.13** with the ``finDeepCore`` + ``finDeepWeb`` layers
(BeautifulSoup + lxml for the scrape; no yfinance).

Local dry run::

    cd Ops/fin-deep-data
    ../../venv-py313/bin/python usrate_handler.py
"""

import pandas as pd
import logging
import os
import sys
from io import StringIO
from dotenv import load_dotenv
import dataUtil as DU
import datetime as dt
from bs4 import BeautifulSoup
import pytz
import requests
from os import environ

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
load_dotenv()

JOB = "usrateHandler"

#: URL of the H.15 selected-interest-rates release.
H15_URL = 'https://www.federalreserve.gov/releases/h15/'


def  HTML2DataFrame(_url):

    # Send a GET request to the webpage and store the response
    response = requests.get(_url)

    soup = BeautifulSoup(response.text, 'html.parser')
    logging.debug(soup.prettify())

    table = soup.find(id='h15table')
    logging.debug(table)

    # StringIO: read_html on a literal string is deprecated in pandas 2.1+.
    data = pd.read_html(StringIO(str(table)))
    data[0]['Instruments'].values

    return data[0]

# DPCREDIT - Discount window primary credit
# 5YTIISNK - 5 years inflation indexed Treasury constant maturities
nInstruments=['Federal_funds', 'CP',
       'NF', 'CP_NF_1_month', 'CP_NF_2_month', 'CP_NF_3_month', 'Fi',
       'CP_Fi_1_month', 'CP_Fi_2_month', 'CP_Fi_3_month', 'Bank_prime_loan',
       'DPCREDIT', 'U.S.',
       'TBill', 'TBill_4_week', 'TBill_3_month',
       'TBill_6_month', 'TBill_1_year', 'TBond', 'Nominal',
       'TBond_1_month', 'TBond_3_month', 'TBond_6_month', 'TBond_1_year', 'TBond_2_year',
       'TBond_3_year',
       'TBond_5_year', 'TBond_7_year', 'TBond_10_year', 'TBond_20_year', 'TBond_30_year',
       'Inflation', '5YTIISNK', '7YTIISNK', '10YTIINK', '20YTIINK',
       '30YTIINK', 'Inf_average']

#: Header rows of the H.15 table: group labels with no rate of their own.
GROUP_ROWS = ['CP', 'NF', 'Fi', 'U.S.', 'TBill', 'TBond', 'Nominal', 'Inflation']


def reshape_rates(df):
    """H.15 table -> one row per date, one column per instrument.

    Pure: the whole transform between the HTTP fetch and the write, so it is
    testable from a saved page. ``n.a.`` becomes NaN and every rate a float.
    """
    df = df.copy()
    df['Instruments'] = nInstruments
    df = df.set_index('Instruments')
    df = df.drop(index=GROUP_ROWS)

    rates = df.transpose().replace(regex={'n.a.': 'nan'}).astype(float)
    rates.index = pd.to_datetime(rates.index)
    rates.index.name = 'Date'

    retDF = rates.reset_index()
    retDF['Date'] = pd.to_datetime(retDF.Date)
    return retDF


def _output_dir(localrun):
    """CWD locally, /tmp on Lambda where the bundle directory is read-only."""
    return DU.out_dir(localrun)


def format_rates(stored, maxdate=None, scraped=None):
    """Render a run's rates as a table for stdout. Pure, so it is testable.

    One row per instrument and one column per date -- the H.15 shape, not the
    stored shape, because 30-odd instrument columns do not fit a terminal.

    When the table is already current ``stored`` is empty, so the latest
    scraped date is shown instead, labelled as already stored: a ``localrun``
    should print rates rather than only a watermark.
    """
    n = 0 if stored is None else len(stored)
    if n:
        body = stored
        head = f"US rates -- {n} new row(s) since {maxdate}"
    elif scraped is not None and len(scraped):
        body = scraped.tail(1)
        head = (f"US rates -- no new rows since {maxdate}; showing the latest "
                f"H.15 date, which is already stored")
    else:
        return f"US rates -- H.15 returned no rows (stored max {maxdate})"

    shown = body.set_index("Date").transpose()
    shown.columns = pd.to_datetime(shown.columns).strftime("%Y-%m-%d")
    shown.index.name = "Instrument"
    return f"{head}\n{shown.to_string()}"


def usrate_run(event, context, dbFlag=True, localrun=False):
    """Scrape H.15 and append rows newer than the stored max. Returns the rows stored.

    ``localrun`` prints the rates to stdout (``format_rates``) on top of
    choosing where a ``dbFlag=False`` CSV lands.
    """

    logging.info(f"** ==> usrate_run(event: {event}, context: {context})")
    ny_time = dt.datetime.now().astimezone( pytz.timezone('US/Eastern'))
    logging.info(f"Current NY Time: {ny_time}")

    DB = environ.get("DBMKTDATA")
    TBL = environ.get("TBLUSRATES")
    data_name = f'{DB}.{TBL}'
    maxdate = DU.get_Max_date(data_name)
    logging.debug(f'{data_name} max_date is {maxdate}')
    if maxdate is None:
        # dt.datetime, not datetime: this module imports datetime as dt, and the
        # original's bare datetime(1800,1,1) raised NameError on an empty table.
        maxdate = dt.datetime(1800,1,1)
    maxdate = maxdate.strftime("%Y-%m-%d")
    logging.info(f'US Rates max date is {maxdate}')

    retDF = reshape_rates(HTML2DataFrame(H15_URL))
    logging.debug(retDF.info())

    stDF = retDF[retDF['Date']>maxdate]
    if localrun:
        # Before the write, so the rates are on screen even if the write fails.
        print(format_rates(stDF, maxdate=maxdate, scraped=retDF))
    if len(stDF)>0:
        if dbFlag:
            logging.debug('Loading data to database ')
            DU.StoreEOD(stDF, DB, TBL)
        else:
            out = os.path.join(_output_dir(localrun), f"USrates_{ny_time:%Y-%m-%d}.csv")
            stDF.to_csv(out, index=False)
            logging.info(f'dbFlag=False: wrote {len(stDF)} row(s) to {out}')
    return stDF


def _audit(table, run_id, started_at, stored, status="ok", error=None):
    """Write the summary row. An audit failure must never fail the load."""
    try:
        n = 0 if stored is None else len(stored)
        dates = [] if not n else list(pd.to_datetime(stored["Date"]).dt.date)
        DU.audit_run([DU.audit_summary(
            JOB, table, run_id, started_at, status=status, n_rows=n,
            date_lo=min(dates) if dates else None, date_hi=max(dates) if dates else None,
            error=error)])
    except Exception:
        logging.error("load_audit summary failed", exc_info=True)


def run(event, context):
    """Lambda entry point: usrate_run() plus one load_audit summary row (U10).

    Event keys: ``dbFlag`` (False = CSV instead of the table, and no audit row),
    ``localrun`` (print the rates to stdout, and put a CSV in the CWD rather
    than /tmp), ``test`` (DEBUG logging).
    """
    event = dict(event or {})
    if event.get("test"):
        logging.getLogger().setLevel(logging.DEBUG)
    dbFlag = bool(event.get("dbFlag", True))
    localrun = bool(event.get("localrun", False))
    run_id, started_at = DU.new_run_id(), DU.utc_now()
    table = environ.get("TBLUSRATES")
    try:
        stored = usrate_run(event, context, dbFlag=dbFlag, localrun=localrun)
    except Exception as e:
        if dbFlag:
            _audit(table, run_id, started_at, None, status="error", error=f"{type(e).__name__}: {e}")
        raise
    if dbFlag:
        _audit(table, run_id, started_at, stored)
    return {"run_id": run_id, "rows": 0 if stored is None else len(stored)}


if __name__ == '__main__':
    # Dry run: reads MySQL for the watermark, writes USrates_<date>.csv, and
    # touches neither USRates nor load_audit.
    print(run({"localrun": True, "dbFlag": True}, None))
