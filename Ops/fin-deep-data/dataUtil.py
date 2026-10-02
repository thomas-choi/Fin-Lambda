"""dataUtil -- the only DB layer of the fin-deep-data service (python3.13).

A fork of ``Ops/fin-cron-data/dataUtil.py`` taken at commit fa1d6ac, extended
with the upstream-track helpers below (PLAN-SR-UPSTREAM Phase A). It is a fork
on purpose: fin-cron-data's copy ships to nine python3.10 functions on the
``finCron`` layer (pandas 1.5.3 / SQLAlchemy 1.4.46) and is frozen, so nothing
in this service can change it. This copy targets python3.13 / SQLAlchemy 2.0 /
pandas 2.2 only.

Contract kept from the original: every function logs and returns ``None`` on
failure instead of raising, and callers rarely check. The two exceptions are
documented on themselves -- :func:`require_env` raises, and
:func:`shard_symbols` raises on a bad shard index.
"""

import os
from os import path
from os import environ
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, insert, text
import logging
from datetime import datetime, timedelta, timezone
import pytz
import socket
import time

dbconn = None
__sTunnel = None

def nowbyTZ(tzName):
    tformats = '%Y-%m-%d %H:%M:%S'
    tzinfo = time.tzname
    tNow = datetime.now()
    logging.info(f'Local System TimeZone info: {tzinfo[0]} and local time: {tNow}')

    targetNow = datetime.strptime(tNow.astimezone(pytz.timezone(tzName)).strftime(tformats), tformats)
    logging.info(f'Target TimeZone {tzName}:   Target time: {targetNow}')
    return targetNow

def get_Symbollist(listname):
    basedir = list_dir()
    logging.debug(f'Load symbol list {listname} from {basedir}')
    listpath = os.path.join(basedir, f'{listname}.csv')
    s_list = pd.read_csv(listpath)['Symbol'].unique()
    return s_list

def get_DBengine():
    global dbconn
    if dbconn is None:
        hostname=environ.get("DBHOST")
        uname=environ.get("DBUSER")
        pwd=environ.get("DBPWD")
        DB = environ.get("DBMKTDATA")
        DBPORT = environ.get("DBPORT")

        dbpath = "mysql+pymysql://{user}:{pw}@{host}:{port}/{db}".format(host=hostname, db=DB, user=uname, pw=pwd,port=DBPORT)
        logging.info(f'setup DBengine to {dbpath}')
        # Create SQLAlchemy engine to connect to MySQL Database
        dbconn = create_engine(dbpath)
        logging.info(f'dbconn=>{dbconn}')
    return dbconn

def get_Max_datetime(dbntable, symbol=None):
    try:
        if symbol is None:
            query = f"SELECT max(Datetime) as maxdate from {dbntable} ;"
        else:
            query = "SELECT max(Datetime) as maxdate from {} where symbol = \'{}\';".format(dbntable, symbol)
        logging.info(f'get_Max_datetime :{query}')
        df = pd.read_sql(query, get_DBengine())
        max_date = df.maxdate.iloc[0]
        logging.info(f"get_Max_datetime() => {max_date} ")
        return max_date
    except Exception as e:
        logging.error("Exception occurred at get_Max_datetime()", exc_info=True)

def get_Max_date(dbntable, symbol=None):
    try:
        if symbol is None:
            query = f"SELECT max(Date) as maxdate from {dbntable} ;"
        else:
            query = f"SELECT max(Date) as maxdate from {dbntable} where symbol = \'{symbol}\';"
        logging.info(f'get_Max_date :{query}')
        df = pd.read_sql(query, get_DBengine())
        max_date = df.maxdate.iloc[0]
        logging.info(f"get_Max_date() => {max_date} ")
        return max_date
    except Exception as e:
        logging.error("Exception occurred at get_Max_date()", exc_info=True)

def get_Max_Options_date(dbntable, symbol=None):
    try:
        if symbol is None:
            query = f'SELECT max(Date) as maxdate, section FROM {dbntable} group by section order by maxdate desc, section desc;'
        else:
            query = f'SELECT max(Date) as maxdate, section FROM {dbntable} where UnderlyingSymbol = \'{symbol}\' group by section order by maxdate desc, section desc;'

        logging.info(f'get_Max_date :{query}')
        df = pd.read_sql(query, get_DBengine())
        logging.debug(df.head())
        max_date = df.maxdate.iloc[0]
        section = df.section.iloc[0]
        logging.info(f"get_Max_date() => {max_date},{section} ")
        return max_date, section
    except Exception as e:
        logging.error("Exception occurred at get_Max_Options_date()", exc_info=True)

def get_Latest_row_by_Symbol(dbntable, symbol):
    try:
        query = f"SELECT * from {dbntable} where Symbol = \'{symbol}\' order by Date desc;"
        logging.info(f'get_Latest_row_by_Symbol :{query}')
        df = pd.read_sql(query, get_DBengine())
        if len(df) > 0:
            max_row = df.iloc[0]
        else:
            max_row = None
        return max_row
    except Exception as e:
        logging.error("Exception occurred at get_Latest_row_by_Symbol()", exc_info=True)

def ExecSQL(query):
    """
    Execute a statement and return the number of rows affected.

    Works on both SQLAlchemy 1.4 and 2.0: `Engine.execute()` was removed in
    2.0, so the statement goes through `Engine.begin()` + `text()`, which
    exist in both. `begin()` commits explicitly, matching the implicit commit
    1.4's legacy autocommit gave the DELETE/TRUNCATE callers.

    Returns the rowcount on success, None on failure. Failures are logged, not
    raised -- callers across this repo rely on that contract.
    """
    logging.info(f"ExecSQL: {query}")
    try:
        with get_DBengine().begin() as conn:
            rowcount = conn.execute(text(query)).rowcount
        logging.info(f'number of rows execed: {rowcount}')
        return rowcount
    except Exception as e:
        logging.error("Exception occurred at ExecSQL()", exc_info=True)

def load_df_SQL(query):
    """
    Return dataframe from SQL statement
    """
    logging.info(f'load_df_SQL({query}).')
    try:
        df = pd.read_sql(query, get_DBengine())
        return df
    except Exception as e:
        logging.error("Exception occurred at load_df_SQL(np.linspace)", exc_info=True)

def load_df(stock_symbol=None, DailyMode=True, lastdt=None, startdt=None, dataMode="P"):
    """
    Return dataframe from histdailyprice3
    """
    HOST=environ.get("DBHOST")
    PORT=environ.get("DBPORT")
    USER=environ.get("DBUSER")
    PASSWORD=environ.get("DBPWD")
    logging.info(f'load_df ( {stock_symbol},{DailyMode},{lastdt},{startdt},{dataMode}).')    
    dpath = None
    if dataMode == "P":
        DBNAME=environ.get("DBMKTDATA")
        TBLName=environ.get("TBLDLYPRICE")
        if stock_symbol is not None:
            dpath = f"{TBLName}/{stock_symbol}.csv"
        elif (lastdt is not None) and (startdt is not None):
            dpath = f"{TBLName}/{startdt}-{lastdt}.csv"
    else:
        DBNAME=environ.get("DBPREDICT")
        TBLName=environ.get("TBLDAILYOUTPUT")

    if (dpath is not None) and path.isfile(dpath) and (not DailyMode):
        logging.info(f'Load data from {dpath}.')
        return pd.load_csv(dpath)
    else:
        logging.info(f'Load data from {DBNAME}.{TBLName} table in MySQL, Daily mode: {DailyMode}.')
        try: 
            wherecl=""
            if stock_symbol is not None:
                wherecl = f"where Symbol = '{stock_symbol}'"
            if startdt is not None:
                if len(wherecl) > 0:
                    wherecl = wherecl + f" and Date>='{startdt}'"
                else:
                    wherecl = f"where Date>='{startdt}'"
            if lastdt is not None:
                if len(wherecl) > 0:
                    wherecl = wherecl + f" and Date<='{lastdt}'"
                else:
                    wherecl = f"where Date<='{lastdt}'"
            nlimit = ""
            # if (stock_symbol is not None) and (startdt is None) and (lastdt is None):
            #     if DailyMode:
            #         nlimit = f" order by Date desc limit {DailySize}"
            #     else:
            #         nlimit = f" order by Date desc limit {TrainSize}"
            if dataMode == "P":
                query = f"SELECT Date, Symbol, Exchange, AdjClose, Close, Open, High, Low, Volume from {DBNAME}.{TBLName} {wherecl} {nlimit};"
            else:
                query = f"SELECT * from {DBNAME}.{TBLName} {wherecl} {nlimit};"

            logging.info(f'load_df query:{query}')
            histdailyprice3 = pd.read_sql(query, get_DBengine())
            # conn.close()
            df = histdailyprice3.copy()
            df = df.sort_values(by=['Date'])
            if (dpath is not None):
                df.to_csv(dpath, index=False)

            return df
        except Exception as e:
            logging.error("Exception occurred at load_df(np.linspace)", exc_info=True)

def StoreEOD(eoddata, DBn, TBLn):
    try:
        logging.info(f'StoreEOD size: {len(eoddata)} in table:{TBLn} on DB:{DBn}')
        dbcon = get_DBengine()
        logging.info(f'StoreEOD dbcon: {dbcon}')
        # Convert dataframe to sql table
        eoddata.to_sql(name=TBLn, con=dbcon, schema=DBn, if_exists='append', index=False)
    except Exception as e:
        logging.error("Exception occurred", exc_info=True)

def StoreWebDaily(df):
    try:
        logging.info(f'StoreWebDaily size: {len(df)}')
        dbname=environ.get("DBWEB")
        table=environ.get("TBLWEBPREDICT")

        ExecSQL(f'TRUNCATE TABLE {dbname}.{table};')
        StoreEOD(df, dbname, table)
    except Exception as e:
        logging.error("Exception occurred at StoreDailyOutput()", exc_info=True)

def load_eod_price(ticker, start, end):
    DB = environ.get("DBMKTDATA")
    TBL = environ.get("TBLDLYPRICE")
    query = f"SELECT * from {DB}.{TBL} where symbol = \'{ticker}\' and Date >= \'{start}\' and Date <= \'{end}\' order by Date;"
    return load_df_SQL(query)

# Versions of GlobalMarketData.current_symbols_* that take no argument. V5 added
# `IN p_type CHAR(1)` ('a' = all, 'o' = optionable; sql/current_symbols_V5.sql),
# and calling a no-argument procedure with one is an error, so the argument is
# sent only for versions outside this set. Unknown future versions are assumed to
# take the type -- that way a V6 gets it without a code change here, and the two
# live versions are pinned by name.
UNTYPED_SYMBOL_PROCS = frozenset({"V1", "V2", "V3", "V4"})

SYMBOL_TYPES = ("a", "o")


def symbol_proc_type(sym_type):
    """Normalise a current_symbols_V5 @type to 'a' (all) or 'o' (optionable).

    Mirrors the procedure's own defaulting: None, '', and anything unrecognised
    mean 'a'. Also what keeps an event-supplied value out of the SQL string --
    the result is always one of SYMBOL_TYPES, never the caller's text.
    """
    value = str(sym_type).strip().lower() if sym_type is not None else ""
    if value in SYMBOL_TYPES:
        return value
    if value:
        logging.error(f"symbol_proc_type: unknown @type {sym_type!r}, using 'a'")
    return "a"


def load_symbols_db(ver="V3", sym_type=None):
    """Symbol list from the stored procedure ``current_symbols_{ver}``.

    ``sym_type`` is V5's @type: 'a' for every non-delisted symbol, 'o' for the
    optionable ones only. It is ignored for the versions in
    UNTYPED_SYMBOL_PROCS, which take no argument, so a caller may pass it
    unconditionally while SYMBOL_PROC_VER still names V4.
    """
    proc = f"GlobalMarketData.current_symbols_{ver}"
    if str(ver).strip().upper() in UNTYPED_SYMBOL_PROCS:
        if sym_type is not None:
            logging.info(f"load_symbol_db: {proc} takes no @type, ignoring {sym_type!r}")
        sql = f"call {proc}"
    else:
        sql = f"call {proc}('{symbol_proc_type(sym_type)}')"
    logging.info(f"load_symbol_db({sql})")
    df = load_df_SQL(sql)
    # symbol_list = df.Symbol.to_list()
    # logging.debug(symbol_list)
    symbol_list = sorted(df.Symbol.unique())
    logging.debug(f'{symbol_list}')
    return symbol_list

def load_symbols(symlistName, ver="V3"):
    """
    # Return list of stock symbols.
    """

    if symlistName == "system":
        return load_symbols_db(ver)
    
    PROD_LIST_DIR = list_dir()
    logging.debug(f"PROD_LIST_DIR is \'{PROD_LIST_DIR}\'")
    logging.debug(f"load_symbols({symlistName})")
    try:
        fpath = os.path.join(PROD_LIST_DIR, f'{symlistName}.csv')
        logging.info(f'Fetch symbols from {fpath}')
        stock_list = pd.read_csv(fpath)
        symbol_list = np.sort(stock_list.Symbol.unique())
        logging.debug(f'{symbol_list}')
        return symbol_list
    except Exception as e:
        logging.error("Exception occurred at load_symbols()", exc_info=True)

def list_dir():
    """Directory the packaged CSVs are read from.

    ``PROD_LIST_DIR`` when it is set, otherwise the directory this module lives
    in. The fin-cron-data copy has no fallback and resolves "." against the CWD,
    which works only because that service is always run from its own folder.
    Here the CSVs are packaged next to this module, so the module's own
    directory is right both on Lambda (/var/task) and in a local dry run,
    whatever the CWD.
    """
    configured = environ.get("PROD_LIST_DIR")
    if configured is not None and str(configured).strip() != "":
        return str(configured).strip()
    return path.dirname(path.abspath(__file__))


def on_lambda():
    """True when running inside a Lambda invocation."""
    return environ.get("AWS_LAMBDA_FUNCTION_NAME") is not None


def out_dir(localrun=False, env_key=None):
    """Directory the dry-run CSVs are written to.

    On Lambda this is always a path under ``/tmp``, because the bundle directory
    (``/var/task``) is read-only: ``localrun`` cannot mean "write beside the
    code" there. Every handler used to decide this for itself as
    ``if localrun or not on Lambda: return "."``, which crashed a ``localrun``
    invoke on Lambda with ``OSError: [Errno 30] Read-only file system``
    (doc/OPERATIONS.md 8.6).

    Locally, ``localrun`` means the CWD -- the documented place to find the
    output of a dry run -- and otherwise ``env_key`` (e.g. ``PORT_OUTPUT_DIR``)
    is honoured when it is set and non-empty.

    A configured directory that is not under ``/tmp`` is ignored on Lambda
    rather than obeyed into a crash.
    """
    configured = environ.get(env_key) if env_key else None
    configured = str(configured).strip() if configured is not None else ""
    if not on_lambda():
        if localrun:
            return "."
        return configured or "."
    target = configured if configured.startswith("/tmp") else "/tmp"
    if configured and target != configured:
        logging.warning(f"{env_key}='{configured}' is not under /tmp; "
                        f"writing to {target} instead (Lambda has no other writable path)")
    try:
        os.makedirs(target, exist_ok=True)
    except OSError:
        logging.error(f"could not create {target}", exc_info=True)
        return "/tmp"
    return target


def load_symbols_dict():
    # Return a {symbol: exchange} map from stock_exchange.csv.

    PROD_LIST_DIR = list_dir()
    try:
        stock_list = pd.read_csv(os.path.join(PROD_LIST_DIR, "stock_exchange.csv"))
        return dict(stock_list.values)
    except Exception as e:
        logging.error("Exception occurred at load_symbols_dict()", exc_info=True)

def load_exchange_tz():
    # Return an {exchange: IANA time zone} map from Exchange_timezone.csv.

    PROD_LIST_DIR = list_dir()
    try:
        tz_list = pd.read_csv(os.path.join(PROD_LIST_DIR, "Exchange_timezone.csv"))
        return dict(tz_list.values)
    except Exception as e:
        logging.error("Exception occurred at load_symbols_dict()", exc_info=True)

def get_Last_Date_by_Sym(tblname, sym):
    FIRSTTRAINDTE = datetime.strptime(environ.get("FIRSTTRAINDTE"), "%Y/%m/%d").date()

    mktdate = get_Max_date(tblname, sym)
    lastdt = FIRSTTRAINDTE
    if mktdate is not None:
        lastdt = mktdate + timedelta(days=1)
    return lastdt

def get_Last_Datetime_by_Sym(tblname, sym):
    FIRSTTRAINDTE = datetime.strptime(environ.get("FIRSTTRAINDTE"), "%Y/%m/%d")

    mktdate = get_Max_datetime(tblname, sym)
    lastdt = FIRSTTRAINDTE
    if mktdate is not None:
        lastdt = mktdate + timedelta(days=1)
    return lastdt
# --------------------------------------------------------------------------
# Upstream-track helpers (PLAN-SR-UPSTREAM Phase A).
#
# Written for python3.13 / SQLAlchemy 2.0 / pandas 2.2, the only runtime this
# service deploys. They use just to_sql(method=callable),
# sqlalchemy.insert().prefix_with() and Engine.begin() + text(), so a later
# back-port into fin-cron-data's 1.4-era copy would need no rewrite. Same
# error contract as the rest of this module: log and return None, never raise.
# --------------------------------------------------------------------------

#: Column order of GlobalMarketData.load_audit (sql/upstream_tables.sql).
AUDIT_COLUMNS = ['run_id', 'job', 'Symbol', 'Exchange', 'date_lo', 'date_hi',
                 'table_name', 'segment', 'n_rows', 'n_ok', 'n_expected',
                 'status', 'error', 'started_at', 'finished_at',
                 'yf_version', 'host']

#: load_audit.error is VARCHAR(512) and the server runs STRICT_ALL_TABLES,
#: which rejects an over-long value instead of truncating it.
AUDIT_ERROR_MAX = 512

_CROCKFORD = '0123456789ABCDEFGHJKMNPQRSTVWXYZ'


def require_env(name):
    """Read a required env var, raising instead of returning None.

    environ.get() returning None interpolates the string 'None' into SQL and
    only surfaces later as a confusing database error.
    """
    value = environ.get(name)
    if value is None or str(value).strip() == "":
        raise RuntimeError(f"required environment variable {name} is not set. "
                           f"Add it to Ops/fin-deep-data/.env (see .env.example).")
    return str(value).strip()


def env_or(name, default):
    """Read an optional env var; blank counts as unset."""
    value = environ.get(name)
    return default if value is None or str(value).strip() == "" else str(value).strip()


def utc_now():
    """Current UTC time, tz-naive -- the form load_audit's DATETIME(3) columns hold."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def new_run_id(now_ms=None):
    """A 26-char ULID: 48-bit ms timestamp + 80 random bits, Crockford base32.

    Sorts by creation time, which is what makes it usable as load_audit.run_id.
    Hand-rolled so the layer needs no ulid dependency.
    """
    if now_ms is None:
        now_ms = int(time.time() * 1000)
    value = ((now_ms & ((1 << 48) - 1)) << 80) | int.from_bytes(os.urandom(10), 'big')
    chars = []
    for _ in range(26):
        chars.append(_CROCKFORD[value & 31])
        value >>= 5
    return ''.join(reversed(chars))


def run_host():
    """'lambda:<function name>' on Lambda, else the hostname. Max 64 chars."""
    fn = environ.get("AWS_LAMBDA_FUNCTION_NAME")
    host = f"lambda:{fn}" if fn else socket.gethostname()
    return host[:64]


def _insert_ignore_method(verb):
    """A pandas to_sql `method` that emits INSERT <verb> and returns the rowcount."""
    def _method(pd_table, conn, keys, data_iter):
        rows = [dict(zip(keys, row)) for row in data_iter]
        if not rows:
            return 0
        result = conn.execute(insert(pd_table.table).prefix_with(verb), rows)
        return result.rowcount
    return _method


def append_ignore(df, schema, table, chunk=500):
    """Append df, silently skipping rows whose primary key already exists.

    INSERT IGNORE on MySQL, INSERT OR IGNORE on sqlite (so the sqlite test
    fixture can exercise it), picked from the engine's dialect. The first value
    written for a key wins, which keeps the append-only semantics of the price
    tables and makes a re-run or an overlapping sweep harmless. The target
    table must already exist: the server runs with sql_require_primary_key=ON,
    so to_sql can never create it.

    Returns the number of rows actually inserted, or None on failure.
    """
    try:
        if df is None or len(df) == 0:
            return 0
        engine = get_DBengine()
        verb = 'OR IGNORE' if engine.dialect.name == 'sqlite' else 'IGNORE'
        inserted = df.to_sql(name=table, con=engine, schema=schema, if_exists='append',
                             index=False, chunksize=chunk,
                             method=_insert_ignore_method(verb))
        inserted = 0 if inserted is None else int(inserted)
        logging.info(f'append_ignore {schema}.{table}: {inserted} of {len(df)} rows inserted')
        return inserted
    except Exception as e:
        logging.error(f"Exception occurred at append_ignore({schema}.{table})", exc_info=True)


def audit_frame(rows):
    """Build a load_audit DataFrame from row dicts, in DDL column order.

    Missing keys become None, Exchange/yf_version default to '', and error is
    cut to AUDIT_ERROR_MAX. Pure -- also used for the dbFlag=False CSV.
    """
    def _int_or_none(v):
        return None if v is None or pd.isna(v) else int(v)

    df = pd.DataFrame(list(rows), columns=AUDIT_COLUMNS).astype(object)
    df = df.where(pd.notna(df), None)
    df['Exchange'] = df['Exchange'].map(lambda v: '' if v is None else v)
    df['yf_version'] = df['yf_version'].map(lambda v: '' if v is None else v)
    df['host'] = df['host'].map(lambda v: run_host() if v is None else v)
    df['error'] = df['error'].map(lambda e: None if e is None else str(e)[:AUDIT_ERROR_MAX])
    # Built as object Series: map() would re-infer float64 once a None is
    # mixed in, and 857 would reach the INT column as 857.0.
    for col in ('n_rows', 'n_ok', 'n_expected'):
        values = [_int_or_none(v) for v in df[col]]
        if col == 'n_rows':
            values = [v or 0 for v in values]
        df[col] = pd.Series(values, index=df.index, dtype=object)
    return df


def audit_run(rows):
    """Insert rows into load_audit. Never raises.

    An audit failure must never fail a data load, so this logs and returns
    None on any error -- including a missing TBLLOADAUDIT. Returns the number
    of rows inserted otherwise.
    """
    try:
        table = require_env("TBLLOADAUDIT")
        schema = require_env("DBMKTDATA")
        return append_ignore(audit_frame(rows), schema, table)
    except Exception as e:
        logging.error("Exception occurred at audit_run()", exc_info=True)


def audit_summary(job, table_name, run_id, started_at, status='ok', n_rows=0,
                  date_lo=None, date_hi=None, n_ok=None, n_expected=None,
                  error=None, yf_version='', finished_at=None):
    """One '*' summary row for load_audit, as every handler in this service writes it."""
    return {'run_id': run_id, 'job': job, 'Symbol': '*', 'Exchange': '',
            'date_lo': date_lo, 'date_hi': date_hi, 'table_name': table_name,
            'segment': 'summary', 'n_rows': int(n_rows or 0), 'n_ok': n_ok,
            'n_expected': n_expected, 'status': status, 'error': error,
            'started_at': started_at,
            'finished_at': finished_at if finished_at is not None else utc_now(),
            'yf_version': yf_version, 'host': run_host()}


def shard_symbols(symbols, i, n):
    """Round-robin shard i of n over the sorted, de-duplicated list."""
    n = max(int(n), 1)
    i = int(i)
    if not 0 <= i < n:
        raise ValueError(f"shard index {i} is outside 0..{n - 1}")
    return sorted(set(symbols))[i::n]


def time_left_ok(context, reserve_ms=120_000):
    """True while the Lambda has more than reserve_ms left. Always True locally."""
    if context is None or not hasattr(context, 'get_remaining_time_in_millis'):
        return True
    return context.get_remaining_time_in_millis() > reserve_ms


def day_start_utc(day, tz='America/New_York'):
    """Midnight of `day` in tz, as a tz-naive UTC datetime."""
    local = pytz.timezone(tz).localize(datetime(day.year, day.month, day.day))
    return local.astimezone(pytz.utc).replace(tzinfo=None)


def missing_for_sweep(job, date, expected, table_name, tz='America/New_York'):
    """Expected symbols with no 'ok'/'empty' load_audit row since `date` began.

    `date` is the run's session date in tz; a row counts when it finished on or
    after that day's local midnight. Returns a sorted list, or None when
    load_audit cannot be read (the caller must not guess -- treating an
    unreadable audit as "nothing done" would re-download the whole list).
    """
    try:
        db = require_env("DBMKTDATA")
        tbl = require_env("TBLLOADAUDIT")
        since = day_start_utc(date, tz)
        query = (f"SELECT DISTINCT Symbol FROM {db}.{tbl} "
                 f"WHERE job = '{job}' AND table_name = '{table_name}' "
                 f"AND segment <> 'summary' AND status IN ('ok', 'empty') "
                 f"AND finished_at >= '{since:%Y-%m-%d %H:%M:%S}';")
        df = load_df_SQL(query)
        if df is None:
            return None
        done = set(df.Symbol)
        return sorted(set(expected) - done)
    except Exception as e:
        logging.error("Exception occurred at missing_for_sweep()", exc_info=True)
