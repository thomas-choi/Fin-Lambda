# TECHNICAL-DESIGN.md

How Fin-Lambda is built: the deployed functions, the shared `dataUtil` layer,
and the tables they write.

> **Scope note.** This file was created alongside `portAssetsHandler` and
> covers that handler in depth plus the shared `dataUtil` API. The nine
> pre-existing handlers have a one-line row each; backfilling deep-dives for
> them is tracked in `TODOS.md` §6.

## Changelog

- 2026-10-01 | Modified | §6.2 `option_chains()` retries twice, not five times — the sweep invocations are the real retry.
- 2026-10-01 | Added | §5 `current_symbols_V5` and its `@type` argument ('a' all-but-delisted, 'o' also drops `options = 0`); §6.2 the data flow now shows which type each collector asks for; §6 `dataUtil.load_symbols_db` gains `sym_type` and `symbol_proc_type`.
- 2026-10-01 | Modified | §5 `current_symbols_V4` row: measured 863 rows, and it is the unfiltered union — the exclusion list is V5's.
- 2026-10-01 | Added | §6 `dataUtil` API gains `out_dir()` / `on_lambda()`; the six handlers no longer compute their own output directory.
- 2026-10-01 | Modified | §6 the optChainEOD dispatcher rationale: `reservedConcurrency` is commented out (account concurrency quota is 10), so the shard fan-out is currently uncapped.
- 2026-08-01 | Added | Initial file: deployed-functions table, `portAssetsHandler` deep-dive, `dataUtil` API reference, `Trading.portfolio_assets_info` schema, stored-procedure dependency list.
- 2026-10-01 | Added | §6 the `fin-deep-data` service (PLAN-SR-UPSTREAM A–F): its eight python3.13 functions, the three-layer split, per-function packaging, the `dataUtil` fork and its Phase A helpers, the `load_audit` / `corp_action_daily` / `v_load_status` tables, and the v2 successors with disabled schedules.
- 2026-10-01 | Modified | §1 deployed-functions table now says which service each function belongs to; §5 adds `current_symbols_V4` and records which procedures the new service depends on.

---

## 1. Deployed functions

Two Serverless services, both in region `us-east-2` with `timeout: 900` and AWS
profile `ServerLessUser`:

- **`fin-cron-data`** (`Ops/fin-cron-data/serverless.yml`) — the ten functions
  below. Nine on python3.10 / `finCron`, one (`portAssetsHandler`) on
  python3.13 / `finPortLib`. Frozen as of 2026-10-01: new work goes to the other
  service.
- **`fin-deep-data`** (`Ops/fin-deep-data/serverless.yml`) — eight python3.13
  functions, §6. Three new data sets and five successors to functions in
  `fin-cron-data`.

The two share no file, no layer and no `.env`.

| Function | Handler | Schedule (UTC) | Runtime | Writes to |
|---|---|---|---|---|
| cronHandler | `handler.run` | `0/10` at 13-21 and 21-00, Mon-Fri | 3.10 | `GlobalMarketData.snapshot` (delete-then-append) |
| optHandler | `opt_handler.run` | `0/10` at 12-21, Mon-Fri | 3.10 | `GlobalMarketData.options_snapshot` (delete-then-append) |
| yfus30minEOD | `eoddata_minhandler_us.run` | 00:05 Tue-Sat | 3.10 | `GlobalMarketData.histminprice` (watermark append) |
| yfasia30minEOD | `eoddata_minhandler_asia.run` | 10:00 Mon-Fri | 3.10 | `GlobalMarketData.histminprice` (watermark append) |
| fffHandler | `fff_handler.run` | monthly, 1st | 3.10 | `GlobalMarketData.famaFrench` — **currently broken**, see `TODOS.md` §4.1 |
| usrateHandler | `usrate_handler.run` | 21:01 Mon-Fri | 3.10 | `GlobalMarketData.USRates` |
| FXrateHandler | `fx_handler.run` | hourly | 3.10 | `GlobalMarketData.FX_snapshot` (delete-then-append) |
| FXHistHandler | `fxeod_handler.run` | 21:10 daily | 3.10 | `GlobalMarketData.FX_histdaily` (watermark append) |
| yfNewshandler | `yf-news-collect.run` | hourly | 3.10 | S3 / Cloudflare R2 |
| **portAssetsHandler** | `port_assets_handler.run` | **22:30 Mon-Fri** | **3.13** | **`Trading.portfolio_assets_info` (only-on-change)** |

Modules present in the folder but **not** deployed — `yfin_handler.py`,
`yfineod_handler.py`, `FOC_data.py`, `opt_ibapi.py`, `yfin_opt.py`, and
`handler.stk_run()` — are superseded or manual-run. Check `serverless.yml`
before assuming a file is live.

### Runtime split

`portAssetsHandler` is the only python3.13 function. Its dependency stack is
incompatible with the other nine:

| | 3.10 functions (`finCron` layer) | 3.13 function (`finPort313` layer) |
|---|---|---|
| pandas | 1.5.3 | 2.2.3 |
| SQLAlchemy | 1.4.46 | 2.0.36 |
| numpy | 1.26.4 | 2.1.3 |

The runtime and the layer ARN are declared **on the function**, never at
provider level, so the 3.13 layer can never be attached to a 3.10 function.
`dataUtil.py` is shared by both and runs unmodified on either stack — see §3.

---

## 2. `portAssetsHandler` — index membership

### Purpose

Collect the current constituent list of the S&P 500 and the NASDAQ-100 and
store each as a dated, point-in-time membership set.

### Data flow

```
run(event, context)
  |
  +-- S&P 500 --------------------------------------------------------------
  |     fetch_sp500()          Wikipedia "List of S&P 500 companies"
  |                            -> DataFrame[Symbol, Sector(GICS)]   503 rows
  |     fetch_spy_holdings()   SSGA SPY daily holdings .xlsx  (cross-check)
  |     log_symmetric_difference()      <- warning only, never fatal
  |
  +-- NASDAQ-100 -----------------------------------------------------------
  |     fetch_ndx100()         api.nasdaq.com list-type/nasdaq100  103 rows
  |                            fallback: Wikipedia "List of NASDAQ-100 companies"
  |                            -> DataFrame[Symbol, Sector="General"]
  |     fetch_ndx100_crosscheck()  Wikipedia    (cross-check)
  |
  +-- per index ------------------------------------------------------------
        sanity_gate()          503 in [490,520] / 103 in [95,110], else STOP
        build_frame()          the 8 table columns, in DDL order
        <write CSV>            always, before any DB work
        get_stored_max_date()  SELECT max(Date) WHERE Port_name = ...
        load_stored_set()      the newest stored set
        has_changed()          full row tuple, sorted, NaN-safe
        resolve_effective_date()  -> write | replace | skip
        write_set()            [DELETE (Date,Port_name)] -> StoreEOD -> recount
```

### Sources

| Index | Role | Source | Shape |
|---|---|---|---|
| S&P 500 | primary | Wikipedia *List of S&P 500 companies* | 503 × 8; `Symbol`, `GICS Sector`, … |
| S&P 500 | cross-check | SSGA **SPY** daily holdings `.xlsx` | header on sheet row 4; `Sector` is `-` on every row, so unusable for sector |
| NASDAQ-100 | primary | `api.nasdaq.com/api/quote/list-type/nasdaq100` | `data.data.rows`, 103 records; needs a browser `User-Agent`; `sector` is `""` on every row |
| NASDAQ-100 | fallback + cross-check | Wikipedia *List of NASDAQ-100 companies* | 103 × 4; ICB taxonomy, **deliberately discarded** |

Rejected during research: `slickcharts.com` (HTTP 403, Cloudflare — will fail
harder from a Lambda IP), Invesco QQQ holdings (HTTP 406, or HTML not CSV),
iShares IVV holdings CSV (returns the HTML landing page).

**Table selection is by content.** `_pick_table()` locates the constituent
table by its required columns rather than by `read_html()[0]`. The NASDAQ-100
list has already moved pages once, and that page alone parses to 30+ tables.

### Design decisions

**`Sector` is populated for `SP500` only.** `NDX100` rows carry the literal
`General`. The Nasdaq API returns no sector, and Wikipedia's NDX page uses
**ICB** while the S&P page uses **GICS** — mixing them in one column would make
`Sector` incomparable across `Port_name`, with `AAPL` reading
`Information Technology` under `SP500` and `Technology` under `NDX100`.
`General` is the sentinel `DJI` and `HSI` already use in this table. If GICS
sectors for NDX names are wanted later, the clean route is a lookup against the
`SP500` rows (~90 % overlap), not a second scrape.

**Symbols are stored dashed** (`BRK-B`, not `BRK.B`). Every price table in this
database is yfinance-fed and uses the dashed form; the point of this table is to
be joinable to price history. `normalize_us_symbol()` does the conversion and is
scoped to US symbols, since a blanket `.`→`-` would corrupt foreign listings
like `0700.HK`.

**`Rate` is the FX rate to USD**, not an index weight — confirmed against the
existing rows, where HKD carries `0.1282` and CNY `0.14`. Both indices are
100 % USD, so `Rate = 1.0` throughout and no weight source is needed.

**Effective date.** NDX100 uses the Nasdaq API's own `data.date`. SP500 uses the
NY-local run date: its primary source carries no as-of date, and the SPY
workbook's `As of` belongs to a different product behind an optional flag —
deriving a primary-key column from an optional input would make the date move
whenever `INDEX_CROSSCHECK` is toggled. SPY's as-of is logged, not used.

**Only-on-change.** `has_changed()` compares the full row tuple
`(Symbol, Class, Sector, Type, Currency, Rate)` sorted by `Symbol`, so a sector
reclassification with unchanged membership still produces a new dated set. The
full set is always rewritten — never deltas — so an as-of query stays a simple
`WHERE Date <= X ORDER BY Date DESC LIMIT 1` followed by an equality join.

**Write verification.** `StoreEOD` wraps `to_sql` in `try/except` and only
*logs* on failure, so a duplicate-key error would be swallowed and the run would
report success having written nothing. `write_set()` therefore re-reads the row
count after every write and logs loudly on a mismatch.

### Write semantics by decision

| Condition | Action |
|---|---|
| No rows stored for this `Port_name` | write (seeds the set) |
| New date > stored max, content changed | write |
| New date == stored max, content changed | `DELETE` that `(Date, Port_name)`, then write |
| New date < stored max | **skip**, log ERROR — stale source |
| Content unchanged (and not `force`) | **skip**, log INFO — the expected outcome |

---

## 3. `dataUtil.py` — the only DB layer

A module-global `dbconn` SQLAlchemy engine created lazily by `get_DBengine()`
(MySQL via PyMySQL, from `DBHOST`/`DBPORT`/`DBUSER`/`DBPWD`/`DBMKTDATA`). No
ORM, no migrations — table schemas live in the database only.

**Errors are swallowed, not raised.** Nearly every function wraps its body in
`try/except` + `logging.error(..., exc_info=True)` and implicitly returns
`None`. Callers rarely check. This is a documented contract (`TODOS.md` §1.7);
if you make a function raise, audit its callers first.

### API

| Function | Returns | Notes |
|---|---|---|
| `get_DBengine()` | `Engine` | cached module-global singleton |
| `load_df_SQL(query)` | `DataFrame` \| `None` | every read goes through here or `load_df` |
| `load_df(...)` | `DataFrame` \| `None` | reads `histdailyprice7` |
| `StoreEOD(df, schema, table)` | `None` | `to_sql(..., if_exists='append')`; **swallows write failures** |
| **`ExecSQL(query)`** | **`int` rowcount, or `None` on failure** | **changed 2026-08-01 — see below** |
| `get_Max_date` / `get_Max_datetime` / `get_Max_Options_date` | scalar \| `None` | watermark queries |
| `load_symbols(name)` | `list` | CSV under `$PROD_LIST_DIR`, **except** `"system"` → stored procedure |
| `load_symbols_dict()` / `load_exchange_tz()` | `dict` | from `stock_exchange.csv` / `Exchange_timezone.csv` |

### `ExecSQL()` — SQLAlchemy 1.4/2.0-agnostic (2026-08-01)

`ExecSQL` previously called `get_DBengine().execute(query)`. `Engine.execute()`
was **removed** in SQLAlchemy 2.0 and already emitted `RemovedIn20Warning` on
the pinned 1.4.46. It was the one genuine incompatibility in a file that is
zipped into every function in this service — the rest of the module (`to_sql`,
`pd.read_sql`) behaves identically on both versions.

```python
def ExecSQL(query):
    logging.info(f"ExecSQL: {query}")
    try:
        with get_DBengine().begin() as conn:
            rowcount = conn.execute(text(query)).rowcount
        logging.info(f'number of rows execed: {rowcount}')
        return rowcount
    except Exception as e:
        logging.error("Exception occurred at ExecSQL()", exc_info=True)
```

`Engine.begin()` and `text()` exist in both versions. `begin()` commits
explicitly where 1.4's legacy autocommit committed implicitly — that equivalence
is pinned by `tests/unit/test_dataUtil.py::test_exec_sql_commits_*` and was
demonstrated against live MySQL, not assumed.

The new return value is backward-compatible (it returned `None` implicitly) and
is what lets `portAssetsHandler` verify its own `DELETE`.

**Blast radius — three live functions** use `ExecSQL`, all for the same
delete-then-append snapshot pattern: `cronHandler`, `optHandler`,
`FXrateHandler`. (`dataUtil.StoreWebDaily` and the undeployed
`yfin_handler.py`/`yfineod_handler.py` also call it.)

A fourth fork of `dataUtil.py` for 3.13 was **considered and rejected** — the
repo already carries three forks (`Ops/fin-cron-data/`, `Dev/fin-cron-data/`,
`Ops/fin-cron-Pgsql/dataUtil_Pgsql.py`), the incompatible surface was one
function, and a fork guarantees every future fix has to be applied twice.

---

## 4. `Trading.portfolio_assets_info`

> **Note:** schema read from the live table via `SHOW CREATE TABLE`, not from
> DDL in this repo. The table **already existed** before `portAssetsHandler`
> and holds seven other portfolios.

```sql
CREATE TABLE `portfolio_assets_info` (
  `Date`      date        NOT NULL,
  `Port_name` varchar(45) NOT NULL,
  `Symbol`    varchar(20) NOT NULL,
  `Class`     varchar(20) DEFAULT NULL,
  `Sector`    varchar(45) DEFAULT NULL,
  `Type`      varchar(20) DEFAULT NULL,
  `Currency`  varchar(4)  DEFAULT NULL,
  `Rate`      float       DEFAULT NULL,
  PRIMARY KEY (`Date`,`Port_name`,`Symbol`)
)
```

### Existing portfolios (pre-dating this handler)

| `Port_name` | Rows | Date range | Sector convention |
|---|---|---|---|
| `DJI` | 30 | 2024-12-30 | `General` |
| `HSI` | 167 | 2024-12-30 → 2025-07-31 | `General` |
| `IAM` | 62 | 2024-12-30 | real sectors (21 values) |
| `TM-ASIA` | 122 | 2024-12-30 → 2025-01-15 | real sectors |
| `TM-CHINA` | 115 | 2024-12-30 → 2025-01-15 | real sectors |
| `US-ETF` | 18 | 2024-12-30 | asset-class labels |
| `US-Top20` | 78 | 2024-12-30 | mixed; some `NoSector`, some with trailing whitespace |

### Columns written by `portAssetsHandler`

| Column | `SP500` | `NDX100` |
|---|---|---|
| `Date` | NY-local run date | Nasdaq API `data.date` |
| `Port_name` | `$SP500_PORT_NAME` (default `SP500`) | `$NDX100_PORT_NAME` (default `NDX100`) |
| `Symbol` | Wikipedia `Symbol`, dashed | Nasdaq `symbol`, dashed |
| `Class` | `$PORT_ASSET_CLASS` (default `Equity`) | same |
| `Sector` | Wikipedia GICS Sector | literal `General` |
| `Type` | `$PORT_ASSET_TYPE` (default `Stock`) | same |
| `Currency` | `USD` | `USD` |
| `Rate` | `1.0` | `1.0` |

---

## 5. Stored-procedure dependencies

None of these live in this repo, so a schema change on the server breaks
handlers silently.

| Procedure | Used by |
|---|---|
| `GlobalMarketData.current_symbols_V3` | `dataUtil.load_symbols("system")` (fin-cron-data) |
| `GlobalMarketData.current_symbols_V4` | `eodDaily`, `optChainEOD` via `dataUtil.load_symbols_db("V4")` (fin-deep-data). Takes no argument. 863 rows on 2026-10-01; it already contains the stock-options list, the ETF-options list and the `portfolio_assets_info` members, so the new handlers add no union of their own. No exclusions — delisted symbols are in it |
| `GlobalMarketData.current_symbols_V5(IN p_type CHAR(1))` | the same two handlers once `SYMBOL_PROC_VER=V5`. V4's union **minus an exclusion list read from `GlobalMarketData.SymbolMaster`**: `@type='a'` drops `delisted = 1` (838 rows), `@type='o'` drops `options = 0 OR delisted = 1` (814). `eodDaily` asks for `'a'`, `optChainEOD` for `'o'`. A symbol with no `SymbolMaster` row is kept by both — 648 of the 863 are in that position, so `'o'` is an exclusion list, not a whitelist. DDL in `Ops/fin-deep-data/sql/current_symbols_V5.sql`; MySQL has no default argument values, so the caller always sends one |
| `GlobalMarketData.get_us_symbol` | `eoddata_minhandler_us.py`; `intraday_min_handler.run_us` |
| `GlobalMarketData.get_asia_symbol` | `eoddata_minhandler_asia.py`; `intraday_min_handler.run_asia` |
| `Trading.sp_etf_trades_v2` | `opt_handler.py` |
| `Trading.sp_stock_trades_V3` | `opt_handler.py` |

`portAssetsHandler` depends on **none** of them — its symbol lists come from
the public sources in §2.

---

## 6. The `fin-deep-data` service (python3.13)

`Ops/fin-deep-data/` — PLAN-SR-UPSTREAM Phases A–F, implemented 2026-10-01 as a
**separate Serverless service** so that no function or layer of `fin-cron-data`
changes. Every function is `python3.13`; the runtime and the layer ARNs are
declared on each function, never at provider level.

### 6.1 Functions

| Function | Handler | Layers | Schedule (America/New_York) | Writes |
|---|---|---|---|---|
| eodDaily | `eod_daily_handler.run` | core + yf | 18:30 Mon–Fri; sweep 19:00 | `$EOD_WRITE_TBL` (INSERT IGNORE), `corp_action_daily`, `load_audit` |
| optChainEOD | `optchain_eod_handler.run` | core + yf | dispatch 17:40; sweeps 18:40, 19:40 | `$OPT_WRITE_TBL` (INSERT IGNORE), R2 `$OPT_RAW_PREFIX/`, `load_audit` |
| statusReport | `status_report_handler.run` | core | 20:00 Mon–Fri | nothing — SNS e-mail + R2 JSON |
| portAssetsHandlerv2 | `port_assets_handler.run` | core + web | 18:30 — **disabled** | `Trading.portfolio_assets_info` (only-on-change), `load_audit` |
| usrateHandlerv2 | `usrate_handler.run` | core + web | 17:05 — **disabled** | `$TBLUSRATES`, `load_audit` |
| FXHistHandlerv2 | `fxeod_handler.run` | core + yf | 17:10 — **disabled** | `$TBLHISTFX`, `load_audit` |
| yfus30minEODv2 | `intraday_min_handler.run_us` | core + yf | 20:05 — **disabled** | `$TBLMINUTEPRICE`, `load_audit` |
| yfasia30minEODv2 | `intraday_min_handler.run_asia` | core + yf | 06:00 — **disabled** | `$TBLMINUTEPRICE`, `load_audit` |

Schedules use EventBridge Scheduler (`method: scheduler`) with an explicit
`timezone`, so they hold their exchange-local time across DST instead of
drifting an hour the way `fin-cron-data`'s UTC cron entries do.

**Why five schedules are disabled.** Each of those five functions writes a table
that its python3.10 counterpart in `fin-cron-data` still writes. Two writers on
one table would duplicate rows (`$TBLMINUTEPRICE`, `$TBLHISTFX`, `$TBLUSRATES`)
or store a spurious membership change (`portfolio_assets_info`). The schedules
therefore render as `AWS::Scheduler::Schedule` with `State: DISABLED`: deploying
the service is always safe, and each data set is cut over on its own, by
enabling one schedule and removing the old function —
`doc/OPERATIONS.md` §11.

`load_audit.job` is the **data set's** job name, not the Lambda's, so these five
write `usrateHandler`, `FXHistHandler`, `yfus30minEOD`, `yfasia30minEOD` and
`portAssetsHandler`. The status report keys on `(table_name, job)`, so it does
not grow a second line at the cutover; `load_audit.host` says which function
wrote the row.

### 6.2 Data flow

```
              current_symbols_{SYMBOL_PROC_VER}
            V4: 863 no exclusions | V5: 'a' 838 / 'o' 814
                              |
            +-----------------+------------------+
            |                                    |
        eodDaily                            optChainEOD
     @type 'a'                            @type 'o'
   shard -> watermark SELECT            dispatch -> N async shards
   -> plan (batch by exchange tz,       -> per underlying:
      singles for first loads/gaps)        option_chains() 2x retry
   -> yf.download(auto_adjust=False,       -> filter_opt_chain()
      actions=True, group_by=ticker)       -> INSERT IGNORE $OPT_WRITE_TBL
   -> reshape_batch -> drop_partial_bar    -> raw CSV -> R2
   -> INSERT IGNORE $EOD_WRITE_TBL         -> load_audit row
   -> extract_actions -> corp_action_daily
   -> load_audit row per symbol + '*' summary per table
            |                                    |
            +----------------+-------------------+
                             v
                    load_audit -> v_load_status
                             v
                       statusReport -> SNS e-mail + R2 status/latest.json
```

Both collectors are idempotent: writes are `INSERT IGNORE`, so a re-run, an
overlapping sweep and a retried async shard can never duplicate a row. The
sweeps re-run only what has no `ok`/`empty` audit row for the session date
(`missing_for_sweep`), and a `time_left_ok` guard marks unstarted symbols
`skipped` rather than letting the Lambda die mid-batch.

### 6.3 Key design decisions

| Decision | Why |
|---|---|
| `INSERT IGNORE`, not `to_sql` append | the collectors re-run (sweeps, async retries, a manual replay) and the price tables have real primary keys; the first value written wins, so a replay never changes history |
| One `GROUP BY` watermark query | the production host job issued 857 separate `MAX(Date)` queries |
| Batch downloads grouped by exchange time zone | a batched `yf.download` mixing US and HK tickers yields NaN rows on dates where one market is closed (finding F7); NaN-`Close` rows are dropped as well |
| 21-day batch window | ≈14 sessions — enough to re-see a late-posted dividend or split, so it doubles as the corporate-action window |
| `drop_partial_bar` | the production cron ran 10 minutes after the close and in winter captured non-final bars (finding L2). A bar dated the exchange's local today is kept only 60 minutes after that exchange's close; crypto, which never closes, is always treated as partial |
| `auto_adjust=False` pinned | from yfinance 0.2.51 the default is `True`, which silently adjusts O/H/L/C and drops `Adj Close` |
| Dispatcher instead of N schedule entries | `OPT_SHARDS` is sized from a measured `T_total`, and changing it means changing one env var rather than editing `serverless.yml`; `reservedConcurrency` was to cap parallel Yahoo traffic from Lambda IPs — but it is **commented out**, because reserving needs ≥ 100 unreserved executions and the account quota is 10, so the fan-out is currently uncapped and shares that pool of 10 with the live python3.10 functions (`doc/OPERATIONS.md` §8.4) |
| `corp_action_daily` keyed `(Date, Symbol, Exchange)` | `INSERT IGNORE` keeps the **first** sighting, which makes `first_seen_at` an honest `available_at` for the action |
| Audit writes wrapped in `try` | an audit failure must never fail a data load; `dataUtil.audit_run` additionally swallows its own errors |
| Two merged intraday handlers | `eoddata_minhandler_us.py` and `_asia.py` differed only in a stored-procedure name and the audit job name; `intraday_min_handler.MARKETS` holds that difference and the two Lambdas point at `run_us` / `run_asia` |

### 6.4 Packaging — one `serverless.yml`, eight zips

`package: individually: true`, a service-wide `'!**'` baseline, and a
`package.patterns` allowlist on each function. Without the `'!**'` line
Serverless would zip the whole folder — `node_modules`, the build trees, any
dry-run CSV left behind — into all eight packages.

| Package | Contents | Size |
|---|---|---|
| eodDaily | `dataUtil.py`, `eod_daily_handler.py`, `stock_exchange.csv`, `Exchange_timezone.csv` | 65 KB |
| optChainEOD | the above + `optchain_eod_handler.py` (imports `exchange_for`) | 71 KB |
| statusReport | `dataUtil.py`, `status_report_handler.py` | 14 KB |
| portAssetsHandlerv2 | `dataUtil.py`, `port_assets_handler.py` | 21 KB |
| usrateHandlerv2 | `dataUtil.py`, `usrate_handler.py` | 11 KB |
| FXHistHandlerv2 | `dataUtil.py`, `fxeod_handler.py` | 11 KB |
| yfus30minEODv2 / yfasia30minEODv2 | `dataUtil.py`, `intraday_min_handler.py`, both exchange CSVs, `intra_blacklist.csv` | 61 KB each |

### 6.5 Three layers, not one

| Layer | Contents | Unzipped | Zipped |
|---|---|---|---|
| `finDeepCore` | pandas 2.2.3, numpy 2.1.3, SQLAlchemy 2.0.36, PyMySQL 1.1.1, python-dotenv, pytz, requests | 101 MB | 29 MB |
| `finDeepYf` | yfinance 0.2.58, curl_cffi, peewee, frozendict, multitasking, platformdirs, beautifulsoup4 | 28 MB | 10 MB |
| `finDeepWeb` | lxml 5.3.0, beautifulsoup4, openpyxl 3.1.5 | 14 MB | 6 MB |

`finDeepYf` and `finDeepWeb` are de-duplicated against `finDeepCore` at build
time, so neither is importable without it. Build procedure and the size ceiling:
`doc/OPERATIONS.md` §10.

### 6.6 `dataUtil.py` is a fork

`Ops/fin-deep-data/dataUtil.py` was taken from `Ops/fin-cron-data/dataUtil.py`
at `fa1d6ac` and is maintained separately. A change in one does **not**
propagate to the other, and that is the point: the fin-cron-data copy ships to
nine python3.10 functions on pandas 1.5 / SQLAlchemy 1.4 and is frozen.

Differences from the original:

| Added / changed | Returns | Notes |
|---|---|---|
| `append_ignore(df, schema, table, chunk=500)` | rows inserted \| `None` | `INSERT IGNORE` (MySQL) or `INSERT OR IGNORE` (sqlite), chosen from the dialect; the sqlite branch exists so the test fixture can exercise it for real |
| `audit_run(rows)` / `audit_frame(rows)` / `audit_summary(...)` | rows inserted \| `None` / `DataFrame` / `dict` | `load_audit` writer, its pure frame builder, and the `'*'` summary row. `error` is cut to 512 chars because the server runs `STRICT_ALL_TABLES` |
| `new_run_id()` / `run_host()` | 26-char ULID / `str` | hand-rolled Crockford base32, so the layer needs no ULID dependency; host is `lambda:<function>` or the hostname |
| `shard_symbols(symbols, i, n)` | `list` | round-robin over the sorted, de-duplicated list; raises on a bad index |
| `time_left_ok(context, reserve_ms=120_000)` | `bool` | `True` when `context` is `None`, i.e. always locally |
| `missing_for_sweep(job, date, expected, table_name)` | `list` \| `None` | expected symbols with no `ok`/`empty` row since that session's local midnight; `None` when `load_audit` cannot be read, so a sweep never guesses |
| `require_env(name)` / `env_or(name, default)` | `str` | `require_env` **raises** — the one place in this module that does. `environ.get()` returning `None` would interpolate the string `'None'` into SQL |
| `day_start_utc(day, tz)` / `utc_now()` | tz-naive `datetime` | UTC, the form `load_audit`'s `DATETIME(3)` columns hold |
| `out_dir(localrun=False, env_key=None)` | `str` | Where a dry run's CSVs go. **Always under `/tmp` on Lambda**, whatever `localrun` says, because `/var/task` is read-only; a configured `env_key` outside `/tmp` is ignored with a warning. Locally `localrun` means the CWD. Replaced six per-handler copies of an inverted condition (`doc/OPERATIONS.md` §8.6) |
| `on_lambda()` | `bool` | `AWS_LAMBDA_FUNCTION_NAME` is set |
| `load_symbols_db(ver="V3", sym_type=None)` | `list` | `CALL GlobalMarketData.current_symbols_{ver}`. `sym_type` is V5's `@type`; it is **withheld** from the versions in `UNTYPED_SYMBOL_PROCS` (`V1`–`V4`), which take no argument, and sent for anything else. That is what lets a handler pass its type unconditionally while `SYMBOL_PROC_VER` still names V4 |
| `symbol_proc_type(sym_type)` | `'a'` \| `'o'` | Normalises the `@type`. `None`, `''` and anything unrecognised become `'a'`, mirroring the procedure's own defaulting, and the result is the only thing interpolated into the SQL — an event-supplied value never is |
| `list_dir()` | `str` | **changed behaviour**: `PROD_LIST_DIR` when set, else this module's own directory. The CSVs are packaged beside `dataUtil.py`, so neither the Lambda nor a local run depends on the CWD. `load_symbols`, `load_symbols_dict`, `load_exchange_tz` and `get_Symbollist` all read through it |

### 6.7 New tables

DDL: `Ops/fin-deep-data/sql/upstream_tables.sql` (idempotent; run by the owner).
All confirmed present on the server on 2026-10-01.

**`GlobalMarketData.load_audit`** — one row per (run, data set, symbol) plus a
`Symbol = '*'`, `segment = 'summary'` row per (run, data set). Written with
`INSERT IGNORE`; never updated. Primary key
`(run_id, table_name, Symbol, Exchange)` — a deliberate deviation from the SR
tech doc's `(run_id, Symbol, Exchange)`, because one `eodDaily` invocation writes
two data sets and needs a summary row for each; under SR's key the second would
be silently dropped. Column reference: `doc/API-REFERENCE.md` §7.

**`GlobalMarketData.corp_action_daily`** — non-zero `Dividends` / `StockSplits`
seen in eodDaily's window, keyed `(Date, Symbol, Exchange)` so the first
sighting and its `first_seen_at` win.

**`GlobalMarketData.v_load_status`** — the latest summary row per
`(table_name, job)`, via `ROW_NUMBER()`. Read by `statusReport` and by
Support-Resistance-Agent's `sr status`.

**`histdailyprice7_shadow` / `OptionChains_shadow`** — `CREATE TABLE … LIKE` of
the production tables, the write targets during the shadow run. The cutover is
an env-var flip of `EOD_WRITE_TBL` / `OPT_WRITE_TBL`.
