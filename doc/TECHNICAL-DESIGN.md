# TECHNICAL-DESIGN.md

How Fin-Lambda is built: the deployed functions, the shared `dataUtil` layer,
and the tables they write.

> **Scope note.** This file was created alongside `portAssetsHandler` and
> covers that handler in depth plus the shared `dataUtil` API. The nine
> pre-existing handlers have a one-line row each; backfilling deep-dives for
> them is tracked in `TODOS.md` §6.

## Changelog

- 2026-08-01 | Added | Initial file: deployed-functions table, `portAssetsHandler` deep-dive, `dataUtil` API reference, `Trading.portfolio_assets_info` schema, stored-procedure dependency list.

---

## 1. Deployed functions

All in `Ops/fin-cron-data/serverless.yml`. Region `us-east-2`, `timeout: 900`,
AWS profile `ServerLessUser`.

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
| `GlobalMarketData.current_symbols_V3` | `dataUtil.load_symbols("system")` |
| `GlobalMarketData.get_us_symbol` | `eoddata_minhandler_us.py` |
| `GlobalMarketData.get_asia_symbol` | `eoddata_minhandler_asia.py` |
| `Trading.sp_etf_trades_v2` | `opt_handler.py` |
| `Trading.sp_stock_trades_V3` | `opt_handler.py` |

`portAssetsHandler` depends on **none** of them — its symbol lists come from
the public sources in §2.
