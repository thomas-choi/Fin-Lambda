# API-REFERENCE.md

This repo exposes **no HTTP endpoints**. Its contracts are the Lambda
invocation events, the `dataUtil` function signatures, the database table
columns, the S3/R2 key layout, and the shapes of the external payloads it
parses. Those *are* its API.

> **Scope note.** Created alongside `portAssetsHandler`. Its event contract and
> table columns are documented in full; the other handlers have their event
> contracts listed but not their internals.

## Changelog

- 2026-10-02 | Deleted | §7.7 the `{STATUS_R2_KEY}` row — statusReport no longer writes R2; §7 its return value drops the `r2` key and `localrun` no longer mentions R2.
- 2026-10-01 | Modified | §4 `load_symbols_db` takes `sym_type` (V5's `@type`) and §7 adds `symbol_proc_type`; §7 the `eodDaily` and `optChainEOD` event contracts gain `symType` and name the procedure their `symbols` default comes from.
- 2026-10-01 | Added | §7 the `current_symbols_V5` call contract — argument values, which handler sends which, and the result column the caller reads.
- 2026-10-01 | Added | §4 `dataUtil.out_dir()` / `on_lambda()` in the function reference. `localrun` no longer changes the output directory on Lambda — it is always under `/tmp`.
- 2026-08-01 | Added | Initial file: per-handler event contracts, `portAssetsHandler` in full, `dataUtil` function reference incl. `ExecSQL`'s changed return type, `portfolio_assets_info` column reference, source-payload shapes.
- 2026-08-02 | Modified | §1 `portAssetsHandler` return value: now a JSON-serialisable list — `frame` is no longer returned and `date` is an ISO-8601 string, not a `datetime.date`.
- 2026-10-01 | Added | §7 the `fin-deep-data` contracts: event contracts for the eight python3.13 functions, the Phase A `dataUtil` helpers, the `load_audit` / `corp_action_daily` / `v_load_status` columns and their SR contract, and the R2 key layout for raw option chains and the status JSON.
- 2026-10-01 | Modified | §1 *Other handlers* now points at §7 for the v2 successors and notes which event keys differ there.

---

## 1. Handler event contracts

Every handler is invoked as `run(event, context)`. The event is a plain dict;
EventBridge passes `{}` on a scheduled invocation, so **every key must have a
safe default**.

### `portAssetsHandler` — `port_assets_handler.run`

| Key | Type | Default | Effect |
|---|---|---|---|
| `localrun` | bool | `False` | Write CSVs to the CWD instead of `PORT_OUTPUT_DIR` |
| `dbFlag` | bool | `True` | `False` suppresses **all** database writes — the dry-run switch |
| `force` | bool | `False` | Bypass the only-on-change check and write the set regardless. Does **not** override the stale-source guard |
| `test` | bool | `False` | Raise the log level to `DEBUG` |
| `NYTIME` | tz-aware `datetime` | set by `run()` | Injected clock, for tests |

Returns a list of per-index summary dicts. **The return value must stay JSON
serialisable** — the Lambda runtime marshals it, and anything `json` cannot
encode raises `Runtime.MarshalError` *after* the run has already done its work
(see `doc/OPERATIONS.md` §8, 2026-08-02):

```python
[{"port_name": "SP500", "source": "wikipedia:List_of_S&P_500_companies",
  "rows": 503, "changed": True, "action": "write",
  "date": "2026-08-03", "reason": "membership set changed",
  "csv": "/tmp/portfolio_assets_info_SP500.csv"}, ...]
```

| Key | Type | Notes |
|---|---|---|
| `port_name` | str | `SP500` / `NDX100` — the `Port_name` value written |
| `source` | str | Which source answered, e.g. `nasdaq-api:list-type/nasdaq100` or the Wikipedia fallback |
| `rows` | int | Rows parsed from the source, before the sanity gate |
| `changed` | bool \| None | `None` when the comparison never ran (gate failure, `dbFlag=False`) |
| `action` | str | See the table below |
| `date` | str \| None | **ISO-8601 date string**, e.g. `"2026-08-03"`; `None` when nothing was written |
| `reason` | str | Human-readable explanation of `action` |
| `csv` | str | Path of the per-index CSV; absent when the sanity gate failed before the dump |

The built DataFrame is used internally to assemble the combined CSV but is
**not** part of the returned payload.

`action` is one of:

| Value | Meaning |
|---|---|
| `write` | A new dated set was appended |
| `replace` | The existing `(Date, Port_name)` rows were deleted, then the set appended |
| `skip` | Nothing written; `reason` says why |
| `failed` | An exception, or the post-write row-count verification did not match |

Dry run:

```bash
cd Ops/fin-cron-data
python port_assets_handler.py     # __main__ supplies {"localrun": True, "dbFlag": False, "test": True}
```

### Other handlers

| Handler | Event keys |
|---|---|
| `handler.run` | `test`; `NYTIME` set internally. Dry run via the `LOCALRUN` **env var**, not an event key |
| `opt_handler.run` | `localrun` (bool), `test` (int — caps iterations), `testing` |
| `eoddata_minhandler_us.run` / `_asia.run` | `localrun`, **`dbFlag`** (`False` suppresses writes), `InitialRun` (full-history backfill), `NYTIME` |
| `fx_handler.run` / `fxeod_handler.run` | `test`; `NYTIME` set internally. Dry run via a module-global `localrun` |
| `usrate_handler.run` | `test` |
| `fff_handler.run` | `test` |
| `yf-news-collect.run` | none; driven by the `LOCALRUN` and `BATCH_SIZE` env vars |

> The dry-run switch is **not uniform**. Check the handler before running one
> against production — see `doc/OPERATIONS.md` §6.

The `fin-deep-data` service has its own, *uniform* contract: `dbFlag` is the
off-switch on every handler and `localrun` only chooses where output files go.
Its eight functions are in §7, including the v2 successors of
`eoddata_minhandler_*`, `fxeod_handler`, `usrate_handler` and
`port_assets_handler`, whose event keys differ from the rows above.

---

## 2. `dataUtil` function reference

| Signature | Returns | Notes |
|---|---|---|
| `get_DBengine()` | `Engine` | Cached module-global. Never raises on a bad URL — the failure surfaces at first use |
| `load_df_SQL(query)` | `DataFrame` \| `None` | `None` on any error |
| `load_df(stock_symbol=None, DailyMode=True, lastdt=None, startdt=None, dataMode="P")` | `DataFrame` \| `None` | |
| `StoreEOD(eoddata, DBn, TBLn)` | `None` | `to_sql(if_exists='append')`. **Swallows write failures** — verify the row count afterwards |
| **`ExecSQL(query)`** | **`int` \| `None`** | **Changed 2026-08-01** — see below |
| `get_Max_date(dbntable, symbol=None)` | `date` \| `None` | |
| `get_Max_datetime(dbntable, symbol=None)` | `datetime` \| `None` | |
| `get_Max_Options_date(dbntable, symbol=None)` | `(date, section)` \| `None` | |
| `get_Latest_row_by_Symbol(dbntable, symbol)` | `Series` \| `None` | |
| `get_Last_Date_by_Sym(tblname, sym)` | `date` | Falls back to `FIRSTTRAINDTE` when the table is empty |
| `get_Last_Datetime_by_Sym(tblname, sym)` | `datetime` | |
| `load_symbols(symlistName, ver="V3")` | `list` \| `None` | `"system"` routes to the `current_symbols_V3` stored procedure |
| `load_symbols_db(ver="V3", sym_type=None)` | `list` | `CALL GlobalMarketData.current_symbols_{ver}`. `sym_type` is sent only for versions outside `UNTYPED_SYMBOL_PROCS` = {`V1`,`V2`,`V3`,`V4`} — those take no argument and passing one is a SQL error. Raises `AttributeError` when the call fails, because `load_df_SQL` returns `None` |
| `load_symbols_dict()` | `dict` \| `None` | symbol → exchange, from `stock_exchange.csv` |
| `load_exchange_tz()` | `dict` \| `None` | exchange → timezone, from `Exchange_timezone.csv` |
| `load_eod_price(ticker, start, end)` | `DataFrame` \| `None` | |
| `StoreWebDaily(df)` | `None` | `TRUNCATE` then append |
| `nowbyTZ(tzName)` | `datetime` | tz-naive, in the target zone |

**Error contract:** these functions **log and return `None` rather than
raising**. Callers across the repo rely on it. Pinned by
`tests/unit/test_dataUtil.py`.

### `ExecSQL(query)` — changed return type

| | Before 2026-08-01 | After |
|---|---|---|
| Success | implicit `None` | **`int`** — rows affected |
| Failure | `None`, logged at ERROR | `None`, logged at ERROR *(unchanged)* |
| Raises | never | never *(unchanged)* |
| SQLAlchemy | 1.4 only (`Engine.execute()`, removed in 2.0) | **1.4 and 2.0** (`Engine.begin()` + `text()`) |

Adding the return value is backward-compatible — every existing caller ignores
it. `Engine.begin()` commits explicitly where 1.4's legacy autocommit committed
implicitly; the equivalence is asserted by
`test_exec_sql_commits_visible_to_independent_connection` and was demonstrated
against live MySQL.

---

## 3. `Trading.portfolio_assets_info`

> **Note:** schema read from the live table via `SHOW CREATE TABLE`, not from
> DDL in this repo. The table pre-dates `portAssetsHandler`.

| Column | Type | Null | Written by `portAssetsHandler` |
|---|---|---|---|
| `Date` | `date` | NOT NULL | SP500: NY-local run date. NDX100: the Nasdaq API's `data.date` |
| `Port_name` | `varchar(45)` | NOT NULL | `$SP500_PORT_NAME` / `$NDX100_PORT_NAME` |
| `Symbol` | `varchar(20)` | NOT NULL | Ticker, **dashed** form (`BRK-B`) |
| `Class` | `varchar(20)` | NULL | `$PORT_ASSET_CLASS`, default `Equity` |
| `Sector` | `varchar(45)` | NULL | SP500: GICS sector. NDX100: literal `General` |
| `Type` | `varchar(20)` | NULL | `$PORT_ASSET_TYPE`, default `Stock` |
| `Currency` | `varchar(4)` | NULL | `USD` |
| `Rate` | `float` | NULL | `1.0` — the **FX rate to USD**, not an index weight |

**Primary key: `(Date, Port_name, Symbol)`.**

Uniqueness assumptions:

- One row per symbol per portfolio per date, enforced by the PK.
- `StoreEOD` appends with no upsert, so writing a `(Date, Port_name)` that
  already exists raises `IntegrityError` — which `StoreEOD` **swallows**.
  `portAssetsHandler` therefore `DELETE`s that `(Date, Port_name)` first when
  re-writing the same date, and re-reads the row count afterwards.
- Sets are complete, never deltas. An as-of query is
  `WHERE Date <= X ORDER BY Date DESC LIMIT 1`, then an equality join.

The `Date` series is **sparse** — a set is stored only when it changes.

---

## 4. External source payloads

These are third-party contracts this repo depends on and cannot control. Each
has a saved fixture in `tests/fixtures/` captured 2026-08-01; a parse test
against the fixture is the early warning when one changes.

### Wikipedia — *List of S&P 500 companies*

`https://en.wikipedia.org/wiki/List_of_S%26P_500_companies`

Requires a descriptive `User-Agent` per Wikimedia's bot policy. The constituent
table is 503 × 8:

```
Symbol | Security | GICS Sector | GICS Sub-Industry |
Headquarters Location | Date added | CIK | Founded
```

Located by required columns (`Symbol`, `GICS Sector`), **not** by
`read_html()[0]`.

### Nasdaq official API

`https://api.nasdaq.com/api/quote/list-type/nasdaq100`

Requires a **browser-like** `User-Agent`; rejects `python-requests`.

```json
{"data": {"date": "Jul 30, 2026",
          "data": {"rows": [{"symbol": "AAPL", "sector": "",
                             "companyName": "Apple Inc. Common Stock",
                             "marketCap": "...", "lastSalePrice": "$308.91"}]}},
 "message": null, "status": {...}}
```

Rows are at **`data.data.rows`** — the doubled `data` is real. `sector` is an
empty string on every row; the handler ignores the field entirely and writes
`General`. `data.date` is the NDX effective date.

### Wikipedia — *List of NASDAQ-100 companies*

`https://en.wikipedia.org/wiki/List_of_NASDAQ-100_companies`

Fallback and cross-check. 103 × 4: `Ticker`, `Company`, `ICB Industry`,
`ICB Subsector`. **Membership only** — the ICB columns are deliberately
discarded.

> The constituent table is **not** on `/wiki/Nasdaq-100`; it was split out to
> `List_of_NASDAQ-100_companies`. Parsing the old page returns 18 tables, none
> of them the component list.

### SSGA SPY daily holdings

`https://www.ssga.com/.../holdings-daily-us-en-spy.xlsx`

Requires a browser `User-Agent` and `openpyxl`. Layout:

| Sheet row | Content |
|---|---|
| 0 | `Fund Name: | State Street® SPDR® S&P 500® ETF Trust` |
| 1 | `Ticker Symbol: | SPY` |
| 2 | `Holdings: | As of 30-Jul-2026` ← the as-of date |
| 3 | blank |
| 4 | header: `Name`, `Ticker`, `Identifier`, `SEDOL`, `Weight`, `Sector`, `Shares Held`, `Local Currency` |
| 5+ | holdings, then blank rows and a legal footer paragraph |

`Sector` is `-` on every row — **unusable for sector**. Used for membership
cross-check only.

---

## 5. S3 / R2 key layout

Written by `yfNewshandler` only.

> **Status: Not documented here.** `yf-news-collect.py`'s key construction has
> not been audited; see `TODOS.md` §2.8. `portAssetsHandler` writes nothing to
> object storage.

---

## 6. `Ops/fin-cron-Pgsql/`

> **Status: Not yet implemented.** Its `serverless.yml` is copied from
> `fin-cron-data` and names handlers that do not exist in the folder. The three
> scripts present cannot run in this environment — `dataUtil_Pgsql.py` hardcodes
> an absolute macOS dotenv path and reads a different key set (`RHOST`, `DB`,
> `PORT`). Do not deploy it as-is.

---

## 7. `fin-deep-data` contracts (python3.13)

`Ops/fin-deep-data/`, added 2026-10-01. Unlike §1, the off-switch is uniform:
**`dbFlag: false` suppresses every database, R2 and `load_audit` write** on every
handler here, and `localrun` only decides whether output files go to the CWD or
`/tmp`.

### 7.1 Event contracts

All eight are invoked as `run(event, context)` and return a JSON-serialisable
dict (or, for `portAssetsHandlerv2`, a list of dicts). EventBridge passes `{}`
or the `input` block declared in `serverless.yml`, so every key has a safe
default.

**`eodDaily` — `eod_daily_handler.run`**

| Key | Type | Default | Effect |
|---|---|---|---|
| `localrun` | bool | `False` | CSVs to the CWD instead of `/tmp` |
| `dbFlag` | bool | `True` | `False` → no table and no audit writes; writes `eod_daily_<asof>.csv`, `corp_action_<asof>.csv`, `load_audit_<asof>.csv` |
| `shard` / `of` | int | `0` / `1` | round-robin slice of the sorted symbol list |
| `sweep` | bool | `False` | process only symbols with no `ok`/`empty` audit row for `asof` |
| `prepend` | bool | `False` | U2b: download `[FIRSTTRAINDTE, MIN(Date)−1]` per symbol, `segment='prepend'` |
| `asof` | `YYYY-MM-DD` | NY date, minus a day before 09:00 | session date |
| `symbols` | list | from `current_symbols_{SYMBOL_PROC_VER}` | list override; skips the procedure entirely |
| `symType` | `'a'` \| `'o'` | `'a'` | V5's `@type`. Ignored by V1–V4, which take no argument. Unrecognised values fall back to `'a'` |
| `test` | int \| bool | — | int caps the symbol count; any truthy value sets DEBUG logging |
| `NYTIME` | datetime | now in NY | injected clock, for tests |

Returns `{run_id, asof, mode, dbFlag, rows_written, actions_written, status,
error, n_expected, n_ok, n_empty, n_error, n_skipped}`.

**`optChainEOD` — `optchain_eod_handler.run`**

| Key | Type | Default | Effect |
|---|---|---|---|
| `dispatch` | bool | `False` | async-invoke this function `OPT_SHARDS` times with `{shard, of, asof}` and return |
| `shard` / `of` | int | `0` / `1` | round-robin slice |
| `sweep` | bool | `False` | only underlyings missing an `ok`/`empty` audit row |
| `asof` | `YYYY-MM-DD` | NY date, minus a day before 09:00 | `Date` written on every row |
| `symbols` | list | from `current_symbols_{SYMBOL_PROC_VER}` | list override; skips the procedure entirely |
| `symType` | `'a'` \| `'o'` | `'o'` | V5's `@type`; `'o'` also excludes `SymbolMaster.options = 0`. Ignored by V1–V4 |
| `localrun` / `dbFlag` | bool | `False` / `True` | `dbFlag=False` → no table, no R2; CSVs plus `optchain_timing_<date>.csv` |
| `test` | int \| bool | — | int caps the symbol count; truthy sets DEBUG |
| `NYTIME` | datetime | now in NY | injected clock |

Returns `{run_id, asof, mode, dbFlag, rows_written, status, error, n_expected,
n_ok, n_empty, n_error, n_skipped, seconds, opt_shards_needed}`; a dispatch run
returns `{mode: "dispatch", asof, shards, started, events}`.

**`statusReport` — `status_report_handler.run`**

| Key | Type | Default | Effect |
|---|---|---|---|
| `localrun` | bool | `False` | print only — no SNS |
| `dbFlag` | bool | `True` | `False` behaves like `localrun` |
| `asof` | `YYYY-MM-DD` | today in NY | report date, used for the staleness test |
| `test` | bool | `False` | DEBUG logging |
| `NYTIME` | datetime | now in NY | injected clock |

Returns `{asof, subject, lines, statuses, error, sns}`. The `r2` key was
removed on 2026-10-02 with the R2 upload itself — a consumer that read it
should query `GlobalMarketData.v_load_status` instead (§6).

**`portAssetsHandlerv2` — `port_assets_handler.run`** — same contract as
`portAssetsHandler` in §1 (`localrun`, `dbFlag`, `force`, `test`, `NYTIME`) and
the same JSON-safe list return, with two more entries in it: `DJIA` and `HSI`.

**`usrateHandlerv2` — `usrate_handler.run`**

| Key | Type | Default | Effect |
|---|---|---|---|
| `dbFlag` | bool | `True` | `False` → `USrates_<date>.csv` instead of the table, and no audit row |
| `localrun` | bool | `False` | CSV to the CWD instead of `/tmp` |
| `test` | bool | `False` | DEBUG logging |

Returns `{run_id, rows}`. **Changed from `usrate_handler.run`**, which accepted
`test` only and had no way to suppress a write.

**`FXHistHandlerv2` — `fxeod_handler.run`**

| Key | Type | Default | Effect |
|---|---|---|---|
| `localrun` | bool | `False` | `USD_dailyFX.csv` only; implies no table and no audit row |
| `dbFlag` | bool | `True` | `False` → same as `localrun` |
| `test` | any | — | DEBUG logging |
| `NYTIME` | datetime | now in NY | injected clock |

Returns `{run_id, rows}`. **Changed from `fxeod_handler.run`**, where the dry run
was a module-global `localrun` only `__main__` could set.

**`yfus30minEODv2` / `yfasia30minEODv2` — `intraday_min_handler.run_us` /
`.run_asia`**

| Key | Type | Default | Effect |
|---|---|---|---|
| `localrun` | bool | `False` | per-symbol `30min_<sym>.csv` |
| `dbFlag` | bool | `True` | `False` → no table and no audit row |
| `InitialRun` | bool | `False` | full available history instead of the watermark (yfinance caps intraday at 60 days) |
| `test` | int \| bool | — | **new**: int caps the symbol count; truthy sets DEBUG |

Returns `{run_id, market, job, rows}`. The underlying `run(event, context,
market="us")` takes the market as a third argument; the two Lambda entry points
are thin wrappers so one module serves both functions.

### 7.2 Phase A `dataUtil` helpers

In `Ops/fin-deep-data/dataUtil.py` only — the `fin-cron-data` copy does not have
them. Error contract as elsewhere in that module: log and return `None`, except
where noted.

| Signature | Returns | Notes |
|---|---|---|
| `append_ignore(df, schema, table, chunk=500)` | `int` rows inserted \| `None` | `INSERT IGNORE` / `INSERT OR IGNORE` by dialect. Target table must exist |
| `audit_run(rows)` | `int` \| `None` | writes `load_audit`; swallows **all** errors, including a missing `TBLLOADAUDIT` |
| `audit_frame(rows)` | `DataFrame` | pure; `AUDIT_COLUMNS` order, `error` cut to 512 chars |
| `audit_summary(job, table_name, run_id, started_at, status='ok', n_rows=0, date_lo=None, date_hi=None, n_ok=None, n_expected=None, error=None, yf_version='', finished_at=None)` | `dict` | the `Symbol='*'`, `segment='summary'` row |
| `new_run_id(now_ms=None)` | `str` (26) | Crockford base32 ULID, sorts by time |
| `run_host()` | `str` (≤64) | `lambda:<function>` or the hostname |
| `shard_symbols(symbols, i, n)` | `list` | **raises** `ValueError` on `i` outside `0..n-1` |
| `time_left_ok(context, reserve_ms=120_000)` | `bool` | `True` when `context` is `None` |
| `missing_for_sweep(job, date, expected, table_name, tz='America/New_York')` | `list` \| `None` | `None` when `load_audit` is unreadable — callers must not treat that as "nothing done" |
| `require_env(name)` | `str` | **raises** `RuntimeError` when unset or blank |
| `env_or(name, default)` | `str` | blank counts as unset |
| `utc_now()` | tz-naive `datetime` | UTC |
| `day_start_utc(day, tz='America/New_York')` | tz-naive `datetime` | local midnight as UTC |
| `out_dir(localrun=False, env_key=None)` | `str` | `/tmp` (or a `/tmp` subdirectory named by `env_key`) whenever `AWS_LAMBDA_FUNCTION_NAME` is set; `"."` for a local `localrun`; otherwise `env_key`'s value or `"."`. Creates the directory. Never returns a path that is read-only on Lambda |
| `on_lambda()` | `bool` | Whether `AWS_LAMBDA_FUNCTION_NAME` is set |
| `symbol_proc_type(sym_type)` | `'a'` \| `'o'` | Pure. `None`, `''`, whitespace and anything unrecognised become `'a'`; case and surrounding spaces are ignored. The only value interpolated into the `CALL`, so a bad `symType` cannot reach SQL |
| `list_dir()` | `str` | `PROD_LIST_DIR`, else this module's directory. **Behaviour change** vs. the `fin-cron-data` copy, which resolves `"."` against the CWD |

### 7.2.1 `GlobalMarketData.current_symbols_V5` call contract

```
CALL GlobalMarketData.current_symbols_V5('a')   -- eodDaily
CALL GlobalMarketData.current_symbols_V5('o')   -- optChainEOD
```

| | |
|---|---|
| Argument | `IN p_type CHAR(1)`. **Required** — MySQL has no default argument values, so `CALL …_V5()` is an error. `NULL`, `''` and any value other than `'o'` are treated as `'a'` inside the procedure as well as in `dataUtil.symbol_proc_type` |
| `'a'` | V4's union minus `SymbolMaster.delisted = 1`. 838 rows on 2026-10-01 |
| `'o'` | also minus `SymbolMaster.options = 0`. 814 rows on 2026-10-01 |
| Result set | one column, **`Symbol`** (`varchar(20)`), `DISTINCT`, `ORDER BY Symbol`. `load_symbols_db` reads `df.Symbol`, so a renamed column raises `AttributeError` |
| Not a whitelist | a symbol absent from `SymbolMaster` is returned by both types — 648 of the 863 union symbols are in that position |
| Version switch | `SYMBOL_PROC_VER`. `V1`–`V4` take no argument and `load_symbols_db` withholds the type; `V5` and anything later receive it |

> **Note:** DDL in `Ops/fin-deep-data/sql/current_symbols_V5.sql`; row counts read
> off the live database on 2026-10-01, not from DDL.

### 7.3 `GlobalMarketData.load_audit`

> **Note:** DDL in `Ops/fin-deep-data/sql/upstream_tables.sql`. This is the
> contract Support-Resistance-Agent reads (`available_at` comes from
> `finished_at`).

| Column | Type | Meaning |
|---|---|---|
| `run_id` | `CHAR(26)` | ULID of the invocation |
| `job` | `VARCHAR(32)` | the **data set's** job: `eodDaily`, `optChainEOD`, `usrateHandler`, `FXHistHandler`, `yfus30minEOD`, `yfasia30minEOD`, `portAssetsHandler`. Not the Lambda's name — a v2 function writes the same value as the function it replaces |
| `Symbol` | `VARCHAR(45)` | `'*'` on a summary row |
| `Exchange` | `VARCHAR(45)` | `''` on summary rows and where the handler has no exchange concept |
| `date_lo` / `date_hi` | `DATE` null | first and last trade date written |
| `table_name` | `VARCHAR(64)` | the data set, e.g. `histdailyprice7_shadow`. `DataName` in the status report |
| `segment` | `VARCHAR(8)` | `first` \| `append` \| `prepend` \| `summary` |
| `n_rows` | `INT` | rows this handler **built** for this symbol or run |
| `n_ok` / `n_expected` | `INT` null | summary rows only: symbols ok/empty, and symbols this shard was given |
| `status` | `VARCHAR(16)` | `ok` \| `empty` \| `error` \| `skipped` |
| `error` | `VARCHAR(512)` null | truncated by the writer; `STRICT_ALL_TABLES` would reject a longer value |
| `started_at` / `finished_at` | `DATETIME(3)` | UTC. `finished_at` is SR's `available_at` |
| `yf_version` | `VARCHAR(16)` | `''` for handlers that do not use yfinance |
| `host` | `VARCHAR(64)` | `lambda:<function name>` or the hostname — this is what distinguishes a v2 writer |

Primary key `(run_id, table_name, Symbol, Exchange)`. **Deviation** from SR tech
doc §4.5.7's `(run_id, Symbol, Exchange)`: one `eodDaily` invocation writes two
data sets and needs a summary row for each, and under SR's key the second would
be dropped by `INSERT IGNORE`. SR's docs and its PK assertion need the matching
amendment. Indexes: `(job, date_hi)` for `missing_for_sweep`,
`(table_name, finished_at)` for `v_load_status` and SR's `available_at` lookup.

Rows are written with `INSERT IGNORE` and **never updated**: a sweep adds a new
row rather than correcting the shard's `skipped` row, so a symbol can have
several rows per day and "did it succeed" means *any* row is `ok`/`empty`.

### 7.4 `GlobalMarketData.corp_action_daily`

| Column | Type | Meaning |
|---|---|---|
| `Date` | `DATE` | ex-date, exchange-local |
| `Symbol` | `VARCHAR(45)` | |
| `Exchange` | `VARCHAR(45)` | |
| `Dividends` | `DOUBLE` null | cash per share, in the traded units of that date |
| `StockSplits` | `DOUBLE` null | ratio, new/old |
| `first_seen_at` | `DATETIME(3)` | UTC; `INSERT IGNORE` on the PK keeps the **first** sighting, which makes this an honest `available_at` |
| `run_id` | `CHAR(26)` | `load_audit.run_id` of the sighting run |

Primary key `(Date, Symbol, Exchange)`.

### 7.5 `GlobalMarketData.v_load_status`

One row per `(table_name, job)` — the latest `segment='summary'` row by
`finished_at`, via `ROW_NUMBER()`. Columns: `table_name, job, last_data_date,
started_at, finished_at, status, n_ok, n_expected, n_rows, error`. Read by
`statusReport` and by SR's `sr status`. A read-only consumer needs `SELECT` on
the view only.

### 7.6 Write targets

| Env var | Shadow value | Production value | Written by |
|---|---|---|---|
| `EOD_WRITE_TBL` | `histdailyprice7_shadow` | `histdailyprice7` | eodDaily |
| `OPT_WRITE_TBL` | `OptionChains_shadow` | `OptionChains` | optChainEOD |

Both are **required with no fallback** — the cutover is deliberately an env-var
flip, and a defaulted value could silently write to the wrong table. Columns
match the production tables exactly (`CREATE TABLE … LIKE`): eodDaily writes
`Date, Symbol, Exchange, Close, Open, High, Low, Volume, AdjClose`, and
optChainEOD writes the 20 `N_COLUMNS` of the ported job, in that order.

### 7.7 R2 key layout

| Key | Written by | Content |
|---|---|---|
| `{OPT_RAW_PREFIX}/{YYYY-MM-DD}/{sym}-PM.csv` | optChainEOD | the **unfiltered** chain for one underlying, as downloaded, CSV |

Bucket `UPSTREAM_R2_BUCKET`, credentials `R2_ENDPOINT` / `R2_ACCESS_KEY_ID` /
`R2_SECRET_ACCESS_KEY` (the `yf-news-collect` credentials reused). With the
bucket unset, optChainEOD logs a warning and still writes its tables — the raw
archive is best-effort, the database write is not.

`optChainEOD` is the only `fin-deep-data` writer of R2. **Removed 2026-10-02:**
`statusReport`'s `{STATUS_R2_KEY}` JSON (`{asof, generated_at, rows: [...]}`,
default `status/latest.json`) and the `STATUS_R2_KEY` env var. `load_audit` /
`v_load_status` is the durable copy of that payload.
