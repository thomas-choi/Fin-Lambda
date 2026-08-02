# API-REFERENCE.md

This repo exposes **no HTTP endpoints**. Its contracts are the Lambda
invocation events, the `dataUtil` function signatures, the database table
columns, the S3/R2 key layout, and the shapes of the external payloads it
parses. Those *are* its API.

> **Scope note.** Created alongside `portAssetsHandler`. Its event contract and
> table columns are documented in full; the other handlers have their event
> contracts listed but not their internals.

## Changelog

- 2026-08-01 | Added | Initial file: per-handler event contracts, `portAssetsHandler` in full, `dataUtil` function reference incl. `ExecSQL`'s changed return type, `portfolio_assets_info` column reference, source-payload shapes.
- 2026-08-02 | Modified | §1 `portAssetsHandler` return value: now a JSON-serialisable list — `frame` is no longer returned and `date` is an ISO-8601 string, not a `datetime.date`.

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
| `load_symbols_db(ver="V3")` | `list` | |
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
