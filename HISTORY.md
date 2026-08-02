# HISTORY.md

Change log for Fin-Lambda. Every code, configuration or architectural change is
recorded here before it is considered complete, per `CLAUDE.md`.

Newest first.

---

## 2026-08-02 — `portAssetsHandler`: fix `Runtime.MarshalError` on the return value

### Goal

The first real AWS invocation (request `d2812e85`, 09:06 UTC) did all of its
work correctly and then failed:

```
[ERROR] Runtime.MarshalError: Unable to marshal response:
        Object of type DataFrame is not JSON serializable
```

Make `run()` return something the Lambda runtime can encode, so a healthy run
stops being reported as a failed invocation.

### Root cause

`run()` returned the internal per-index summary dicts as-is. Two keys are not
JSON-encodable:

- `frame` — the built `DataFrame`, put on the summary so the combined golden
  CSV can be assembled from all indices at the end of `run()`.
- `date` — a `datetime.date` on the write path.

The Lambda runtime JSON-encodes the handler's return value *after* the handler
body completes, so both CSVs were written and both indices were correctly
skipped before the marshal step blew up. No data was lost or corrupted; the
damage is that every invocation surfaces as an error in CloudWatch, which would
mask a genuine failure later.

### Implementation detail

Added `_jsonable_summary()` to
[Ops/fin-cron-data/port_assets_handler.py](Ops/fin-cron-data/port_assets_handler.py):
it drops `frame` and renders `date` as an ISO-8601 string (normalising a
`pd.Timestamp` to its date first). `run()` now returns
`[_jsonable_summary(s) for s in summaries]`.

`frame` deliberately stays on the in-process summary — the combined-CSV step
consumes it — and is stripped only on the way out. Nothing else about the
control flow, the write decisions or the CSV outputs changed.

### Related files

- `Ops/fin-cron-data/port_assets_handler.py` — `_jsonable_summary()`, `run()` return, `run()` docstring
- `tests/unit/test_port_assets_handler.py` — new §T15
- `doc/API-REFERENCE.md` — §1 return-value contract
- `doc/OPERATIONS.md` — §8 first incident entry, §9 troubleshooting row

### Test coverage

**Existing verification — passes.** Full unit suite green under the pinned
layer versions (`venv-cron7`): `pytest tests/unit -q` → **103 passed**, up from
97, no failures and no changes to any pre-existing test. No test asserted on
the `frame` or `date` keys of `run()`'s return value, so none was invalidated.

**Removed.** None. No handler, event flag, CSV output or stored-procedure
dependency was retired.

**Added — 6 tests, §T15 of `tests/unit/test_port_assets_handler.py`:**

| Test | Covers |
|---|---|
| `test_run_result_is_json_serialisable_on_a_dry_run` | `json.dumps(run(...))` on the `dbFlag=False` path — exactly what the runtime does |
| `test_run_result_carries_no_dataframe` | No summary carries a `frame` key |
| `test_run_result_is_json_serialisable_when_rows_are_written` | The write/`replace` path, where `date` is populated; asserts `"2026-08-03"` / `"2026-07-30"` as strings |
| `test_run_result_is_json_serialisable_when_an_index_fails` | The exception path, whose summary has a different key set |
| `test_jsonable_summary_normalises_a_timestamp_date` | `pd.Timestamp` → `"2026-08-03"`, `frame` dropped |
| `test_jsonable_summary_leaves_a_missing_date_alone` | `date=None` survives untouched |

**Not re-run.** `serverless print` / `serverless package` — this change touches
no `serverless.yml` entry, no layer and no dependency. A plain
`serverless deploy function -f portAssetsHandler` ships it.

**Post-deploy check.** The next 22:30 UTC invocation should log the same
`SUMMARY` lines and end with `END`/`REPORT` and **no** `[ERROR]` line.

---

## 2026-08-01 — `portAssetsHandler`: S&P 500 / NASDAQ-100 index membership

### Goal

Add a scheduled Lambda that collects the current constituent list of the
**S&P 500** and the **NASDAQ-100**, writes each to a CSV for eyeball
verification, and appends the membership set to
`Trading.portfolio_assets_info` — writing a new dated set **only when the set
has actually changed**. The function runs on **python3.13** with its own layer,
unlike the nine existing python3.10 functions.

This is the first change made under the mandatory-rules regime, so it also
creates `HISTORY.md`, the four `doc/*.md` files, the `pytest` harness and
`.env.example`.

### Findings that changed the plan

Three things were checked against the live database before any code was
written, and two of them contradicted `PLAN.md`:

1. **`Trading.portfolio_assets_info` already exists and is populated** — 592
   rows across 7 portfolios (`DJI`, `HSI`, `IAM`, `TM-ASIA`, `TM-CHINA`,
   `US-ETF`, `US-Top20`), dated 2024-12-30 → 2025-07-31. Its live DDL matches
   the DDL `PLAN.md` §3 proposed, primary key included. **The manual `CREATE
   TABLE` step in the plan is not needed and must not be run.**
2. **The existing "no sector" convention is not `N/A`.** `DJI` (30 rows) and
   `HSI` (167 rows) both use `General`; `US-Top20` uses `NoSector`. The plan
   specified the literal `N/A`, which would have introduced a third sentinel.
   Changed to **`General`**, matching the two index-membership portfolios that
   are the closest analogue to SP500/NDX100.
3. **`Rate` is confirmed to be the FX rate to USD** — HKD rows carry `0.1282`,
   CNY `0.14`, KRW `0.00073`, USD `1.0`. Both new indices are 100 % USD, so
   `Rate = 1.0` throughout, and no index-weight source is needed.

Assumption **A3** (write grants on `Trading`) resolved: the configured user
holds `GRANT ALL PRIVILEGES ON Trading.*`.

Assumption **A2** (symbol form) had **no decisive live evidence** — the daily
price table contains no US class shares at all, and neither does `SymbolMaster`
nor the existing `portfolio_assets_info` rows. The only in-repo evidence was
`stock_exchange.csv`, which is dotted. Resolved by decision: symbols are stored
in the **dashed yfinance form** (`BRK-B`, `BF-B`), because every price table in
this database is yfinance-fed and the point of the table is to be joinable to
price history. `normalize_us_symbol()` does the conversion.

Incidental correction: the daily price table is **`histdailyprice7`**, not
`histdailyprice3` as `PLAN.md` and `CLAUDE.md` both state.

### Implementation detail

**New handler — `Ops/fin-cron-data/port_assets_handler.py`.** Follows the
existing handler shape (module-level `load_dotenv()`, `run(event, context)`,
a `__main__` block supplying the dry-run event). Everything that parses a
payload or decides what to write is a pure, importable function; only thin
wrappers touch the network or the database.

* **Sources.** S&P 500 from Wikipedia *List of S&P 500 companies* (503 × 8,
  carries GICS Sector), cross-checked against the SSGA **SPY** daily-holdings
  workbook. NASDAQ-100 from the official Nasdaq API
  (`api.nasdaq.com/api/quote/list-type/nasdaq100`, 103 rows, needs a
  browser-like `User-Agent`), with Wikipedia *List of NASDAQ-100 companies* as
  automatic fallback and cross-check.
* **Table selection is by content, not index.** `_pick_table()` finds the
  constituent table by its required columns. `read_html()[0]` happens to be
  correct today, but the NASDAQ-100 table has already moved pages once, and
  that page alone parses to 30+ tables.
* **`Sector` is populated for `SP500` only** (GICS, from Wikipedia). `NDX100`
  rows carry the literal `General`: the Nasdaq API returns an empty sector on
  every row, and Wikipedia's NDX page classifies under **ICB** while the S&P
  page uses **GICS**. Mixing the two taxonomies in one column would make
  `Sector` incomparable across `Port_name` — `AAPL` would read
  `Information Technology` under `SP500` and `Technology` under `NDX100`.
* **Sanity gate before any write** — S&P 500 must yield 490–520 symbols,
  NASDAQ-100 95–110. Outside the band, nothing is written. This is the guard
  against a page restructure producing a 3-row frame that then wipes a good
  membership set.
* **Only-on-change.** `has_changed()` compares the full row tuple
  `(Symbol, Class, Sector, Type, Currency, Rate)` sorted by `Symbol`, so a
  sector reclassification with unchanged membership still produces a new dated
  set. Row order is irrelevant and `NaN`/`None` sectors compare equal.
* **Effective date.** NDX100 uses the Nasdaq API's own `data.date`. SP500 uses
  the NY-local run date — **deviating from `PLAN.md` §4.3**, which listed the
  SPY workbook's `As of` date as a source. SPY belongs to a different product
  and sits behind the optional `INDEX_CROSSCHECK` flag; deriving a primary-key
  column from an optional input would make the date move whenever that flag is
  toggled, which can strand the set behind the stale-source guard. The SPY
  as-of date is logged, not used.
* **Write path.** Same date as the stored max → `DELETE` that
  `(Date, Port_name)` then append, making a same-day re-run idempotent. Older
  than the stored max → skip and log an error. `StoreEOD` swallows write
  failures, so the row count is re-read after every write and a mismatch is
  logged loudly.

**Shared-module change — `Ops/fin-cron-data/dataUtil.py`.** `ExecSQL()` was
made SQLAlchemy 1.4/2.0-agnostic; it is the one genuine incompatibility in a
file that is zipped into **every** function in this service.

* Root cause: line 109 called `get_DBengine().execute(query)`.
  `Engine.execute()` was **removed** in SQLAlchemy 2.0, and on the pinned
  1.4.46 it already emitted `RemovedIn20Warning`. The rest of the module
  (`to_sql`, `pd.read_sql`) behaves identically on both versions, and the file
  already parses cleanly under a 3.8 feature set — so this one function was the
  entire blocker.
* Now uses `Engine.begin()` + `text()`, which exist in both versions, and
  returns the rowcount (previously an implicit `None`) so callers can verify a
  `DELETE` actually deleted. The `try/except` + `logging.error` swallow is kept
  deliberately — `TODOS.md` §1.7 pins that as the module's contract.
* A **fourth fork** (`dataUtil_313.py`) was considered and rejected: the repo
  already carries three forks of this file, the incompatible surface is one
  function, and a fork guarantees every future fix has to be applied twice.

**Layer and build.** New `requirements_port313.txt` and a `finPort313.zip`
Makefile target. The target cross-builds for `cp313`/`manylinux2014_x86_64`
rather than resolving against the local interpreter (3.10 in this environment),
installs into `python/lib/python3.13/site-packages` — not the `python3.10` path
every other target hardcodes — and passes `--no-compile`. Without
`--no-compile`, pip byte-compiles pure-Python packages with the *local* 3.10
interpreter and ships 2,507 useless `cpython-310.pyc` files: **176 MB → 138 MB
unzipped, 52 MB → 39 MB zipped**. That matters, because 52 MB exceeds Lambda's
50 MB direct-upload limit and would have forced an S3 round trip.

**`serverless.yml`.** `portAssetsHandler` added with a per-function
`runtime: python3.13` override and the layer ARN scoped to the function, never
at provider level, so the 3.13 layer can never be attached to a 3.10 function.

### Related files

| File | Change |
|---|---|
| `Ops/fin-cron-data/port_assets_handler.py` | new — the handler |
| `Ops/fin-cron-data/dataUtil.py` | modified — `ExecSQL()` only, plus one import |
| `Ops/fin-cron-data/serverless.yml` | modified — new function; comment on `configValidationMode` |
| `Ops/fin-cron-data/requirements_port313.txt` | new — python3.13 layer deps |
| `Ops/fin-cron-data/portfolio_assets_info*.csv` | new — golden dry-run references |
| `Makefile` | modified — new `finPort313.zip` target |
| `pytest.ini`, `requirements-dev.txt` | new — test harness |
| `tests/conftest.py`, `tests/unit/`, `tests/fixtures/` | new |
| `.env.example` | new — closes `TODOS.md` §3.6 |
| `HISTORY.md`, `doc/*.md` | new |
| `TODOS.md`, `CLAUDE.md` | updated |

### Test coverage

**Added — `tests/unit/test_port_assets_handler.py` (87 tests).** All
fixture-driven; no test touches the network, MySQL or S3.

| Ref | Covers |
|---|---|
| T1 | `parse_sp500_wikipedia()` → 503 rows, GICS (not sub-industry) sector, **dashed** `BRK-B`/`BF-B`, table picked by content not index |
| T2 | `parse_ndx_nasdaq_api()` → 103 symbols; the `data.data.rows` path is followed and a shallower payload raises |
| T3 | `parse_ndx_wikipedia()` → 103 tickers, ICB sectors **not** propagated |
| T4 | `parse_spy_holdings()` → header located on sheet row 4, metadata and footer rows dropped, `As of 30-Jul-2026` parsed |
| T5 | NDX `Sector` is the literal `General` — not `None`, `NaN` or `""` — **and stays so even if the API starts returning sectors** |
| T6 | `build_frame()` → the 8 DDL columns in order, constants, `Date` is a `date` not a `Timestamp` |
| T7 | `sanity_gate()` → 12 parametrised cases incl. both inclusive boundaries |
| T8 | `has_changed()` → add / remove / sector-only / row-order / `NaN`-vs-`None` / rate / float noise |
| T9 | `resolve_effective_date()` → all five rows of the decision table, plus `force` semantics |
| T10 | Missing `DBTRADING` / `TBLPORTASSETS` raises a **named** error before any SQL is built |
| T11 | Documented defaults apply when the optional vars are unset |
| T12 | `dbFlag=False` writes both CSVs and issues **zero** DB calls |
| T13 | `force=True` writes even when unchanged, via `DELETE`-then-append |
| T14 | Autouse `no_network` fixture fails any unit test that opens a real socket |
| — | Sanity-gate failure blocks the SP500 write while leaving NDX100 unaffected; one index failing does not stop the other; the post-write recount catches a swallowed `StoreEOD` failure |

**Added — `tests/unit/test_dataUtil.py` (10 tests)** for the shared change:

| Ref | Covers |
|---|---|
| D1 | `ExecSQL()` — `CREATE`/`INSERT`/`DELETE` take effect |
| D2 | returns the rowcount (`2`), and `0` when nothing matched — distinct from the `None` failure |
| D3 | **commits** — a second independent connection sees the change; plus the full delete-then-append cycle |
| D4 | error contract preserved — invalid SQL and a dead engine both log at ERROR and return `None`, never raise |
| D5 | emits no `RemovedIn20Warning`, **with a control test** asserting the old idiom really did warn, so D5 cannot pass vacuously |

**Results.**

* `pytest tests/unit -v` on python3.10 with the pinned layer versions
  (pandas 1.5.3, SQLAlchemy 1.4.46, numpy 1.26.4): **97 passed**.
* The same suite inside `python:3.13-slim` against the built layer
  (pandas 2.2.3, SQLAlchemy 2.0.36, numpy 2.1.3): **96 passed, 1 skipped** —
  the skip is D5's control, which only applies to SQLAlchemy 1.4. This is the
  proof that `dataUtil` is genuinely version-agnostic rather than merely
  claimed to be.

**Existing verification re-run** (required, because `dataUtil.py` is shared):

| Check | Result |
|---|---|
| `fx_handler.py` dry run (`FXrateHandler`) | exit 0, 17 FX rows in `USD_FX.csv` |
| `handler.py` dry run (`cronHandler`, `LOCALRUN=localrun`) | exit 0; `snapshot_yf.csv` 392 rows vs 385 reference, column set and dtypes identical, no all-`NaN` columns (the symbol list comes from a stored procedure and has grown) |
| `opt_handler.py` dry run (`optHandler`, uncapped) | exit 0, **0 tracebacks**; `options_list.csv` 77 vs 79, `options_snapshot.csv` 75 vs 70, column set and dtypes identical. The 85 `ERROR` log lines are the handler's own expired-contract filter messages and predate this change |
| **Delete-then-append on live MySQL**, non-production `djtest` schema | **PASS** — `ExecSQL` `DELETE` returned 3, the row count went to 0 *before* the append (proving `Engine.begin()` commits where 1.4's legacy autocommit did implicitly), and the append left exactly the 2 new rows with no duplication. Probe table dropped and the schema verified byte-identical to before |
| `serverless print` / `serverless package` | exit 0; CloudFormation renders `python3.13` for `portAssetsHandler` and `python3.10` for all nine others — the per-function override does not disturb them |
| Layer import check under a **real** python3.13 (docker `python:3.13-slim`) | `pandas`, `sqlalchemy`, `pymysql`, `lxml`, `openpyxl`, `numpy`, `requests`, `bs4`, `dotenv`, `pytz` all import, and so do `dataUtil` and `port_assets_handler` |
| New-handler dry run | exit 0; 503 SP500 + 103 NDX100 = 606 rows; SPY cross-check disagreed on only 2 non-index lines (`-`, `2602335D`) |

**Removed verification: none.** No handler, event flag, CSV output, symbol list
or stored-procedure dependency is retired by this change, and no golden CSV was
deleted. `yfin_handler.py` and `yfineod_handler.py` also call `ExecSQL` but were
deliberately **not** dry-run: neither is in `serverless.yml` (`TODOS.md` §4.2
dead code). Stated so the omission reads as a decision.

**New golden reference.** `Ops/fin-cron-data/portfolio_assets_info.csv` (606
rows) plus the per-index files. There was no prior reference for this handler —
this change creates the baseline.

### Known constraints discovered

* The MySQL server runs with **`sql_require_primary_key=ON`**. Any `CREATE
  TABLE` without a primary key fails, which means `StoreEOD`'s
  `to_sql(if_exists='append')` can **never** auto-create a table here — a new
  target table must always be created manually with a PK first. Found while
  running the delete-then-append probe.
* **Serverless Framework 3.38.0 does not know `python3.13`** — its runtime
  allowlist stops at `python3.11`, so the new function raises a config
  validation *warning*. It is cosmetic: `serverless package` renders
  `python3.13` into CloudFormation correctly. But `configValidationMode: error`
  must stay commented out in `serverless.yml`, or every deploy breaks.

### Not done — requires a human

* Publishing the `finPort313` layer and uncommenting its ARN in
  `serverless.yml`.
* Adding `DBTRADING` and `TBLPORTASSETS` to `Ops/fin-cron-data/.env`
  (`CLAUDE.md` forbids Claude editing `.env`). The dry run supplied them inline.
* `serverless deploy function -f portAssetsHandler`, and the first real
  invocation that seeds both `Port_name` sets.
