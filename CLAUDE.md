# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Fin-Lambda is a collection of scheduled AWS Lambda functions that collect financial market data — stock/ETF snapshots, options chains, intraday bars, FX rates, US interest rates, Fama-French factors, and news — and write them into MySQL (`GlobalMarketData`, `Trading` schemas), PostgreSQL, or S3/Cloudflare R2.

This is **not** a conventional Python package: there is no test suite, no `src/` layout, no `__init__.py`. Each handler is a flat top-level module that Serverless zips together with `dataUtil.py` and the CSVs beside it. Imports are flat (`import dataUtil as DU`) and several CSV reads use relative paths, so **handlers only work when the process CWD is their own directory**.

## Layout

| Path | Role |
|---|---|
| `Ops/fin-cron-data/` | **The live service.** All deployed handlers + `serverless.yml`. MySQL backend. |
| `Ops/fin-cron-Pgsql/` | Partial PostgreSQL port (3 scripts only). Its `serverless.yml` is copied from fin-cron-data and references handlers that do not exist in the folder — do not deploy it as-is. |
| `Dev/fin-cron-data/` | Research: HMM/GMM regime detection, notebooks, its own `dataUtil.py` fork. |
| `Product_List/` | Symbol CSVs (`Symbol` column) used when a list is loaded from file. |
| repo root | Legacy CodeBuild path: `lambda_function.py`, `eod_usrate.py`, `buildspec.yml` (python3.8, updates `fin-Lambda-fun`). Superseded by Serverless; leave alone unless asked. |
| `Makefile` | Builds Lambda layer zips. |

`Ops/venv-*`, `venv*/`, `python/`, and `finCron.zip` are prebuilt virtualenvs and layer build artifacts — gitignored noise; never read or edit them.

## Deployed functions (`Ops/fin-cron-data/serverless.yml`)

| Function | Handler | Schedule (UTC) | Notes |
|---|---|---|---|
| cronHandler | `handler.run` | `0/10` at 13-21 and 21-00, Mon-Fri | Stock/ETF snapshot |
| optHandler | `opt_handler.run` | `0/10` at 12-21, Mon-Fri | Options snapshot |
| yfus30minEOD | `eoddata_minhandler_us.run` | 00:05 Tue-Sat | US intraday bars |
| yfasia30minEOD | `eoddata_minhandler_asia.run` | 10:00 Mon-Fri | Asia intraday bars |
| fffHandler | `fff_handler.run` | monthly, 1st | Fama-French 3-factor |
| usrateHandler | `usrate_handler.run` | 21:01 Mon-Fri | Fed H.15 scrape (BeautifulSoup) |
| FXrateHandler | `fx_handler.run` | hourly | FX spot snapshot |
| FXHistHandler | `fxeod_handler.run` | 21:10 daily | FX daily EOD |
| yfNewshandler | `yf-news-collect.run` | hourly | News → S3/R2 |
| portAssetsHandler | `port_assets_handler.run` | 22:30 Mon-Fri | S&P 500 / NASDAQ-100 membership → `Trading.portfolio_assets_info`. **The only `python3.13` function** — own layer (`finPort313`), own `requirements_port313.txt` |

`timeout: 900`, `region: us-east-2`, AWS profile `ServerLessUser`. Unlisted modules in the same folder (`yfin_handler.py`, `yfineod_handler.py`, `FOC_data.py`, `opt_ibapi.py`, `yfin_opt.py`) are superseded or manual-run — check `serverless.yml` before assuming a file is live.

Despite the `30min` naming, both `eoddata_minhandler_*.py` download `interval='15m'`.

## Architecture patterns

These conventions repeat across every handler; follow them rather than introducing new idioms.

**`dataUtil.py` is the only DB layer.** It holds a module-global `dbconn` SQLAlchemy engine created lazily by `get_DBengine()` (MySQL via PyMySQL, from `DBHOST/DBPORT/DBUSER/DBPWD/DBMKTDATA`). Every read goes through `load_df_SQL()` / `load_df()` returning a DataFrame; every write goes through `StoreEOD(df, schema, table)`, which is `df.to_sql(..., if_exists='append')`. No ORM, no migrations — table schemas live in the database only.

**Errors are swallowed, not raised.** Nearly every `dataUtil` function wraps its body in `try/except` + `logging.error(..., exc_info=True)` and implicitly returns `None`. Callers rarely check. If you make a function raise, audit its callers first.

**Snapshot tables are delete-then-append, not upsert.** `handler.py`, `opt_handler.py`, and `fx_handler.py` all run `DU.ExecSQL(f"DELETE FROM {DB}.{TBL} where (Symbol != '1');")` then `StoreEOD(...)`; the table holds only the newest snapshot. Historical tables (`TBLMINUTEPRICE`, `TBLHISTFX`) instead append after querying `get_Max_datetime()` / `get_Max_date()` for a watermark.

**Symbol lists come from two interchangeable sources.** `DU.load_symbols(name)` reads `$PROD_LIST_DIR/{name}.csv` — *except* when `name == "system"`, which calls the stored procedure `GlobalMarketData.current_symbols_V3`. Other server-side procedures the code depends on: `GlobalMarketData.get_us_symbol`, `GlobalMarketData.get_asia_symbol`, `Trading.sp_etf_trades_v2`, `Trading.sp_stock_trades_V3`. None are in the repo, so a schema change there breaks handlers silently.

**Timezones are per-exchange, not per-handler.** The intraday handlers map symbol → exchange via `stock_exchange.csv` (`DU.load_symbols_dict()`) and exchange → tz via `Exchange_timezone.csv` (`DU.load_exchange_tz()`), then store `Datetime` tz-naive in exchange-local time alongside a `UTCDatetime` copy and a `timezone` column. `intra_blacklist.csv` subtracts known-bad symbols; a symbol missing from `stock_exchange.csv` is skipped entirely.

**Two data sources for snapshots.** `handler.stk_run()` uses `DDSClient.py`, a raw TCP socket client for a proprietary feed with a numeric field-code map (`'3'` → last, `'1'` → bid, …) at `defaultIP:defaultPort`. `handler.yf_stk_run()` is the yfinance replacement and is what `run()` actually calls today; `stk_run` is dead but retained.

## Tests

There **is** a test suite now (added 2026-08-01, `TODOS.md` §0):

```bash
pytest tests/unit -v          # hermetic; an autouse fixture fails any test that opens a socket
pytest -m integration         # opt-in, hits live sources
```

Run it from a venv carrying the **pinned layer versions** (`venv-cron7` here),
not whatever is newest. `pytest.ini` sets `pythonpath = Ops/fin-cron-data`
because handlers use flat imports.

Coverage so far is `dataUtil.ExecSQL` and `port_assets_handler` only; the other
nine handlers remain uncovered (`TODOS.md` §2).

## Local runs

Verification also means running a handler locally with DB writes disabled — it prints to CSV instead.

```bash
cd Ops/fin-cron-data          # required: flat imports + relative CSV paths
python opt_handler.py         # the __main__ block supplies the event
```

The off-switch is **not uniform** — check the handler before running one against production:

- `eoddata_minhandler_us.py` / `_asia.py` — event flags `{"localrun": True, "dbFlag": False}`; `dbFlag=False` is what actually suppresses writes. Also accepts `"InitialRun": True` for a full-history backfill.
- `opt_handler.py` — event flag `{"localrun": True}`; `{"test": N}` caps iterations.
- `handler.py` — module-global `localrun`, set from the `LOCALRUN=localrun` env var.
- `fx_handler.py` — module-global `localrun`, set in the `__main__` block.
- `yf-news-collect.py` — `LOCALRUN` env var (defaults to `True`), `BATCH_SIZE` tickers per invocation.
- `port_assets_handler.py` — event flags `{"localrun": True, "dbFlag": False, "test": True}`, supplied by its `__main__`; `dbFlag=False` is what suppresses writes. `{"force": True}` bypasses the only-on-change check.

Outputs land in the CWD as `snapshot_yf.csv`, `options_list.csv`, `options_snapshot.csv`, `30min_{sym}.csv`, `USD_FX.csv`. `DEBUG=debug` in `.env` raises log level everywhere.

## Build & deploy

```bash
# Lambda layer: installs Ops/fin-cron-data/requirements_cron.txt into python/lib/python3.10/site-packages
make finCron.zip                              # then upload manually as a layer in AWS
make finPort313.zip                           # python3.13 layer for portAssetsHandler only

cd Ops/fin-cron-data && serverless deploy      # deploy all functions
serverless deploy function -f optHandler       # single function, much faster
```

**Two runtimes, two layers.** `finPort313` pins `pandas==2.2.3`,
`SQLAlchemy==2.0.36`, `numpy==2.1.3` for `portAssetsHandler`; `finCron` keeps
the 3.10 pins below for the other nine. The runtime and layer ARN are declared
**on the function**, never at provider level, so the two can never cross.
`dataUtil.py` is shared and runs unmodified on both — `ExecSQL` uses
`Engine.begin()` + `text()` rather than the `Engine.execute()` that 2.0 removed.

Serverless Framework 3.38 does not recognise `python3.13` and emits a config
validation *warning*; it renders correctly into CloudFormation, but
`configValidationMode: error` must stay commented out or every deploy breaks.

Serverless v3 with `serverless-dotenv-plugin` and `useDotenv: true`, so `.env` values become Lambda environment variables at deploy time. Layer deps are pinned tight (`yfinance==0.2.58`, `pandas==1.5.3`, `SQLAlchemy==1.4.46`, `numpy==1.26.4`) — SQLAlchemy is 1.4-era, so `engine.execute(...)` still works and 2.0 idioms will not. Changing `requirements_cron.txt` means rebuilding and re-uploading the layer, not just redeploying.

Other layers (`finWebLib`, `finSvrLib`, `finVisLib`, `finDataLib`) build from the `req_*.txt` files at repo root and are rarely touched.

## Configuration

Each Ops subfolder has its own `.env`, loaded by a module-level `load_dotenv()`. **`.env.example` at the repo root is the committed template** (added 2026-08-01, closing `TODOS.md` §3.6). If you add or rename an env var, update `.env.example` in the same change, and record it in `doc/OPERATIONS.md`.

Groups: DB (`DBHOST`, `DBPORT`, `DBUSER`, `DBPWD`, `DBMKTDATA`, `DBTRADING`, `DBPREDICT`, `DBWEB`) · tables (`TBLDLYPRICE`, `TBLMINUTEPRICE`, `TBLSNAPSHOOT`, `TBLFXSNAPSHOT`, `TBLHISTFX`, `TBLUSRATES`, `TBLOPTCHAIN`, `TBLPORTASSETS`, `TBLWEBPREDICT`, …) · lists (`PROD_LIST_DIR`, `SYMBOLLIST`, `DEFAULT_TICKERS`, `FX_TICKERS` — a Python list literal parsed with `ast.literal_eval`) · feeds (`defaultIP`, `defaultPort`, `VENDOR`, `OptDataEngine`, `API_KEY`, `POLYGON_API_KEY`) · storage (`S3_BUCKET`, `R2_ENDPOINT`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`) · index membership (`SP500_PORT_NAME`, `NDX100_PORT_NAME`, `PORT_ASSET_CLASS`, `PORT_ASSET_TYPE`, `INDEX_CROSSCHECK`, `PORT_OUTPUT_DIR`) · misc (`DEBUG`, `LOCALRUN`, `BATCH_SIZE`, `FIRSTTRAINDTE`, `LASTTRAINDATE`).

`TBLDLYPRICE` is **`histdailyprice7`** — this file previously said `histdailyprice3`, which does not exist on the server.

The MySQL server runs with **`sql_require_primary_key=ON`**, so `StoreEOD`'s `to_sql(if_exists='append')` can never auto-create a table: any new target table must be created manually with a primary key first.

`Ops/fin-cron-Pgsql/dataUtil_Pgsql.py` hardcodes an absolute macOS dotenv path (`/Users/huangjunyi/...`) that does not exist in this WSL environment, and reads a different key set (`RHOST`, `DB`, `PORT`) — that folder cannot run locally without fixing this first.

## Research / ML (`Dev/fin-cron-data/`)

`RegimeDetection.py` (GaussianHMM / GMM / AgglomerativeClustering), `Regime_Detect_Strategy.py`, trained models under `hmm_model/`, `gmm_model/`, `cluster_model/`, plus exploratory notebooks. Uses its own `dataUtil.py` fork — changes there do not propagate to `Ops/`.

---

## Mandatory Rules for All Changes

> `HISTORY.md` and the four `doc/*.md` files were created on 2026-08-01 by the `portAssetsHandler` change. They are scoped to that change; the backfill for the nine older handlers is tracked in `TODOS.md` §6.

### 1. Every Change Must Be Logged in `HISTORY.md`

Any code change, configuration change, or architectural decision **must** be documented in `HISTORY.md` before the change is considered complete. Each entry should include:

- **Goal** — What problem was solved or feature added
- **Root cause** (for bugs) — Why it happened
- **Implementation detail** — What was changed and how
- **Related files** — Key files modified (absolute paths or paths from repo root)
- **Test coverage** — What tests were added/modified and their results

This applies to all changes regardless of size — a single-line env default fix, a multi-file refactor, a test addition, or a configuration update.

### 2. Keep the Four Canonical `doc/` Files Current

`doc/` has exactly four markdown files. Every change that touches the areas below **must** update the corresponding doc before the change is considered complete. Do not create new doc files — update the existing ones.

**How to record a change in a doc file:** At the top of the file, under a `## Changelog` section, append a dated one-line entry in this format:

```
- YYYY-MM-DD | Added | <what was added and where>
- YYYY-MM-DD | Modified | <what changed and in which section>
- YYYY-MM-DD | Deleted | <what was removed and why>
```

Each doc already has (or must have) a `## Changelog` section as its first section. Every edit to the doc body must be accompanied by a matching changelog line.

| Doc | Update when… | What to update in the body |
|---|---|---|
| `doc/TECHNICAL-DESIGN.md` | Adding/removing/renaming a Lambda handler or its `serverless.yml` entry; changing a cron schedule; changing a `dataUtil.py` function or its write semantics (delete-then-append vs. watermark append); changing the column set written to a DB table; adding or dropping a dependency on a server-side stored procedure; changing symbol-list resolution or the exchange→timezone mapping; switching a handler's data source (yfinance / DDS TCP / IBAPI / Tiingo) | The deployed-functions table, that handler's deep-dive section, the data-flow diagram, the `dataUtil` API section, the per-table schema section, and the stored-procedure dependency list. Record which handlers are live vs. superseded whenever that changes. |
| `doc/OPERATIONS.md` | Changing `serverless.yml` provider settings (region, timeout, IAM statements, AWS profile); changing layer contents (`requirements_cron.txt`, `Makefile` targets) or the Python runtime; adding/renaming/re-defaulting an env var in any `.env`; changing the deploy or layer-upload procedure; after any production incident (missed or overlapping run, yfinance rate-limit or schema break, DB connection failure, layer/runtime mismatch, Lambda timeout) | The relevant section (Environment Setup, Layer Build & Upload, Serverless Deploy, Schedules & Timezones, Known Incidents, Troubleshooting). For new env vars: add to the env table with which handler reads it and its default. For new incidents: add to Known Incidents with root cause, blast radius, and fix. Note explicitly when a change requires a layer rebuild rather than just a redeploy. |
| `doc/PRODUCT-GUIDE.md` | Adding/removing a dataset or instrument class; changing a symbol list's membership rules; adding/removing fields in a snapshot or bar; changing collection frequency, coverage window, or market/timezone scope; changing where a consumer reads the data (MySQL table, S3/R2 bucket) | The dataset catalogue — per dataset: what it contains, coverage, frequency, expected freshness/lag, and where to read it. Plain language for data consumers — no handler names, SQLAlchemy, or event-flag internals. Mark partial or unimplemented coverage explicitly (e.g. the PostgreSQL port, Asia-market gaps, blacklisted symbols). |
| `doc/API-REFERENCE.md` | Changing a handler's invocation event contract (`localrun`, `dbFlag`, `InitialRun`, `test`, `NYTIME` keys and their semantics); changing a public `dataUtil` function signature or return type; changing a written table's columns, types, or key/uniqueness assumptions; changing an S3/R2 key layout or stored JSON payload; changing the result columns a consumed stored procedure is expected to return | The per-handler event-contract table, the `dataUtil` function reference, the per-table column reference, and the S3/R2 key-layout section. This repo exposes no HTTP endpoints — these contracts *are* its API. Mark not-yet-implemented surfaces (e.g. `Ops/fin-cron-Pgsql/` handlers named in its `serverless.yml`) with `> **Status: Not yet implemented**`, and mark table schemas read off a live database rather than from DDL with `> **Note:** Schema inferred from live table, not from DDL`. |

### 3. All Change Plans Must Include a Test Section

This repo has **no automated test suite yet** — no `pytest`, no `conftest.py`, no CI. That is being fixed incrementally:

> **Every new Lambda function ships with a test file. Every change to an existing function adds or updates a test for the path it touches.** The retroactive backlog for the handlers that predate this rule is tracked in [`TODOS.md`](TODOS.md) — check it before writing tests so you build on the shared fixtures in section 0 rather than a parallel harness.

Until the harness in `TODOS.md` §0 exists, "testing" also means an explicit, reproducible verification run against real data with writes disabled. A change plan must address all four points below; substitute verification steps for test cases only where no test file exists yet.

1. **Must pass all existing verification** — Every check that applied before the change must still pass. Concretely:
   - Every handler touched by the change still runs to completion as a local dry run (see *Local runs*) with writes suppressed — `{"dbFlag": False}` for the intraday handlers, `{"localrun": True}` or `LOCALRUN=localrun` for the rest. Non-zero exit or a new stack trace in the log is a failure.
   - Output CSVs are compared against the previous run: row count, column set, and dtypes must match, and spot-checked values must be sane (no all-`NaN` columns, no zero-row output for a live market window). The CSVs checked into `Ops/fin-cron-data/` (`snapshot_yf.csv`, `options_list.csv`, `options_snapshot.csv`) are the de facto golden references — state which one you diffed against.
   - `serverless print` (and `serverless package` for deploy-affecting changes) succeeds from `Ops/fin-cron-data/`, validating `serverless.yml` without deploying.
   - Any module you edited still imports cleanly under Python 3.10 with the **pinned layer versions** (`pandas==1.5.3`, `SQLAlchemy==1.4.46`, `numpy==1.26.4`, `yfinance==0.2.58`) — not whatever is newest in your local venv. A change that only works on SQLAlchemy 2.x or pandas 2.x is a regression.
2. **Announce removal of obsolete verification** — If the change retires a handler, an event flag, a CSV output, a symbol list, or a stored-procedure dependency, say so explicitly and state why the corresponding dry run or golden CSV no longer applies. Deleting a stale golden CSV counts and must be announced.
3. **Announce addition of new tests** — A new Lambda function requires a `pytest` file; this is not optional. For changes to existing functions, add a unit test covering the path you touched wherever the logic is pure and importable (date/timezone math, symbol filtering, field mapping, DataFrame reshaping), and fall back to a documented dry run — with the exact event dict and expected output shape — only for code that cannot be isolated from the network or the DB. New env vars need a check that the handler behaves correctly when the var is **absent**, since `environ.get()` returns `None` silently and the failure surfaces later as a bad SQL string.
4. **Document added/removed verification** — Record every added or removed check in `HISTORY.md` under *Test coverage*, and in the matching `doc/*.md` file (dry-run procedures and incident checks in `doc/OPERATIONS.md`; event contracts and table columns in `doc/API-REFERENCE.md`). Update this `CLAUDE.md` if the way a handler is verified changes.

Verification types to consider:
- **Local dry run** — the primary tool; per-handler flags differ, see *Local runs* above.
- **Golden-CSV diff** — compare dry-run output against the committed CSVs for shape and dtype drift.
- **Config validation** — `serverless print` / `serverless package`; confirm cron expressions are UTC and that a schedule change doesn't collide with the exchange-local window the handler assumes.
- **Layer/dependency validation** — rebuild `make finCron.zip` and confirm imports resolve when `requirements_cron.txt` changes; a layer change is not verified by a redeploy alone.
- **DB write validation** — run against a non-production schema, or dry-run the generated SQL. Note that `StoreEOD` appends with no uniqueness check and the snapshot handlers `DELETE` before writing, so a bad run corrupts a production table rather than erroring.
- **Post-deploy check** — after the next scheduled invocation, confirm CloudWatch has no errors and that `get_Max_datetime()` / row count on the target table advanced as expected.
- **Regression check for fixed bugs** — reproduce the bug's trigger (a specific symbol, date range, or empty result) as a dry run and record it in `HISTORY.md`.
