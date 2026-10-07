# PLAN-SR-UPSTREAM — Fin-Lambda upstream track for Support-Resistance-Agent

Status: **SHADOW RUN LIVE (G1/U5) as of 2026-10-06.** Phases A–F done and tested in `Ops/fin-deep-data` (python3.13); the service was deployed 2026-10-01 and the three new functions — `eodDaily`, `optChainEOD`, `statusReport` — are now enabled against the `*_shadow` tables, with the SNS e-mail subscription confirmed. The five v2 ports remain disabled pending their per-data-set cutover (`doc/OPERATIONS.md` §11.2). Current enable state, and the three issues it surfaces, are in *Live enable state* below. Next: finish the ten-session shadow diff, then U6 cutover → U2b prepend → U7 archive.

> **2026-10-01 — re-implementation.** The owner added constraints that the
> 2026-09-25 implementation did not meet: no deployed function or layer of
> `Ops/fin-cron-data` may change, every new function runs python3.13, all new
> files live in `Ops/fin-deep-data`, one `serverless.yml` controls them while
> each Lambda uploads only the files it needs, Phase E's function is named
> `portAssetsHandlerv2`, and a layer must stay inside ~80 MB.
>
> What that changed, relative to the phase text below:
> - **A** — `dataUtil.py` is a *fork* in the new folder, not an edit of the
>   shared one. It also gains `list_dir()` so the packaged CSVs no longer depend
>   on the CWD.
> - **B, C, D** — same handlers, new folder, layers `finDeepCore` + `finDeepYf`
>   (or just core for `statusReport`) instead of `finPort313`.
> - **E** — deploys as `portAssetsHandlerv2` with its schedule disabled;
>   `load_audit.job` stays `portAssetsHandler`.
> - **F** — the four python3.10 handlers could not be edited, so each is a v2
>   function in the new service with its schedule disabled. The two intraday
>   handlers are merged into one module, `intraday_min_handler.py`.
> - **P3** is superseded: three layers (`finDeepCore` 101 MB/29 MB,
>   `finDeepYf` 28/10, `finDeepWeb` 14/6) built by
>   `Ops/fin-deep-data/build_layers.sh`, replacing the single `finPort313`
>   rebuild. `finPort313` itself is untouched.
> - **P6** is superseded by `Ops/fin-deep-data/.env.example`, which adds
>   `FINDEEPCORE_LAYER_ARN` / `FINDEEPYF_LAYER_ARN` / `FINDEEPWEB_LAYER_ARN` in
>   place of `FINPORT313_LAYER_ARN`.
> - **G** — the cutover is now per data set and the runbooks live in
>   `doc/OPERATIONS.md` §11.
>
> Details, measurements and the dry-run results: `HISTORY.md`, 2026-10-01.
Revised 2026-09-25: added the Python 3.13 policy and moved every resource requirement into the Pre-Phase.
Owner: Thomas Choi
Sources: `Support-Resistance-Agent/docs/BUILD-PLAN.md` §17.2 A7/A8, `SR_Technical_Document.md` §4.5.7

## Live enable state — old vs. new functions and the tables they write

Verified against AWS on **2026-10-06** (`aws scheduler list-schedules` for
`fin-deep-data`, `aws events list-rules` for `fin-cron-data`, both
`us-east-2` / profile `ServerLessUser`). This is the authoritative state table;
`doc/OPERATIONS.md` §10.5 lists the *intended* times, which is not the same
thing.

**The shadow run (G1/U5) is live.** `eodDaily`, `optChainEOD` and
`statusReport` are enabled and writing to `*_shadow` tables while the droplet
cron keeps writing production. The SNS e-mail subscription is **confirmed**.

### Paired data sets — one table, two possible writers

Each row is a data set whose new writer exists. `BOTH ENABLED` on any row means
duplicate rows in that table; `BOTH DISABLED` means the data set has no writer
at all.

| Data set | Table written | Old function (`fin-cron-data`, py3.10) | Old state | New function (`fin-deep-data`, py3.13) | New state | Status |
|---|---|---|---|---|---|---|
| Daily bars | `GlobalMarketData.histdailyprice7` → shadow: `histdailyprice7_shadow` | *(none — droplet cron in myFinData)* | ENABLED (host cron) | `eodDaily` 18:30 + 19:00 sweep | **ENABLED** | **Shadow run** — no collision: new writer is on `_shadow` |
| Corporate actions | `GlobalMarketData.corp_action_daily` | *(none — new data set)* | — | `eodDaily` | **ENABLED** | New data set, writes production directly |
| EOD option chains | `GlobalMarketData.OptionChains` → shadow: `OptionChains_shadow` | *(none — droplet cron in myFinData)* | ENABLED (host cron) | `optChainEOD` 17:40 + 18:40 sweep + 19:40 sweep | **ENABLED (2 of 3)** | **Shadow run**, but the 19:40 sweep is DISABLED — see below |
| Load status report | *(reads `v_load_status`; writes nothing)* | *(none — new function)* | — | `statusReport` 20:00 | **ENABLED** | SNS e-mail only; `load_audit` is the durable copy |
| Index membership | `Trading.portfolio_assets_info` | `portAssetsHandler` 22:30 UTC | **DISABLED** | `portAssetsHandlerv2` 18:30 ET | **DISABLED** | ⚠️ **BOTH DISABLED — no writer. SP500/NDX100 have stopped updating, and DJIA/HSI never started (§5.12).** |
| US interest rates | `GlobalMarketData.USRates` | `usrateHandler` 21:01 UTC | ENABLED | `usrateHandlerv2` 17:05 ET | DISABLED | Correct — awaiting §11.2 cutover |
| FX daily EOD | `GlobalMarketData.FX_histdaily` | `FXHistHandler` 21:10 UTC | ENABLED | `FXHistHandlerv2` 17:10 ET | DISABLED | Correct — awaiting §11.2 cutover |
| US 15-min bars | `GlobalMarketData.histminprice` | `yfus30minEOD` 00:05 UTC | ENABLED | `yfus30minEODv2` 20:05 ET | DISABLED | Correct — awaiting §11.2 cutover |
| Asia 15-min bars | `GlobalMarketData.histminprice` | `yfasia30minEOD` 10:00 UTC | ENABLED | `yfasia30minEODv2` 06:00 ET | DISABLED | Correct — awaiting §11.2 cutover |

### Unpaired — old functions with no 3.13 successor yet

These are what TODOS §5.10 still has to port before `python3.10` is blocked
from redeploy.

| Data set | Table written | Old function | Old state | Note |
|---|---|---|---|---|
| Stock/ETF snapshot | `GlobalMarketData.snapshot` (delete-then-append) | `cronHandler` 0/10 at 13-21 + 21-00 | ENABLED | Table name is **hardcoded** in `handler.py:146`, not read from `TBLSNAPSHOOT` |
| Options snapshot | `GlobalMarketData.options_snapshot` (delete-then-append) | `optHandler` 0/10 at 12-21 | ENABLED | Table name hardcoded in `opt_handler.py:140` |
| FX spot snapshot | `GlobalMarketData.FX_snapshot` (delete-then-append) | `FXrateHandler` hourly | ENABLED | `TBLFXSNAPSHOT` |
| Fama-French 3-factor | `GlobalMarketData.famaFrench` | `fffHandler` monthly, 1st | ENABLED | ⚠️ Schedule is enabled but the handler **cannot run** — `NameError` on every invocation (TODOS §4.1) |
| News | S3 / Cloudflare R2 | `yfNewshandler` hourly | ENABLED | No DB table |

### Schedule-level detail for the three enabled new functions

| Schedule | Expression (`America/New_York`) | State |
|---|---|---|
| `EodDailySchedulerSchedule1` | `cron(30 18 ? * MON-FRI *)` | ENABLED |
| `EodDailySchedulerSchedule2` (sweep) | `cron(0 19 ? * MON-FRI *)` | ENABLED |
| `OptChainEODSchedulerSchedule1` (dispatch) | `cron(40 17 ? * MON-FRI *)` | ENABLED |
| `OptChainEODSchedulerSchedule2` (sweep +60) | `cron(40 18 ? * MON-FRI *)` | ENABLED |
| `OptChainEODSchedulerSchedule3` (sweep +120) | `cron(40 19 ? * MON-FRI *)` | **DISABLED** |
| `StatusReportSchedulerSchedule1` | `cron(0 20 ? * MON-FRI *)` | ENABLED |

### Three things this state table surfaces

1. **`serverless.yml` still says `enabled: false` for all eleven schedules.**
   The three enabled ones were enabled outside the repo, so the next
   `cd Ops/fin-deep-data && serverless deploy` silently reverts them to
   DISABLED and the shadow run stops with no error. Either set `enabled: true`
   on those four entries (eodDaily ×2, optChainEOD dispatch + sweep1,
   statusReport) to match reality, or do not deploy this service until the
   shadow run ends.
2. **`optChainEOD`'s 19:40 sweep is DISABLED while its dispatch and 18:40 sweep
   are ENABLED.** `doc/OPERATIONS.md` §10.4.1 says to enable all of a
   function's entries together: a symbol that fails both the 17:40 dispatch and
   the 18:40 sweep now has no third attempt, so the shadow diff will show gaps
   that are an artefact of the half-enablement rather than a real defect.
3. **Account Lambda `ConcurrentExecutions` is still 10** (re-checked
   2026-10-06), and `optChainEOD` is enabled with `OPT_SHARDS=4` and no
   `reservedConcurrency` — the exact condition TODOS §2.13 said to fix *before*
   enabling it. Four shards plus the 18:40 sweep draw from the same pool of 10
   that the nine live `fin-cron-data` functions use, and `cronHandler` /
   `optHandler` both fire every 10 minutes through that window. Expect
   `TooManyRequestsException` throttles on either side. Raise the quota
   (Service Quotas `L-B99A9384` → 1000) as the fix.

## Context

Support-Resistance-Agent (SR) BUILD-PLAN §17.2 **A7/A8** and its tech doc §4.5.7 (D10) split the work between two repos by **who writes MySQL**:
- **Fin-Lambda** owns every job that writes `GlobalMarketData`/`Trading`. It absorbs myFinData's two host-cron jobs.
- **SR** reads MySQL read-only.

The contract between them is **data** (tables and their meaning), not shared code.

SR needs:
1. Final bars with an exact `available_at`, taken from `load_audit.finished_at`.
2. Corporate actions with a first-seen time.
3. History from 2008.
4. DJIA and HSI membership.
5. A daily status line per data set.

This track (≈24–28 h) does **not gate** SR P1. Rows written before the cutover are never changed.

**Deliverables in this repo:**
- 3 new Lambdas: `eodDaily`, `optChainEOD`, `statusReport`.
- 1 extension: `portAssetsHandler` gets DJIA and HSI.
- `dataUtil` helpers for insert-ignore, audit, sharding and sweep.
- `load_audit` in 5 older handlers.
- The python3.13 layer `finPort313`, extended with yfinance (done 2026-09-25) and shared by every 3.13 function. **All new code in this track targets python3.13** (see *Python 3.13 policy*).
- New tables, a shadow run, the cutover, the 2008 prepend, and archiving myFinData.

## U0 — production host facts (confirmed 2026-09-25)

| Fact | Value | Consequence |
|---|---|---|
| Host | DO droplet `ubuntu-s-4vcpu-8gb-sfo3-01`, **TZ `Etc/UTC`** | The eod cron `10 21 * * 1-5` runs at **17:10 ET in summer and 16:10 ET in winter**, ten minutes after the close. L2 is confirmed: winter bars may not be final. The options cron `40 21` runs at 17:40 EDT / 16:40 EST. |
| Code | myFinData **`19c8509`** (May 2025), present in the local clone. The local checkout at `37c9493` is stale. | **Port source is `19c8509`**, read with `git show 19c8509:Ops/<file>`. |
| venv | `~/env/myFinData.v2`: `yfinance 0.2.58`, `pandas 1.5.3`, python3.10 | `finData313` pins `yfinance==0.2.58`, the same version as production and as the `finCron` layer. |
| Symbol list | `CALL GlobalMarketData.current_symbols_V4` returns **857 rows**. According to the owner it already includes the stock options list, the ETF options list and the `portfolio_assets_info` members. | Both jobs use **V4**, not V2. SR's L5 union is already done server-side, so the port adds no union. |
| Loader cache | `Ops/yfinance` 362 MB, `Ops/OptionsChain` **4.1 GB** | U7 archives it **compressed** (tar.zst). Uncompressed, 4.5 GB would push SR's R2 use past the 10 GB free tier. |
| Logs | `/tmp/eod_extlog.txt`, `/tmp/optPMlog.txt` | They do not survive a reboot. Nothing to migrate. |

### What the production code at `19c8509` actually does (the port must keep this)

**eod** (`eoddata_ext_fetch.py -m` → `common_fetch_eod`):
- Downloads per symbol with `yf.download(sym, sdate, edate, auto_adjust=False)`. **L1 is already fixed in production.**
- Flattens multi-level columns and renames `Adj Close` → `AdjClose` if present.
- Exchange comes from `stock_exchange.csv`. `^HSI` maps to `HK`; any other symbol falls back to its ticker suffix, or `""` if it has none.
- Writes `StoreEOD` **per symbol when the symbol has more than 100 rows**, and otherwise once at the end with the list.
- Contains a `VENDOR=tiingo` branch. The port drops it and supports yfinance only.

**options** (`optchain_fetch.py -S PM -U -m`):
- The list is **V4 (857 symbols)**.
- For each symbol, `option_chains` retries 5 times with a 5 s pause. `UnderlyingPrice` is `history(period='1d').Close`, taken before the expiries are fetched.
- `filter_opt_chain` keeps rows with `lastPrice > 0.05` and `openInterest >` the chain's 25th percentile, for puts and calls.
- It adds `contractSize=100`, `Section='PM'` and `Date=todt`, and casts `inTheMoney` to bool.
- It writes **per underlying** with `StoreEOD(saveDF[nColumns], DB, opt_tbl)`.
- It saves a raw chain CSV to `OptionsChain/{sym}_{date}-PM.csv`. With `-U` it writes no `loadDB/` CSV.

### SR assumption this corrects

SR BUILD-PLAN §17.3 and tech doc §4.5.7 assume **≈50 option underlyings and `OPT_SHARDS=3`**. The production list is up to 857 symbols. Many of them (HK names, indices, crypto) have no US options and return quickly, but the time the job needs is **unmeasured**. U3 measures it and sizes from that. **SR's docs must be amended** with the measured count (an SR-repo change, raised as a follow-up, not done here).

## Other findings (verified in this repo 2026-09-24)

| # | Finding | Action |
|---|---|---|
| F1 | `Ops/fin-cron-data/.env` has `TBLDLYPRICE="histdailyprice6"`; `.env.example` and the docs say `histdailyprice7`. Only `dataUtil.load_df`/`load_eod_price` read it. | Fix it to `histdailyprice7`. New handlers write to the **required** `EOD_WRITE_TBL` / `OPT_WRITE_TBL`, which have no fallback. Cutover then means flipping these from `_shadow` to production. |
| F2 | `CLAUDE.md` puts `.env.example` at the repo root; it is actually at `Ops/fin-cron-data/.env.example`. | Correct `CLAUDE.md`. |
| F3 | Fin-Lambda's `DU.load_symbols("system")` calls V3. | New handlers call `DU.load_symbols_db("V4")`. Record the new stored-procedure dependency. |
| F4 | The `finPort313` Makefile target has its cross-build flags commented out, and `portAssetsHandler` actually runs on `finPortLib:3`. | P3 re-enables the flags on the `finPort313` target (`--platform manylinux2014_x86_64 --python-version 3.13 --only-binary=:all: --no-compile`) before the rebuild. |
| F5 | The existing `DJI` (30) and `HSI` (167 rows, 2 dates) portfolios in `portfolio_assets_info` were loaded manually with sector `General`. | U8 reuses those `Port_name`s and matches their Symbol format, Currency and Rate, read from the live table first. |
| F6 | `FIRSTTRAINDTE` is also the empty-table fallback in `fxeod_handler` and both intraday handlers. | Before flipping it to `2008/01/01`, audit those three. The intraday handlers already cap at 59 days; FX would backfill a new ticker from 2008, which is acceptable. Record the result in HISTORY. |
| F7 | A batched `yf.download` that mixes US and HK tickers yields NaN rows on dates where one market is closed. | Batch per exchange-tz group and drop NaN-`Close` rows. |

## Python 3.13 policy

`requirements_port313.txt` and `finPort313` exist to move this service from python3.10 to python3.13. From this track on, **all new development targets python3.13**:

| Code | Runtime | Verified under |
|---|---|---|
| New handlers (`eod_daily_handler`, `optchain_eod_handler`, `status_report_handler`) | `python3.13` + `finPort313`, set on the function | py313 venv (P1) only. No 3.10 compatibility is required or tested. |
| `port_assets_handler` (Phase E) | already `python3.13` | py313 venv |
| New `dataUtil.py` helpers (Phase A) | shared by both runtimes | Written for 3.13 first. Also run under the 3.10 venv, because `dataUtil` ships unmodified to the nine `finCron` functions. The helpers use only `Engine.begin()` + `text()`, which work on SQLAlchemy 1.4 and 2.0. |
| Phase F call sites in the four python3.10 handlers | stay `python3.10` / `finCron` in this track | 3.10 venv (their runtime) plus an import check under py313. Moving them to 3.13 is a separate item, `TODOS.md` §5. |

**Verification gate for new Python code.** A phase is not done until each new or edited module passes all of these in the py313 venv:
1. `python -m py_compile <module>` and a clean `import <module>` from `Ops/fin-cron-data/`.
2. Its `pytest` file passes.
3. No `FutureWarning` or `DeprecationWarning` is raised **from our own modules**. The py313 run uses `-W error::FutureWarning:<module> -W error::DeprecationWarning:<module>`, because `pytest.ini` currently ignores all `DeprecationWarning`. pandas 2.2 warnings about behaviour that changes in pandas 3 are fixed, not suppressed.
4. The dry run from its `__main__` block completes with writes off.

## Pre-Phase P — resources (ready before any coding) ≈ 3 h + owner time

Every resource the later phases need is listed here, so all of it can be provisioned and checked before Phase A starts. Items marked **owner** need AWS console, MySQL admin or Cloudflare access. Each item has a readiness check; the Pre-Phase is done when every check passes.

| # | Resource | Who | Needed by | Readiness check |
|---|---|---|---|---|
| **P1** ✅ 2026-09-25 | **Python 3.13 dev venv** (`venv-py313/`, gitignored): `uv python install 3.13` (not installed locally today; uv has 3.13.15), `uv venv --python 3.13 venv-py313`, then install `requirements_port313.txt` (P2) plus dev-only `pytest`, `boto3`. `boto3` is dev-only because the Lambda python3.13 runtime already provides it. | Claude | A–F tests, all dry runs | `venv-py313/bin/python -V` shows 3.13.x. `pytest tests/unit` passes in it (the existing `test_dataUtil` / `test_port_assets_handler` tests are the baseline). |
| **P2** | **`requirements_port313.txt` extended** (no separate data file): the existing pins plus `yfinance==0.2.58`, `curl_cffi==0.11.4`, `peewee==3.19.0`, `frozendict==2.4.7`, `multitasking==0.0.11`, `platformdirs==4.3.8` (2025-era, not newest). **Done 2026-09-25.** | Claude | P1, P3 | ✅ Resolves with `--only-binary=:all: --python-version 3.13`. ✅ Under 3.13.15, imports work, a batched `yf.download(["AAPL","0700.HK"], auto_adjust=False, actions=True, group_by="ticker")` and `Ticker("AAPL").option_chain()` return rows, and `pytest tests/unit` passes (102 passed, 1 skipped). |
| **P3** 🟡 built 2026-09-25 (49.5 MB zipped / 164 MB), owner to publish + set `FINPORT313_LAYER_ARN` | **Rebuild layer `finPort313`**: re-enable the cross-build flags on the existing `make finPort313.zip` target (F4), rebuild, and publish a new version with `--compatible-runtimes python3.13`. `portAssetsHandler` moves to the new version as well. | Claude builds, **owner** uploads | B, C, D deploy | Unzipped size < 250 MB (scratch cross-build measured 164 MB unzipped / 48 MB zipped, 2 MB under the 50 MB direct-upload limit). A `portAssetsHandler` dry run passes on the new layer. The layer ARN (with version) is recorded in `serverless.yml` comments and doc/OPERATIONS. A throwaway 3.13 function (or `serverless invoke` of a stub) imports `yfinance` and `pymysql` from the layer. |
| **P4** ✅ objects confirmed on the server 2026-09-25 | **DDL** in `Ops/fin-cron-data/sql/upstream_tables.sql` (`sql_require_primary_key=ON`): `load_audit`, `corp_action_daily`, `v_load_status` from SR tech doc §4.5.7 (`~/projects/Support-Resistance-Agent`), and `histdailyprice7_shadow`, `OptionChains_shadow` via `CREATE TABLE … LIKE`. **SQL written and validated 2026-09-25; waiting for the owner to run it.** One deviation from SR: the `load_audit` PK adds `table_name` so that eodDaily's two summary rows per run don't collide. SR's docs need the matching amendment (follow-up). | Claude writes ✅, **owner** runs | A (sqlite mirror for tests), B–D, G | `SHOW CREATE TABLE` for all four tables and `SELECT * FROM v_load_status LIMIT 1` succeed. The Lambda DB user has `SELECT, INSERT` on all of them and `EXECUTE` on `current_symbols_V4`. |
| **P5** ✅ 857 rows, column `Symbol` | **Stored procedure `GlobalMarketData.current_symbols_V4`** (exists; F3). | **owner** confirms | B, C | `CALL GlobalMarketData.current_symbols_V4` from the Lambda DB user returns ≈857 rows with the column name the new `load_symbols_db` will read. |
| **P6** ✅ 2026-09-25 (+ `FINPORT313_LAYER_ARN`, `STATUS_R2_KEY` in `.env`) | **Env vars** in `.env.example`, `.env`, doc/OPERATIONS §2, CLAUDE.md config groups. New: `EOD_WRITE_TBL=histdailyprice7_shadow`, `OPT_WRITE_TBL=OptionChains_shadow`, `TBLLOADAUDIT=load_audit`, `TBLCORPACTION=corp_action_daily`, `SYMBOL_PROC_VER=V4`, `EOD_SHARDS=1`, `EOD_BATCH=200`, `OPT_SHARDS=3` (placeholder until the Phase C timing run), `OPT_MAX_PARALLEL=4`, `UPSTREAM_R2_BUCKET`, `OPT_RAW_PREFIX=raw/optchain`, `STATUS_EMAIL`, `STATUS_R2_KEY=status/latest.json`, `DJIA_PORT_NAME=DJI`, `HSI_PORT_NAME=HSI`. Fixes: `TBLDLYPRICE` → `histdailyprice7` (F1). `FIRSTTRAINDTE` → `2008/01/01` is **deferred** to G3, after the F6 audit. | Claude | A–F | Every new key is present in both files. `serverless print` shows them in the rendered environment. |
| **P7** | **Cloudflare R2**: bucket `UPSTREAM_R2_BUCKET`, with an API token that can read and write the `raw/optchain/`, `status/` and `sr-agent/raw/myfindata-cache/` prefixes. The `R2_ENDPOINT` / `R2_ACCESS_KEY_ID` / `R2_SECRET_ACCESS_KEY` credentials already used by `yf-news-collect` are reused if that token covers the bucket. | **owner** | C, D, G4 | From P1, a boto3 `put_object` / `get_object` / `delete_object` of a test key under each prefix succeeds. |
| **P8** 🟡 declared; owner deploys + confirms | **SNS topic and e-mail subscription** for `statusReport`, declared in `serverless.yml` `resources:` and deployed **once, early**, so the owner can confirm the subscription e-mail before Phase D. | Claude declares, **owner** confirms the mail | D | The subscription status is `Confirmed`, not `PendingConfirmation`. A manual `aws sns publish` reaches `STATUS_EMAIL`. |
| **P9** ✅ rendered by `serverless package` | **IAM statements** in `provider.iam.role.statements`: `lambda:InvokeFunction` on `optChainEOD` itself (dispatcher) and `sns:Publish` on the P8 topic. The EventBridge Scheduler role for `method: scheduler` is created by the Framework. | Claude | C, D | `serverless package` renders both statements. After the first deploy, the role shows both. |
| **P10** | **Lambda concurrency headroom**: reserved concurrency `OPT_MAX_PARALLEL` needs the account's *unreserved* concurrency to stay ≥ 100 afterwards. New accounts can have a limit of 10, and then any reservation fails. | **owner** checks | C | `aws lambda get-account-settings --region us-east-2` shows `ConcurrentExecutions` ≥ 100 + `OPT_MAX_PARALLEL`, or a quota increase has been requested. |
| **P11** ✅ `AWS::Scheduler::Schedule` × 6 with `America/New_York` | **Serverless Framework features**: `schedule.method: scheduler` with `timezone` and `input`, and `reservedConcurrency`. Local CLI is **3.35.2** (CLAUDE.md says 3.38). `python3.13` produces the known validation warning; `configValidationMode: error` stays commented out. | Claude | B, C, D | A scratch `serverless package` with one scheduler entry renders an `AWS::Scheduler::Schedule` with `ScheduleExpressionTimezone: America/New_York`. |
| **P12** ✅ | **Port source**: myFinData `19c8509` in `~/projects/myFinData`. | Claude | B, C | `git -C ~/projects/myFinData show 19c8509:Ops/eoddata_ext_fetch.py` and `…:Ops/optchain_fetch.py` both print. |
| **P13** | **Golden baseline data**: read-only SELECT access to the last 5 sessions of `histdailyprice7`, and copies of the droplet's `Ops/OptionsChain/{sym}_{date}-PM.csv` for 10 sample underlyings from one date (`scp` into a gitignored `tests/golden/`). | **owner** (droplet `scp`) | B, C test section 4 | The files are present locally, and the SELECT returns rows for the sample symbols. |
| **P14** ⚠️ HSI ✅; the Wikipedia DJIA page has no ticker table any more — DJIA now from SSGA DIA holdings | **Index sources for Phase E**: the Wikipedia DJIA components page and the Hang Seng Index constituents page. Also read the existing `DJI` / `HSI` rows (Symbol format, Currency, Rate) from `portfolio_assets_info` (F5). | Claude | E | Both pages return HTTP 200 and `pandas.read_html` in P1 finds a table in the expected band (30; 50–110). The F5 values are recorded in the Phase E commit notes. |

Resources **not** provisionable up front: the real `OPT_SHARDS` value (it comes from the Phase C full-V4 timing run; P6 holds a placeholder) and the shadow-seed data (seeded at G1 so the watermarks are current).

## Phase A — `dataUtil` helpers (U4 + U1) ≈ 1.5 h — ✅ DONE 2026-09-25

Needs P1, P2, P4. The DDL, layer and env work that used to be in this phase is now P3, P4 and P6.

**A1. `dataUtil.py` additions.** Same idiom as the existing code: they log and return `None` on failure. Written for python3.13 / SQLAlchemy 2.0 and kept working on SQLAlchemy 1.4 via `Engine.begin()` + `text()` (see *Python 3.13 policy*):
- `append_ignore(df, schema, table, chunk=500) -> int|None`:
  - Uses `INSERT IGNORE` (MySQL) or `INSERT OR IGNORE` (sqlite, so the existing `sqlite_engine` fixture can test it). The verb is picked from `engine.dialect.name`.
  - Returns the number of rows inserted.
- `new_run_id()` gives a 26-char ULID with no new dependency. `run_host()`.
- `audit_run(rows)` inserts into `load_audit` via `append_ignore`. Callers wrap it in `try`, so an audit failure never fails a data load. It truncates `error` to 512 chars, because the server's `STRICT_ALL_TABLES` rejects longer values (verified in P4).
- `shard_symbols(symbols, i, n)` is round-robin over the sorted list. `time_left_ok(context, reserve_ms=120_000)` is True when `context` is None.
- `missing_for_sweep(job, date, expected)` returns the expected symbols with no `ok`/`empty` row for that date.
- `load_symbols_db(ver)` calls `GlobalMarketData.current_symbols_{ver}` (F3, P5).

## Phase B — `eodDaily` (U2) ≈ 5 h — `eod_daily_handler.py` — ✅ DONE 2026-09-25

A port of `myFinData@19c8509 Ops/eoddata_ext_fetch.py::common_fetch_eod`, keeping a `# ported from …@19c8509` comment. Its shape follows `port_assets_handler.py`: pure functions first, then `run(event, context)`, a JSON-safe return (the 2026-08-02 MarshalError lesson), `_output_dir()`, and a `__main__` block that supplies the event.

**Event contract:** `localrun`, `dbFlag`, `shard`/`of`, `sweep`, `prepend`, `asof`, `symbols` (override), `test` (cap N).

**Pipeline:**
1. List: `DU.load_symbols_db(SYMBOL_PROC_VER)` → `shard_symbols`.
2. `exchange_for(sym, exch_dict)` keeps production's rule exactly: CSV first, then `^HSI`→`HK`, then the ticker suffix, else `""`.
3. Watermark: one `SELECT Symbol, MAX(Date) … GROUP BY Symbol` on `EOD_WRITE_TBL`. It replaces the old code's 857 separate queries. A symbol with no rows starts at `FIRSTTRAINDTE`.
4. `plan_downloads(starts, asof)`:
   - Symbols with `start ≥ asof−21d` go into batches of `EOD_BATCH`, grouped by exchange tz (F7). Each batch downloads a common window from `asof−21d`, which is ≈14 sessions and also serves as the action window.
   - Older starts (first loads, gaps) are downloaded one symbol at a time.
5. `yf.download(..., auto_adjust=False, actions=True, group_by='ticker', progress=False)`, with the flags pinned.
6. `reshape_batch(raw, symbols)`:
   - Output is long, with production's `savColumns` order: `Date, Symbol, Exchange, Close, Open, High, Low, Volume, AdjClose`.
   - Multi-level columns are flattened, NaN-`Close` rows dropped, and rows filtered to `Date ≥ start`.
7. `drop_partial_bar(df, now_utc, exch_tz)`: drops `Date == local today` unless ≥ 60 min have passed since that exchange's close (L2).
8. `extract_actions(raw)`: non-zero `Dividends`/`Stock Splits` rows go to `corp_action_daily` with `first_seen_at` (first sighting wins).
9. Write with `append_ignore` per batch (L3). Then one per-symbol `load_audit` row (`segment` first/append/prepend, `date_lo/hi`, `n_rows`, status ok/empty/error/skipped, UTC times, `yf.__version__`, host) plus a `'*'` summary row.
10. `time_left_ok` guard: unstarted symbols are marked `skipped`. The sweep retries the symbols from `missing_for_sweep`.
11. Prepend (U2b): `prepend_ranges(min_dates, FIRSTTRAINDTE)` gives `[FIRSTTRAINDTE, MIN(Date)−1]` for each symbol with a gap. Written with `segment='prepend'`, INSERT IGNORE only.
12. `dbFlag=False` writes `eod_daily_{asof}.csv`, `corp_action_{asof}.csv` and `load_audit_{asof}.csv`.

**Schedule:**
- `runtime: python3.13` and the `finPort313` layer (P3) are set on the function, never at provider level.
- `schedule: {method: scheduler, timezone: America/New_York, rate: cron(30 18 ? * MON-FRI *), input: {shard: 0, of: 1}}`, plus a sweep at 19:00.

## Phase C — `optChainEOD` (U3) ≈ 5 h — `optchain_eod_handler.py` — ✅ DONE 2026-09-25 (measured T_total 1,965 s → `OPT_SHARDS=4`; dispatcher kept)

A port of `myFinData@19c8509 Ops/optchain_fetch.py`. Behaviour stays **identical**:
- `option_chains()` with 5 retries at a 5 s pause, and `UnderlyingPrice` from `history(period='1d')`.
- `filter_opt_chain()`, `contractSize=100`, `Section='PM'`, `inTheMoney` as bool, and the `nColumns` list.
- The date rule: NY date, minus one day if the hour is < 9.

Changes from the old job:
- List: V4 via `load_symbols_db`.
- Each underlying is written right after it is fetched, with `append_ignore` to `OPT_WRITE_TBL`, followed by its `load_audit` row.
- The raw unfiltered chain goes to R2 at `{OPT_RAW_PREFIX}/{date}/{sym}-PM.csv`. It uses the boto3 `endpoint_url` client pattern from `yf-news-collect.py`, not the host disk. Bucket and credentials come from P7.

**Sizing (857 symbols):**
- The dry run is first run over the full V4 list with `dbFlag=False`, and records per-symbol seconds. The result gives `T_total` and `OPT_SHARDS = ⌈T_total / 540 s⌉`.
- **Fan-out uses a dispatcher, not N schedule entries.** One scheduled invocation `{"dispatch": true}` starts at 17:40 America/New_York (production's summer time, held fixed). It async-invokes itself with `{"shard": i, "of": n}` for each shard.
- The function's reserved concurrency is `OPT_MAX_PARALLEL` (default 4; account headroom checked in P10). This caps parallel Yahoo traffic from Lambda IPs; throttled async events are retried by Lambda.
- A scheduled `{"sweep": true}` runs at dispatch + 60 min, and a second one at +120 min.
- IAM: `lambda:InvokeFunction` on the function itself (P9).
- Runtime `python3.13` + `finPort313`, like `eodDaily`.
- If `T_total` turns out to be ≤ 3 × 540 s, fall back to three staggered schedule entries as in SR's tech doc, and drop the dispatcher.

## Phase D — `statusReport` (U9) ≈ 3 h — `status_report_handler.py` — ✅ DONE 2026-09-25

- Reads `v_load_status`. For data sets whose handler does not write audit rows yet, it takes `MAX(Date)` from a module-level table list and marks it `(inferred)`.
- `status_for(row, today)` gives ok / partial / error / stale (stale = last data older than the previous weekday). `aggregate_shards()` combines shard rows. `format_report()` produces SR tech doc §4.5.7's format — reproduced in D.1, with the cases in D.2.
- Outputs: SNS e-mail and stdout. ~~R2 `STATUS_R2_KEY` as JSON~~ — the R2 upload,
  `to_json()` and `STATUS_R2_KEY` were **removed 2026-10-02**: the report is a
  formatted view of `load_audit`, which already keeps every figure it shows.
- The SNS topic, e-mail subscription and `sns:Publish` statement already exist from P8/P9. The ARN reaches the function through `environment: STATUS_TOPIC_ARN: !Ref`.
- Runtime `python3.13` + `finPort313`.
- Schedule: Mon–Fri 20:00 America/New_York.

### D.1 The report body

SR tech doc §4.5.7's format, reproduced here so the plan is self-contained.
Seven columns, fixed width `(21, 14, 12, 26, 9, 13)` and `Rows` unpadded as the
last cell (`HEADER` / `WIDTHS` in the handler):

```
DataName (table)     Job           Last data   Last run (ET)             Status   Symbols      Rows
histdailyprice7      eodDaily      2026-09-24  2026-09-24 18:31–18:44    ok       1212/1212    1212
OptionChains         optChainEOD   2026-09-24  2026-09-24 17:40–18:27    partial  49/50        61330
corp_action_daily    eodDaily      2026-09-24  2026-09-24 18:31–18:44    ok       —            7
USRates              usrateHandler 2026-09-23  2026-09-24 17:01–17:01    ok       —            1
FX_histdaily         (inferred)    2026-09-24  —                         —        —            —
```

The e-mail is this block under a `Fin-Lambda load status for {date} (ET)`
heading, followed by one indented `table/job: error` line for each
error/stale/partial line that carries a message. Subject: `Fin-Lambda status
{date}: all ok` or `: {n} issue(s)`. `print(body)` always runs, before any
delivery, so a local run and the e-mail show the same text.

**The grain is one line per `(table_name, job)`, not per Lambda.** `eodDaily`
appears twice — as `histdailyprice7` and as `corp_action_daily` — which is why
P4 adds `table_name` to the `load_audit` primary key.

Where each column comes from, per `(table, job)` pair in `v_load_status`:

| Column | Source |
|---|---|
| DataName (table) | `load_audit.table_name` |
| Job | `load_audit.job` — the **data set's** job name, so a v2 function reports as `usrateHandler`, not `usrateHandlerv2` |
| Last data | `MAX(date_hi)` over the day's summary rows; `MAX(Date)` on the table itself if every summary left it null |
| Last run (ET) | `MIN(started_at)` → `MAX(finished_at)` over **all** of that ET day's summary rows, UTC→ET. The end repeats its date only when it differs from the start's |
| Status | `status_for()`, below |
| Symbols | `n_good/n_expected` from `COUNT(DISTINCT Symbol)` over the day's **per-symbol** rows (`segment <> 'summary'`), good = any row `ok`/`empty`. Falls back to summing the summaries' own `n_ok`/`n_expected` for the U10 handlers, which write no per-symbol rows |
| Rows | `SUM(n_rows)` over the day's summary rows |

Counting distinct symbols rather than summing shard counters is what makes
`Symbols` and `partial` meaningful under Phase C's fan-out: a symbol skipped by
a shard on its time guard and recovered by the 19:40 sweep counts **once, as
good**.

### D.2 The cases

**Two kinds of line.** A data set is *audited* if any `load_audit` row exists
for it, and *inferred* otherwise:

| | Audited line | Inferred line |
|---|---|---|
| Source | `v_load_status` + that day's `load_audit` rows | `MAX(Date)` on the table (`MAX(Datetime)` for `histminprice`) |
| Job | the real job name | `(inferred)` |
| Last run, Rows, Symbols | real values | `—` |
| Status | ok / partial / error / stale | `stale` or `—` only — there is no run to have failed |

`INFERRED_TABLES` is the six **production** tables: `histdailyprice7`,
`OptionChains`, `USRates`, `FX_histdaily`, `histminprice`,
`portfolio_assets_info`. A table drops off that list automatically once it has
audit rows. This is what lets the report work *during* the shadow run (U5): the
v2 function audits `histdailyprice7_shadow` while the live `fin-cron-data`
counterpart writes `histdailyprice7` and audits nothing, so both appear and the
last-data dates can be compared across the cutover.

**Status, resolved in precedence order** `error > stale > partial > ok`:

| Status | Case |
|---|---|
| `error` | any of the day's summary rows has `status='error'`. Its message is appended under the table |
| `stale` | last data date older than `previous_weekday(today)` — Monday compares against Friday. Also the verdict when there is no last data date at all |
| `partial` | `n_expected` is known and `n_good < n_expected`: some symbol was still `skipped`/`error` after both sweeps |
| `ok` | a run finished, data is current, every expected symbol accounted for |
| `—` | an inferred line that is not stale: nothing is known about the run, and nothing looks wrong |

**On-change data sets are judged differently.** `EVENT_TABLES` —
`corp_action_daily` and `portfolio_assets_info` — legitimately write **zero
rows** on a quiet day, so their last *data* date says nothing about freshness.
They are staleness-checked against the ET date of their last *run* instead, and
an inferred line for one is never stale. Without this the report would report
`stale` every week with no split and no index reshuffle.

**Holidays are deliberately not modelled.** The SR watchdog applies the
exchange calendar (S5), so this report may legitimately say `stale` the day
after a US holiday and must not be wired to a pager on its own.

**Delivery failures are not report failures.** SNS and R2 are attempted
independently, each in its own `try/except`; a missing `STATUS_TOPIC_ARN` or
`UPSTREAM_R2_BUCKET` is logged and the other sink still goes out. If
`v_load_status` itself cannot be read, the report is still sent — empty, with
`ERROR: could not read …` appended to the body and `(view unreadable)` on the
subject — because silence is the one outcome a status report must never
produce. Return value: `{asof, subject, lines, statuses, error, sns, r2}`.

## Phase E — `portAssetsHandler` DJIA + HSI (U8) ≈ 3 h — ✅ DONE 2026-09-25 (DJIA source changed to SSGA DIA holdings; see HISTORY)

- The sources were confirmed in P14 (HTTP status and a real parse of the Wikipedia DJIA components and Hang Seng Index constituents tables). Re-check them at the start of the phase in case a page changed.
- Add `parse_djia_wikipedia` and `parse_hsi_wikipedia`, reusing `_pick_table` and `sanity_gate`. The bands are DJIA 30–30 and HSI 50–110.
- HSI symbols are normalised to the existing `HSI` rows' format (F5), likely `0700.HK`. Currency and Rate are copied from the existing rows.
- `DJIA_PORT_NAME=DJI` and `HSI_PORT_NAME=HSI`. The only-on-change logic is reused unchanged.
- Trimmed HTML fixtures go in `tests/fixtures/`, and tests are added to `test_port_assets_handler.py`.
- Because V4 already includes `portfolio_assets_info` members, new DJIA/HSI members flow into eod and options collection automatically.

## Phase F — `load_audit` in 5 older handlers (U10) ≈ 2 h — ✅ DONE 2026-09-25

- The 5 handlers are `usrate_handler`, `fxeod_handler`, `eoddata_minhandler_us`, `eoddata_minhandler_asia` and `port_assets_handler`.
- Each writes one `DU.audit_run([summary])` at the end of `run()`, wrapped in `try`.
- `port_assets_handler` is already python3.13. The other four stay on python3.10 / `finCron` (SQLAlchemy 1.4) for this track; the helpers use only `begin()` + `text()`, so they work there. Their edits are verified in the 3.10 venv and import-checked in py313 (*Python 3.13 policy*). Migrating them to 3.13 is `TODOS.md` §5.
- Each gets one unit test with `mock_engine`.

## Phase G — shadow, cutover, prepend, archive (owner-run from runbooks in doc/OPERATIONS.md)

1. **U5 shadow run, 10 sessions:**
   - Seed `_shadow` with the last 30 days of production rows (`INSERT … SELECT`), so each symbol's watermark matches production.
   - Deploy with `*_WRITE_TBL=*_shadow` while the old cron keeps writing production.
   - Every morning, a diff SQL (kept in OPERATIONS) compares key sets and checks `|ΔC|/C ≤ 1e-6` on ≥ 99.9 % of rows. Expected and explainable differences:
     - Winter-time bars that the old 16:10 ET run captured before they were final (L2).
     - Options quotes captured at 17:40 ET versus the old run's time. After the DST change on 2026-11-01, the old cron runs at 16:40 ET; the new one stays at 17:40.
   - `statusReport` runs throughout.
2. **U6 cutover** (a Friday after U5 passes):
   - On the droplet, comment out the two crontab lines, flip `*_WRITE_TBL` to production, and `serverless deploy`.
   - Rollback: re-enable the lines. `max(Date)+1` refills any gap.
3. **U2b prepend** (after the cutover and the F6 audit):
   - Set `FIRSTTRAINDTE=2008/01/01`, then invoke `serverless invoke -f eodDaily -d '{"prepend":true,"shard":i,"of":n}'` for each shard.
   - Check: every live symbol's `MIN(Date)` is the later of 2008-01-02 and its first yfinance session, and there is one prepend row per symbol.
4. **U7 archive:**
   - Compress the cache (`tar --zstd` of `Ops/yfinance` and `Ops/OptionsChain`) and put it in R2 `sr-agent/raw/myfindata-cache/` as write-once.
   - `git tag final-cron 19c8509` in myFinData and add a README pointer to Fin-Lambda.
   - Retire the droplet scripts after 30 days.

## Order and dependencies

```
P pre-phase: venv-py313, finPort313 rebuild, DDL, V4 check, env,
  R2, SNS (+ owner confirm), IAM, concurrency, Serverless scheduler,
  port source, golden data, index sources          ── all checks pass
   → A (dataUtil helpers)
               ├→ B eodDaily ─────┐
               ├→ D statusReport ─┼→ deploy to shadow → U5 (2 weeks elapsed)
               └→ C optChainEOD ──┘   (full-V4 timing dry run sets OPT_SHARDS)
   during U5: E (U8 DJIA/HSI), F (U10 audit rows)
   after U5:  U6 cutover → U2b prepend → U7 archive
```

Each phase is its own commit, with a `HISTORY.md` entry (Goal / Root cause / Implementation / Related files / Test coverage) and changelog lines in the affected `doc/*.md`:
- **TECHNICAL-DESIGN:** functions table, handler sections, dataUtil API, new tables, dependency on `current_symbols_V4`, live vs superseded (the myFinData jobs).
- **OPERATIONS:** env table, `finPort313` shared by all 3.13 functions, scheduler and timezones, dispatcher and reserved concurrency, shadow/cutover/prepend/archive runbooks, SNS subscription. The U0 findings go under Known Incidents, including the UTC-host winter 16:10 ET run.
- **API-REFERENCE:** event contracts, the `load_audit` / `corp_action_daily` / `v_load_status` columns and their SR contract, the R2 keys.
- **PRODUCT-GUIDE:** daily prices from 2008, the corp-actions dataset, DJIA/HSI, the status report, and the options coverage (V4 underlyings, filtered chain).

Also update `CLAUDE.md` (functions table, Local runs, the two-runtimes note rewritten as the *Python 3.13 policy* — new code targets 3.13 and is verified in `venv-py313` — and F2) and `TODOS.md` (§5: new handlers start on 3.13, the four Phase F handlers are queued for migration; new rows).

## Test section (CLAUDE.md §3)

1. **Existing verification must pass:**
   - `pytest tests/unit -v` stays green under `venv-cron7` (3.10) **and** `venv-py313`. New-handler tests run only in `venv-py313`; the `dataUtil` tests run in both.
   - Every new or edited module passes the *Python 3.13 policy* verification gate.
   - Touched old handlers get a dry run with writes off (`port_assets_handler` `{"localrun":true,"dbFlag":false,"test":true}`, the intraday handlers `{"localrun":true,"dbFlag":false}`), and their output CSVs are diffed against the previous run.
   - `serverless print` and `serverless package` pass, and the scheduler entries render with `America/New_York`.
2. **Removed:** none in this repo. myFinData's cron jobs have no tests. Their retirement is announced in HISTORY at U6/U7.
3. **Added** (one file per new Lambda):
   - `test_eod_daily_handler.py`: shard, `exchange_for` (CSV / `^HSI` / suffix / blank), `plan_downloads` (batch vs individual, tz grouping), `reshape_batch` against a saved multi-ticker yfinance fixture (NaN drop, column order, `Adj Close` rename), `drop_partial_bar` at the 59/60 min boundary in NY and HK, `extract_actions`, `prepend_ranges`, audit rows (segment and status; `skipped` via a fake context), `yf.download` kwargs pinned (mock), and handler behaviour when the env var is **absent** for `EOD_WRITE_TBL` / `TBLLOADAUDIT`.
   - `test_optchain_eod_handler.py`: column set and dtypes, `filter_opt_chain` (lastPrice and OI-quantile edges), retries then an empty frame, `Section`, `contractSize`, empty chain, per-underlying write, time guard → `skipped`, sweep takes only missing symbols, dispatcher emits n shard events, and the R2 key.
   - `test_status_report_handler.py`: status rules including stale-on-Monday, shard aggregation, the `(inferred)` fallback, and formatting.
   - `test_dataUtil.py`: `append_ignore` on sqlite, `audit_run` swallowing its own failure, ULID shape, `time_left_ok`.
   - `test_port_assets_handler.py`: DJIA/HSI parsers, bands and normalisation. Plus one audit-row test per U10 handler.
4. **Golden baselines**, recorded in HISTORY *Test coverage* and doc/OPERATIONS §6:
   - **eod:** the dry run for the last 5 sessions is diffed against rows the old cron already wrote to `histdailyprice7` (read-only SELECT; O/H/L/C/V/AdjClose equal after the float cast).
   - **options:** the dry run's pre-filter chain is diffed against the droplet's `Ops/OptionsChain/{sym}_{date}-PM.csv` for a sample of 10 underlyings on the same date, allowing for quote drift. The column set and the filter output shape must match exactly.
   - **End to end:** the U5 shadow diff.

## Verification (end to end)

```bash
cd /home/thomas/branch/Fin-Lambda
venv-py313/bin/python -m pytest tests/unit -v \
  -W error::FutureWarning:eod_daily_handler -W error::FutureWarning:optchain_eod_handler \
  -W error::FutureWarning:status_report_handler -W error::FutureWarning:dataUtil
venv-cron7/bin/python -m pytest tests/unit/test_dataUtil.py -v       # shared helpers on 3.10
cd Ops/fin-cron-data
../../venv-py313/bin/python eod_daily_handler.py                      # __main__: localrun, dbFlag False
../../venv-py313/bin/python optchain_eod_handler.py                   # full-V4 timing run → OPT_SHARDS
../../venv-py313/bin/python status_report_handler.py
serverless print && serverless package
cd ../.. && make finPort313.zip                                       # < 250 MB unzipped, imports OK
```

During U5:
- The `statusReport` e-mail arrives at 20:00 ET.
- `load_audit` has one row per symbol.
- The shadow diff is clean for 10 sessions.

After U6: SR's `test_upstream_contract.py -m integration` passes against live MySQL.
