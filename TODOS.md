# TODOS.md

Backlog for this repo. Working agreement going forward:

> **Every new Lambda function ships with a test file. Every change to an existing function adds or updates a test for the path it touches.** The retroactive backlog below covers the handlers that predate this rule — they are unblocked work, not a prerequisite for new work.

Status: `TODO` · `WIP` · `DONE` · `BLOCKED`

---

## 0. Test scaffolding — do this first

Nothing below can start until the harness exists. There is currently no `pytest`, no `conftest.py`, no CI.

| # | Item | Status |
|---|---|---|
| 0.1 | Add `requirements-dev.txt`: `pytest`, `pytest-mock`, `freezegun`, `responses` (kept out of `requirements_cron.txt` so the Lambda layer stays lean) | **DONE** 2026-08-01 |
| 0.2 | Add `pytest.ini` at repo root: `testpaths = tests`, `pythonpath = Ops/fin-cron-data`, markers `unit` / `integration` | **DONE** 2026-08-01 |
| 0.3 | Create `tests/unit/` + `tests/integration/` + `tests/conftest.py` | **DONE** 2026-08-01 |
| 0.4 | `conftest.py` fixtures: `env` (monkeypatch all `DB*`/`TBL*`/`PROD_LIST_DIR` vars), `reset_dbconn` (autouse — clear `dataUtil.dbconn` between tests), `mock_engine`, `chdir_handler_dir`, `frozen_ny_time` | **DONE** 2026-08-01 — also added `sqlite_engine`, `unset_env`, `no_network`, and the payload fixtures |
| 0.5 | `tests/fixtures/` — saved yfinance response frames, a saved Fed H.15 HTML page, small symbol CSVs, and a golden `stock_exchange.csv` / `Exchange_timezone.csv` subset | WIP — directory exists with the four `portAssetsHandler` source payloads (Wikipedia S&P 500 + NDX, Nasdaq API JSON, SSGA SPY xlsx). The yfinance frames, H.15 page and symbol CSVs are still TODO |
| 0.6 | Decide the network policy and enforce it: no unit test may hit yfinance, MySQL, S3/R2, or the DDS socket. Add an autouse fixture that fails the test if a real socket is opened | **DONE** 2026-08-01 — autouse `no_network` fixture raises `NetworkAccessAttempted`; `@pytest.mark.integration` is exempt |
| 0.7 | Document how to run tests (`pytest tests/unit/ -v`) in `CLAUDE.md` and `doc/OPERATIONS.md` | **DONE** 2026-08-01 |

> The HTML fixtures are trimmed to their `<table>` elements with attributes
> stripped (2.1 MB → 241 KB) so the repo stays small while the
> table-selection logic remains under test.

**Known obstacles to solve in 0.4** — these are why the suite doesn't exist yet:

- **Module-level side effects.** `load_dotenv()` runs at import in every handler, and `eoddata_minhandler_*.py` and `yf-news-collect.py` read `environ` into module constants at import time. Tests must set env *before* import, or `importlib.reload()` after monkeypatching.
- **Flat imports.** Handlers do `import dataUtil as DU` with no package, so `pythonpath` must include `Ops/fin-cron-data` (0.2).
- **Relative CSV reads.** `eoddata_minhandler_*.py` reads `intra_blacklist.csv` from the CWD. Tests need the `chdir_handler_dir` fixture or a monkeypatched path.
- **Global engine singleton.** `dataUtil.dbconn` persists across tests; without the autouse reset a mocked engine leaks into the next test.
- **`yf-news-collect.py` has a hyphen in its name** — not importable via `import yf-news-collect`. Use `importlib.import_module` / `SourceFileLoader`, or rename the module (a rename means updating `serverless.yml`).

---

## 1. `dataUtil.py` — highest leverage, do before the handlers

Every handler routes through it, so these tests protect all of them at once.

| # | Target | What to cover | Status |
|---|---|---|---|
| 1.1 | `get_DBengine()` | Builds the correct `mysql+pymysql://` URL from env; caches the singleton; behavior when a var is missing (currently interpolates `None` into the URL) | TODO |
| 1.2 | `load_symbols()` | CSV path resolution under `PROD_LIST_DIR`; the `"system"` branch routing to `current_symbols_V3`; sorted + de-duplicated output | TODO |
| 1.3 | `get_Max_date` / `get_Max_datetime` / `get_Max_Options_date` | Correct SQL emitted with and without a symbol; `None` returned on an empty table | TODO |
| 1.4 | `StoreEOD()` | Calls `to_sql` with `if_exists='append'` and the right schema/table; empty-DataFrame path | TODO |
| 1.5 | `get_Last_Date_by_Sym` / `get_Last_Datetime_by_Sym` | `FIRSTTRAINDTE` fallback when the table is empty; `+1 day` increment otherwise | TODO |
| 1.6 | `load_symbols_dict` / `load_exchange_tz` | CSV → dict mapping; missing-file path | TODO |
| 1.7 | Error-swallowing contract | Assert the documented behavior: these functions log and return `None` rather than raising. Pin it so a future refactor is a conscious decision | **PARTIAL** 2026-08-01 — pinned for `ExecSQL` (invalid SQL and a dead engine both log and return `None`). The other functions are still unpinned |
| 1.8 | Decide what to do about the **three forks** of this file (`Ops/fin-cron-data/`, `Dev/fin-cron-data/`, `Ops/fin-cron-Pgsql/dataUtil_Pgsql.py`). Test the Ops one; decide whether the Dev fork is worth keeping in sync | TODO — 2026-08-01: a **fourth** fork (`dataUtil_313.py` for the python3.13 function) was considered and **rejected**. The only 3.13-incompatible surface was `ExecSQL`, fixed in place, so the single file now runs on Python 3.8→3.13 and SQLAlchemy 1.4→2.0. Still three forks; consolidation remains open |
| 1.9 | `ExecSQL` / SQLAlchemy 2.0 | **DONE** 2026-08-01 — `Engine.execute()` (removed in 2.0) replaced with `Engine.begin()` + `text()`; now returns the rowcount. Covered by `tests/unit/test_dataUtil.py` D1–D5, three live-handler dry runs, and a delete-then-append cycle against a non-production MySQL schema. **This unblocks §5.2** |

---

## 2. Per-handler backlog

All nine deployed functions currently have zero tests. Ordered by risk × how much pure logic is extractable.

| # | Handler | Pure logic worth unit-testing | Status |
|---|---|---|---|
| 2.1 | `eoddata_minhandler_us.py` | `check_exchange_from_ticker()`, `yf_exchange_code()` fallback, `yf_get_max_datetime()` 59-day fallback, blacklist subtraction, tz localize/convert, the `(Datetime > sdatetime) & (<= edatetime)` window filter, and the `len(sDF) > 100 and dbFlag` branch that decides per-symbol vs. batched writes | TODO |
| 2.2 | `eoddata_minhandler_asia.py` | Near-identical to 2.1 — differs only in `load_asia_symbols()`. **Decide first:** extract the shared body into one module and test once, or duplicate the tests. Extraction is preferred but touches deployed code | TODO |
| 2.3 | `opt_handler.py` | `keyformat()`, expired-contract filtering (`today > expiration`), `KEY` de-duplication, the dtype-coercion block, and the empty-`option_chain` path | TODO |
| 2.4 | `fxeod_handler.py` | The **5 PM rollover rule** (`current_time < today5PM` → use previous day) including the boundary, watermark start-date selection vs. `FIRSTTRAINDTE`, ticker → `target_cur` split on `=`, and the empty-result path | TODO |
| 2.5 | `handler.py` | `run()`'s 09:30–16:00 NY-time branch (use `freezegun`; assert both sides), and `yf_stk_run()`'s yfinance-`info` → market-row mapping including missing fields. Note `stk_run()` (DDS path) is dead code — decide delete vs. test | TODO |
| 2.6 | `fx_handler.py` | `fetch_exchange_rates()` reshaping, `ffill().iloc[-1]` last-row selection, `FX_TICKERS` parsing via `ast.literal_eval` (and its failure mode when the var is absent or malformed) | TODO |
| 2.7 | `usrate_handler.py` | `HTML2DataFrame()` against a **saved H.15 HTML fixture** — this is a scraper, so it will break silently when the Fed changes their page; a fixture-based test is the only early warning | TODO |
| 2.8 | `yf-news-collect.py` | `df_to_serializable()`, S3/R2 key construction, `read_progress`/`save_progress` batch resumption (`BATCH_SIZE`), and `fetch_full_text()` HTML extraction. Mock `boto3` with `moto` or a stub — never a live bucket | TODO |
| 2.9 | `fff_handler.py` | Max-date filtering of new factor rows. **See 4.1 — this handler is currently broken; fix before testing** | TODO |
| 2.10 | `DDSClient.py` | `convertRecord()` field-code → name mapping, malformed/short message handling. Pure string parsing, no socket needed | TODO |
| 2.11 | `port_assets_handler.py` | **DONE** 2026-08-01 — `tests/unit/test_port_assets_handler.py`, 87 tests (T1–T14). Shipped with the handler, per the working agreement |

---

## 6. Documentation backfill

`HISTORY.md` and the four `doc/*.md` files were created 2026-08-01 with the
first change under the mandatory-rules regime. They were deliberately scoped to
that change.

| # | Item | Status |
|---|---|---|
| 6.1 | `doc/TECHNICAL-DESIGN.md` — the nine pre-existing handlers have a one-line row each. Backfill a deep-dive section per handler | TODO |
| 6.2 | `doc/PRODUCT-GUIDE.md` — only the index-membership dataset is documented in full; the other ten catalogue entries are one-liners | TODO |
| 6.3 | `doc/API-REFERENCE.md` — the S3/R2 key layout (§5) is unaudited, and the older handlers' event contracts are listed but not explained | TODO |
| 6.4 | `doc/OPERATIONS.md` — the Known Incidents section is empty. Populate it retroactively from CloudWatch if any past incident is worth recording | TODO |

---

## 3. Non-unit verification to automate

| # | Item | Status |
|---|---|---|
| 3.1 | Config test: `serverless print` parses, every `handler:` target resolves to a real file, and cron expressions are valid | TODO |
| 3.2 | Config test: each handler's UTC schedule actually covers the exchange-local window it assumes (the US/Asia intraday split is the trap) | TODO |
| 3.3 | Env completeness test: every `environ.get("X")` in the codebase has `X` present in the corresponding `.env` — catches the silent `None`-into-SQL failure mode | TODO |
| 3.4 | Import test: every deployed handler imports cleanly under Python 3.10 with the **pinned layer versions**, not the local venv | TODO |
| 3.5 | Golden-CSV regression: dry-run output diffed against the committed CSVs for row count, column set, and dtypes | TODO — done manually for `handler.py`, `opt_handler.py`, `fx_handler.py` and `port_assets_handler.py` on 2026-08-01, but not yet automated. Note the CSVs in `Ops/fin-cron-data/` are **untracked**, so there is no committed baseline to diff against — commit them or the check has nothing to compare |
| 3.6 | Create `.env.example` from the discovered key list — there is currently no committed template, so 3.3 has nothing to check against | **DONE** 2026-08-01 — `.env.example` at the repo root, covering all 39 existing keys plus the 8 new `portAssetsHandler` ones |
| 3.7 | Serverless Framework 3.38 does not know `python3.13` — it warns and passes the value through, which is fine, but `configValidationMode: error` can never be enabled while a 3.13 function exists. Decide whether to upgrade the framework or keep validation off permanently | TODO |

---

## 4. Bugs found while surveying — fix alongside the tests

| # | Finding | Status |
|---|---|---|
| 4.1 | **`fff_handler.py` is broken and cannot run.** Line 15 calls `datetime.now()` and `pytz.timezone()`, but the module imports only `datetime as dt` and never imports `pytz` → `NameError` on every monthly invocation. Separately, `getFamaFrenchFactors` (line 1) is not in `requirements_cron.txt`, so the import likely fails first. Verify against CloudWatch whether `fffHandler` has ever succeeded, then fix and cover with 2.9 | TODO |
| 4.2 | Dead-code decision: `handler.stk_run()`, `yfin_handler.py`, `yfineod_handler.py`, `yfin_opt.py`, `FOC_data.py`, `opt_ibapi.py` are not in `serverless.yml`. Delete or document before writing tests for them — do not test dead code | TODO |
| 4.3 | `handler.stk_run()` and `fx_handler.fx_run()` both call `.strftime()` on an already-formatted string in their no-`NYTIME` branch → `AttributeError`. Currently unreachable because `run()` always sets `NYTIME`, but it is a trap for anyone invoking the inner function directly (including a test) | TODO |
| 4.4 | `Ops/fin-cron-Pgsql/dataUtil_Pgsql.py` hardcodes `/Users/huangjunyi/...` as its dotenv path and reads a different key set (`RHOST`/`DB`/`PORT`). That folder cannot run or be tested in this environment until it is fixed | BLOCKED |

---

## 5. Runtime upgrade — deadline-driven

**AWS Lambda drops support for `python3.10` before October 2026.** Every function in this repo runs on it. Once the runtime is deprecated, existing functions keep executing for a grace period but **can no longer be updated or redeployed** — so this blocks all future deploys, not just new work. Confirm the exact block-update and block-create dates on the AWS Lambda runtime deprecation page before planning the window; the dates below are the constraint as understood today, not a quote from AWS.

| # | Item | Status |
|---|---|---|
| 5.1 | Confirm AWS's actual `python3.10` deprecation dates (block-update vs. block-create) and pick a target runtime. `python3.12` is the sensible target; `python3.11` is the smaller hop if the pinned deps resist | TODO |
| 5.2 | Verify the pinned layer deps on the target runtime — this is the real work, not the `runtime:` line. `pandas==1.5.3` predates 3.12 and has no cp312 wheels, so a 3.12 move forces a **pandas 2.x bump**, which changes `append`/dtype/timezone behavior this codebase leans on heavily. ~~`SQLAlchemy==1.4.46` also needs checking: the 2.0 idiom shift would break `engine.execute()`, used throughout `dataUtil.py`~~ | **PARTIALLY UNBLOCKED** 2026-08-01 — the SQLAlchemy half is done (§1.9); `dataUtil.py` now runs unmodified on 1.4 and 2.0, verified by running the unit suite under both. `finPort313` proves the whole 3.13 dependency set resolves (pandas 2.2.3 / numpy 2.1.3 / SQLAlchemy 2.0.36). The remaining work is the **pandas 1.5→2.x** behaviour audit inside the nine handlers |
| 5.3 | Update `runtime:` in **both** `Ops/fin-cron-data/serverless.yml` and `Ops/fin-cron-Pgsql/serverless.yml` | TODO |
| 5.4 | Update the layer build path in `Makefile` — every target hardcodes `python/lib/python3.10/site-packages`. A layer built for 3.10 will not be importable by a 3.12 function | TODO — the `finPort313.zip` target added 2026-08-01 is the worked example: correct site-packages path, explicit `--platform`/`--python-version`/`--only-binary`, and `--no-compile` (without it pip ships cp310 `.pyc` files and the zip grows past Lambda's 50 MB direct-upload limit). Copy that pattern for the other targets |
| 5.5 | Rebuild and re-upload every layer (`finCron` first; `finWebLib`/`finSvrLib`/`finVisLib`/`finDataLib` if still in use), then redeploy all nine functions | TODO |
| 5.6 | `buildspec.yml` pins `python: 3.8`, which is **already** past EOL. Decide whether the legacy CodeBuild path (`lambda_function.py`, `eod_usrate.py`, `fin-Lambda-fun`) is still live — retire it if not, rather than migrating it | TODO |
| 5.7 | Re-run the full dry-run set on the new runtime and diff against the golden CSVs (§3.5). A pandas 2.x bump is exactly the kind of change that silently alters dtypes and timezone handling in stored data | TODO |
| 5.8 | Update the pinned versions listed in `CLAUDE.md` (*Build & deploy* and §3.4's import check) once the target is settled | TODO |

> The test suite is the safety net for this migration. §3.4 (import check under pinned versions) and §3.5 (golden-CSV diff) are what turn a pandas 2.x bump from a gamble into a verifiable change — worth having in place *before* 5.2, not after.

---

## Suggested order

1. Section 0 (scaffolding) — one sitting, unblocks everything.
2. 1.1–1.7 (`dataUtil`) — largest coverage gain per test written.
3. 4.1 (`fff_handler` bug) — a live scheduled function that appears to be failing silently.
4. 5.1 — cheap to answer, and the date determines whether section 5 preempts everything below it.
5. 2.1 → 2.4 → 2.3 (intraday, FX EOD, options) — the handlers with real date/timezone logic, where a silent bug corrupts stored history.
6. 3.3 + 3.6 (env completeness + `.env.example`) — cheap, catches a whole failure class.
7. 3.4 + 3.5, then the rest of section 5 — get the import and golden-CSV checks in place, then do the runtime move behind them.
8. Remaining handlers as they are next touched, per the working agreement at the top.
