# HISTORY.md

Change log for Fin-Lambda. Every code, configuration or architectural change is
recorded here before it is considered complete, per `CLAUDE.md`.

Newest first.

---

## 2026-10-02 — statusReport: R2 JSON archive removed, e-mail only

### Goal

`statusReport` delivered the same report twice: as an SNS e-mail and as
`status/latest.json` in Cloudflare R2. The R2 copy was redundant — the report is
a formatted projection of `GlobalMarketData.load_audit` (via `v_load_status`),
which already holds every figure in it, keyed and queryable for any past day,
while the JSON file was overwritten each run and so kept no history at all. It
also made the only non-collector function depend on four R2 env vars and on a
third-party endpoint being reachable, for no data that was not already in MySQL.
Dropping it leaves `load_audit` as the single source of record for load status.

### Implementation detail

`Ops/fin-deep-data/status_report_handler.py`:

- Deleted `put_r2_json()` and `to_json()` (the latter existed only to build the
  R2 payload) and the now-unused `import json`.
- `run()` returns `{..., sns}` instead of `{..., sns, r2}`, and its delivery
  block no longer has the second `try/except`. `localrun` / `dbFlag=False` still
  mean print-only; the only remaining side effect is the SNS publish.
- Module and `run()` docstrings say so, and name `load_audit` as the durable copy.
- `__main__` now passes `{"localrun": True, "dbFlag": False}` instead of
  `{"localrun": False}`. The old event set `send=True`, so a local "dry run"
  would publish SNS whenever `STATUS_TOPIC_ARN` was exported and did attempt the
  R2 upload — the same class of footgun as the 2026-10-01 `port_assets_handler`
  incident (`doc/OPERATIONS.md` §8). This is the last `fin-deep-data` `__main__`
  that was not already an off-switch.

Configuration: `STATUS_R2_KEY` removed from `Ops/fin-deep-data/.env.example`
(with a comment recording why) and from the local `.env`, so it stops being
pushed into the Lambda environment by `serverless-dotenv-plugin`. The
`statusReport` comment in `serverless.yml` no longer advertises R2 JSON. No IAM
change: R2 is reached with access keys, never the execution role, so the only
statement that function needs is the existing `sns:Publish`. `UPSTREAM_R2_BUCKET`
and the three `R2_*` credentials stay — `optChainEOD` still archives raw chains.

### Related files

- `Ops/fin-deep-data/status_report_handler.py`
- `Ops/fin-deep-data/tests/unit/test_status_report_handler.py`
- `Ops/fin-deep-data/tests/conftest.py`
- `Ops/fin-deep-data/.env.example`, `Ops/fin-deep-data/serverless.yml`
- `doc/TECHNICAL-DESIGN.md`, `doc/OPERATIONS.md`, `doc/PRODUCT-GUIDE.md`,
  `doc/API-REFERENCE.md`, `CLAUDE.md`, `PLAN-SR-UPSTREAM.md`

### Test coverage

Removed, and announced as obsolete:

- `test_to_json_is_serialisable` — `to_json()` no longer exists.
- The `put_r2_json` stub in `test_run_sends_sns_and_r2` (renamed
  `test_run_sends_sns`) and in `test_run_delivery_failures_are_logged_not_raised`,
  along with their `out["r2"]` assertions. That test's payload assertion
  (`'"asof": "2026-09-25"' in sent["payload"]`) is replaced by one on the e-mail
  body, which is now the only delivered artefact.
- `STATUS_R2_KEY` from the `env` fixture in `tests/conftest.py`.
- `doc/OPERATIONS.md` §9's `statusReport returns r2: null` troubleshooting row is
  marked impossible rather than deleted: seeing it now means a pre-2026-10-02
  package is still deployed.

Added:

- `test_run_has_no_r2_upload_path` — asserts neither `put_r2_json` nor `to_json`
  is reachable on the module and that a sending run's return value carries no
  `r2` key, so the upload cannot be reintroduced unnoticed.
- `test_run_combines_shards_and_sweep` and the SNS test now assert
  `"r2" not in out`, pinning the new return contract.

Verification, all in `venv-py313` from `Ops/fin-deep-data`:

- `tests/unit/test_status_report_handler.py` — 23 passed (was 23: one test
  removed, one added). Full root suite **320 passed, 1 skipped**, unchanged.
- Warning gate clean: `-W error::FutureWarning:status_report_handler
  -W error::DeprecationWarning:status_report_handler` — 23 passed.
- `py_compile` and a clean `import` from the service folder.
- `__main__` dry run against live MySQL: read `v_load_status` plus six
  `MAX(date)` queries, printed nine lines (`OptionChains_shadow` ok 810/810,
  `histdailyprice7_shadow` ok 837/837, `corp_action_daily` ok, six inferred,
  `USRates` stale), sent nothing, wrote nothing.
- `serverless print` succeeds (only the known `python3.13` validation warnings)
  and `STATUS_R2_KEY` no longer appears in the rendered environment.

No golden-CSV diff applies: `statusReport` writes no CSV.

### Follow-up for the deploy

`serverless deploy function -f statusReport` is enough — no layer rebuild, since
nothing in `finDeepCore` changed. Until that redeploy the live package still
carries the R2 path, which is harmless (`UPSTREAM_R2_BUCKET` is unset, so it
logs one error per run). The schedule remains `enabled: false`.

---

## 2026-10-02 — PLAN-SR-UPSTREAM Phase D: the report body and its cases written down

### Goal

Phase D's six bullets described `statusReport` by naming its functions and
deferring its actual output to SR tech doc §4.5.7 in another repository
(`~/projects/Support-Resistance-Agent`). Anyone reading the plan alone could not
tell what the report looks like, which column comes from where, or when a line
reads `partial` rather than `stale`. Documentation only — no code, config or
schedule changed.

### Implementation detail

Two subsections added to `PLAN-SR-UPSTREAM.md` after the existing Phase D
bullets, which are left as they stand:

- **D.1 The report body** — SR §4.5.7's five-line example reproduced in full, the
  `HEADER`/`WIDTHS` geometry, the e-mail subject and the indented per-error
  lines, the `(table_name, job)` grain and its link to P4's primary-key
  deviation, and a per-column source table. Records that `Symbols` counts
  **distinct** symbols over the day's `segment <> 'summary'` rows rather than
  summing shard counters, which is what makes `partial` meaningful under Phase
  C's fan-out, and the U10 fallback to summed `n_ok`/`n_expected`.
- **D.2 The cases** — audited vs. inferred lines as a two-column table;
  `INFERRED_TABLES` as the six production tables and why that makes the report
  readable during the U5 shadow run; the five status outcomes in their
  `error > stale > partial > ok` precedence plus `—`; `EVENT_TABLES`
  (`corp_action_daily`, `portfolio_assets_info`) being judged on last *run*
  because a quiet day legitimately writes zero rows; holidays deliberately not
  modelled; and that SNS and R2 fail independently while an unreadable
  `v_load_status` still sends a report.

The `format_report()` bullet now points at D.1/D.2 instead of at §4.5.7 alone.

`EVENT_TABLES` and the `—` status were in the handler but in no plan or doc
file; D.2 is the first written record of either.

### Related files

- `PLAN-SR-UPSTREAM.md` — Phase D, new §D.1 and §D.2 (+91 lines)

### Test coverage

No verification added or removed: no module, event contract, env var or table
changed. The content was read off
`Ops/fin-deep-data/status_report_handler.py` (`HEADER`/`WIDTHS`,
`INFERRED_TABLES`, `EVENT_TABLES`, `status_for`, `aggregate_shards`,
`audited_lines`, `format_line`, `run`) and cross-checked against SR tech doc
§4.5.7 line 1223 and row U9 line 1151; the behaviour it describes is already
covered by `tests/unit/test_status_report_handler.py` (320-test suite, Phase D
rows unchanged). No `doc/*.md` body changed, so no changelog line was due
there — the four canonical docs document the `load_audit`/`v_load_status`
columns and the R2 key, neither of which this touches.

---

## 2026-10-01 — `optChainEOD`: `max_retries` 5 → 2

### Goal

Five attempts per underlying is too many. A yfinance failure here is a
rate-limit or a dead ticker, and neither clears inside a 20 s inline wait, so
attempts 3–5 spent the shard's time budget to fail again.

### Implementation detail

`max_retries = 2` in `Ops/fin-deep-data/optchain_eod_handler.py` (`retry_delay`
unchanged at 5 s). The constant carries the reasoning, and `option_chains`'s
docstring no longer claims the loop matches production's — the attempt count is
now the one deliberate difference from `myFinData@19c8509`.

The two scheduled sweeps (18:40, 19:40 ET) are the real retry: they run 60 and
120 min after the dispatch, by which time a rate limit has reset.

### Measured effect

Full 863-symbol timing dry run, writes off, against the committed
`optchain_timing_2026-10-01.csv` (2,063 s):

| status | n, before → after | total s, before → after | mean s |
|---|---|---|---|
| `error` | 39 → 38 | **914.8 → 238.6** | 23.5 → 6.3 |
| `ok` | 624 → 379 | 1,101.4 → 677.6 | 1.76 → 1.79 |
| `empty` | 200 → 446 | 46.8 → 215.8 | 0.23 → 0.48 |

An errored underlying costs ~6 s instead of ~24 s, and the mean for a symbol
that returns a chain is unchanged — the change touches the failure path only.

**The 1,131.9 s total is not the new `T_total`.** 245 underlyings that returned a
chain in the baseline came back **empty** in this run (`MRVL`, `MS`, `MRSH`,
`HYGH`, `CBAT` among them), and still did when re-requested individually, with no
error — Yahoo serving no expiries to this IP at 23:50 local (04:50 UTC). Adding
them back at the measured 1.79 s mean gives ~1,570 s, so `opt_shards_needed` is
3 on either reading; the run printed 3.

**`OPT_SHARDS` left at 4.** At 3 shards a ~1,570 s run leaves ~523 s per shard
against the 540 s budget — 3 % margin, measured on an anomalous night. Re-measure
at 17:40 ET before dropping it.

### Finding, not changed: the sweeps do not retry `empty`

`dataUtil.missing_for_sweep` counts `status IN ('ok', 'empty')` as done, so a
throttled dispatch run loses those 245 underlyings for the session and reports
`status: ok` with a clean `statusReport`. Narrowing it to re-try `empty` would
re-request the ~200 legitimately empty symbols on every sweep, so it is recorded
rather than changed (`doc/OPERATIONS.md` §10.6, plus a §9 row: watch `n_empty`,
not only `n_error`).

### Related files

- `Ops/fin-deep-data/optchain_eod_handler.py` — `max_retries`, `option_chains` docstring
- `Ops/fin-deep-data/tests/unit/test_optchain_eod_handler.py`
- `doc/OPERATIONS.md` §10.6 + §9, `doc/TECHNICAL-DESIGN.md` §6.2

### Test coverage

**Modified** (320 passing, 1 skipped, `venv-py313`; count unchanged — no new
test file, two rewritten cases):

- `test_option_chains_retries_then_succeeds` — `fail_times` 2 → 1, asserts a
  single 5 s sleep. At `max_retries = 2` the old fixture could no longer succeed.
- `test_option_chains_gives_up_after_five` → `…_after_max_retries` — asserts 2
  calls, one sleep, **and `H.max_retries == 2`**, so the constant cannot drift
  without a test saying so.

**Verification run:** the python3.13 warning gate passes; `py_compile` clean; the
full timing dry run above completed `status: ok`, `n_expected: 863`,
`rows_written: 27,562`.

**Nothing retired.** `retry_delay`, the event contract and every other dry run
are unchanged. No consumer-visible change, so `doc/PRODUCT-GUIDE.md` is untouched
— the sweeps already covered what the inline retries now skip.

---

## 2026-10-01 — `current_symbols_V5`: a symbol list with an `@type` exclusion

### Goal

Move the `fin-deep-data` collectors onto `GlobalMarketData.current_symbols_V5`,
which takes `@type` and subtracts an exclusion list read from
`GlobalMarketData.SymbolMaster`:

| `@type` | excludes | rows (2026-10-01) |
|---|---|---|
| `'a'` (default) | `delisted = 1` | 838 |
| `'o'` | `options = 0 OR delisted = 1` | 814 |

`eodDaily` asks for `'a'`, `optChainEOD` for `'o'` — every symbol V5 drops for
`'o'` is one whose chain request would have come back empty, which is the
cheapest available relief for the shard sizing in `TODOS.md` §2.13. V4 (863 rows,
no exclusions) stays in place and `SYMBOL_PROC_VER` still names it; the flip to
V5 is the owner's, once they have run the DDL.

### Implementation detail

**The procedure** — `Ops/fin-deep-data/sql/current_symbols_V5.sql`, new, for the
owner to run. V4's five-way union verbatim, then one `DELETE … JOIN SymbolMaster`
whose predicate depends on `@type`. Three decisions worth recording:

- **MySQL has no default argument values.** "`@type` default `'a'`" is therefore
  enforced twice — `dataUtil` sends `'a'` unless told otherwise, and the body maps
  `NULL`, `''` and anything other than `'o'` to `'a'`. `CALL …_V5()` with no
  argument is still an error, so the caller always sends one.
- **`'o'` is an exclusion list, not a whitelist.** A symbol with no `SymbolMaster`
  row is kept by both types. 648 of the 863 union symbols are in that position,
  which is why `'o'` only removes 24 symbols beyond `'a'`'s 25. Confirmed as the
  intent; the whitelist reading would return 166 and would drop 38 underlyings
  that `Trading.Stock_Options` / `ETF_Options` actually hold.
- **The column is `options`, not `option`** — see *Root cause* below.

**`dataUtil.load_symbols_db(ver, sym_type=None)`** — sends the argument only for
versions outside `UNTYPED_SYMBOL_PROCS` = {`V1`,`V2`,`V3`,`V4`}, which take none.
A handler can therefore pass its type unconditionally while `SYMBOL_PROC_VER`
still names V4, and the flip to V5 is an `.env` edit with no code change. The set
lists the legacy versions rather than the typed ones so that a future V6 receives
the argument by default instead of silently losing it. New
`dataUtil.symbol_proc_type()` normalises the value and is the only thing
interpolated into the `CALL`, so an event-supplied `symType` never reaches SQL.

**The handlers** — module constants `SYMBOL_TYPE = "a"` (eodDaily) and `"o"`
(optChainEOD), overridable per invocation with `{"symType": …}`. The empty-list
error now names the type it asked for.

**Unrelated bug fixed in passing:** `eod_daily_handler.__main__` passed
`dbFlag: True` while its own comment said `False` — the exact shape of the
2026-10-01 incident in `doc/OPERATIONS.md` §8.2, and it would have written to
`histdailyprice7_shadow` and `load_audit` on any "dry run". Now `False`.

### Root cause (incident found while verifying) — `option` vs `options`

The first dry run failed inside `dataUtil.load_df_SQL`:

```
(1054, "Unknown column 'option' in 'where clause'")
[SQL: call GlobalMarketData.current_symbols_V4]
```

`GlobalMarketData.SymbolMaster` is 215 rows with `Symbol, stock, crypto,
options, brenchmark, fund, delisted`. There is no `option`, so a V4 carrying that
spelling fails on every call; it was repaired on the server the same morning
(`LAST_ALTERED 2026-10-02 05:41:51` UTC, the same evening local time) and now
returns 863. The V5 SQL in this
change uses `options` throughout — the draft it came from had `option`, so the
typo was one `CREATE PROCEDURE` away from being reintroduced.

What is worth keeping is the **failure mode**: `load_df_SQL` logs and returns
`None`, so a handler reports `"symbol list … returned nothing"` with
`n_expected: 0` and `statusReport` shows a 0-row load. Nothing distinguishes a
broken procedure from an empty list. Recorded as `doc/OPERATIONS.md` §8.7 with
the `CALL`-it-by-hand check and two troubleshooting rows (the second for the
`ValueError: unsupported format character` that a literal `%` in a hand-written
`dataUtil` query raises — hit while probing `information_schema`, pre-existing,
not introduced here).

### Related files

- `Ops/fin-deep-data/sql/current_symbols_V5.sql` (new)
- `Ops/fin-deep-data/dataUtil.py` — `load_symbols_db`, `symbol_proc_type`, `UNTYPED_SYMBOL_PROCS`, `SYMBOL_TYPES`
- `Ops/fin-deep-data/eod_daily_handler.py`, `optchain_eod_handler.py`
- `Ops/fin-deep-data/.env`, `.env.example` — `SYMBOL_PROC_VER` comment; the value stays `V4`
- `Ops/fin-deep-data/tests/unit/test_dataUtil.py`, `test_eod_daily_handler.py`, `test_optchain_eod_handler.py`
- `CLAUDE.md`, `doc/TECHNICAL-DESIGN.md`, `doc/API-REFERENCE.md`, `doc/OPERATIONS.md`, `doc/PRODUCT-GUIDE.md`

### Test coverage

**Added** (42 new, 278 → 320 passing, 1 skipped, in `venv-py313`):

- `test_dataUtil.py` — `symbol_proc_type` parametrised over `a`/`o`/`O`/`" a "`/
  `None`/`""`/`"x"`/`"a'; DROP"`; V5 receives `('a')`/`('o')`, and `None` → `('a')`;
  V3/V4/`v4` receive **no** argument even when a type is passed; `V6` does receive
  it; `load_symbols("system", "V5")` sends the `'a'` default. A shared `seen_sql`
  fixture replaces the one-off fake.
- `test_eod_daily_handler.py` / `test_optchain_eod_handler.py` — each asserts the
  `(ver, sym_type)` its handler sends (`'a'` / `'o'`), that `{"symType": …}`
  overrides it, and that an explicit `symbols` list skips the procedure entirely.
  The `wired` fixtures now record the call instead of ignoring its arguments.

**Verification run** (all in `venv-py313`, writes off):

- `py_compile` + clean `import` of the three edited modules from the service
  folder; the python3.13 warning gate (`-W error::FutureWarning:<module>`,
  `-W error::DeprecationWarning:<module>` for all eight modules) passes.
- **The procedure body executed against the live database as the Lambda DB user**
  — statements extracted from the `.sql` file, run into a session temp table:
  `@type='o'` 863 → 814, `@type='a'` 863 → 838, column `Symbol`. This is the
  check the Lambda user can run before the procedure exists; `EXECUTE` on V5
  still has to be granted after the owner creates it.
- `eod_daily_handler.py` `__main__` (`dbFlag: False`, `test: 25`): completes,
  list fetched from V4 with the type correctly withheld, writes the three CSVs.
  The 25-symbol cap takes the head of a sorted list, which is all `.HK`, so
  `rows_written: 0` / `n_empty: 24` — the same shape as before this change. With
  `{"symbols": ["AAPL","MSFT","SPY"]}` the same handler writes 9,435 rows.
- `optchain_eod_handler.run` with `{"dbFlag": False, "test": 3}` and with
  `{"symbols": ["SPY"]}`: 368 rows, `status: ok`.
- **Golden-CSV diff**, `options_eod_2026-10-01.csv` (committed) vs. the SPY dry
  run: column set identical, no all-`NaN` column. One dtype differs —
  `openInterest` `int64` vs the golden's `float64` — which is CSV inference on a
  single-underlying file with no missing values, not a change to what is written.
- `serverless print` from `Ops/fin-deep-data/` succeeds; `SYMBOL_PROC_VER: V4`
  renders.

**Not added.** No test asserts V5's own SQL, because the procedure is server-side
and not in a schema this repo can create; the executed-body run above is the
substitute, and it has to be repeated by the owner after the real `CREATE`
(`doc/OPERATIONS.md` §11.1 step 1).

**Nothing retired.** V4 keeps working and stays the configured version, so every
existing dry run and golden CSV still applies.

---

## 2026-10-01 — first invoke of `fin-deep-data`: broken layers and a read-only CSV path

### Goal

Get a deployed function to actually run. The deploy from the previous entry
succeeded, but the first manual invoke of `eodDaily` failed at import, and fixing
that exposed a second failure behind it. Both were mine; neither touched data.

### Root cause 1 — `strip` corrupted numpy's bundled OpenBLAS

```
[ERROR] Runtime.ImportModuleError: Unable to import module 'eod_daily_handler':
Unable to import required dependencies:
numpy: Error importing numpy: you should not try to import numpy from
        its source directory; ...
```

`build_layers.sh`'s `prune_layer()` ran `strip --strip-unneeded` over every `.so`
to save about 7 MB. That rewrote `numpy.libs/libscipy_openblas64_-ff651d7f.so`
into `ELF load command address/offset not page-aligned`: `auditwheel` patches
those bundled manylinux libraries with a non-standard page alignment that `strip`
does not preserve. numpy reports it as an import-location problem and the real
cause appears only in a chained `Original error was:` line that the Lambda log
does not print — reproduced locally by importing the build tree under
`venv-py313`, which is the same cp313/x86_64 target.

The comment I had written on that line — "keeps the dynamic symbols the loader
needs, so the extensions still import" — was true about symbols and irrelevant to
the failure. It had never been tested by importing the tree.

Why no check caught it: the only layer verification in place compared
`CodeSha256` against the local zip (§10.3.1). The broken layer matched exactly,
because the upload *was* intact. Nothing imported the contents before Lambda did.

### Root cause 2 — `localrun` sent dry-run CSVs to the read-only `/var/task`

With the layers fixed, the invoke downloaded and cleaned bars, then died on
`OSError: [Errno 30] Read-only file system: './eod_daily_2026-10-01.csv'`.

Six handlers each carried their own copy of

```python
if localrun or os.environ.get("AWS_LAMBDA_FUNCTION_NAME") is None:
    return "."
```

where `localrun or` lets the flag override the Lambda check, so a `localrun` dry
run *on Lambda* writes into the read-only bundle. The docstring said "CWD
locally, /tmp on Lambda" — the intent, not the code. `port_assets_handler` had
the same inversion in a different shape, and `intraday_min_handler` had no
directory logic at all, writing bare `30min_{sym}.csv`. This is exactly the
invoke-only verification path the owner was told to use, so it would have failed
on every function in turn.

### Implementation detail

- `Ops/fin-deep-data/build_layers.sh` — the strip pass is deleted, with the
  reason recorded on the spot so it is not reintroduced for 7 MB.
- `Ops/fin-deep-data/dataUtil.py` — new `out_dir(localrun, env_key=None)` and
  `on_lambda()`. On Lambda `out_dir` always returns a path under `/tmp` whatever
  `localrun` says, creates it, and ignores a configured directory outside `/tmp`
  with a warning rather than obeying it into a crash. Locally `localrun` still
  means the CWD, so documented dry-run output locations are unchanged.
- `eod_daily_handler.py`, `fxeod_handler.py`, `optchain_eod_handler.py`,
  `usrate_handler.py`, `port_assets_handler.py` — `_output_dir` now delegates to
  it. `intraday_min_handler.py` — both `to_csv` calls routed through it.
- Layers rebuilt and republished as **v2**; `.env` pins `:2`. v1 of
  `finDeepCore` and `finDeepYf` is unusable and left published so the broken
  hashes stay identifiable.

Sizes grew, because the strip was saving much more than the comment claimed:
core 94 → 101 MB unzipped (27 → 29 zipped), yf 10 → 28 MB (3 → 10 zipped) — the
latter is `curl_cffi`'s bundled libcurl. Web is unchanged in size and differs
only in hash, because lxml and openpyxl ship pre-stripped `.so` files; that is
also the tell for which layers the strip actually altered. Both still pass the
80 MB zip ceiling, and the worst per-function total is core+yf at 129 MB unzipped
against AWS's 250 MB.

No `Ops/fin-cron-data` file changed, and `finCronLib` / `finPortLib` were never
touched — those are built by the `Makefile`, which has no strip step.

### Related files

- `Ops/fin-deep-data/build_layers.sh`, `dataUtil.py`, and the six handlers above
- `Ops/fin-deep-data/.env` — layer ARNs bumped to `:2`
- `doc/OPERATIONS.md` — §8.5, §8.6; §10.3.2 the import test; §7 the H.15 lag;
  layer tables with v2 hashes and the broken v1s; four §9 troubleshooting rows
- `doc/TECHNICAL-DESIGN.md`, `doc/API-REFERENCE.md` — `out_dir` / `on_lambda`
- `Makefile`, `CLAUDE.md`, `PLAN-SR-UPSTREAM.md`, `Ops/fin-deep-data/README.md`
  — corrected layer sizes

### Test coverage

**21 new tests, 278 → 299 passed (1 skipped).**

- `tests/unit/test_dataUtil.py` — 11 tests for `out_dir` / `on_lambda`: CWD for a
  local `localrun`, a configured directory honoured locally, `localrun` beating
  that directory locally, and the invariant that matters — on Lambda the result
  is always under `/tmp`, including a parametrised case where
  `PORT_OUTPUT_DIR` is `.`, `/var/task`, `output` or empty.
- `tests/unit/test_phase_f_handlers.py` — 10 tests, one per handler per
  environment, asserting each `_output_dir` still delegates. The bug was six
  copies of one wrong condition, so the regression risk is a handler going back
  to deciding for itself.

New verification procedure, `doc/OPERATIONS.md` §10.3.2: import-test each build
tree in the layer combinations the functions mount, including
`np.linalg.det` so OpenBLAS is really exercised — a plain `import numpy` is not
enough. This is what a `CodeSha256` match cannot tell you, and it now runs after
every layer build.

Verified on Lambda with writes off after the fix: `eodDaily`
`{"n_expected": 3, "n_ok": 3, "rows_written": 551, "actions_written": 1,
"status": "ok"}`; `statusReport` 7 lines / 1 stale (`histdailyprice7_shadow`,
correctly — its last data is 2026-09-25 from the timing run); `usrateHandlerv2`
and `FXHistHandlerv2` both `rows: 0`, each verified correct rather than assumed:
`FX_histdaily` already held 2026-10-01 from the live python3.10 handler, and the
H.15 page carried only 09-24…09-30 against a 09-30 watermark (now §7).

---

## 2026-10-01 — first deploy of `fin-deep-data`: two CloudFormation failures fixed

### Goal

Deploy the new python3.13 service for the first time. `sls deploy` failed twice
before succeeding; both causes were configuration, not handler code, and both are
now structural rather than "remember to set this".

### Root cause 1 — an empty `STATUS_EMAIL` rejected the whole SNS topic

`CREATE_FAILED: StatusTopic (AWS::SNS::Topic)`, *"Invalid parameter: Endpoint"*.
`StatusTopic` declared the address inline as a `Subscription` entry on the topic,
with `Endpoint: ${env:STATUS_EMAIL}`, and `.env` shipped `STATUS_EMAIL=""`. SNS
rejects a subscription with an empty endpoint, and because the subscription was a
property of the topic, it took the topic — and so the whole stack — down with it.
Who gets the report is a reporting detail and must not be able to block the topic
the eight functions depend on.

Confirmed from the timeline rather than assumed: the topic failed at 22:26:55 UTC
and `.env` was edited at 22:30 UTC, i.e. the owner filled the address in *after*
the failure, so the value really was empty at deploy time.

### Root cause 2 — the account's Lambda concurrency quota is 10

`CREATE_FAILED: OptChainEODLambdaFunction`, *"is not updatable with parameters
provided"* (`NotUpdatable`), with the other seven functions cancelled.
`optChainEOD` carried `reservedConcurrency: ${env:OPT_MAX_PARALLEL, 4}`;
reserving concurrency requires ≥ 100 unreserved executions to remain, and
`aws lambda get-account-settings` reports `ConcurrentExecutions: 10` for this
account. No reservation value is accepted, so this was never a matter of lowering
4. CloudFormation's message names neither concurrency nor the quota.

### Implementation detail

`Ops/fin-deep-data/serverless.yml`, two changes:

1. the e-mail subscription became its own `AWS::SNS::Subscription` resource
   guarded by a CloudFormation condition
   (`HasStatusEmail: Fn::Not[Fn::Equals[${env:STATUS_EMAIL, ''}, '']]`), and
   `StatusTopic` now carries no subscription properties. An empty value deploys a
   working topic and no subscription; setting the value and redeploying adds one.
   An `${env:...}` lookup is safe here, unlike in `enabled:` — `Fn::Equals`
   compares the string exactly, and a wrong address surfaces as a never-confirmed
   subscription, not as a silently armed schedule.
2. `reservedConcurrency` is commented out, with the consequence written on the
   line: nothing now caps parallel Yahoo traffic, and the `OPT_SHARDS` shards
   draw from the same pool of 10 as the nine live `fin-cron-data` functions, so a
   fan-out can throttle them. `optChainEOD`'s schedules stay `enabled: false`
   until the quota is raised to 1000 and the line is restored — in that order.

No handler code changed, and `Ops/fin-cron-data` was not touched.

### Deployed state, verified against AWS rather than the deploy output

Eight functions, all `python3.13`, with the designed layer sets (core only for
`statusReport`; core+`finDeepYf` for the five downloaders; core+`finDeepWeb` for
`portAssetsHandlerv2` and `usrateHandlerv2`); zips 11–72 KB. Eleven
`AWS::Scheduler::Schedule` entries, **every one `DISABLED`**, every one
`America/New_York`. One e-mail subscription, `PendingConfirmation`.

Also found, reported and deliberately not fixed here: `UPSTREAM_R2_BUCKET` is
still empty. `statusReport` wraps its R2 upload in `try/except`, so the e-mail
still goes out and the run returns `r2: None`; `optChainEOD` uses `env_or` and
only warns, so **the raw chain archive is silently skipped**.

### Related files

- `Ops/fin-deep-data/serverless.yml` — conditional SNS subscription;
  `reservedConcurrency` commented out
- `doc/OPERATIONS.md` — §8.3 and §8.4 (the two incidents, with the quota-raise
  sequence); §8's two earlier incidents numbered 8.1/8.2; §10.4 records the first
  deploy, its live verification commands and the corrected expected-render list;
  four §9 troubleshooting rows
- `TODOS.md` — 2.13, the concurrency quota, marked as blocking `optChainEOD`
- `CLAUDE.md` — deployed-state note; the `optChainEOD` row no longer claims a
  reservation

### Test coverage

No test added or removed: both failures were in CloudFormation rendering, which
the suite does not and should not reach. The verification was instead a
`serverless package` assertion on the rendered template, run **both ways** —
with `STATUS_EMAIL` set (subscription resource present, condition
`Fn::Equals[<address>, '']` false) and with `STATUS_EMAIL=""` forced (condition
`Fn::Equals['', '']`, so CloudFormation skips the resource and the topic creates
clean) — plus, after the deploy, the `aws scheduler get-schedule`,
`aws lambda get-function-configuration` and `aws sns list-subscriptions-by-topic`
checks recorded in §10.4. The existing suite still passes unchanged: **278
passed, 1 skipped**.

---

## 2026-10-01 — `fin-deep-data`: PLAN-SR-UPSTREAM Phases A–F as a new python3.13 service

### Goal

Re-implement PLAN-SR-UPSTREAM Phases A–F under the constraints the owner set
on 2026-10-01:

1. no deployed function or layer of `Ops/fin-cron-data` changes;
2. every new function runs on python3.13;
3. all new files live in a new folder, `Ops/fin-deep-data`;
4. one `serverless.yml` controls all of them, but each Lambda uploads only the
   files it needs;
5. Phase E's handler is named `portAssetsHandlerv2` and eventually replaces the
   old one;
6. a layer must stay inside a size ceiling of roughly 80 MB.

The earlier (2026-09-25) implementation of these phases edited
`Ops/fin-cron-data` in place and shared the `finPort313` layer. Constraint 1
rules that out, so this is a re-implementation in a second Serverless service
rather than a move of those files.

### Root cause

Not a bug fix. One behaviour of the earlier implementation was wrong and is
corrected here: `port_assets_handler.py`'s `__main__` block passed
`{"dbFlag": True}`, so the documented "dry run" wrote to production. See
*Incident* below.

### Implementation detail

**A new service, not a move.** `Ops/fin-deep-data/` is a self-contained
Serverless service (`service: fin-deep-data`) with its own `serverless.yml`,
`.env`, `package.json`, `pytest.ini`, test tree and `dataUtil.py`. Nothing in
`Ops/fin-cron-data/` is touched: `git status` shows no modification there, and
the two services share no file, no layer and no `.env`.

**`dataUtil.py` is a fork, not a copy by reference** (Phase A). Taken from
`Ops/fin-cron-data/dataUtil.py` at `fa1d6ac` and extended with the Phase A
helpers: `append_ignore` (INSERT IGNORE / INSERT OR IGNORE by dialect),
`new_run_id` (hand-rolled 26-char ULID, no new dependency), `run_host`,
`audit_frame` / `audit_run` / `audit_summary`, `shard_symbols`, `time_left_ok`,
`day_start_utc`, `missing_for_sweep`, `require_env` / `env_or`, `utc_now`. The
fork is python3.13 / SQLAlchemy 2.0 / pandas 2.2 only, though the helpers still
use only APIs that exist on SQLAlchemy 1.4 so a later back-port needs no
rewrite. One behavioural change beyond the helpers: `list_dir()` makes the three
CSV loaders default to the module's own directory instead of resolving
`PROD_LIST_DIR` or `"."` against the CWD, because the CSVs are packaged beside
`dataUtil.py` in each function's zip.

**Eight functions, one `serverless.yml`, eight zips.** `package: individually:
true` with a `'!**'` baseline and a per-function allowlist. Measured package
sizes: 11–71 KB each (`usrateHandlerv2` 11 KB, `optChainEOD` 71 KB), each
carrying only the modules and CSVs that function imports or reads.

| Function | Handler | Layers | Schedule (America/New_York) |
|---|---|---|---|
| eodDaily | `eod_daily_handler.run` | core + yf | 18:30 Mon–Fri, sweep 19:00 |
| optChainEOD | `optchain_eod_handler.run` | core + yf | dispatch 17:40, sweeps 18:40 / 19:40 |
| statusReport | `status_report_handler.run` | core | 20:00 Mon–Fri |
| portAssetsHandlerv2 | `port_assets_handler.run` | core + web | 18:30 |
| usrateHandlerv2 | `usrate_handler.run` | core + web | 17:05 |
| FXHistHandlerv2 | `fxeod_handler.run` | core + yf | 17:10 |
| yfus30minEODv2 | `intraday_min_handler.run_us` | core + yf | 20:05 |
| yfasia30minEODv2 | `intraday_min_handler.run_asia` | core + yf | 06:00 |

Every schedule above ships **disabled**; the times are when each fires once
enabled.

**All eleven schedules ship `enabled: false`** (owner's call, 2026-10-01): a
deploy creates the eight functions and runs nothing, each is verified by manual
invoke, then enabled on its own. The five v2 functions carry a second condition —
each writes a table its still-live python3.10 counterpart writes, so enabling one
before the old function is retired would put two writers on one table; that is
the per-data-set cutover runbook in `doc/OPERATIONS.md` §11.2. Phases B–D's own
data sets have no counterpart, so they are the three safe ones to enable first.

`enabled` is a **literal** in `serverless.yml`, never an `${env:...}` lookup.
Measured 2026-10-01 against `serverless package`: Serverless honours only the
exact strings `false` and `true`, while an empty value, `0`, `yes` and `True`
(capital T) all render `State: ENABLED`. The sole signal is a `must be boolean`
config-validation warning, which this service suppresses because `python3.13`
already warns and `configValidationMode: error` would block every deploy — so a
one-character slip in `.env` would silently arm a schedule. A literal also makes
each enablement a reviewable one-line diff.

**Three layers instead of one** (constraint 6). `finPort313` is 164 MB
unzipped / 49.5 MB zipped in one tree. Here the dependency set is split and
de-duplicated by `Ops/fin-deep-data/build_layers.sh`:

| Layer | Contents | Unzipped | Zipped | Mounted on |
|---|---|---|---|---|
| finDeepCore | pandas, numpy, SQLAlchemy, PyMySQL, python-dotenv, pytz, requests | 94 MB | 27 MB | all 8 |
| finDeepYf | yfinance 0.2.58 + curl_cffi, peewee, frozendict, multitasking, platformdirs, bs4 | 10 MB | 3 MB | 5 |
| finDeepWeb | lxml, beautifulsoup4, openpyxl | 14 MB | 6 MB | 2 |

The build cross-builds for cp313/manylinux2014_x86_64, resolves the yf and web
trees against `requirements_deep_core.txt` as a constraints file, then deletes
every top-level entry `finDeepCore` already provides — without which yfinance
drags in a second 100 MB copy of pandas/numpy. Pruning `tests/`, `__pycache__`
and unneeded `.so` symbols takes the core tree from 124 MB to 94 MB. The script
fails the build if a zip exceeds `LAYER_ZIP_MAX_MB` (80).

Every zip is well inside the ceiling. The one number above 80 MB is
finDeepCore *unzipped* at 94 MB: pandas (30) + numpy (22) + numpy.libs
openBLAS (23) + tzdata/pytz (6) is the floor for this stack, and removing
openBLAS would break `import numpy`. The AWS limit that matters — 250 MB
unzipped for a function and all its layers — is met with room: the largest
combination is core + web at 108 MB.

**Phases B, C, D** are ports of the 2026-09-25 handlers, unchanged in logic
(including every `# ported from myFinData@19c8509` comment), with the docstrings
and the layer references updated and `__main__` fixed to a true dry run.

**Phase E** keeps the 2026-09-25 DJIA/HSI work — `parse_djia_dia_holdings`
(SSGA DIA holdings; the Wikipedia DJIA page no longer carries a ticker table),
`normalize_hk_symbol`, `parse_hsi_wikipedia`, `stored_currency_rate`, bands
30–30 and 50–110 — and deploys as `portAssetsHandlerv2`. `load_audit.job` stays
`portAssetsHandler`: it names the data set's job, not the Lambda, so the status
report does not grow a second line at the cutover, and `load_audit.host`
records which function wrote the row. The same rule applies to the other four
v2 functions (`usrateHandler`, `FXHistHandler`, `yfus30minEOD`,
`yfasia30minEOD`).

**Phase F could not edit the four python3.10 handlers** (constraint 1), so each
is re-implemented as a v2 copy in this service, with the audit row and three
small corrections:

- `usrate_handler`: `pd.read_html` is given a `StringIO` (a literal HTML string
  is deprecated in pandas 2.1+ and raises in 3.0); the empty-table fallback is
  `dt.datetime(1800,1,1)`, fixing a `NameError` in the original; `{"dbFlag":
  false}` writes `USrates_<date>.csv` instead of the table, which the original
  had no way to do.
- `fxeod_handler`: `localrun` comes from the event instead of a module global
  only `__main__` could set.
- `intraday_min_handler`: one module replaces the near-identical
  `eoddata_minhandler_us.py` / `_asia.py` pair — they differed only in a stored
  procedure name and the audit job name, so the market is a parameter
  (`MARKETS`) and the two Lambdas point at `run_us` / `run_asia`. A symbol whose
  exchange is missing from `Exchange_timezone.csv` is now skipped with a
  warning; the originals indexed the map directly, so one unmapped exchange
  raised `KeyError` and lost every symbol after it. `reshape_bars` is split out
  as a pure function, `test: N` caps the symbol count for a dry run, and
  `InitialRun` is passed down instead of mutating a module global.

**Tests live with the service.** `Ops/fin-deep-data/pytest.ini` +
`tests/` is a second pytest root, because both folders contain modules named
`dataUtil` and `port_assets_handler` and only one of the two can be on
`sys.path` in a session. The repo-root config collects `tests/` only, so it
never reaches this tree and the existing suite is unaffected.

### Incident — the Phase E dry run wrote to production

Running `python port_assets_handler.py` as a dry run appended four real
membership sets to `Trading.portfolio_assets_info` (SP500 503 rows dated
2026-10-01, NDX100 101 @ 2026-09-30, DJI 30 @ 2026-09-29, HSI 85 @ 2026-10-01)
plus one `load_audit` row, run_id `01M3VGEPHH8WD5ZY34HY8EJ45N`, host `ml3090`.

Root cause: the `__main__` block carried over from 2026-09-25 passed
`{"localrun": True, "dbFlag": True, "test": True}`, while `CLAUDE.md` documents
this handler's dry run as `dbFlag: False`. `dbFlag=True` is the writing path,
and an only-on-change handler treats a fresh scrape as a change.

Blast radius: additive only — four point-in-time sets and one audit row, no
delete and no overwrite. The data is correct (every sanity gate passed and each
write verified its own row count). Because `current_symbols_V4` includes
`portfolio_assets_info` members, the new DJI/HSI members now also appear in the
list the eod and options jobs collect, which is what Phase E intends, just
earlier than planned.

Fix: `__main__` now passes `dbFlag: False` with a comment saying why, and the
`__main__` block of all eight modules in this service was audited — every one is
a dry run that touches no table. Rollback SQL, if the owner wants the rows gone,
is in `doc/OPERATIONS.md` §8.

### Related files

- New: `Ops/fin-deep-data/` — `serverless.yml`, `dataUtil.py`,
  `eod_daily_handler.py`, `optchain_eod_handler.py`, `status_report_handler.py`,
  `port_assets_handler.py`, `usrate_handler.py`, `fxeod_handler.py`,
  `intraday_min_handler.py`, `build_layers.sh`,
  `requirements_deep_{core,yf,web}.txt`, `.env.example`, `.gitignore`,
  `package.json`, `pytest.ini`, `README.md`, `sql/upstream_tables.sql`,
  `stock_exchange.csv`, `Exchange_timezone.csv`, `intra_blacklist.csv`,
  `tests/conftest.py`, `tests/fixtures/` (7 files), `tests/unit/` (6 files)
- Modified: `Makefile` (three new targets, no existing target touched),
  `CLAUDE.md`, `TODOS.md`, `PLAN-SR-UPSTREAM.md`, `doc/TECHNICAL-DESIGN.md`,
  `doc/OPERATIONS.md`, `doc/API-REFERENCE.md`, `doc/PRODUCT-GUIDE.md`
- Untouched, deliberately: everything under `Ops/fin-cron-data/`, every
  `req_*.txt` and `requirements_cron.txt` / `requirements_port313.txt`,
  `pytest.ini` and `tests/` at the repo root

### Test coverage

**Added** — `Ops/fin-deep-data/tests/unit/`, 278 passed / 1 skipped in the
python3.13 venv (`venv-py313`, pandas 2.2.3 / SQLAlchemy 2.0.36 / numpy 2.1.3 /
yfinance 0.2.58 — the exact layer pins). The one skip is
`test_exec_sql_engine_execute_would_have_warned`, which cannot run on
SQLAlchemy 2.x.

- `test_dataUtil.py` — `ExecSQL` commit semantics on a real sqlite engine, plus
  every Phase A helper: `append_ignore` (counts, first-value-wins, chunking,
  MySQL vs sqlite verb, error swallowing), ULID shape and ordering,
  `audit_frame` defaults / 512-char truncation / int dtypes, `audit_run`
  swallowing its own failure, `shard_symbols`, `time_left_ok`,
  `missing_for_sweep` returning `None` on an unreadable audit table.
- `test_eod_daily_handler.py` — `exchange_for` (CSV / `^HSI` / suffix / blank),
  `session_date`, `start_dates`, `plan_downloads` (batch vs single, tz grouping,
  batch size, window boundary), `prepend_ranges`, `reshape_batch` against a
  saved multi-ticker yfinance frame (NaN drop, column order, `Adj Close`
  rename), `drop_partial_bar` at the 59/60-minute boundary in NY and HK,
  `extract_actions`, audit rows per segment and status including `skipped` via a
  fake context, pinned `yf.download` kwargs, and behaviour when
  `EOD_WRITE_TBL` / `TBLLOADAUDIT` are **absent**.
- `test_optchain_eod_handler.py` — `N_COLUMNS` set and dtypes,
  `filter_opt_chain` at the `lastPrice` and OI-quantile edges, retry-then-empty,
  `Section` / `contractSize` / `inTheMoney`, empty chain, per-underlying write,
  time guard → `skipped`, sweep taking only missing symbols, dispatcher event
  fan-out, `shards_needed`, and the R2 key.
- `test_status_report_handler.py` — status rules including stale-on-Monday and
  the on-change tables, shard aggregation, the `(inferred)` fallback, span
  formatting, and the report layout.
- `test_port_assets_handler.py` — the SP500 / NDX100 / DJIA / HSI parsers
  against saved fixtures, sanity bands, `normalize_hk_symbol`,
  `stored_currency_rate`, only-on-change logic, and the `load_audit` summary row.
- `test_phase_f_handlers.py` — one summary-row test plus failure and
  audit-failure paths for each of `usrateHandlerv2`, `FXHistHandlerv2`,
  `yfus30minEODv2` and `yfasia30minEODv2`; `reshape_rates` (group rows dropped,
  `n.a.` → NaN, float cast); `HTML2DataFrame` raising no pandas deprecation;
  `reshape_bars` (local-naive `Datetime` with `UTCDatetime` kept, window
  filter); the unmapped-exchange skip; `run_us` / `run_asia` market routing; and
  `load_blacklist` not depending on the CWD.

**Python 3.13 policy gate** — the whole suite re-run with
`-W error::FutureWarning:<module> -W error::DeprecationWarning:<module>` for all
eight modules: no warning from our own code. Every module also passes
`py_compile` and a clean `import` from the service folder under python3.13.

**Dry runs** (writes off, against the live MySQL and live sources):

| Handler | Event | Result |
|---|---|---|
| `eod_daily_handler` | `{"localrun": true, "dbFlag": false, "test": 25}` | 25 symbols, 24 ok / 1 error (`0011.HK`, delisted at Yahoo), 72 bars × 9 columns with no nulls across 24 symbols, 5 dividends, 27 audit rows (24 append-ok, 1 first-error, 2 summary) |
| `status_report_handler` | `{"localrun": true}` | 8 lines — 2 audited data sets from the live `v_load_status` + 6 inferred tables; `3 issue(s)` in the subject; no SNS and no R2 |
| `port_assets_handler` | `{"localrun": true, "dbFlag": false, "test": true}` | SP500 503, NDX100 101, DJIA 30, HSI 85 rows; all four sanity gates inside band; four CSVs + the combined 719-row CSV; `action=skip (dbFlag=False)` for every index |
| `usrate_handler` | `{"localrun": true, "dbFlag": false}` | H.15 scraped, 5 dates × 30 instrument rates, 0 rows newer than the stored max, no write |
| `intraday_min_handler` (us, asia) | `{"localrun": true, "dbFlag": false, "test": 5}` | both markets run to completion, 10 per-symbol CSVs, exchange-local `Datetime` correct for NY and HK |
| `fxeod_handler` | `{"localrun": true, "dbFlag": false}` | runs to completion with 0 rows: `FX_histdaily` is already loaded to 2026-09-30 and the local clock is before 17:00 ET, so start > end. Pre-existing semantics of this handler, not a regression — see `doc/OPERATIONS.md` §7 |
| `optchain_eod_handler` | `{"localrun": true, "dbFlag": false}` | full-V4 timing run over all 857 underlyings: **`T_total` 1,948 s (32.5 min), `opt_shards_needed` 4**, confirming the 1,965 s measured on 2026-09-25 and the `OPT_SHARDS=4` already in `.env`. 615 ok / 208 empty / 34 error / 0 skipped; 458,772 raw contract rows filtered to 106,920 written rows, whose column list matches `N_COLUMNS` exactly and carries no repeated header from the per-underlying append. The 34 errors are delisted or no-data tickers and cost 814 s of the total — 25 s each in the ported 5 × 5 s retry loop — so they, not the live underlyings, are the single largest term in the sizing |

**Config validation** — `serverless print` and `serverless package` both pass
from `Ops/fin-deep-data/`. The rendered CloudFormation has eight
`AWS::Lambda::Function` resources on `python3.13`, eleven
`AWS::Scheduler::Schedule` resources all with
`ScheduleExpressionTimezone: America/New_York` and all `State: DISABLED`
(re-verified after the 2026-10-01 change: 11 schedules, 0 enabled),
`ReservedConcurrentExecutions: 4` on `optChainEOD`, the
`lambda:InvokeFunction` and `sns:Publish` statements, and
`STATUS_TOPIC_ARN: {"Ref": "StatusTopic"}` on `statusReport`. The eight package
zips contain exactly the allowlisted files. The `python3.13` validation
*warning* is the known cosmetic one; `configValidationMode: error` stays
commented out.

**Layer validation** — all three layers cross-built and zipped by
`./build_layers.sh`; sizes as tabled above; the dev venv `venv-py313` installs
the same three requirement files and imports pandas, yfinance, SQLAlchemy,
PyMySQL, lxml, openpyxl and bs4 under 3.13.15.

**Tooling** — the layer-publish step needs the AWS CLI, which was not installed
on this machine. Installed user-local (`~/.local/aws-cli`, v2.37.8) and verified:
`aws sts get-caller-identity --profile ServerLessUser` returns
`arn:aws:iam::567575054547:user/ServerLessUser`. `serverless deploy` itself does
not need it — it reads `~/.aws/credentials` directly. A read-only `list-layers`
confirmed the three `finDeep*` names are unused, and turned up a naming
correction now recorded in `doc/OPERATIONS.md` §10.3: the published layers are
`finCronLib` v5 and `finPortLib` v5, not `finCron` / `finPort313` as this repo's
zip and requirements-file names suggest.

**Layers published and verified** (owner published them 2026-10-01 21:36 UTC;
verified here afterwards): `finDeepCore:1`, `finDeepYf:1`, `finDeepWeb:1`, all
`python3.13`. Each layer's `CodeSha256` from `get-layer-version` equals the
base64 SHA-256 of the local zip, so each published layer is byte-identical to
what `build_layers.sh` produced and what the test suite ran against;
`finDeepWeb:1` was additionally downloaded through its presigned URL and is
`cmp`-identical to `finDeepWeb.zip`, with every entry under
`python/lib/python3.13/site-packages/`. The three versioned ARNs are now in
`Ops/fin-deep-data/.env` and render into every function's `layers:` list.
Procedure: `doc/OPERATIONS.md` §10.3.1.

**Not verified here** (owner steps, unchanged from the plan): confirming the SNS
subscription e-mail, the account concurrency headroom check, the golden-CSV
diff against `histdailyprice7` and the droplet's `OptionsChain/` samples, and
the U5 shadow run.

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
