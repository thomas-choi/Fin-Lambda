# OPERATIONS.md

Running, deploying and debugging Fin-Lambda.

> **Scope note.** Created alongside `portAssetsHandler`. Sections marked
> *(all handlers)* are complete; the incident log starts empty because nothing
> has been recorded before now.

## Changelog

- 2026-10-08 | Modified | §10.6 the intraday v2 dry-run row: the `30min_*.csv` files now hold the frame that would be stored — `SAVE_COLUMNS`, `Datetime` exchange-local and tz-naive, `UTCDatetime` with no offset — so the exchange-local storage contract is checkable from a dry run; the per-symbol CSV was previously the raw UTC download. Adds the `downloaded DF` / `in window` pair and the expected zone offsets.
- 2026-10-07 | Added | §8.9 — a `histminprice` watermark older than 60 days made Yahoo reject the whole 15m request, stalling 61 of 803 symbols permanently; a matching §9 row.
- 2026-10-07 | Modified | §10.6 the intraday v2 dry-run row: the closing `collected / stored` log line, and the stale-watermark warning to expect.
- 2026-10-06 | Added | §8.8 — `FXHistHandlerv2`'s download window collapsed to zero width whenever the watermark was the previous day, and Yahoo returns no rows for such a request; a matching §9 row.
- 2026-10-06 | Modified | §7 the `FXHistHandlerv2` 0-row constraint: the cause is the window's `end` being exclusive, and the after-17:00 case was data loss, not expected behaviour. §10.6 the `USD_dailyFX.csv` row no longer calls 0 rows expected — 16 rows, one per ticker, dated the run's own date, is the pass condition.
- 2026-10-06 | Modified | §10.6 the `usrateHandlerv2` dry-run row: `localrun` now prints the rates to stdout, with both the new-rows and the already-current forms, and an empty print named as the failure signal.
- 2026-10-06 | Added | §10.5 a live enable-state note: `eodDaily`, `optChainEOD` and `statusReport` enabled against the `*_shadow` tables (shadow run G1/U5 live), SNS subscription confirmed, `fin-cron-data` `portAssetsHandler` found DISABLED with its v2 also disabled, `optChainEOD`'s 19:40 sweep still DISABLED, and the concurrency quota still 10. Authoritative state table lives in `PLAN-SR-UPSTREAM.md`.
- 2026-10-02 | Deleted | `STATUS_R2_KEY` from the §2 env table and statusReport from the R2 var rows — its R2 JSON upload is removed; §9 troubleshooting row for `r2: null` marked impossible; §10.4 unset-bucket note now concerns optChainEOD only.
- 2026-10-01 | Added | §10.6 the `max_retries` 5 → 2 measurement: an errored underlying costs ~6 s instead of ~24 s, why `OPT_SHARDS` stays 4 for now, and the empty-chain mode the sweeps do not retry.
- 2026-10-01 | Added | §8.7 — a `current_symbols_V4` carrying `option` instead of `options` returned an empty symbol list instead of an error; §11.1 step 1 now covers `sql/current_symbols_V5.sql` and the `SYMBOL_PROC_VER=V5` flip.
- 2026-10-01 | Modified | §10.2 `SYMBOL_PROC_VER` row records V5's `@type` and which handler sends which value; §10.1 the python3.13 suite is 320 tests, not 278.
- 2026-10-01 | Added | §8.5 and §8.6 — two failures found by the first invoke: `strip --strip-unneeded` in `build_layers.sh` corrupting numpy's bundled OpenBLAS (layers rebuilt and republished as v2; v1 of core and yf is unusable), and six handlers writing their dry-run CSVs to the read-only `/var/task` (replaced by `dataUtil.out_dir`). Four matching §9 rows.
- 2026-10-01 | Added | §10.3.2 import-test a layer build before publishing — the check a `CodeSha256` match cannot make, run in the layer combinations each function mounts.
- 2026-10-01 | Modified | §7 the H.15 page lags the `usrateHandlerv2` schedule by a business day, so a 0-row run is normally correct; §10.3/§10.3.1 record layer v2 hashes, the broken v1s and the new sizes (core 101/29 MB, yf 28/10 MB).
- 2026-10-01 | Added | §8.3 and §8.4 — the two failures of the first `fin-deep-data` deploy: an empty `STATUS_EMAIL` rejecting the whole SNS topic (fixed with a CloudFormation `Condition`), and `reservedConcurrency` against an account concurrency quota of 10 (commented out; quota request tracked). Four matching §9 troubleshooting rows.
- 2026-10-01 | Modified | §10.4 records the 2026-10-01 first deploy and how it was verified live; corrects the expected-render list (no `ReservedConcurrentExecutions`, conditional SNS subscription) and notes that `UPSTREAM_R2_BUCKET` is still unset.
- 2026-08-01 | Added | Initial file: environment setup, test harness, env-var table incl. 8 new `portAssetsHandler` keys, `finPort313` layer build/upload, the 3.10-vs-3.13 split, schedules, dry-run procedures, known constraints, troubleshooting.
- 2026-08-02 | Added | §8 Known incidents: first entry — `portAssetsHandler` `Runtime.MarshalError` on a non-JSON return value; matching §9 troubleshooting row.
- 2026-10-01 | Modified | §10.4/§10.5 every schedule in `fin-deep-data` now ships `enabled: false`, not just the five v2 ones; added §10.4.1 bringing one function online (invoke-only verification, enable-together rule, and why `enabled` must not be an env lookup).
- 2026-10-01 | Added | §10.3.1 how to verify a published layer: why the console's S3 location is unreachable, the `CodeSha256`-vs-local-zip check, the presigned-URL download, and the three verified v1 hashes.
- 2026-10-01 | Added | §10.1 AWS CLI v2 as a prerequisite for publishing layers (installed user-local 2026-10-01) with its readiness check; §10.3 the layers actually present in the account, the finCronLib/finPortLib naming correction, and the serverless-managed-layers alternative.
- 2026-10-01 | Added | §10.6 the measured full-V4 options timing run (T_total 1,948 s -> OPT_SHARDS 4) and where the time goes.
- 2026-10-01 | Added | §10 the `fin-deep-data` service: setup, env vars, the three-layer build and size ceiling, deploy, schedules, dry-run procedures and expected output shapes. §11 the per-data-set cutover runbooks (shadow, flip, prepend, archive).
- 2026-10-01 | Modified | §7 Known constraints: two additions — `FXHistHandlerv2` finishing with 0 rows before 17:00 ET, and `append_ignore` counting inserted rather than offered rows. §9 gains five troubleshooting rows for the new service.
- 2026-10-01 | Added | §8 incident — a `port_assets_handler` "dry run" wrote four membership sets to production because its `__main__` passed `dbFlag: True`; rollback SQL included. Matching §9 troubleshooting rows.

---

## 1. Environment setup

```bash
python3 -m venv venv-cron7 && source venv-cron7/bin/activate
pip install -r Ops/fin-cron-data/requirements_cron.txt
pip install -r requirements-dev.txt          # tests only, never deployed
cp .env.example Ops/fin-cron-data/.env       # then fill in real values
```

The venv **must carry the pinned layer versions** — `pandas==1.5.3`,
`SQLAlchemy==1.4.46`, `numpy==1.26.4`, `yfinance==0.2.58` — or a dry run
verifies nothing about what actually runs in Lambda.

### Running the tests

```bash
pytest tests/unit -v          # fast, hermetic; no network, no MySQL, no S3
pytest -m integration         # opt-in, hits live sources
```

`pytest.ini` sets `pythonpath = Ops/fin-cron-data` because handlers use flat
imports (`import dataUtil as DU`) with no package. An autouse fixture fails any
unit test that opens a real socket.

To run the suite against the **python3.13** stack, which is what
`portAssetsHandler` actually executes on:

```bash
make finPort313.zip
docker run --rm -v "$PWD:/repo" -w /repo python:3.13-slim bash -c \
  "pip install -q pytest pytest-mock freezegun && \
   PYTHONPATH=/repo/python/lib/python3.13/site-packages python -m pytest tests/unit -q"
```

---

## 2. Environment variables

Every key is read from `Ops/fin-cron-data/.env`. `serverless.yml` sets
`useDotenv: true` with `serverless-dotenv-plugin`, so these become Lambda
environment variables **at deploy time** — a key added to `.env` after a deploy
is not in production until the next one.

`.env.example` at the repo root is the committed template. Keep it in sync in
the same change that adds, renames or removes a variable.

### New in 2026-08-01 (`portAssetsHandler`)

| Var | Default | Purpose |
|---|---|---|
| `DBTRADING` | **required, no default** | Target schema, e.g. `Trading`. Read with an explicit presence check — the handler raises a named error at startup if it is missing, rather than interpolating `None` into SQL |
| `TBLPORTASSETS` | **required, no default** | `portfolio_assets_info` |
| `SP500_PORT_NAME` | `SP500` | `Port_name` for the S&P 500 set |
| `NDX100_PORT_NAME` | `NDX100` | `Port_name` for the NASDAQ-100 set |
| `PORT_ASSET_CLASS` | `Equity` | `Class` column constant |
| `PORT_ASSET_TYPE` | `Stock` | `Type` column constant |
| `INDEX_CROSSCHECK` | `True` | `False` drops the SPY `.xlsx` cross-check, and with it the `openpyxl` dependency |
| `PORT_OUTPUT_DIR` | `.` locally, `/tmp` on Lambda | Where the verification CSVs are written |

The two required keys are **not yet in `.env`** — add them before the first
real invocation.

### Pre-existing groups *(all handlers)*

- **DB** — `DBHOST`, `DBPORT`, `DBUSER`, `DBPWD`, `DBMKTDATA`, `DBPREDICT`, `DBWEB`
- **Tables** — `TBLDLYPRICE` (**`histdailyprice7`**, not `histdailyprice3`), `TBLMINUTEPRICE`, `TBLSNAPSHOOT`, `TBLFXSNAPSHOT`, `TBLHISTFX`, `TBLUSRATES`, `TBLOPTCHAIN`, `TBLOPTFEATURE`, `TBLWEBPREDICT`, `TBLDAILYOUTPUT`, `TBLDAILYPERF`, `TBLDLYPRED`
- **Lists** — `PROD_LIST_DIR`, `SYMBOLLIST`, `DEFAULT_TICKERS`, `FX_TICKERS` (a Python list literal parsed with `ast.literal_eval`), `FIRSTTRAINDTE`, `LASTTRAINDATE`, `VOL_CON_DIR`
- **Feeds** — `defaultIP`, `defaultPort`, `VENDOR`, `OptDataEngine`, `API_KEY`, `FX_KEY`, `POLYGON_API_KEY`
- **Storage** — `S3_BUCKET`, `R2_ENDPOINT`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`
- **Misc** — `DEBUG` (`debug` raises log level everywhere), `LOCALRUN`, `BATCH_SIZE`

---

## 3. Layer build and upload

Two layers, one per runtime. **They must never be merged or cross-attached.**

| Layer | Runtime | Requirements | Attached to |
|---|---|---|---|
| `finCron` | python3.10 | `Ops/fin-cron-data/requirements_cron.txt` | the nine original functions |
| `finPort313` | python3.13 | `Ops/fin-cron-data/requirements_port313.txt` | `portAssetsHandler` only |

```bash
make finCron.zip          # 3.10 -> python/lib/python3.10/site-packages
make finPort313.zip       # 3.13 -> python/lib/python3.13/site-packages
```

### Why the 3.13 target looks different

It cross-builds instead of resolving against the local interpreter:

```make
pip3 install -r Ops/fin-cron-data/requirements_port313.txt \
    --platform manylinux2014_x86_64 \
    --implementation cp --python-version 3.13 \
    --only-binary=:all: \
    --no-compile \
    -t python/lib/python3.13/site-packages
```

- The local WSL interpreter is **3.10**, so a plain `pip3 install` resolves
  cp310 wheels that a 3.13 Lambda cannot import.
- `--only-binary=:all:` turns a missing cp313 wheel into a build failure rather
  than a silent source build against the wrong Python.
- `--no-compile` is not cosmetic. Without it pip byte-compiles pure-Python
  packages with the *local* 3.10 interpreter and ships 2,507 useless
  `cpython-310.pyc` files: **176 MB → 138 MB unzipped, 52 MB → 39 MB zipped**.
  52 MB exceeds Lambda's **50 MB direct-upload limit** and would force an S3
  round trip.
- The site-packages path is `python3.13`. Every other Makefile target hardcodes
  `python3.10`; a layer built under the wrong path is silently unimportable.

### Measured size

| | zipped | unzipped |
|---|---|---|
| `finPort313` | 39 MB | 138 MB |

Lambda's limit is 250 MB unzipped across all layers plus the function bundle.
If it gets tight, the drop levers in order are `openpyxl` (set
`INDEX_CROSSCHECK=False`) then `beautifulsoup4`.

### Upload

Layers are **not** declared at provider level in `serverless.yml`; they are
published separately and the ARN is referenced per function.

```bash
aws lambda publish-layer-version \
  --layer-name finPort313 \
  --zip-file fileb://finPort313.zip \
  --compatible-runtimes python3.13 \
  --region us-east-2 --profile ServerLessUser
```

Then uncomment the `layers:` block under `portAssetsHandler` in
`serverless.yml` and set the returned version number.

> **A layer change is never verified by a redeploy alone.** After rebuilding,
> confirm the imports resolve under a real interpreter of the target version:
>
> ```bash
> docker run --rm -v "$PWD/python:/opt/python:ro" python:3.13-slim python -c \
>   "import sys; sys.path.insert(0,'/opt/python/lib/python3.13/site-packages'); \
>    import pandas, sqlalchemy, pymysql, lxml, openpyxl, numpy, requests; print('ok')"
> ```

---

## 4. Deploy

```bash
cd Ops/fin-cron-data
npx serverless print                              # validate, no deploy
npx serverless package                            # render CloudFormation
npx serverless deploy                             # all functions
npx serverless deploy function -f portAssetsHandler   # one function, much faster
```

### Serverless does not know `python3.13`

Serverless Framework **3.38.0**'s runtime allowlist stops at `python3.11`, so
`portAssetsHandler` raises:

```
Warning: Invalid configuration encountered
  at 'functions.portAssetsHandler.runtime': must be equal to one of the allowed
  values [... python3.10, python3.11, ruby2.7, ruby3.2]
```

This is **cosmetic**. `serverless package` renders `Runtime: python3.13` into
CloudFormation correctly, and CloudFormation accepts it — verified. But
`configValidationMode: error` **must stay commented out** in `serverless.yml`,
or the warning becomes a hard error and blocks every deploy in the service.

### Before the first `portAssetsHandler` invocation

1. `DBTRADING` and `TBLPORTASSETS` added to `Ops/fin-cron-data/.env`.
2. `finPort313` published and its ARN uncommented in `serverless.yml`.
3. No table DDL is needed — `Trading.portfolio_assets_info` already exists with
   the correct composite primary key.

---

## 5. Schedules and timezones

| Function | Cron (UTC) | Exchange-local intent |
|---|---|---|
| cronHandler | `0/10 13-21` + `0/10 21-00` Mon-Fri | US regular session |
| optHandler | `0/10 12-21` Mon-Fri | US session incl. pre-open |
| yfus30minEOD | `05 00` Tue-Sat | after the US close |
| yfasia30minEOD | `00 10` Mon-Fri | after the Asia close |
| usrateHandler | `1 21` Mon-Fri | after the Fed H.15 update |
| FXrateHandler | hourly | continuous |
| FXHistHandler | `10 21` daily | after the FX day roll |
| fffHandler | `0 0 1 * ?` | monthly |
| yfNewshandler | hourly | continuous |
| **portAssetsHandler** | **`30 22 ? * MON-FRI *`** | **~1.5 h after the US close in EDT, ~2.5 h in EST** |

Index committees announce membership changes to take effect at an open, so a
daily post-close check catches every change within one business day.
Only-on-change makes a no-change day nearly free: two HTTP fetches, one
`SELECT`, exit.

---

## 6. Dry-run procedures

There is no framework-level integration test. Verification means running a
handler locally with DB writes disabled. **The off-switch is not uniform.**

| Handler | Switch | Output |
|---|---|---|
| `handler.py` | `LOCALRUN=localrun` env var | `snapshot_yf.csv` |
| `opt_handler.py` | event `{"localrun": True}`; `{"test": N}` caps iterations | `options_list.csv`, `options_snapshot.csv` |
| `fx_handler.py` | module-global `localrun`, set in `__main__` | `USD_FX.csv` |
| `fxeod_handler.py` | module-global `localrun` | `USD_dailyFX.csv` |
| `eoddata_minhandler_us.py` / `_asia.py` | event `{"localrun": True, "dbFlag": False}` — **`dbFlag=False` is what suppresses writes**; also accepts `{"InitialRun": True}` | `30min_{sym}.csv` |
| `yf-news-collect.py` | `LOCALRUN` env var (defaults `True`), `BATCH_SIZE` | S3/R2 keys |
| **`port_assets_handler.py`** | **event `{"localrun": True, "dbFlag": False, "test": True}` — supplied by its `__main__`** | **`portfolio_assets_info.csv` + per-index files** |

All of them require the CWD to be the handler directory:

```bash
cd Ops/fin-cron-data          # required: flat imports + relative CSV paths
python port_assets_handler.py
```

Expected for `portAssetsHandler`: ~503 SP500 rows, ~103 NDX100 rows, 606
combined, zero stack traces, exit 0.

### Golden-CSV comparison

Diff dry-run output against the committed CSVs in `Ops/fin-cron-data/` for row
count, column set and dtypes. No all-`NaN` columns; no zero-row output inside a
live market window. `portfolio_assets_info.csv` is the reference for
`portAssetsHandler` and was created by this change — there is no earlier
baseline for it.

---

## 7. Known constraints

**`sql_require_primary_key=ON` on the MySQL server.** Any `CREATE TABLE`
without a primary key fails with error 3750. This means `StoreEOD`'s
`to_sql(if_exists='append')` can **never** auto-create a table here — a new
target table must always be created manually with a PK first. Discovered
2026-08-01 while running a delete-then-append probe.

**`StoreEOD` swallows write failures.** It wraps `to_sql` in `try/except` and
only logs, so a failed write reports success. Any new handler should re-read the
row count afterwards; `port_assets_handler.write_set()` is the pattern.

**Snapshot tables are delete-then-append, not upsert.** `handler.py`,
`opt_handler.py` and `fx_handler.py` all `DELETE` before writing, so a bad run
corrupts a production table rather than erroring.

**`pymysql` cannot take a literal `%` in a query string.** `load_df_SQL("… LIKE
'BRK%'")` raises `ValueError: unsupported format character`; escape it as `%%`.

**`FXHistHandlerv2` writes nothing when the table is already current.** Its date
rule takes yesterday's date before 17:00 ET, so if `FX_histdaily` is loaded
through yesterday the window is `start > end`; the handler now logs
`is current through <date>: nothing to fetch` and skips the download instead of
sending 16 requests for an inverted window. The run finishes `ok` with 0 rows.
It is why the function is scheduled at 17:10 ET, past the FX day roll.

Until 2026-10-06 the *same* situation **after** 17:00 ET also produced 0 rows,
and that was data loss rather than a constraint: the window was
`[watermark + 1, mToday]` with `end` exclusive, so it had zero width and Yahoo
returned nothing. Fixed — the window is inclusive of its last date now. See
§8.8.

**The H.15 page lags the `usrateHandlerv2` schedule by a day.** Measured
2026-10-01 at 20:50 ET: the page offered 2026-09-24 … 09-30 only, with no row for
2026-10-01, while `GlobalMarketData.USRates` was already loaded through 09-30. So
the run finishes `ok` with **0 rows** — correct, not a failure. The practical
effect is that each business day's rates are picked up on the *following* run, so
`USRates` normally trails the current date by one business day, and a status
report on a quiet day shows it one day behind. The live python3.10
`usrateHandler` at 21:01 UTC behaves identically, so this is inherited, not a
regression — but it means 17:05 ET is too early to ever catch same-day rates, and
moving the schedule later would only help if the Fed's publication time is the
reason rather than a deliberate next-morning release. Do not "fix" a 0-row
`usrateHandlerv2` run without first checking the dates the page actually carries:

```bash
cd Ops/fin-deep-data && ../../venv-py313/bin/python -c "
from dotenv import load_dotenv; load_dotenv('.env', override=True)
import usrate_handler as U
print([str(d.date()) for d in U.reshape_rates(U.HTML2DataFrame(U.H15_URL))['Date']])"
```

**`append_ignore` reports rows *inserted*, not rows offered.** On a re-run most
rows are skipped by the primary key and the count is legitimately lower than
`len(df)`. `load_audit.n_rows` records what the handler *built*, so the two
differ by design on a replay.

**`Ops/fin-cron-Pgsql/` cannot run here.** `dataUtil_Pgsql.py` hardcodes an
absolute macOS dotenv path and reads a different key set (`RHOST`, `DB`,
`PORT`). Its `serverless.yml` also references handlers that do not exist in the
folder — do not deploy it as-is.

---

## 8. Known incidents

Add an entry here after any production incident — missed or overlapping run,
yfinance rate-limit or schema break, DB connection failure, layer/runtime
mismatch, Lambda timeout — with root cause, blast radius and fix.

### 8.1 2026-08-02 — `portAssetsHandler` invocation failed with `Runtime.MarshalError`

**Symptom.** Request `d2812e85` (09:06 UTC, the first real AWS test run) logged
a complete, healthy run — both indices fetched, both CSVs written to `/tmp`,
both correctly skipped as unchanged — and then died:

```
[ERROR] Runtime.MarshalError: Unable to marshal response:
        Object of type DataFrame is not JSON serializable
```

**Root cause.** `run()` returned the internal summary dicts unchanged. Each one
carried the built DataFrame under `frame` (kept so the combined golden CSV can
be assembled) and a `datetime.date` under `date`. The Lambda runtime
JSON-encodes the handler's return value, and neither type is encodable.

**Blast radius.** Cosmetic but misleading: everything the function exists to do
had already happened before the marshal step, so no data was lost or corrupted.
The cost is that **every invocation is reported as an error** — CloudWatch
error metrics, alarms and any retry policy see a failed function, which would
have masked a genuine failure later. It also fires on the write path, not just
the skip path.

**Fix.** `_jsonable_summary()` strips `frame` and renders `date` as an ISO-8601
string on the way out; `frame` stays on the in-process summary. Covered by six
regression tests in `tests/unit/test_port_assets_handler.py` (§T15) that call
`json.dumps()` on the result of `run()` across the skip, write and
one-index-failed paths.

**Generalisation.** Any handler whose `run()` returns a DataFrame, a `date`, a
`Timestamp`, a `namedtuple` field or a numpy scalar will fail the same way.
Returning `None` or a plain dict of primitives is the safe default.

### 8.2 2026-10-01 — a `port_assets_handler` dry run wrote to production

**Symptom.** `../../venv-py313/bin/python port_assets_handler.py`, run from
`Ops/fin-deep-data/` to verify the new service, appended four membership sets to
`Trading.portfolio_assets_info` and one row to `GlobalMarketData.load_audit`:

| Port_name | Date | Rows |
|---|---|---|
| SP500 | 2026-10-01 | 503 |
| NDX100 | 2026-09-30 | 101 |
| DJI | 2026-09-29 | 30 |
| HSI | 2026-10-01 | 85 |

`load_audit` run_id `01M3VGEPHH8WD5ZY34HY8EJ45N`, host `ml3090`.

**Root cause.** The `__main__` block carried over from the 2026-09-25
implementation passed `{"localrun": True, "dbFlag": True, "test": True}`.
`localrun` only decides *where the CSVs go*; `dbFlag` is what suppresses writes,
and it was `True`. `CLAUDE.md` documents this handler's dry run as
`dbFlag: False`, so the file disagreed with the documentation. Because the
handler is only-on-change and the stored sets were months old, a fresh scrape
counted as a change and all four were written.

**Blast radius.** Additive only: four point-in-time sets and one audit row, no
`DELETE` and no overwrite. The data is correct — every sanity gate passed and
each write verified its own row count. One knock-on effect:
`current_symbols_V4` includes `portfolio_assets_info` members, so the new DJI and
HSI members now also appear in the list `eodDaily` / `optChainEOD` collect. That
is what Phase E intends, just earlier than planned.

**Fix.** `Ops/fin-deep-data/port_assets_handler.py`'s `__main__` now passes
`dbFlag: False`, with a comment saying why, and every `__main__` block in the
service was audited — all eight are dry runs that touch no table. The lesson is
in §6/§10.4: in this repo `localrun` and `dbFlag` are **not** synonyms, and only
`dbFlag=False` (or `{"dbFlag": false}` in the event) suppresses a write.

**Rollback**, if the rows are not wanted. Verify first, then delete:

```sql
SELECT Port_name, Date, count(*) FROM Trading.portfolio_assets_info
WHERE (Port_name, Date) IN (('SP500','2026-10-01'), ('NDX100','2026-09-30'),
                            ('DJI','2026-09-29'), ('HSI','2026-10-01'))
GROUP BY Port_name, Date;                      -- expect 503 / 101 / 30 / 85

DELETE FROM Trading.portfolio_assets_info
WHERE (Port_name, Date) IN (('SP500','2026-10-01'), ('NDX100','2026-09-30'),
                            ('DJI','2026-09-29'), ('HSI','2026-10-01'));

DELETE FROM GlobalMarketData.load_audit
WHERE run_id = '01M3VGEPHH8WD5ZY34HY8EJ45N';
```

Keeping them is also fine: they are a legitimate point-in-time snapshot, and
`DJI`/`HSI` are sets the service is meant to start maintaining.

### 8.3 2026-10-01 — first `fin-deep-data` deploy failed on an empty `STATUS_EMAIL`

**Symptom.** `sls deploy`, first create of the stack:

```
CREATE_FAILED: StatusTopic (AWS::SNS::Topic)
Resource handler returned message: "Invalid parameter: Endpoint
  (Service: Sns, Status Code: 400, ...)" (HandlerErrorCode: InvalidRequest)
```

**Root cause.** `StatusTopic` declared its e-mail inline, as a `Subscription`
entry on the topic itself, with `Endpoint: ${env:STATUS_EMAIL}`. `.env` shipped
`STATUS_EMAIL=""`, so SNS was asked to create a subscription with an empty
endpoint and rejected the **whole topic**. A missing address is a reporting
detail; it should never be able to stop the topic the functions depend on.

**Blast radius.** None beyond the deploy: nothing had been created yet, so
CloudFormation rolled back to `UPDATE_ROLLBACK_COMPLETE` with only
`ServerlessDeploymentBucket` present (that bucket is created in an earlier,
separate phase, which is why the stack already existed). No cleanup and no
`sls remove` were needed — the next `deploy` updated it in place. A stack that
fails on its *very first* create instead lands in `ROLLBACK_COMPLETE` and does
have to be deleted before retrying.

**Fix.** The subscription became its own resource, guarded by a CloudFormation
condition, and the topic now carries no subscription properties at all:

```yaml
resources:
  Conditions:
    HasStatusEmail:
      Fn::Not:
        - Fn::Equals: [ "${env:STATUS_EMAIL, ''}", '' ]
  Resources:
    StatusTopic:
      Type: AWS::SNS::Topic
      Properties:
        TopicName: ${self:service}-${sls:stage}-status
    StatusEmailSubscription:
      Type: AWS::SNS::Subscription
      Condition: HasStatusEmail
      Properties: { TopicArn: !Ref StatusTopic, Protocol: email, Endpoint: "${env:STATUS_EMAIL, ''}" }
```

An empty value now deploys a working topic and no subscription. Setting
`STATUS_EMAIL` and redeploying adds one; clearing it removes the subscription and
leaves the topic. Unlike the `enabled:` switch (§10.4.1), an `${env:...}` lookup
is safe here: `Fn::Equals` compares the string exactly, and a bad address shows
up as a never-confirmed subscription rather than as a silently armed schedule.

### 8.4 2026-10-01 — second deploy failed: account concurrency quota is 10

**Symptom.** With §8.3 fixed, the next `sls deploy`:

```
CREATE_FAILED: OptChainEODLambdaFunction (AWS::Lambda::Function)
Resource handler returned message: "Resource of type 'AWS::Lambda::Function'
  with identifier 'OptChainEODLambdaFunction' is not updatable with parameters
  provided." (HandlerErrorCode: NotUpdatable)
```

and the other seven functions `Resource creation cancelled`.

**Root cause.** `optChainEOD` carried `reservedConcurrency: ${env:OPT_MAX_PARALLEL, 4}`.
Reserving concurrency requires that at least **100** unreserved executions remain
in the account, and this account's total quota is **10**:

```bash
aws lambda get-account-settings --region us-east-2 --profile ServerLessUser \
  --query 'AccountLimit'
# { ... "ConcurrentExecutions": 10, "UnreservedConcurrentExecutions": 10 }
```

So *no* reservation value is accepted, not just 4. CloudFormation reports this as
a generic `NotUpdatable`, which names neither concurrency nor the quota — check
`get-account-settings` whenever a Lambda resource fails that way.

**Blast radius.** Deploy only; rolled back cleanly again.

**Fix (partial — this is a quota request, not a code change).**
`reservedConcurrency` is commented out in `serverless.yml` so the service can
deploy. **This removes the cap on parallel Yahoo traffic**, and the `OPT_SHARDS`
shards then draw from the same pool of 10 as the nine live `fin-cron-data`
functions, so a fan-out can throttle *them* — `optChainEOD` dispatches at 17:40
ET = 21:40 UTC, while `cronHandler` is running its `0/10` 21–00 UTC window.
Dispatcher + 4 shards + `cronHandler` = 6 of 10, which fits, but with no margin
and no protection if a shard runs long.

`optChainEOD`'s three schedules therefore stay `enabled: false` until the quota
is raised and the line is restored — **in that order**:

1. request `ConcurrentExecutions` → 1000 for us-east-2 (Service Quotas, code
   `L-B99A9384`);
2. uncomment `reservedConcurrency` and `serverless deploy`;
3. confirm `ReservedConcurrentExecutions: 4` on the deployed function;
4. only then enable the schedules (§10.4.1).

Tracked as `TODOS.md` 2.13. The other seven functions are unaffected: none
reserves concurrency.

### 8.5 2026-10-01 — `strip` on the layer `.so` files broke numpy

**Symptom.** First invoke of `eodDaily` after a successful deploy:

```
[ERROR] Runtime.ImportModuleError: Unable to import module 'eod_daily_handler':
Unable to import required dependencies:
numpy: Error importing numpy: you should not try to import numpy from
        its source directory; please exit the numpy source tree, and relaunch
        your python interpreter from there.
```

Init took 147 ms and used 47 MB, i.e. it never got as far as the handler.

**Root cause.** `build_layers.sh`'s `prune_layer()` ran
`find … -name '*.so' -exec strip --strip-unneeded {} +` to save about 7 MB. That
rewrote `numpy.libs/libscipy_openblas64_-ff651d7f.so` into a library the loader
rejects. The real error is chained and does **not** appear in the Lambda log:

```
Original error was: libscipy_openblas64_-ff651d7f.so:
  ELF load command address/offset not page-aligned
```

`auditwheel` patches those bundled manylinux libraries with a non-standard page
alignment, and `strip` does not preserve it. numpy's own message blames the
import location and names neither `strip` nor OpenBLAS, so the log alone points
nowhere near the cause. The code comment on that line claimed stripping "keeps
the dynamic symbols the loader needs, so the extensions still import" — true
about symbols, irrelevant to the actual failure, and it had never been tested by
importing the built tree.

Why it was not caught earlier: the only layer check in place compared
`CodeSha256` against the local zip (§10.3.1). The broken layer matched perfectly
— the upload *was* intact. Nothing imported the tree before Lambda did.

**Blast radius.** No data. Every `fin-deep-data` function would have failed at
import; the service was still fully disabled, so only a manual invoke hit it.
`finDeepCore:1` and `finDeepYf:1` are the affected versions. `finCronLib` /
`finPortLib` and the nine live python3.10 functions were never touched — those
layers are built by the `Makefile`, which has no strip step.

**Fix.** The strip pass is removed, with the reason written on the spot so it is
not reintroduced for the 7 MB. Rebuilt and republished as v2; `.env` pins `:2`.
Sizes grew: core 94 → 101 MB unzipped (27 → 29 zipped) and yf 10 → 28 MB
(3 → 10 zipped), the latter because `curl_cffi` bundles a large libcurl — so the
strip was saving far more than the 7 MB the comment claimed. Both still pass the
80 MB zip ceiling, and the worst per-function total is core+yf at 129 MB
unzipped against AWS's 250 MB.

**Prevention.** §10.3.2 import-tests each build tree, in the layer combinations
the functions actually mount, including `np.linalg` so OpenBLAS is really loaded.
Run it after every layer build.

### 8.6 2026-10-01 — a `localrun` invoke wrote to the read-only bundle

**Symptom.** With the layers fixed, the same invoke got through the download and
then died:

```
[ERROR] OSError: [Errno 30] Read-only file system: './eod_daily_2026-10-01.csv'
  File "/var/task/eod_daily_handler.py", line 465, in finish
    bars.to_csv(os.path.join(outdir, f"eod_daily_{asof}.csv"), index=False)
```

**Root cause.** Six handlers each had their own copy of:

```python
if localrun or os.environ.get("AWS_LAMBDA_FUNCTION_NAME") is None:
    return "."
```

The `localrun or` makes the flag override the Lambda check, so a `localrun`
dry run *on Lambda* targets `/var/task`, which is read-only. The docstring
("CWD locally, /tmp on Lambda") described the intent, not the code.
`port_assets_handler` had the same inversion in a different shape
(`if localrun: return "."` before computing `on_lambda`), and
`intraday_min_handler` had no directory logic at all — it wrote bare
`30min_{sym}.csv`.

This is the invoke-only verification path from §10.4.1, so it would have hit
every function the first time anyone verified one by hand, and only that path: a
scheduled run has `localrun` false and writes to the database.

**Blast radius.** None. Writes were off, and the crash is after all downloads —
no partial database state. It cost one invoke per function.

**Fix.** One helper, `dataUtil.out_dir(localrun, env_key=None)`, replaces all six
copies. On Lambda it always returns a path under `/tmp`, whatever `localrun`
says, and a configured directory outside `/tmp` is ignored with a warning rather
than obeyed into a crash. Locally `localrun` still means the CWD, so the
documented dry-run output locations are unchanged. 21 regression tests cover the
matrix, including one per handler asserting it still delegates — the bug was six
copies of one wrong condition, so what needs guarding is that no handler goes
back to deciding for itself.

**Verified after the fix**, on Lambda, writes off: `eodDaily` →
`{"n_expected": 3, "n_ok": 3, "rows_written": 551, "actions_written": 1,
"status": "ok"}`; `statusReport` → 7 lines, 1 stale; `usrateHandlerv2` and
`FXHistHandlerv2` → `rows: 0`, both correct (see §7).

---

### 8.7 2026-10-01 — a broken symbol procedure looks like "no symbols", not an error

**Symptom.** A local `eod_daily_handler` dry run logged, from inside
`dataUtil.load_df_SQL`:

```
sqlalchemy.exc.OperationalError: (pymysql.err.OperationalError)
  (1054, "Unknown column 'option' in 'where clause'")
[SQL: call GlobalMarketData.current_symbols_V4]
...
AttributeError: 'NoneType' object has no attribute 'Symbol'
```

**Root cause.** `current_symbols_V4` had been recreated on the server with a
`SymbolMaster` clause reading `option > 0`. That column is `options` —
`GlobalMarketData.SymbolMaster` is 215 rows with `Symbol, stock, crypto,
options, brenchmark, fund, delisted`, and no `option`. Every call failed. The
procedure was repaired within minutes (`LAST_ALTERED 2026-10-02 05:41:51` UTC,
i.e. the evening of 2026-10-01 locally — the DB server's clock is UTC); it now
returns 863 symbols.

**Why it is worth an entry.** The failure mode, not the typo. `load_df_SQL`
logs and returns `None`, so `load_symbols_db` raises `AttributeError` on
`df.Symbol` — but when a *handler* is the caller the result is indistinguishable
from an empty list: `run()` reports
`"symbol list current_symbols_V4(a) returned nothing"` with `status: error` and
`n_expected: 0`, and `statusReport` shows a 0-row load. Nothing says the
procedure is broken. Both collectors' schedules were `DISABLED`, so this cost
nothing; with them enabled it would have been a silent empty night.

**Fix / check.** None in code — the procedure is the owner's. When a collector
reports a 0-symbol list, check the procedure directly before looking at the
handler:

```sql
CALL GlobalMarketData.current_symbols_V4;        -- expect 863 rows
CALL GlobalMarketData.current_symbols_V5('a');   -- expect 838
CALL GlobalMarketData.current_symbols_V5('o');   -- expect 814
SELECT ROUTINE_NAME, LAST_ALTERED FROM information_schema.ROUTINES
 WHERE ROUTINE_SCHEMA = 'GlobalMarketData'
   AND ROUTINE_NAME IN ('current_symbols_V4', 'current_symbols_V5');
```

`sql/current_symbols_V5.sql` uses `options` throughout and its header records
this incident, so recreating V5 from the file cannot reintroduce the typo.

---

### 8.8 2026-10-06 — an exclusive `end` cost `FX_histdaily` a day

**Symptom.** A `fxeod_handler` dry run collected nothing. All 16 tickers logged

```
AUD=X: yfinance received OHLC data: EMPTY
...
INFO:root:Start_dt = 2026-10-06   ---  end_dt = 2026-10-06
INFO:root:dry run: wrote 0 row(s) to ./USD_dailyFX.csv
```

and `GlobalMarketData.FX_histdaily` stood at `2026-10-05`, with no row for
2026-10-06 although the live 17:10 ET run had already happened.

**Root cause.** `yf.download`'s `end` is exclusive and `fx_run` passed the last
date it wanted as `end`. With the table loaded through yesterday,
`start == end == 2026-10-06`: a zero-width window, which Yahoo answers with no
rows at all. Probed directly, one ticker:

```
[2026-10-05, 2026-10-05] -> 0 rows        [2026-10-06, 2026-10-07] -> 2 rows
[2026-10-06, 2026-10-06] -> 0 rows        (so period2's own bar IS returned,
[2026-10-07, 2026-10-07] -> 0 rows         but only in a non-degenerate range)
```

The window collapsed this way on every run whose previous day already had a bar
— in steady state every Tuesday-to-Friday run. Monday runs were safe because
their watermark is Friday, so the window spans the weekend.

**Why the table still looked healthy.** Yahoo had been answering the degenerate
request with the in-progress bar often enough to hide it. `first_seen` shows the
days it did not: 2026-08-28 and 2026-09-18 (both Fridays, written a day late by
the Saturday run), 2026-08-31 (8 of 16 tickers only), and 2026-10-06 (missing).

**Blast radius.** None moved by the fix: `FXHistHandlerv2` is still `DISABLED`,
and the live python3.10 `FXHistHandler` is what writes this table. The failure
mode is a late row, not a wrong one — the next day's window spans two days, so
the 2026-10-07 run collects 2026-10-06 as well. No back-fill was needed, and the
same-day rows that did land match Yahoo's final values exactly.

**Fix.** `fetch_exchange_rates` is inclusive of `end_dt`: it asks for
`end_dt + 1 day` and `_complete_bars` clamps the result back, dropping bars past
`end_dt` and bars whose `Close` is still NaN. The clamp is required, not tidying
— once the London FX day has rolled, Yahoo returns the `end_dt + 1` bar too
(seen at 22:44 ET as a `2026-10-07` row with a NaN `Close`), and storing it
would both append junk and push the watermark past the real bar, which no later
run can repair.

**The frozen copy still has it.** `Ops/fin-cron-data/fxeod_handler.py` carries
the identical window and is the live writer, so expect further one-day-late FX
rows there until the §11.2 cutover retires it.

---

### 8.9 2026-10-07 — a watermark past the 60-day intraday limit stalls a symbol for good

**Symptom.** An `intraday_min_handler` dry run logged, per old symbol:

```
Loading 0003.HK minute OHLC from Yahoo 2025-09-25 16:00:00+08:00 to 2026-10-07 12:34:30+08:00!
response code=422
YFPricesMissingError('... (Yahoo error = "15m data not available for
  startTime=1758787200 and endTime=1791347618. The requested range must be
  within the last 60 days.")')
0003.HK is downloaded DF : 0 records
```

**Root cause.** `yf_get_max_datetime` applied the `MAX_INTRADAY_DAYS` floor only
when the table held **no** row for the symbol; an existing watermark was passed
to yfinance however old it was. Yahoo serves no 15m bar older than 60 days and
**rejects the whole request** rather than truncating it, so the download returned
nothing — and with nothing written the watermark never moved. The next run
repeated the same impossible request. The stall is permanent: once a symbol falls
more than 60 days behind, it never recovers on its own.

**Blast radius.** 61 of 803 symbols in `GlobalMarketData.histminprice`, both
markets (36 US-listed, 14 `.HK`, 4 `.L`, 2 each `.SZ`/`.SS`/`.NS`, 1 `.JK`), the
oldest stuck since 2025-05-12. The live python3.10 pair has the same logic, so
the backlog grew there.

**Fix.** The start is clamped forward to `localnow - MAX_INTRADAY_DAYS` with a
warning naming the gap. The bars between the old watermark and that floor are
**not recoverable** — Yahoo does not serve them — so each affected symbol keeps a
one-off hole and resumes from the floor.

**Check the backlog.** This query is the measurement; expect the stalled bucket
to be empty a few runs after the v2 functions are enabled:

```sql
SELECT CASE WHEN mx >= NOW() - INTERVAL 60 DAY THEN 'fresh' ELSE 'stalled' END bucket,
       count(*) symbols, min(mx) oldest
  FROM (SELECT Symbol, max(Datetime) mx
          FROM GlobalMarketData.histminprice GROUP BY Symbol) s
 GROUP BY bucket;
```

**The frozen copies still have it.** `Ops/fin-cron-data/eoddata_minhandler_us.py`
and `…_asia.py` are the live writers and keep the defect until the §11.2 cutover.

---

## 9. Troubleshooting

| Symptom | Likely cause |
|---|---|
| SQL contains the literal string `None` | An env var is unset; `environ.get()` returned `None`. Check `.env` against `.env.example` |
| `Unknown database 'X'` | Schema in `.env` does not exist on this server |
| error 3750, *"without a primary key"* | `sql_require_primary_key=ON`; create the table manually with a PK |
| `RemovedIn20Warning` from SQLAlchemy | A legacy `engine.execute()` call — `dataUtil.ExecSQL` was fixed 2026-08-01, but `pd.read_sql` still emits this on 1.4 |
| Layer imports fail at runtime | Wrong site-packages path (`python3.10` vs `python3.13`) or wheels built for the wrong cp tag |
| `portAssetsHandler` writes nothing, logs `sanity_gate ... outside` | A source page restructured; the gate is working as intended. Check the logged source name |
| `portAssetsHandler` skips with *"older than the stored max"* | The source's as-of date went backwards. Investigate before using `force` — `force` deliberately does **not** override this guard |
| `Runtime.MarshalError: ... is not JSON serializable`, after a clean log | The handler returned a non-JSON type (DataFrame, `date`, numpy scalar). The work already completed; only the invocation is marked failed. See §8.1 |
| `YFPricesMissingError … must be within the last 60 days`, `0 records` for one symbol | Its `histminprice` watermark is over 60 days old. Fixed in `intraday_min_handler` (§8.9), which clamps and warns; in the live python3.10 handlers it stalls that symbol permanently |
| Serverless config validation error on `python3.13` | `configValidationMode: error` was uncommented; re-comment it |
| `optChainEOD` reports many more `empty` underlyings than usual | Yahoo is serving no expiries to that IP; it is not an error and the sweeps **do not** retry `empty`. Compare `n_empty` with the previous session. See §10.6 |
| A "dry run" wrote to the database | `localrun` is not the off-switch — `dbFlag=False` is. See §8.2 |
| `eodDaily` / `optChainEOD` reports `symbol list current_symbols_… returned nothing` | Not an empty market — the stored procedure itself is failing, and `load_df_SQL` swallowed the error. `CALL` it by hand; check the `SymbolMaster` column spellings (`options`, not `option`). See §8.7 |
| `ValueError: unsupported format character` from a `dataUtil` query | A literal `%` in the SQL (e.g. `LIKE 'x%'`). pymysql's paramstyle is `format`, so `%` must be doubled in a hand-written query |
| `fin-deep-data` function logs `ModuleNotFoundError: No module named 'pandas'` | `finDeepCore` is not mounted. `finDeepYf` and `finDeepWeb` are de-duplicated against it and are unusable alone |
| `required environment variable X is not set` at startup | `dataUtil.require_env` raising by design, before any SQL is built. Add the key to `Ops/fin-deep-data/.env` |
| Two sets of rows per session in `histminprice` / `FX_histdaily` / `USRates` | A v2 schedule was enabled while the python3.10 function was still deployed. Disable one; see §11 |
| `import dataUtil` in a test picks up the wrong service | The two pytest roots were mixed. Run `fin-deep-data` tests from its own folder |
| `Runtime.ImportModuleError` … `you should not try to import numpy from its source directory` | numpy's compiled stack is broken, **not** an import-location problem. On this service it was `strip` corrupting the bundled OpenBLAS (§8.5). The real cause is in the chained `Original error was:` line, which Lambda's log does not show — reproduce locally per §10.3.2 |
| `OSError: [Errno 30] Read-only file system: './something.csv'` | A handler is writing to the CWD on Lambda (`/var/task`). It must go through `DU.out_dir()` → `/tmp` (§8.6) |
| `usrateHandlerv2` finishes `ok` with 0 rows | Usually correct: the H.15 page has nothing newer than the stored max. Check the page's dates before investigating (§7) |
| `FXHistHandlerv2` finishes `ok` with 0 rows | Correct when `FX_histdaily` already holds today — e.g. the live python3.10 `FXHistHandler` ran at 21:10 UTC first; the log then says `is current through <date>: nothing to fetch` (§7). Without that line, and with `Start_dt` equal to `end_dt`, it is the §8.8 zero-width window instead — check that `fetch_exchange_rates` still asks Yahoo for `end_dt + 1` |
| `CREATE_FAILED: StatusTopic ... "Invalid parameter: Endpoint"` | `STATUS_EMAIL` is empty. Fixed structurally in §8.3; if it recurs, the subscription is back inside the topic's properties |
| `AWS::Lambda::Function ... "is not updatable with parameters provided"` (`NotUpdatable`) | Almost certainly `reservedConcurrency` against an account quota below 100. Check `aws lambda get-account-settings`; see §8.4 |
| Deploy fails and every other function says `Resource creation cancelled` | Only the *first* `CREATE_FAILED` matters — the rest are collateral. `aws cloudformation describe-stack-events ... --query 'StackEvents[?ResourceStatus==\`CREATE_FAILED\`]'` |
| `statusReport` returns `r2: null` with a logged upload error | **Cannot occur after 2026-10-02** — the R2 upload and the `r2` return key are gone. If you see it, a pre-2026-10-02 package is still deployed: redeploy the function (§10.4) |

---

## 10. The `fin-deep-data` service (python3.13)

A second Serverless service, added 2026-10-01 (PLAN-SR-UPSTREAM A–F). Operating
it never touches `fin-cron-data`: separate `serverless.yml`, separate `.env`,
separate layers, separate pytest root.

### 10.1 Setup

```bash
uv venv --python 3.13 venv-py313                         # gitignored
venv-py313/bin/python -m pip install \
  -r Ops/fin-deep-data/requirements_deep_core.txt \
  -r Ops/fin-deep-data/requirements_deep_yf.txt \
  -r Ops/fin-deep-data/requirements_deep_web.txt \
  pytest freezegun boto3                                 # dev-only extras

cp Ops/fin-deep-data/.env.example Ops/fin-deep-data/.env # then fill it in
cd Ops/fin-deep-data && npm install                      # serverless-dotenv-plugin
```

**AWS CLI v2 is a prerequisite for publishing layers** (§10.3) and nothing else:
`serverless deploy` reads `~/.aws/credentials` itself and never shells out to
`aws`. It was not installed on this machine; install it user-local, no sudo and
nothing outside `$HOME`:

```bash
curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o awscliv2.zip
unzip -q awscliv2.zip
./aws/install --install-dir "$HOME/.local/aws-cli" --bin-dir "$HOME/.local/bin"
aws sts get-caller-identity --profile ServerLessUser --region us-east-2
```

The last line is the readiness check: it must print the `ServerLessUser` ARN in
account `567575054547`. To remove the CLI again:
`rm -rf ~/.local/aws-cli ~/.local/bin/aws ~/.local/bin/aws_completer`.
Installed and verified 2026-10-01 (v2.37.8).

The venv installs **the same three requirement files the layers are built from**,
so a dry run exercises the versions that actually run in Lambda
(pandas 2.2.3 / SQLAlchemy 2.0.36 / numpy 2.1.3 / yfinance 0.2.58 on 3.13.15).
`boto3` is dev-only: the Lambda python3.13 runtime provides it, which is why it
is in no layer.

Tests:

```bash
cd Ops/fin-deep-data
../../venv-py313/bin/python -m pytest tests/unit -v       # 320 passed, 1 skipped
```

Run them **from that folder**. Both services have a module named `dataUtil`, so
only one can be on `sys.path` per session; each has its own `pytest.ini` and the
repo-root config collects `tests/` only. The python3.13 policy gate adds:

```bash
../../venv-py313/bin/python -m pytest tests/unit -q \
  $(for m in dataUtil eod_daily_handler optchain_eod_handler status_report_handler \
             port_assets_handler usrate_handler fxeod_handler intraday_min_handler; do \
      printf -- "-W error::FutureWarning:%s -W error::DeprecationWarning:%s " "$m" "$m"; done)
```

### 10.2 Environment variables

All in `Ops/fin-deep-data/.env`; `serverless-dotenv-plugin` turns each into a
Lambda environment variable at deploy time. Keep `.env.example` in step with any
addition.

| Variable | Read by | Default / note |
|---|---|---|
| `DBHOST` `DBPORT` `DBUSER` `DBPWD` | all | no default |
| `DBMKTDATA` `DBTRADING` | all | `GlobalMarketData`, `Trading` |
| `EOD_WRITE_TBL` | eodDaily | **required, no fallback** — `histdailyprice7_shadow`, flipped at cutover |
| `OPT_WRITE_TBL` | optChainEOD | **required, no fallback** — `OptionChains_shadow` |
| `TBLLOADAUDIT` | all | **required** — `load_audit` |
| `TBLCORPACTION` | eodDaily | **required** — `corp_action_daily` |
| `SYMBOL_PROC_VER` | eodDaily, optChainEOD | `V4` → `GlobalMarketData.current_symbols_V4` (863 symbols, no exclusions). Set to `V5` once the owner has run `sql/current_symbols_V5.sql`: V5 takes an `@type` and drops delisted symbols — eodDaily sends `'a'` (838), optChainEOD sends `'o'` (814, also without `SymbolMaster.options = 0`). No code change goes with the flip; `dataUtil` withholds the argument from V1–V4, which take none. Unset falls back to `V4` |
| `EOD_SHARDS` `EOD_BATCH` | eodDaily | `1`, `200` |
| `OPT_SHARDS` | optChainEOD dispatcher | `ceil(T_total / 540 s)` from the timing run, §10.6 |
| `OPT_MAX_PARALLEL` | `serverless.yml` | `4` → `reservedConcurrency` on optChainEOD. **Currently unread**: the line is commented out because the account quota forbids reserving (§8.4) |
| `FIRSTTRAINDTE` | eodDaily, FXHistHandlerv2 | first date for a symbol with no rows, and FX's empty-table fallback. `2008/01/01` only after the F6 audit (§11.3) |
| `FX_TICKERS` | FXHistHandlerv2 | Python list literal, `ast.literal_eval` |
| `SYMBOLLIST` | intraday v2 | one list name, or unset to loop over four |
| `PROD_LIST_DIR` | `dataUtil.list_dir()` | **leave empty** — then the packaged folder is used, which is right on Lambda and locally |
| `TBLDLYPRICE` `TBLOPTCHAIN` `TBLUSRATES` `TBLHISTFX` `TBLMINUTEPRICE` `TBLPORTASSETS` | statusReport (inferred lines) and the v2 handlers | `histdailyprice7`, `OptionChains`, `USRates`, `FX_histdaily`, `histminprice`, `portfolio_assets_info` |
| `R2_ENDPOINT` `R2_ACCESS_KEY_ID` `R2_SECRET_ACCESS_KEY` | optChainEOD | reused from `yf-news-collect`. **statusReport no longer reads any R2 var** (2026-10-02) |
| `UPSTREAM_R2_BUCKET` | optChainEOD | unset → raw chains are **not** archived (logged as a warning, the run still succeeds) |
| `OPT_RAW_PREFIX` | optChainEOD | `raw/optchain`. `STATUS_R2_KEY` was removed on 2026-10-02 with statusReport's R2 upload — delete it from `.env` if an old copy still carries it |
| `STATUS_EMAIL` | `serverless.yml` resources | **must be set before the first deploy** — an empty `Endpoint` makes the SNS subscription fail in CloudFormation |
| `STATUS_TOPIC_ARN` | statusReport | injected by CloudFormation (`Ref: StatusTopic`); do **not** put it in `.env` |
| `SP500_PORT_NAME` `NDX100_PORT_NAME` `DJIA_PORT_NAME` `HSI_PORT_NAME` | portAssetsHandlerv2 | `SP500`, `NDX100`, `DJI`, `HSI` |
| `PORT_ASSET_CLASS` `PORT_ASSET_TYPE` `INDEX_CROSSCHECK` `PORT_OUTPUT_DIR` | portAssetsHandlerv2 | `Equity`, `Stock`, `True`, `/tmp` |
| `FINDEEPCORE_LAYER_ARN` `FINDEEPYF_LAYER_ARN` `FINDEEPWEB_LAYER_ARN` | `serverless.yml` | versioned ARNs, **no defaults** — a wrong layer deploys fine and fails on the first import |
| `DEBUG` | all | `debug` raises the log level |

A variable the handlers require is read through `dataUtil.require_env`, which
**raises** at startup rather than letting the string `'None'` reach SQL.

### 10.3 Layer build and upload

```bash
make finDeepCore.zip finDeepYf.zip finDeepWeb.zip     # or: make finDeep
# equivalently
Ops/fin-deep-data/build_layers.sh                     # all three
Ops/fin-deep-data/build_layers.sh core                # one
```

Build `core` first, or the other two fail with a message saying so: they are
de-duplicated against it. The script cross-builds for
cp313/manylinux2014_x86_64 (the local interpreter is 3.10, so a plain
`pip install` would silently resolve cp310 wheels), resolves the yf and web
trees against `requirements_deep_core.txt` as a constraints file, prunes
`tests/`, `__pycache__` and unneeded `.so` symbols, and refuses to finish if a
zip is larger than `LAYER_ZIP_MAX_MB` (default 80).

Measured 2026-10-01:

| Layer | Unzipped | Zipped | Mounted on |
|---|---|---|---|
| `finDeepCore` | 101 MB | 29 MB | all eight functions |
| `finDeepYf` | 28 MB | 10 MB | eodDaily, optChainEOD, FXHistHandlerv2, both intraday v2 |
| `finDeepWeb` | 14 MB | 6 MB | portAssetsHandlerv2, usrateHandlerv2 |

Against the ~80 MB working ceiling: every zip is far inside it, and the one
number above it is `finDeepCore` **unzipped** at 101 MB. pandas (30) + numpy (22)
+ numpy.libs openBLAS (23) + tzdata/pytz (6) is the floor for this stack —
dropping openBLAS breaks `import numpy`. The AWS hard limits are met with room:
50 MB per zipped direct upload, and 250 MB unzipped for a function plus all its
layers, where the worst case here is core + web = 108 MB.

Publish (owner; needs the AWS CLI from §10.1), then record the versioned ARNs in
`.env`:

```bash
for L in finDeepCore finDeepYf finDeepWeb; do
  aws lambda publish-layer-version --layer-name $L \
    --zip-file fileb://$L.zip --compatible-runtimes python3.13 \
    --region us-east-2 --profile ServerLessUser \
    --query 'LayerVersionArn' --output text
done
```

Changing a `requirements_deep_*.txt` means rebuilding **and republishing** that
layer and updating its ARN in `.env`. A redeploy alone changes nothing.

### 10.3.1 Verifying a published layer

The console's **S3 location** field, e.g.
`arn:aws:s3:::awslambda-us-east-2-layers/snapshots/567575054547/finDeepWeb-<uuid>`,
names an **AWS-owned** bucket. It is not in your account and not reachable:
`aws s3 ls s3://awslambda-us-east-2-layers/...` returns `AccessDenied`, and it
always will. Do not treat that as a broken layer or a missing permission.

Verify through the Lambda API instead. `get-layer-version` returns `CodeSha256`,
`CodeSize` and `Content.Location` — a **presigned HTTPS URL, valid about ten
minutes**, which is the only supported way to read a layer's bytes back:

```bash
for L in finDeepCore finDeepYf finDeepWeb; do
  V=$(aws lambda list-layer-versions --layer-name $L --region us-east-2 \
        --profile ServerLessUser --query 'LayerVersions[-1].Version' --output text)
  R=$(aws lambda get-layer-version --layer-name $L --version-number $V \
        --region us-east-2 --profile ServerLessUser \
        --query 'Content.CodeSha256' --output text)
  printf '%-13s v%-3s remote=%s local=%s\n' "$L" "$V" "$R" \
         "$(openssl dgst -sha256 -binary $L.zip | base64)"
done
```

`CodeSha256` is the base64 SHA-256 of the **zip**, so
`openssl dgst -sha256 -binary <layer>.zip | base64` must match it exactly. That
single comparison is the real check: equal hashes mean the published layer is
byte-identical to the zip `build_layers.sh` produced, which is what the dry runs
and the test suite were run against.

To look inside, download via the presigned URL and confirm the paths — a layer
whose contents do not sit under `python/lib/python3.13/site-packages/` imports as
if it were empty, which is the single most common layer mistake:

```bash
URL=$(aws lambda get-layer-version --layer-name finDeepWeb --version-number 1 \
        --region us-east-2 --profile ServerLessUser \
        --query 'Content.Location' --output text)
curl -fsS -o /tmp/layer.zip "$URL"
unzip -l /tmp/layer.zip | head -20          # expect python/lib/python3.13/site-packages/...
```

**v1 of all three is unusable — do not deploy it.** It was built with a
`strip --strip-unneeded` pass that corrupted numpy's bundled OpenBLAS, so any
function mounting `finDeepCore:1` fails at import (§8.5). v2 is the first working
set. The versions are left published rather than deleted so the broken hashes
stay identifiable.

| Layer | v | `CodeSha256` | Size | Local zip |
|---|---|---|---|---|
| `finDeepCore` | 1 ❌ | `Hiwt1HdEVPgBGCkFmt1OKf0OF2rt5z1SJzUQwJbHpq4=` | 27 MB | match — **broken, strips OpenBLAS** |
| `finDeepYf` | 1 ❌ | `Ncuwi5VfZDqXcvCnyBByyRQyxt9aBTv6CqJfv3VhoJU=` | 3 MB | match — **broken** |
| `finDeepWeb` | 1 | `3GVROYvUVydFik5xD25803IIybQjoiQqL/GZwrZcWXg=` | 5,366,836 B | match, `cmp`-identical |
| `finDeepCore` | **2** | `Zx0LIO7/qh70BOjQ/Hjdmp69kO6sZJ5xSEEz7RPvQrg=` | 30,054,025 B | match |
| `finDeepYf` | **2** | `HJ6OJ6909aLehULMR65nd/sKrUWP2as52Y5CXCgHqx4=` | 9,467,063 B | match |
| `finDeepWeb` | **2** | `FAAoM8D7tUlAWw/nz6ysillPUbTnmYHcdfv+VEmTOYw=` | 5,366,836 B | match |

`finDeepWeb` v2 is byte-identical in *size* to v1 and differs only in hash: its
wheels (lxml, openpyxl) ship already-stripped `.so` files, so the strip pass
changed nothing in them and only the zip's stored mtimes moved. That is also the
tell for which layers the strip actually touched — numpy's bundled OpenBLAS is
*not* pre-stripped, which is exactly why it was the casualty.

A hash match only proves the upload arrived intact. It says nothing about whether
the contents import — v1 matched too. The check that would have caught §8.5 is
the import test now in §10.3.2.

The three ARNs are recorded in `Ops/fin-deep-data/.env` and render into every
function's `layers:` list — check with `serverless print | grep layer:finDeep`.

Layers in account `567575054547` / `us-east-2`, read 2026-10-01:

| Layer | Latest | Runtime | Note |
|---|---|---|---|
| `finCronLib` | v5 | python3.10 | the nine old functions. **Named `finCronLib`, not `finCron`** as this repo's docs and `Makefile` target say |
| `finPortLib` | v5 | python3.13 | `portAssetsHandler`; `serverless.yml` pins `:3`. **Named `finPortLib`, not `finPort313`** (plan finding F4) |
| `finServerLib`, `AgentsData`, `configure`, `mysqlclient-py39-lambda` | v1–v2 | 3.9/3.10 | unrelated to these services |
| `finDeepCore`, `finDeepYf`, `finDeepWeb` | v2 | python3.13 | the eight `fin-deep-data` functions. **v1 of core and yf is broken** (§8.5); `.env` pins `:2` |

The `finCron` / `finPort313` names in this repo are *zip and requirements-file*
names, not the published layer names. Keep that in mind when reading an ARN: the
only authority on what a function actually mounts is `serverless.yml`.

**Alternative, if you would rather not publish by hand.** Serverless can own the
layers itself — a top-level `layers:` block in `serverless.yml`, referenced as
`{Ref: FinDeepCoreLambdaLayer}` instead of an ARN from `.env`. That removes the
CLI step and the ARN bookkeeping, and uploads via S3 so the 50 MB direct-upload
limit stops applying. The trade-off is that the layers become part of this
stack's lifecycle (a `serverless remove` would delete them unless `retain: true`
is set) and a new version is published on every content change. Not adopted here:
the current split keeps layer lifetime independent of the function stack, which
is what makes a rollback to an older layer version a one-line `.env` edit.

### 10.3.2 Import-test a layer build before publishing it

**Run this after every layer build.** It is the check that the `CodeSha256`
comparison in §10.3.1 cannot make, and the one that would have prevented §8.5:
`venv-py313` is the same cp313 / x86_64 target as the Lambda runtime, so the
build trees can be imported directly off disk, in the same *combinations* the
functions mount them in.

```bash
cd /home/thomas/projects/Fin-Lambda
B=build/fin-deep-data; SP=python/lib/python3.13/site-packages; PY=venv-py313/bin/python

# core alone -- statusReport's layer set. np.linalg exercises OpenBLAS, which a
# plain `import numpy` on a stripped build does NOT always reach.
PYTHONPATH=$B/core/$SP $PY -c "
import numpy, pandas, sqlalchemy, pymysql, dotenv, pytz, requests, numpy as np
print(numpy.__version__, pandas.__version__, sqlalchemy.__version__)
print('blas:', float(np.linalg.det(np.array([[1.,2.],[3.,4.]]))))"

# core + yf -- the five downloaders
PYTHONPATH=$B/core/$SP:$B/yf/$SP $PY -c "import yfinance, curl_cffi; print(yfinance.__version__)"

# core + web -- portAssetsHandlerv2, usrateHandlerv2
PYTHONPATH=$B/core/$SP:$B/web/$SP $PY -c "
import lxml.html, bs4, openpyxl
from lxml import etree; print(etree.fromstring('<a/>').tag)"
```

Each must print its versions and exit 0. The order of `PYTHONPATH` entries
mirrors Lambda's: layers all unpack into the same
`/opt/python/lib/python3.13/site-packages`, so a file present in two layers is
resolved once — which is why the yf and web trees are de-duplicated against core
and why neither works without it.

Note what a *failing* numpy looks like, because the message misdirects: numpy
reports `you should not try to import numpy from its source directory`, and the
real cause is only in the chained `Original error was:` line.

### 10.4 Deploy

```bash
cd Ops/fin-deep-data
serverless print                     # renders config, no AWS call
serverless package                   # renders CloudFormation + the eight zips
serverless deploy                    # all eight functions
serverless deploy function -f eodDaily
```

Expected and harmless: a config-validation **warning** for every
`runtime: python3.13` (Serverless 3.x's allowlist stops at 3.11) and one
`CONFIG_VALIDATION_MODE_DEFAULT_V3` deprecation notice.
`configValidationMode: error` must stay commented out or every deploy breaks.

**Every schedule ships disabled.** A first deploy creates all eight functions
and all eleven schedules and runs **nothing**. Each function is verified by hand,
then enabled on its own — §10.4.1.

What a correct `serverless package` renders, worth re-checking after any edit:

- eight `AWS::Lambda::Function` resources, all `python3.13`, with 1–2 layers
  each and **no** `ReservedConcurrentExecutions` — the account quota forbids it,
  see §8.4;
- eleven `AWS::Scheduler::Schedule` resources, every one with
  `ScheduleExpressionTimezone: America/New_York` and `State: DISABLED`;
- `lambda:InvokeFunction` on `…-optChainEOD` and `sns:Publish` on `StatusTopic`
  in the role;
- `STATUS_TOPIC_ARN: {"Ref": "StatusTopic"}` on `statusReport`;
- eight zips of 11–71 KB, each containing only its allowlisted files.

- a bare `AWS::SNS::Topic` plus a separate `AWS::SNS::Subscription` carrying
  `Condition: HasStatusEmail`, which CloudFormation skips when `STATUS_EMAIL` is
  empty (§8.3). The subscription is never part of the topic's own properties.

After the first deploy the owner must confirm the SNS subscription e-mail sent
to `STATUS_EMAIL`; until then the topic shows `PendingConfirmation` and no
report arrives.

**First deploy: 2026-10-01**, after the two failures in §8.3 and §8.4. Verified
live, not from the deploy output:

```bash
aws scheduler list-schedules --region us-east-2 --profile ServerLessUser \
  --query 'Schedules[?contains(Name,`fin-deep-data`)].Name' --output text
# then, per name: aws scheduler get-schedule --name <name> ... --query '[State,ScheduleExpressionTimezone]'
aws lambda get-function-configuration --function-name fin-deep-data-dev-eodDaily \
  --region us-east-2 --profile ServerLessUser --query '[Runtime,Layers[].Arn]'
aws sns list-subscriptions-by-topic \
  --topic-arn arn:aws:sns:us-east-2:567575054547:fin-deep-data-dev-status ...
```

Result: 8 functions, all `python3.13`, layer sets as designed (core only for
`statusReport`, core+yf for the five downloaders, core+web for the two
scrapers); 11 schedules, all `DISABLED`, all `America/New_York`; one e-mail
subscription in `PendingConfirmation`. Zip sizes 11–72 KB.

Still unset after that deploy, and deliberately not blocking it:
`UPSTREAM_R2_BUCKET`. `optChainEOD` uses `env_or` and only warns, so **the raw
chain archive is silently not written**; set the bucket before trusting it as a
source of record. `statusReport` is unaffected — on 2026-10-02 its R2 upload was
removed, because the report is a formatted view of `load_audit` and that table
is the durable copy. Its only delivery is the SNS e-mail, and its return value
no longer carries an `r2` key.

### 10.4.1 Bringing one function online

A deployed-but-disabled function is invoke-only, which is exactly what a manual
verification wants:

```bash
cd Ops/fin-deep-data
# dry run in the real Lambda environment -- real layers, real runtime, no writes
serverless invoke -f eodDaily  -d '{"dbFlag":false,"test":5}'           --log
serverless invoke -f statusReport -d '{"dbFlag":false}'                 --log
serverless invoke -f portAssetsHandlerv2 -d '{"dbFlag":false,"test":true}' --log
```

This is the step a local dry run cannot replace: it is the first thing that
proves the layer set, the runtime, the packaged file list and the deployed env
vars are right together. Check the returned JSON summary, then the CloudWatch log
for a clean import and no stack trace. When the function then writes for real,
confirm `load_audit` gained a summary row and the target table's `MAX(Date)`
advanced.

Enable it by setting `enabled: true` on that function's schedule(s) in
`serverless.yml` and redeploying. Enable **all** of a function's entries
together — eodDaily's sweep is useless without its 18:30 run, and optChainEOD's
two sweeps are useless without the dispatcher.

**Do not turn `enabled` into an `${env:...}` lookup.** Tested 2026-10-01:
Serverless honours only the exact literal `false` / `true`. An empty value, `0`,
`yes`, or `True` with a capital T all render `State: ENABLED`, and the only
signal is a `must be boolean` config-validation warning — which this service
suppresses, because `python3.13` already produces warnings and
`configValidationMode: error` would block every deploy. A one-character slip in
`.env` would silently arm a schedule. A literal in `serverless.yml` also makes
each enablement a reviewable one-line diff.

### 10.5 Schedules

All eleven ship `DISABLED` (§10.4.1). **Four are now enabled** — verified against
AWS 2026-10-06, so the table below is the intended firing time, not the live
state. The authoritative state table, old function against new function per data
set, is in `PLAN-SR-UPSTREAM.md` §*Live enable state*.

> **Enabled as of 2026-10-06:** `eodDaily` (18:30 + 19:00 sweep),
> `optChainEOD` (17:40 dispatch + 18:40 sweep — the **19:40 sweep is still
> DISABLED**), `statusReport` (20:00). `EOD_WRITE_TBL` and `OPT_WRITE_TBL` point
> at the `*_shadow` tables, so this is the §11.1 shadow run, not the cutover.
> The SNS e-mail subscription is confirmed.
>
> Three caveats carried by that state:
> - `serverless.yml` still reads `enabled: false` for all eleven, so the next
>   `serverless deploy` of this service **reverts the four and silently stops the
>   shadow run**. Match the file to reality before deploying.
> - The 19:40 sweep being off removes the third attempt for any symbol that
>   failed both earlier runs, so shadow-diff gaps may be an artefact of the
>   half-enablement (§10.4.1 says to enable a function's entries together).
> - Account `ConcurrentExecutions` is **still 10** (re-checked 2026-10-06) and
>   `optChainEOD` runs `OPT_SHARDS=4` with no `reservedConcurrency` — the
>   condition §8.4 / TODOS §2.13 said to fix before enabling it. Throttling is
>   expected on both services; raise the quota (`L-B99A9384` → 1000).
>
> Separately, `fin-cron-data`'s `portAssetsHandler` rule is **DISABLED** and
> `portAssetsHandlerv2` is also disabled, so `Trading.portfolio_assets_info` has
> **no writer at all**. See TODOS §5.12.

| Function | Local time | UTC in EDT / EST | Why |
|---|---|---|---|
| eodDaily | 18:30 Mon–Fri | 22:30 / 23:30 | ≥ 60 min after the NYSE close in every season, so no bar is partial |
| eodDaily (sweep) | 19:00 Mon–Fri | 23:00 / 00:00 | retries symbols with no `ok`/`empty` audit row |
| optChainEOD (dispatch) | 17:40 Mon–Fri | 21:40 / 22:40 | production's summer time, held fixed |
| optChainEOD (sweeps) | 18:40, 19:40 | — | dispatch + 60 and + 120 min |
| statusReport | 20:00 Mon–Fri | 00:00 / 01:00 | after the sweeps |
| the five v2 functions | see §10.2 table | — | additionally blocked until their cutover: the old function must be retired first (§11.2) |

All are EventBridge Scheduler entries with an explicit `timezone`, so unlike
`fin-cron-data`'s UTC cron expressions they do not drift an hour at a DST
change.

### 10.6 Dry-run procedures

Every handler runs locally with writes off. **`localrun` is not the
off-switch — `dbFlag` is** (see the 2026-10-01 incident in §8).

```bash
cd Ops/fin-deep-data                                   # flat imports
../../venv-py313/bin/python eod_daily_handler.py       # localrun, dbFlag False, test 25
../../venv-py313/bin/python optchain_eod_handler.py    # full-V4 timing run
../../venv-py313/bin/python status_report_handler.py   # prints the report, sends nothing
../../venv-py313/bin/python port_assets_handler.py     # CSVs only
../../venv-py313/bin/python usrate_handler.py
../../venv-py313/bin/python fxeod_handler.py
../../venv-py313/bin/python intraday_min_handler.py        # US
../../venv-py313/bin/python intraday_min_handler.py asia
```

Each `__main__` supplies a dry-run event; all of them read MySQL (symbol lists
and watermarks) and the live sources, and none writes a table, R2 object or
`load_audit` row. Outputs land in the CWD and are gitignored.

What a healthy run looks like (2026-10-01, for comparison on the next run):

| Handler | Output | Shape |
|---|---|---|
| eodDaily | `eod_daily_<asof>.csv` | `Date, Symbol, Exchange, Close, Open, High, Low, Volume, AdjClose`, no nulls; 72 rows across 24 symbols for a two-to-three-session catch-up window |
| eodDaily | `corp_action_<asof>.csv` | `Date, Symbol, Exchange, Dividends, StockSplits, first_seen_at, run_id`; 5 rows |
| eodDaily | `load_audit_<asof>.csv` | 17 columns; one row per symbol plus two `'*'` summary rows (bars table and `corp_action_daily`) |
| optChainEOD | `options_eod_<date>.csv`, `optchain_timing_<date>.csv`, `OptionsChain/<sym>_<date>-PM.csv` | 20 `N_COLUMNS`; the timing CSV gives per-symbol seconds and the summary prints `opt_shards_needed` |
| portAssetsHandlerv2 | `portfolio_assets_info_{SP500,NDX100,DJI,HSI}.csv` + combined | 503 / 101 / 30 / 85 rows, 719 combined; every `sanity_gate` logged *inside* its band; `action=skip … dbFlag=False` |
| usrateHandlerv2 | `USrates_<date>.csv` | 31 columns — `Date` plus 30 instrument rates (38 H.15 rows minus 8 group headers); **no file when nothing is newer than the stored max**, which is the normal case intra-day. Since 2026-10-06 the run also **prints** the rates to stdout, transposed to one row per instrument: `US rates -- <n> new row(s) since <max>` when there are new rows, or `no new rows since <max>; showing the latest H.15 date, which is already stored` followed by that date's 30 rates. A `localrun` that prints no rate table at all is the failure signal — it means the scrape returned nothing |
| intraday v2 | `30min_<sym>.csv` per symbol, `30min_<SYMBOLLIST>.csv` pooled | **the frame that would be stored**, not the download: the 11 `SAVE_COLUMNS` in table order, `Datetime` tz-naive in the exchange's own time, `UTCDatetime` beside it with no offset either (both table columns are plain `datetime`), and `timezone` naming the zone. `Datetime - UTCDatetime` must equal that zone's offset on the bar's date — `08:00` for `Asia/Hong_Kong`, `09:00` for `Asia/Seoul`, `-04:00`/`-05:00` for `America/New_York` across the DST boundary — which is the one thing a dry run exists to check. A symbol yfinance returns nothing for still gets a 0-row CSV carrying that header. Before 2026-10-08 the per-symbol CSV held the raw UTC-indexed download instead, with none of the derived columns. `<sym> is downloaded DF : <n>` and `<sym> is in window : <m>` bracket the watermark filter, and `m = n - 1` is normal — the bar sitting exactly on the watermark is excluded by `Datetime > sdatetime`. The log line `reshape_bars: <sym> from … to …` must show exchange-local times. The closing line reports `<n> row(s) collected, <n> stored` — on a dry run `stored` is 0 and `collected` is the signal, because the returned `rows` counts only what reached the table. A `watermark … further back than the 59-day intraday limit` warning is expected for a symbol with a stale watermark and is **not** a failure (§8.9); `0 records` plus a `YFPricesMissingError` is |
| FXHistHandlerv2 | `USD_dailyFX.csv` | `Date, base_cur, target_cur, Open, High, Low, Close, Adj Close, Volume, server_time`. Run past 17:00 ET with the table loaded through yesterday: **16 rows, one per ticker, all dated the run's own date** — that is the pass condition since 2026-10-06, and 0 rows there is the §8.8 regression. 0 rows *is* expected before 17:00 ET, or on a re-run once the table already holds today; the log then says `is current through <date>: nothing to fetch` (§7) |

A zero-row or all-NaN column, a `sanity_gate … outside` line, or a new stack
trace is a failure. The eodDaily and optChainEOD outputs are also the inputs to
the golden diffs: bars against the last five sessions of `histdailyprice7`, and
the pre-filter chains against the droplet's `Ops/OptionsChain/{sym}_{date}-PM.csv`
for ten sample underlyings. **No golden CSV is committed for this service** — the
files carry live market data and change daily; the shapes above are the baseline
instead.

`OPT_SHARDS` is sized from the timing run: `OPT_SHARDS = ceil(T_total / 540 s)`,
which the handler prints as `opt_shards_needed`. Measured 2026-10-01 over the
full 857-symbol V4 list:

| | |
|---|---|
| `T_total` | **1,948 s** (32.5 min) — per-symbol sum and wall clock agree exactly |
| `opt_shards_needed` | **4**, so `OPT_SHARDS=4` |
| per symbol | mean 2.27 s, median 1.32 s, max 28.9 s |
| outcome | 615 `ok`, 208 `empty`, 34 `error`, 0 `skipped` |
| volume | 458,772 raw contract rows → 106,920 written after filtering |

This reproduces the 1,965 s measured independently on 2026-09-25, so the figure
is stable. Note where the time goes: the **34 errored underlyings account for
814 s of the 1,948** — delisted or no-data tickers, 25 s each in the ported
5 × 5 s retry loop — against 1,084 s for all 615 that returned a chain. The
sizing is therefore driven more by dead tickers in `current_symbols_V4` than by
live ones, which is worth remembering twice over: pruning them would cut the
run by ~40 %, and `T_total` moves whenever Yahoo's coverage of them changes.
Re-run the timing dry run after any large change to the list, and confirm the
real figure from the first production shard runs — a Lambda IP is not this
machine's IP, and Yahoo does not treat them identically.

#### `max_retries` 5 → 2 (2026-10-01)

Measured on the same 863-symbol list, against the committed
`optchain_timing_2026-10-01.csv` (2,063 s; the table above is an earlier run of
the same list):

| status | n, before → after | total s, before → after | mean s |
|---|---|---|---|
| `error` | 39 → 38 | **914.8 → 238.6** | 23.5 → 6.3 |
| `ok` | 624 → 379 | 1,101.4 → 677.6 | 1.76 → 1.79 |
| `empty` | 200 → 446 | 46.8 → 215.8 | 0.23 → 0.48 |
| total | | 2,063.0 → 1,131.9 | |

The retry cut does what it was meant to and nothing else: an errored underlying
costs ~6 s instead of ~24 s (one 5 s sleep instead of four), and the mean for a
symbol that *returns* a chain is unchanged. The two sweep invocations are the
real retry — they run 60 and 120 min later, when a rate limit has reset, which a
20 s inline wait never outlasts.

**Do not read the 1,131.9 s as the new `T_total`.** 245 underlyings that returned
a chain in the baseline returned **empty** in this run — `MRVL`, `MS`, `MRSH`,
`HYGH`, `CBAT` among them — and still did when re-requested one at a time,
without an error. That is Yahoo serving no expiries to this IP at 23:50 local
(04:50 UTC), not a consequence of the retry change. Adding those 245 back at the
measured 1.79 s mean gives ~1,570 s, so `opt_shards_needed` is 3 either way; the
run printed 3.

**`OPT_SHARDS` stays 4** until this is re-measured at the scheduled hour
(17:40 ET). At 3 shards a ~1,570 s run leaves ~523 s per shard against the 540 s
budget — a 3 % margin, measured on an anomalous night. A fourth shard costs one
concurrent invocation out of the account's 10 and buys back the margin.

**The empty-chain mode is worth knowing for its own sake.** An `empty` status is
legitimate for ~200 of these symbols, so nothing errors and the run reports
`status: ok`. But `dataUtil.missing_for_sweep` counts `status IN ('ok','empty')`
as done, so **the sweeps do not retry an underlying that came back empty**: a
throttled dispatch run silently collects 245 fewer underlyings and still looks
clean in `statusReport`. Before the cutover, check `n_empty` against the previous
session rather than only `n_error` — a jump is the signal. Narrowing the sweep to
re-try `empty` is not obviously right (it would re-request ~200 legitimately
empty symbols three times a night) and has not been changed.

---

## 11. Cutover runbooks

### 11.1 Shadow run (new data sets)

1. Owner runs `Ops/fin-deep-data/sql/upstream_tables.sql` (idempotent; the four
   tables and the view were already present on 2026-10-01) and
   `Ops/fin-deep-data/sql/current_symbols_V5.sql`, then grants the Lambda DB
   user `EXECUTE` on the new procedure. Confirm
   `CALL GlobalMarketData.current_symbols_V5('a')` and `('o')` return 838 / 814
   rows as that user before setting `SYMBOL_PROC_VER=V5` and redeploying; while
   it stays `V4` both collectors keep the unfiltered 863-symbol list.
2. Seed the shadow tables so every symbol's watermark matches production:
   `INSERT … SELECT` the last 30 days of `histdailyprice7` / `OptionChains`.
   Without this the first night looks like a first load for all 857 symbols.
3. Deploy with `EOD_WRITE_TBL=histdailyprice7_shadow` and
   `OPT_WRITE_TBL=OptionChains_shadow`, verify `eodDaily`, `optChainEOD` and
   `statusReport` by manual invoke (§10.4.1), then set `enabled: true` on their
   schedules and redeploy. The droplet cron keeps writing production throughout.
4. Each morning for ten sessions, compare key sets and check
   `|ΔC|/C ≤ 1e-6` on ≥ 99.9 % of rows. Two differences are expected and
   explainable: winter-time bars the old 16:10 ET run captured before they were
   final, and option quotes captured at 17:40 ET rather than the old run's time.
5. `statusReport` runs throughout; `load_audit` should show one row per symbol
   per session and `n_ok == n_expected` after the sweep.

### 11.2 Flipping one data set (v2 functions)

Per data set, never in bulk, because both writers append to one table:

1. Dry-run the v2 handler and compare its output with the current table
   contents.
2. Comment the old function out of `Ops/fin-cron-data/serverless.yml` **or**
   disable its schedule, and `serverless deploy` that service.
3. Remove `enabled: false` from the v2 function's schedule in
   `Ops/fin-deep-data/serverless.yml` and `serverless deploy` this one.
4. After the next scheduled run: CloudWatch shows no error, the target table's
   `MAX(Date)`/`MAX(Datetime)` advanced exactly one session, and a `load_audit`
   summary row exists for that job.
5. Rollback is the reverse, in the same order. Both handlers write append-only,
   so a gap refills from `max(Date) + 1` on the next run.

For `eodDaily` / `optChainEOD` the equivalent step is the env flip: set
`EOD_WRITE_TBL=histdailyprice7` and `OPT_WRITE_TBL=OptionChains`, comment the
two crontab lines on the droplet, and `serverless deploy`.

### 11.3 2008 prepend

After the cutover **and** the F6 audit of `FIRSTTRAINDTE` — it is also
`FXHistHandlerv2`'s empty-table fallback, where a new ticker would back-fill
from 2008 (accepted) — set `FIRSTTRAINDTE=2008/01/01` and run, per shard:

```bash
serverless invoke -f eodDaily -d '{"prepend":true,"shard":0,"of":1}'
```

Check: each live symbol's `MIN(Date)` is the later of 2008-01-02 and its first
yfinance session, and there is exactly one `segment='prepend'` audit row per
symbol.

### 11.4 Archive the droplet jobs

Compress the loader cache (`tar --zstd` of `Ops/yfinance` and
`Ops/OptionsChain`, 4.5 GB raw) into R2 `sr-agent/raw/myfindata-cache/` as
write-once, `git tag final-cron 19c8509` in myFinData with a README pointer
here, and retire the droplet scripts after 30 days.
