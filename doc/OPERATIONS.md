# OPERATIONS.md

Running, deploying and debugging Fin-Lambda.

> **Scope note.** Created alongside `portAssetsHandler`. Sections marked
> *(all handlers)* are complete; the incident log starts empty because nothing
> has been recorded before now.

## Changelog

- 2026-08-01 | Added | Initial file: environment setup, test harness, env-var table incl. 8 new `portAssetsHandler` keys, `finPort313` layer build/upload, the 3.10-vs-3.13 split, schedules, dry-run procedures, known constraints, troubleshooting.
- 2026-08-02 | Added | §8 Known incidents: first entry — `portAssetsHandler` `Runtime.MarshalError` on a non-JSON return value; matching §9 troubleshooting row.

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

**`Ops/fin-cron-Pgsql/` cannot run here.** `dataUtil_Pgsql.py` hardcodes an
absolute macOS dotenv path and reads a different key set (`RHOST`, `DB`,
`PORT`). Its `serverless.yml` also references handlers that do not exist in the
folder — do not deploy it as-is.

---

## 8. Known incidents

Add an entry here after any production incident — missed or overlapping run,
yfinance rate-limit or schema break, DB connection failure, layer/runtime
mismatch, Lambda timeout — with root cause, blast radius and fix.

### 2026-08-02 — `portAssetsHandler` invocation failed with `Runtime.MarshalError`

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
| `Runtime.MarshalError: ... is not JSON serializable`, after a clean log | The handler returned a non-JSON type (DataFrame, `date`, numpy scalar). The work already completed; only the invocation is marked failed. See §8, 2026-08-02 |
| Serverless config validation error on `python3.13` | `configValidationMode: error` was uncommented; re-comment it |
