# PLAN — `portAssetsHandler`: S&P 500 / NASDAQ-100 index membership → `Trading.portfolio_assets_info`

Status: **DRAFT — awaiting approval**
Created: 2026-08-01
Owner: Thomas Choi

---

## 1. Goal

A new scheduled Lambda in `Ops/fin-cron-data/` that:

1. Downloads the current constituent list of the **S&P 500** and the **NASDAQ-100** from reliable public sources.
2. Writes each list to a **CSV in the working directory** for eyeball verification.
3. Appends the membership set to **`Trading.portfolio_assets_info`** in MySQL via `dataUtil.StoreEOD()` (pandas `DataFrame` → `to_sql`), the repo's standard write path.
4. Runs on **Python 3.13**, unlike the nine existing `python3.10` functions, with its own layer and its own build tooling.

### Decisions already made (from requirements review)

| Question | Decision |
|---|---|
| What is `Rate`? | **FX rate to USD.** Both indices are 100% USD → `Rate = 1.0` on every row. No index-weight source needed. |
| `Date` cadence | **Only on change.** A new dated membership set is written only when the constituent set (or a constituent's attributes) actually differs from the latest stored set. |
| `Port_name` values | **Configurable via `.env`** (`SP500_PORT_NAME`, `NDX100_PORT_NAME`), defaulting to `SP500` / `NDX100`. |
| NASDAQ-100 source | Official **Nasdaq API JSON** (user-supplied), with Wikipedia as fallback. |
| NASDAQ-100 `Sector` | The Nasdaq API carries no sector → write the literal **`"N/A"`** on every NDX row. No sector join, no second taxonomy. |
| `Class` / `Type` | **`Equity`** / **`Stock`** on every row (env-overridable). |

### Open assumptions — flag now, confirm before coding

| # | Assumption | Why it matters |
|---|---|---|
| A1 | ~~`Class` / `Type` values~~ | **RESOLVED** — `Equity` / `Stock`. |
| A2 | Symbols are stored in the **dotted** form (`BRK.B`, `BF.B`), matching `Product_List/stock_exchange.csv`. | Confirmed: `stock_exchange.csv` contains `BRK.A`, `BRK.B`, `BF.A`, `BF.B` — dotted. But **yfinance-fed tables use the dashed form** (`BRK-B`). Joining `portfolio_assets_info` to `histdailyprice7` will silently miss these ~5 symbols unless one side is normalized. Needs a check against the live `histdailyprice7` before coding — see Phase 0. |
| A3 | The MySQL user in `DBUSER` has `CREATE`/`INSERT`/`DELETE` on the `Trading` schema. | `opt_handler.py` already calls `Trading.sp_etf_trades_v2` through the same engine, so *read* access is proven; write access is not. |

---

## 2. Source research — what was actually tested

All checks run from this WSL2 environment on **2026-08-01**. Verified by HTTP status, payload size, and a real `pandas` parse — not from memory.

### 2.1 Selected sources

| Index | Role | Source | URL | Result |
|---|---|---|---|---|
| S&P 500 | **primary** | Wikipedia — *List of S&P 500 companies* | `https://en.wikipedia.org/wiki/List_of_S%26P_500_companies` | HTTP 200. `read_html()[0]` → **503 × 8**: `Symbol`, `Security`, `GICS Sector`, `GICS Sub-Industry`, `Headquarters Location`, `Date added`, `CIK`, `Founded`. |
| S&P 500 | **cross-check** | State Street SPDR **SPY** daily holdings (XLSX) | `https://www.ssga.com/us/en/intermediary/library-content/products/fund-data/etfs/us/holdings-daily-us-en-spy.xlsx` | HTTP 200, 54 KB. Parsed: 597 rows, real header on sheet row 4 → `Name`, `Ticker`, `Identifier`, `SEDOL`, `Weight`, `Sector`, `Shares Held`, `Local Currency`. Carries an explicit as-of date (`As of 30-Jul-2026`). **`Sector` is `-` for every row** — unusable for sector; used for membership + currency only. Requires `openpyxl`. |
| NASDAQ-100 | **primary** | Official Nasdaq API (JSON) | `https://api.nasdaq.com/api/quote/list-type/nasdaq100` | HTTP 200, 22 KB. `data.data.rows` → **103** records: `symbol`, `companyName`, `marketCap`, `lastSalePrice`, `sector`. `data.date` = `"Jul 30, 2026"` (usable as effective date). **Requires a browser-like `User-Agent`**; `sector` is an empty string on every row → `Sector` is written as `"N/A"`. |
| NASDAQ-100 | **fallback / cross-check** | Wikipedia — *List of NASDAQ-100 companies* | `https://en.wikipedia.org/wiki/List_of_NASDAQ-100_companies` | HTTP 200. `read_html()[0]` → **103 × 4**: `Ticker`, `Company`, `ICB Industry`, `ICB Subsector`. Used for **membership only** — its ICB sectors are deliberately not used (see §3). |

> **Note:** the constituent table is **no longer on the `Nasdaq-100` Wikipedia page** — it was split out to `List_of_NASDAQ-100_companies`. Parsing `/wiki/Nasdaq-100` returns 18 tables, none of them the component list. Use the `List_of_...` URL.

### 2.2 Sources tested and rejected

| Source | Result | Verdict |
|---|---|---|
| `https://www.slickcharts.com/nasdaq100` and `/sp500` | **HTTP 403** with a Chrome UA, a Safari UA, `python-requests`, and no UA at all (Cloudflare challenge, ~5.5 KB block page). | **Not usable.** It will also fail from a Lambda IP, which is more heavily filtered than a residential one. Noted because it was suggested in requirements — it was tested and does not work. |
| Invesco **QQQ** holdings download (`invesco.com/.../action=download&ticker=QQQ`) | HTTP **406** with a browser UA; HTTP 200 with other UAs but the body is a **2,135-line HTML page**, not CSV. | Not usable. This is why no free authoritative NDX *weight* source exists — moot, since `Rate = 1.0`. |
| iShares **IVV** holdings CSV (`ishares.com/.../IVV_holdings`) | HTTP 200, `content-type: text/csv`, but the body is the HTML fund landing page. | Not usable. |
| `stockanalysis.com` screener API | HTTP 404 on the guessed endpoint. | Not pursued. |

### 2.3 Resilience posture

Every one of these is a scrape or an unofficial endpoint, and this repo already has one silent scraper (`usrate_handler.py`, the Fed H.15 page). The design therefore:

- Fetches **two independent sources per index** and logs the symmetric difference.
- Treats a source disagreement as **a warning, not a failure**, but applies a **sanity gate** before writing: S&P 500 must yield 490–520 symbols, NASDAQ-100 must yield 95–110. Outside the band → log an error and **write nothing**. This is the guard against a page-structure change producing a 3-row DataFrame that then wipes a day's membership.
- Records the source actually used in the log line for every run.

---

## 3. Target table

The table does **not** exist yet. `StoreEOD` uses `to_sql(if_exists='append')`, which will happily create a table with no primary key — so the DDL must be run **manually first**, before the first non-dry run.

```sql
CREATE TABLE IF NOT EXISTS `Trading`.`portfolio_assets_info` (
  `Date`      DATE         NOT NULL,
  `Port_name` VARCHAR(45)  NOT NULL,
  `Symbol`    VARCHAR(20)  NOT NULL,
  `Class`     VARCHAR(20)  NULL,
  `Sector`    VARCHAR(45)  NULL,
  `Type`      VARCHAR(20)  NULL,
  `Currency`  VARCHAR(4)   NULL,
  `Rate`      FLOAT        NULL,
  PRIMARY KEY (`Date`, `Port_name`, `Symbol`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

### Column mapping

| Column | S&P 500 | NASDAQ-100 |
|---|---|---|
| `Date` | Effective date of the set (§4.3) | same |
| `Port_name` | `$SP500_PORT_NAME` (default `SP500`) | `$NDX100_PORT_NAME` (default `NDX100`) |
| `Symbol` | Wikipedia `Symbol` | Nasdaq API `symbol` |
| `Class` | `$PORT_ASSET_CLASS` (default `Equity`) | same |
| `Sector` | Wikipedia `GICS Sector` | literal **`"N/A"`** |
| `Type` | `$PORT_ASSET_TYPE` (default `Stock`) | same |
| `Currency` | `USD` | `USD` |
| `Rate` | `1.0` | `1.0` |

> **Sector is populated for `SP500` only.** The Nasdaq API returns an empty sector, so `NDX100` rows carry the literal string `"N/A"` — not `NULL`, so that "we know there is no sector" is distinguishable from "the join failed". This is a deliberate simplification: Wikipedia's NDX page classifies under **ICB** while the S&P 500 page uses **GICS**, and mixing the two taxonomies in one column would make `Sector` incomparable across `Port_name` (`AAPL` would read `Information Technology` under `SP500` and `Technology` under `NDX100`). Recorded in `doc/PRODUCT-GUIDE.md`. If GICS sectors for NDX names are wanted later, the clean route is a lookup against the `SP500` rows — ~90 % of NDX constituents are also in the S&P 500 — not a second scrape.

### `StoreEOD` risk

`StoreEOD` wraps `to_sql` in `try/except` and only **logs** on failure. A duplicate-PK `IntegrityError` will therefore be swallowed and the run will report success having written nothing. Mitigations:

- Delete the target `(Date, Port_name)` rows before appending, so a same-day re-run is idempotent.
- Verify the row count after the write and log a **loud error** if it does not match the DataFrame length. (Do **not** change `StoreEOD` itself — it is shared by all nine live handlers.)

---

## 4. Design

### 4.1 Files

| File | Status | Purpose |
|---|---|---|
| `Ops/fin-cron-data/port_assets_handler.py` | **new** | The handler. No hyphen in the name — `yf-news-collect.py` is not importable, per `TODOS.md` §0. |
| `Ops/fin-cron-data/dataUtil.py` | **modified** | `ExecSQL()` only — made SQLAlchemy 1.4/2.0-agnostic (§4.5). Shared by all ten functions; drives the §7.1 verification. |
| `Ops/fin-cron-data/serverless.yml` | modified | New `portAssetsHandler` function with a per-function `runtime: python3.13` override and its own layer ARN. |
| `Ops/fin-cron-data/requirements_port313.txt` | **new** | Python 3.13 layer dependency set. Kept separate from `requirements_cron.txt` so the 3.10 layer is untouched. |
| `Ops/fin-cron-data/.env` | modified | New keys (§4.6). Edited **by the user**, not by Claude. |
| `.env.example` (repo root) | **new** | Closes `TODOS.md` §3.6 for at least the new keys. |
| `Makefile` | modified | New `finPort313.zip` target. |
| `Ops/fin-cron-data/portfolio_assets_info.csv` | **new** | Golden dry-run reference, committed. |
| `tests/unit/test_port_assets_handler.py` | **new** | Mandatory per the working agreement. |
| `tests/conftest.py`, `pytest.ini`, `requirements-dev.txt` | **new** | Minimal slice of `TODOS.md` §0 (§7 below). |
| `HISTORY.md`, `doc/*.md` (4 files) | **new** | First change under the mandatory-rules regime; see §8. |

### 4.2 Handler shape

Mirrors `fxeod_handler.py` / `eoddata_minhandler_us.py`: module-level `load_dotenv()`, `run(event, context)` entry point, `if __name__ == '__main__'` block supplying the event.

```
run(event, context)
├── configure logging from event["test"]
├── read env → port names, DB/table, class/type constants; fail loud if any is missing
├── for each index in (SP500, NDX100):
│   ├── fetch_<index>()            → DataFrame[Symbol, Sector, source_asof]
│   │                                 SP500: Sector = GICS from Wikipedia
│   │                                 NDX100: Sector = "N/A" (source has none)
│   ├── cross_check_<index>()      → log symmetric difference (non-fatal)
│   ├── sanity gate on row count   → abort this index if outside band
│   ├── build_frame()              → the exact 8 table columns, in order
│   ├── write CSV                  → always, before any DB work
│   ├── load_latest_set()          → newest stored set for this Port_name
│   ├── has_changed()              → skip index if identical and not event["force"]
│   └── resolve_effective_date() → DU.ExecSQL(DELETE same-date) → StoreEOD() → verify count
└── log a per-index summary: source used, row count, changed y/n, date written
```

**Pure, importable functions** (the unit-test surface): `parse_sp500_wikipedia(html)`, `parse_ndx_nasdaq_api(json_dict)`, `parse_ndx_wikipedia(html)`, `parse_spy_holdings(bytes)`, `build_frame(...)`, `has_changed(new_df, old_df)`, `resolve_effective_date(...)`, `sanity_gate(df, lo, hi)`. Everything touching the network or the DB stays in thin wrappers around these.

With `ExecSQL` fixed per §4.5, the handler uses `DU.ExecSQL` for its `DELETE` like every other handler in the service — no local SQL helper, no new idiom.

### 4.3 Effective-date rule

Priority order for the `Date` written:

1. The source's own as-of date — Nasdaq API `data.date`, SPY holdings `As of …` — when available and parseable.
2. Otherwise, the **NY-local** date of the run (`pytz.timezone('US/Eastern')`, consistent with every other handler).

Then, against `max(Date)` already stored for that `Port_name`:

| Condition | Action |
|---|---|
| No rows stored yet | Write the set. First run seeds the table. |
| `new_date > stored_max` and content changed | Write the set. |
| `new_date == stored_max` and content changed | `DELETE` that `(Date, Port_name)` then write. Makes a same-day re-run idempotent and lets a mid-day correction land. |
| `new_date < stored_max` | **Skip**, log an error. Indicates a stale source; writing would create an out-of-order set that breaks as-of queries. |
| Content unchanged | **Skip**, log at INFO. This is the expected outcome on the vast majority of runs. |

### 4.4 Change detection

`has_changed()` compares the two frames on the full row tuple `(Symbol, Class, Sector, Type, Currency, Rate)`, sorted by `Symbol` — not just the symbol set. A GICS reclassification with no membership change therefore produces a new dated set, which is correct for a table whose purpose is point-in-time attributes.

Comparison ignores `Date` and `Port_name` (constant within a set) and is `NaN`-safe (`fillna('')` before comparison, since `Sector` can legitimately be missing).

The full set is always rewritten — **never deltas**. An as-of query stays a simple `WHERE Date <= X ORDER BY Date DESC LIMIT 1` on the date, then an equality join.

### 4.5 Python 3.13 and the shared `dataUtil.py`

This is the sharpest edge in the change and needs to be stated plainly.

`dataUtil.py` is zipped into the deployment package of **every** function in this service. The new function will execute that same file on a **completely different dependency stack**:

| | 3.10 functions (existing) | 3.13 function (new) |
|---|---|---|
| pandas | `1.5.3` | `2.2.x` — no cp313 wheels exist for 1.5.3 |
| SQLAlchemy | `1.4.46` | `2.0.x` — 1.4 does not support 3.13 |
| numpy | `1.26.4` | `2.1.x` |

#### The real axis of incompatibility is SQLAlchemy, not Python

Worth separating, because it changes what "make it downward compatible to 3.8" actually means:

- **Python syntax: already compatible.** Verified — `dataUtil.py` parses cleanly under `ast.parse(..., feature_version=(3,8))`. It uses f-strings and nothing newer. No syntax work is needed for 3.8, 3.10, or 3.13.
- **The one genuine break is `ExecSQL`.** Line 109 calls `get_DBengine().execute(query)`. `Engine.execute()` was **removed** in SQLAlchemy 2.0. Verified on the pinned 1.4.46 in this venv: that call already emits `RemovedIn20Warning: Deprecated API features detected! These feature(s) are not compatible with SQLAlchemy 2.0.`
- Everything else in the module is safe: `StoreEOD` (`to_sql`) and `load_df_SQL` / `load_df` / `get_Max_*` (`pd.read_sql` with a raw string) behave identically on 1.4 and 2.0.

#### Decision: one shared `dataUtil.py`, made version-agnostic — **not** a 3.13 fork

The requirement offered two routes. Recommending the first, for three reasons.

| Route | Verdict |
|---|---|
| **(A) Make `dataUtil.py` work on both SQLAlchemy 1.4 and 2.0** | **Chosen.** It is a **3-line change to one function**. The resulting file runs unmodified on Python 3.8 → 3.13 and on SQLAlchemy 1.4 → 2.0, so the eventual `TODOS.md` §5 migration of the other nine functions needs no further `dataUtil` work. |
| (B) Fork `dataUtil_313.py` | Rejected. The repo **already carries three forks** of this file (`Ops/fin-cron-data/`, `Dev/fin-cron-data/`, `Ops/fin-cron-Pgsql/dataUtil_Pgsql.py`), and `TODOS.md` §1.8 already lists fork sprawl as an open problem. A fourth fork buys nothing here — the incompatible surface is one function — and guarantees that every future `dataUtil` fix has to be applied twice, silently diverging the moment someone forgets. |

The change:

```python
from sqlalchemy import text          # new import

def ExecSQL(query):
    logging.info(f"ExecSQL: {query}")
    try:
        with get_DBengine().begin() as conn:      # was: get_DBengine().execute(query)
            results = conn.execute(text(query))
        logging.info(f'number of rows execed: {results.rowcount}')
        return results.rowcount                   # was: implicit None
    except Exception:
        logging.error("Exception occurred at ExecSQL()", exc_info=True)
```

**Verified working on the pinned SQLAlchemy 1.4.46** in this environment (`CREATE` / `INSERT` / `DELETE` against an in-memory engine returned the correct rowcount of 2), and it is the documented 2.0 idiom. `Engine.begin()` and `text()` exist in both versions.

Three secondary points:

- `begin()` makes the statement **transactional and auto-committed**. On 1.4 with the default `autocommit` behaviour a bare `engine.execute("DELETE …")` also committed, so the observable behaviour of the existing `DELETE`/`TRUNCATE` callers does not change — but this must be confirmed by the dry runs in §7.1, not assumed.
- The `try/except` + `logging.error` swallow is **kept deliberately**. `TODOS.md` §1.7 pins that as this module's documented contract; changing it is a separate decision with its own caller audit.
- Adding a return value is backward-compatible (it returned `None` implicitly) and is what lets the new handler verify its own `DELETE` actually deleted.

#### Blast radius

`ExecSQL` callers, from `grep`:

| Caller | Deployed? |
|---|---|
| `handler.py` ×2 (`cronHandler`) | **yes** |
| `opt_handler.py` (`optHandler`) | **yes** |
| `fx_handler.py` (`FXrateHandler`) | **yes** |
| `dataUtil.StoreWebDaily` (`TRUNCATE`) | not called by any deployed handler in this service |
| `yfin_handler.py`, `yfineod_handler.py` | no — not in `serverless.yml` (`TODOS.md` §4.2 dead code) |

**Three live functions.** All three use it for the same delete-then-append snapshot pattern, so one verification pattern covers all three. This is what makes route (A) affordable — and it moves the §7.1 verification from "nothing to check" to "three handlers must be dry-run", which is reflected below.

The `numpy` 2.x / `pandas` 2.x split remains why the new layer is separate and why the new function's `runtime:` is overridden per-function rather than at the provider level.

### 4.6 New environment variables

All read with an explicit presence check that raises a clear message — never a bare `environ.get()` that interpolates `None` into SQL (`TODOS.md` §3.3 records this as an existing failure class in this repo).

| Var | Default | Read by | Purpose |
|---|---|---|---|
| `DBTRADING` | *(required)* | `port_assets_handler` | Target schema, e.g. `Trading`. Not currently in `.env` — must be added. |
| `TBLPORTASSETS` | *(required)* | `port_assets_handler` | `portfolio_assets_info` |
| `SP500_PORT_NAME` | `SP500` | `port_assets_handler` | `Port_name` for the S&P 500 set |
| `NDX100_PORT_NAME` | `NDX100` | `port_assets_handler` | `Port_name` for the NASDAQ-100 set |
| `PORT_ASSET_CLASS` | `Equity` | `port_assets_handler` | `Class` constant (A1) |
| `PORT_ASSET_TYPE` | `Stock` | `port_assets_handler` | `Type` constant (A1) |
| `INDEX_CROSSCHECK` | `True` | `port_assets_handler` | Enables the SPY-holdings cross-check; set `False` to drop the `openpyxl` dependency |
| `PORT_OUTPUT_DIR` | `.` local / `/tmp` on Lambda | `port_assets_handler` | Where the verification CSVs are written |

### 4.7 Event contract

| Key | Type | Default | Effect |
|---|---|---|---|
| `localrun` | bool | `False` | Write CSVs to the CWD instead of `PORT_OUTPUT_DIR` |
| `dbFlag` | bool | `True` | `False` suppresses **all** DB writes — the dry-run switch, matching `eoddata_minhandler_*.py` |
| `force` | bool | `False` | Bypass the only-on-change check and write the set regardless |
| `test` | bool | `False` | Raise log level to `DEBUG` |
| `NYTIME` | datetime | *(set by `run`)* | Injected clock, for tests |

Local dry run:

```bash
cd Ops/fin-cron-data          # required: flat imports + relative CSV paths
python port_assets_handler.py # __main__ supplies {"localrun": True, "dbFlag": False, "test": True}
```

### 4.8 Schedule

`cron(30 22 ? * MON-FRI *)` — 22:30 UTC, ~1.5 h after the US close in EDT, ~2.5 h in EST. Membership changes are announced by the index committees and take effect at an open, so a daily post-close check catches every change within one business day. Only-on-change makes a daily run essentially free: on a no-change day the handler does two HTTP fetches, one `SELECT`, and exits.

---

## 5. Python 3.13 layer and build tooling

`requirements_port313.txt`:

```
PyMySQL==1.1.1
python-dotenv==1.0.1
SQLAlchemy==2.0.36
pandas==2.2.3
numpy==2.1.3
requests==2.32.3
lxml==5.3.0            # pandas.read_html parser
beautifulsoup4==4.12.3 # read_html fallback flavor
openpyxl==3.1.5        # SSGA SPY .xlsx cross-check; droppable if INDEX_CROSSCHECK=False
```

New `Makefile` target. It must **not** build against the local interpreter — WSL here is Python 3.10, and pip would resolve cp310 wheels that a 3.13 Lambda cannot import. Use explicit platform targeting so the build is reproducible regardless of the local interpreter:

```make
finPort313.zip:
	$(RM) -rf ./python
	pip3 install -r Ops/fin-cron-data/requirements_port313.txt \
		--platform manylinux2014_x86_64 \
		--implementation cp --python-version 3.13 \
		--only-binary=:all: \
		-t python/lib/python3.13/site-packages
	zip -r9 $@ python/
```

Note the path is `python3.13`, not the hardcoded `python3.10` every existing target uses (`TODOS.md` §5.4 flags this same trap for the wider migration).

**Layer attachment.** `serverless.yml` currently declares no `layers:` at all — layers are attached by hand in the console (per `CLAUDE.md`). For this function the layer ARN will be declared **on the function only**, so the 3.13 layer can never be picked up by a 3.10 function:

```yaml
  portAssetsHandler:
    handler: port_assets_handler.run
    runtime: python3.13
    layers:
      - arn:aws:lambda:us-east-2:<acct>:layer:finPort313:<ver>
    events:
      - schedule: cron(30 22 ? * MON-FRI *)
```

The ARN is only known after the first manual layer upload, so Phase 3 is ordered accordingly.

**Size check:** `pandas` + `numpy` + `lxml` + `openpyxl` will land near the 250 MB unzipped limit's comfortable zone but should be well under. Measure the unzipped size during Phase 3 and record it in `doc/OPERATIONS.md`; if it is tight, dropping `openpyxl` (`INDEX_CROSSCHECK=False`) and `beautifulsoup4` is the first lever.

---

## 6. Phases

| Phase | Work | Gate |
|---|---|---|
| **0 — Confirm** | Resolve A2 (symbol form) — one query against the live `histdailyprice7` to see whether it stores `BRK.B` or `BRK-B`. Confirm A3 (write grants on `Trading`). | Nothing is coded until A2 is answered; it is expensive to change after the first write. |
| **1 — Test harness** | `requirements-dev.txt`, `pytest.ini`, `tests/conftest.py`, `tests/unit/`, `tests/fixtures/`. Save the four HTTP payloads already fetched during research as fixtures. | `pytest tests/unit/ -v` runs and collects zero tests without error. |
| **1b — `dataUtil.ExecSQL`** | The 3-line §4.5 change, **landed and verified on its own**, before the new handler exists. Add `tests/unit/test_dataUtil.py::test_exec_sql_*` (closes `TODOS.md` §1.4-adjacent work), then dry-run `handler.py`, `opt_handler.py`, `fx_handler.py`. | Isolating it means that if a snapshot handler misbehaves later, the cause is unambiguous. **Do not bundle this commit with the new handler.** |
| **2 — Handler + tests** | `port_assets_handler.py` and `test_port_assets_handler.py`, written together. Dry runs only, no layer needed — the local venv can run it. | All §7 unit tests green; dry run produces both CSVs with the expected shape. |
| **3 — Layer** | `requirements_port313.txt`, `Makefile` target, build, measure size, upload to AWS, record the ARN. | The zip imports cleanly under a Python 3.13 interpreter. |
| **4 — Deploy** | Table DDL run manually; `.env` keys added by the user; `serverless.yml` entry; `serverless print` → `serverless package` → `serverless deploy function -f portAssetsHandler`. | First real invocation seeds both `Port_name` sets; row counts verified in MySQL. |
| **5 — Document** | `HISTORY.md` + the four `doc/*.md` files; `TODOS.md` updated with the `ExecSQL`/SQLAlchemy-2.0 item. | §8 complete. |

---

## 7. Test section (mandatory per `CLAUDE.md` §3)

### 7.1 Must pass all existing verification

Because §4.5 modifies the **shared** `dataUtil.py`, this is no longer a purely additive change and the existing surface must be verified by running it, not by showing it is untouched.

**Mandatory dry runs — the three live `ExecSQL` callers.** Each still runs to completion on Python 3.10 with the pinned layer versions (`pandas==1.5.3`, `SQLAlchemy==1.4.46`, `numpy==1.26.4`, `yfinance==0.2.58`), from `Ops/fin-cron-data/`, with writes suppressed. Non-zero exit or a new stack trace is a failure.

| Handler | Dry-run switch | Golden CSV to diff |
|---|---|---|
| `handler.py` (`cronHandler`) | `LOCALRUN=localrun` env var | `snapshot_yf.csv` |
| `opt_handler.py` (`optHandler`) | `{"localrun": True}` | `options_list.csv`, `options_snapshot.csv` |
| `fx_handler.py` (`FXrateHandler`) | module-global `localrun` in `__main__` | `USD_FX.csv` |

Row count, column set, and dtypes must match the committed reference; no all-`NaN` columns, no zero-row output inside a live market window.

**The `DELETE` semantics are the specific thing at risk** and a CSV diff will not catch it — the dry runs suppress the write path entirely. So additionally: against a **non-production schema**, run one delete-then-append cycle through the new `ExecSQL` and confirm the row count before and after matches the pre-change behaviour. `Engine.begin()` commits explicitly where 1.4's legacy autocommit committed implicitly; that equivalence is asserted in §4.5 and must be *demonstrated* here.

**Other existing surface:**

- `serverless print` and `serverless package` from `Ops/fin-cron-data/` succeed with the new function present — the check that a **per-function `runtime:` override does not disturb the other nine**.
- `git diff` confirms no change to `requirements_cron.txt` or to any existing `*_handler.py`; the only shared-file change is the single `ExecSQL` body plus one import.
- The two dead callers (`yfin_handler.py`, `yfineod_handler.py`) are **not** dry-run — they are not deployed (`TODOS.md` §4.2). Stated explicitly so their absence reads as a decision, not an oversight.
- `Makefile`: the existing `finCron.zip` target and its `python3.10` path are untouched; only a new target is added.

### 7.2 Removal of obsolete verification

**None.** No handler, event flag, CSV output, symbol list, or stored-procedure dependency is retired by this change. No golden CSV is deleted.

### 7.3 New tests

`tests/unit/test_port_assets_handler.py` — a test file for a new Lambda is not optional. All fixture-driven; **no test touches the network, MySQL, or S3**.

| # | Target | Assertion |
|---|---|---|
| T1 | `parse_sp500_wikipedia()` | Saved HTML fixture → 503 rows; columns `Symbol`/`Sector`; `BRK.B` present in the form chosen in A2. |
| T2 | `parse_ndx_nasdaq_api()` | Saved JSON fixture → 103 symbols; the `data.data.rows` path is followed, not `data.rows`. |
| T3 | `parse_ndx_wikipedia()` | Saved HTML fixture → 103 tickers. Membership only — assert the ICB sector columns are **not** propagated into the output frame. |
| T4 | `parse_spy_holdings()` | Saved XLSX fixture → header correctly located on sheet row 4; the 3 leading metadata rows and any trailing cash/footer rows are dropped; as-of date parsed from `As of 30-Jul-2026`. |
| T5 | NDX sector | Every NDX row has `Sector == "N/A"` — the literal string, not `None`, not `NaN`, not `""`. Also asserted when the API payload happens to contain a non-empty `sector` value, so a future API change cannot silently start mixing taxonomies into the column. |
| T6 | `build_frame()` | Exactly the 8 table columns, in DDL order; `Class == 'Equity'`, `Type == 'Stock'`, `Rate == 1.0`, `Currency == 'USD'` on every row; `Date` is a `date`, not a `Timestamp`. |
| T7 | `sanity_gate()` | 503 → pass; 3 → fail; 520/95 boundaries behave as specified. |
| T8 | `has_changed()` | Identical sets → `False`. Symbol added → `True`. Symbol removed → `True`. **Sector-only change → `True`.** Row order differs only → `False`. `NaN` vs `None` sector → `False`. |
| T9 | `resolve_effective_date()` | All five rows of the §4.3 table, including `new < stored_max` → skip, and `new == stored_max` → delete-then-write. |
| T10 | Missing env vars | With `DBTRADING` / `TBLPORTASSETS` unset, `run()` raises a named error **before** any SQL is built. Explicitly required by `CLAUDE.md` §3.3 for new env vars. |
| T11 | Env defaults | With `SP500_PORT_NAME` / `PORT_ASSET_CLASS` unset, the documented defaults are used and the run proceeds. |
| T12 | `dbFlag=False` | Neither `DU.StoreEOD` nor `DU.ExecSQL` is called; both CSVs are still written. |
| T13 | `force=True` | Writes even when `has_changed()` returns `False`. |
| T14 | Network policy | Autouse fixture fails any test that opens a real socket (`TODOS.md` §0.6). |

`tests/unit/test_dataUtil.py` — **new file, Phase 1b**, covering the shared-module change:

| # | Target | Assertion |
|---|---|---|
| D1 | `ExecSQL()` executes | Against an in-memory SQLite engine: `CREATE` / `INSERT` / `DELETE` all take effect. Already prototyped and passing on the pinned 1.4.46. |
| D2 | `ExecSQL()` returns rowcount | `DELETE` affecting 2 rows returns `2`; previously always `None`. This is what the new handler's write verification depends on. |
| D3 | `ExecSQL()` commits | A second, independent connection sees the change — the explicit-`begin()`-vs-legacy-autocommit equivalence from §4.5, pinned as a test rather than left as an argument. |
| D4 | Error contract preserved | Invalid SQL → logs at `ERROR` and returns `None`; **does not raise**. `TODOS.md` §1.7 — callers all over the repo rely on this. |
| D5 | No deprecated API | `ExecSQL` runs without emitting `RemovedIn20Warning` (`pytest.warns(None)` / `filterwarnings("error")`). This is the test that actually proves the 2.0-readiness claim. |

Harness pieces built here, scoped to be the **start of `TODOS.md` §0** rather than a parallel harness: `pytest.ini` (`pythonpath = Ops/fin-cron-data`), and the `env`, `reset_dbconn` (autouse), `mock_engine`, `sqlite_engine`, `chdir_handler_dir`, `frozen_ny_time`, `no_network` fixtures in `tests/conftest.py`.

### 7.4 Non-unit verification

| Check | Detail |
|---|---|
| Local dry run | `cd Ops/fin-cron-data && python port_assets_handler.py` with `{"localrun": True, "dbFlag": False, "test": True}`. Expect: 503-ish SP500 rows, 103-ish NDX100 rows, zero stack traces, non-zero exit is a failure. |
| Golden CSV — new | Commit the dry-run output as `Ops/fin-cron-data/portfolio_assets_info.csv`. It becomes the reference for `TODOS.md` §3.5. There is **no prior golden CSV for this handler to diff against** — this change creates the baseline, stated explicitly so the absence is not read as a skipped check. |
| Golden CSV — existing | The `ExecSQL` change *does* have prior references: `snapshot_yf.csv`, `options_list.csv`, `options_snapshot.csv` are diffed against the Phase 1b dry runs per §7.1. |
| Config validation | `serverless print` (parses, all nine existing handlers still resolve) and `serverless package` (the deploy-affecting check, since a per-function runtime override is new to this service). |
| Layer validation | Build `finPort313.zip`, unzip, and import `pandas`, `sqlalchemy`, `pymysql`, `lxml`, `openpyxl` under a real 3.13 interpreter. `TODOS.md` §3.4's rule — a layer change is never verified by a redeploy alone. |
| DB write validation | First write against a **non-production schema** (`Trading_test` or equivalent), not `Trading`. Confirm 606-ish rows land, the composite PK holds, and a second immediate run inserts **zero** rows (the only-on-change path). |
| Post-deploy check | After the first scheduled invocation: CloudWatch shows no errors, and `SELECT Port_name, max(Date), count(*) FROM Trading.portfolio_assets_info GROUP BY Port_name` returns two rows with plausible counts. |
| Source-drift regression | Re-run T1–T4 against **live** fetches as a marked `@pytest.mark.integration` test, run manually. This is the early warning for a Wikipedia table restructure or a Nasdaq API change — the same failure mode `usrate_handler.py` has no guard against today. |

### 7.5 Documentation of tests

Every test above is recorded in `HISTORY.md` under *Test coverage*, in `doc/OPERATIONS.md` (dry-run procedure, layer build, new env vars), and in `doc/API-REFERENCE.md` (event contract, table columns). `TODOS.md` §0 is updated to mark which scaffolding items this change completed.

---

## 8. Documentation obligations

`HISTORY.md` and `doc/` **do not exist** in this repo. `CLAUDE.md` states the first change under the mandatory rules creates them — this is that change.

| File | Scope for this change |
|---|---|
| `HISTORY.md` | Create; first entry covers this function (goal, implementation, related files, test coverage). |
| `doc/TECHNICAL-DESIGN.md` | Create. Full deployed-functions table (all ten), a deep-dive section for `portAssetsHandler`, the `dataUtil` API section **including the changed `ExecSQL` signature and its new return value**, and the `Trading.portfolio_assets_info` schema. Existing handlers get a **one-line row each** rather than full deep-dives — backfilling nine deep-dives is its own task and should not be smuggled into this one. |
| `doc/OPERATIONS.md` | Create. Env-var table including the eight new keys, the `finPort313` layer build/upload procedure, the **3.10-vs-3.13 split** and why, schedules, and the dry-run procedure for all ten handlers. |
| `doc/PRODUCT-GUIDE.md` | Create. Dataset entry for index membership: what it covers, only-on-change cadence, how to write an as-of query, and the fact that **`Sector` is populated for `SP500` only** — `NDX100` reads `N/A`. |
| `doc/API-REFERENCE.md` | Create. Event contract for `portAssetsHandler`, the full column reference for `portfolio_assets_info` (marked as from DDL, not inferred), the source-payload shapes, and `ExecSQL`'s changed return type (`int` on success, `None` on failure). |
| `TODOS.md` | (a) **Close** the `ExecSQL` / SQLAlchemy-2.0 item — fixed here rather than deferred, removing one blocker from §5.2; (b) update §1.8 (fork sprawl) to record that a fourth `dataUtil` fork was considered and rejected; (c) tick off the §0 scaffolding items completed in Phase 1. |

Creating four repo-wide docs is a real chunk of work sitting behind a ~250-line handler. It is called out here rather than discovered in Phase 5 — if the scope should be trimmed to `HISTORY.md` plus stubs, that is a decision to make now.

---

## 9. Risks

| Risk | Likelihood | Mitigation |
|---|---|---|
| Wikipedia table restructure → wrong or empty parse | Medium (it already happened once — the NDX table moved pages) | Sanity gate on row count; cross-check against a second source; integration test in §7.4. |
| Nasdaq API blocks the Lambda IP or requires new headers | Medium — AWS egress IPs are filtered harder than residential | Wikipedia `List_of_NASDAQ-100_companies` is the automatic fallback; the source used is logged every run. |
| Wikipedia blocks the Lambda UA | Low | Set a descriptive `User-Agent` per Wikimedia's bot policy. Never the default `python-requests/x` UA. |
| A2 wrong → the table cannot be joined to price history | **High impact**, low likelihood | Phase 0 gate. Resolving it after data is written means a `UPDATE`/reload of every stored set. |
| 3.13 layer picked up by a 3.10 function | Low | Layer ARN declared on the new function only, never at provider level. `serverless package` inspects the rendered config before deploy. |
| `StoreEOD` silently swallows a write failure | **Certain if it occurs** | Post-write row-count verification with a loud log; delete-then-append for idempotency. |
| Layer exceeds the unzipped size limit | Low | Measured in Phase 3; `openpyxl` + `beautifulsoup4` are the drop levers. |
| **`ExecSQL` change breaks a live snapshot handler** | Low likelihood, **high impact** — `cronHandler`, `optHandler`, `FXrateHandler` all `DELETE` before appending, so a `DELETE` that silently stops committing would duplicate a snapshot table on every run | Landed as its own commit in Phase 1b, ahead of the new handler. Covered by D1–D5 plus three dry runs and a non-production delete-then-append cycle (§7.1). The transactional-commit equivalence is the specific thing being tested, not assumed. |

---

## 10. Not in scope

- Migrating the nine existing functions to 3.13. (Their `dataUtil.py` dependency is unblocked by the §4.5 fix, but the `pandas` 1.5→2.x and `numpy` 1.x→2.x jumps in each handler are the real work — `TODOS.md` §5.2.)
- Any further `dataUtil.py` work: the error-swallowing contract (`TODOS.md` §1.7), `load_df`'s dead `pd.load_csv` typo at line 148, and consolidating the three existing forks (§1.8) all stay untouched. Only `ExecSQL` changes.
- Index **weights** — `Rate` is the FX rate to USD (`1.0`), so no weight source is needed. This is why the SPY holdings file is only a membership cross-check and Invesco/QQQ being unavailable does not block anything.
- Any non-US index, and any non-USD currency handling.
- Backfilling historical membership. The first run seeds a single dated set; there is no history before it.
