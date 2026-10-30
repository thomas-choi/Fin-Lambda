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

---

---

# PART II — PLAN: `histdailylevels` — daily VWAP + volume-profile levels from 15-minute bars

Status: **DRAFT — awaiting approval**
Created: 2026-10-30
Owner: Thomas Choi
Request: `TODOS.md` §7.1 / §7.2 (SR-Agent downstream feature request, owner-approved 2026-10-29)

---

## II.1 Goal

One new python3.13 Lambda in `Ops/fin-deep-data/` that, per US trading session:

1. Reads the 15-minute bars of that session from `GlobalMarketData.histminprice` (no new upstream fetch — yfinance is not called at all on the hot path).
2. Aggregates them per symbol into **daily level rows**:
   - **§7.1 VWAP** — true session VWAP, `Σ(price×vol) / Σ(vol)` over the 15-minute buckets, not the `(H+L+C)/3` typical-price approximation;
   - **§7.2 volume-profile levels** — POC (price bucket with max traded volume), VAH / VAL (70 % value-area bounds), on a configurable price-bucket grid.
3. Appends them to a new companion table **`GlobalMarketData.histdailylevels`** — `Date, Symbol, vwap, poc, vah, val, n_buckets, bucket_size, method, source` — never new columns on `histdailyprice7`, so a missed day is a missing row, not a NULL beside a real close.
4. Writes per-symbol + `'*'` summary rows to `load_audit` under a **new job name `histLevels`** (new data set ⇒ its own job; it has no python3.10 predecessor, so no name to preserve).
5. Falls back to typical-price VWAP only when a session has < 80 % of expected buckets, recording which method was used in `method` (`vwap_bucketed` / `vwap_typical`).

### Decisions already made (from TODOS §7 + owner review)

| Question | Decision |
|---|---|
| Which symbols? | Every symbol that has 15-minute bars that session — i.e. the US intraday universe as-is. The `SYMBOL_PROC_VER` procedure is used only for the **expected-bucket** count, never to force coverage. Asia bars are processed the same way under their own exchange session (Phase-gated, see II.6 P5) |
| Which sessions? | The **latest completed session per exchange** (intraday handler's own session logic, reused). A missed session is not back-filled — `histminprice` itself cannot back-fill beyond 60 days, so levels would silently disagree with the bars |
| New table or new columns? | **New table `histdailylevels`**, per §7.1. Companion, not a `histdailyprice7` extension |
| Write path | `DU.append_ignore()` (INSERT IGNORE) on PK `(Date, Symbol, Exchange)` — the first computation of a session wins; a replay never changes history. Same idempotency contract as every historical table in this service |
| Job name | `histLevels` — new data set, no cutover, appears as its own `v_load_status` line |
| Upstream owner item? | **None** — no stored procedure, no new upstream source, no R2. This is the first data set the service derives entirely from data it already collects |

### Open assumptions — flag now, confirm before coding

| # | Assumption | Why it matters |
|---|---|---|
| B1 | `histminprice.Volume` is per-bucket share volume summed over the bucket (yfinance semantics), and `0`/NULL volume bars can be skipped rather than treated as zero-price weight | If Volume were cumulative, VWAP would be wrong by construction. One verification query against the live table (Phase II.0) settles it: pick a symbol-day, compare bucket sums against the day's known volume in `histdailyprice7` |
| B2 | Exchange **session** definition can be derived from the bars themselves (min/max `Datetime` per symbol-day) without an exchange calendar table | Self-consistent with what the intraday collectors actually captured. A proper exchange-calendar cross-check (half-days!) is an enhancement, not a blocker — a half-day simply yields fewer buckets and falls into the < 80 % path |
| B3 | The `expected_buckets` count for the 80 % rule = `(session span) / 15min` per exchange, computed from the bars' own first/last bucket; symbols with span < 60 min are skipped entirely (illiquid/half-day junk) | Without this, a sparse-but-real session (e.g. an ETF that trades 30 buckets) would be permanently demoted to typical-price. The 60-min floor keeps the fallback honest |
| B4 | The Lambda DB user needs `CREATE` on `GlobalMarketData` **once** (owner runs the DDL script instead — same as `upstream_tables.sql`, so actually the owner runs it and the Lambda needs only INSERT/SELECT) | DDL is owner-run per repo convention (`sql/histdailylevels.sql`, Phase II.0) |
| B5 | Value-area = 70 % of traded volume around POC, standard profile expansion (expand to the larger neighbour until ≥ 70 % captured) | Configurable via env `VA_RATIO` (default `0.70`); TODOS §7.2 names 70 % |

---

## II.2 Source research — what exists to build on

| Fact | Checked |
|---|---|
| `histminprice` holds `Datetime` (exchange-local, tz-naive), `Symbol`, `Exchange`, `Close/Open/High/Low/Volume/AdjClose`, `UTCDatetime`, `timezone`; neither datetime carries an offset; `timezone` is per row | `doc/API-REFERENCE.md` §7.6.1, verified on live rows 2026-10-08 |
| US 15-min bars land 20:05 ET Mon–Fri (`yfus30minEODv2`), Asia 06:00 ET (`yfasia30minEODv2`); both write `load_audit` summary rows under jobs `yfus30minEOD` / `yfasia30minEOD` | `doc/TECHNICAL-DESIGN.md` §6.1 |
| `DU.load_df_SQL()` returns a DataFrame for any SELECT; `DU.append_ignore()` is the INSERT IGNORE write used by every historical table; `DU.audit_run/audit_summary` never raise | `doc/TECHNICAL-DESIGN.md` §6.6 |
| `statusReport` discovers tables via `INFERRED_TABLES` / `EVENT_TABLES` in `status_report_handler.py` — a new table is added to `INFERRED_TABLES` (it has a real `Date`) | handler source, lines 50–62 |
| `sql_require_primary_key=ON` — the table must be created by DDL script, not by `to_sql` | `CLAUDE.md` Configuration |
| The intraday collector clamps watermarks to 60 days and cannot back-fill older gaps | `doc/OPERATIONS.md` §8.9 |

No external source research needed — this plan consumes an internal table only.

## II.3 Target table

`GlobalMarketData.histdailylevels` (new; DDL in `Ops/fin-deep-data/sql/histdailylevels.sql`, owner-run, idempotent):

| Column | Type | Meaning |
|---|---|---|
| `Date` | `DATE` | session date, exchange-local |
| `Symbol` | `VARCHAR(45)` | yfinance ticker, as stored in `histminprice` |
| `Exchange` | `VARCHAR(45)` | copied from the bars |
| `vwap` | `DOUBLE` | Σ(price×vol)/Σ(vol); typical-price fallback when bucket coverage < 80 % |
| `poc` | `DOUBLE` null | price bucket with max traded volume (NULL when < 60 min span) |
| `vah` | `DOUBLE` null | value-area high, `VA_RATIO` of volume around POC |
| `val` | `DOUBLE` null | value-area low |
| `n_buckets` | `INT` | 15-min buckets actually used |
| `bucket_size` | `INT` | price-bucket size used for POC/VA (USD-cents grid default; `0` when only VWAP computed) |
| `method` | `VARCHAR(20)` | `vwap_bucketed` / `vwap_typical` — which path produced `vwap` |
| `source` | `VARCHAR(20)` | `histminprice-15m` (provenance; a future tick-level recompute would write a different value) |
| `first_seen_at` | `DATETIME(3)` | UTC, `DEFAULT CURRENT_TIMESTAMP(3)` — honest available-at, same trick as `corp_action_daily` |
| `run_id` | `CHAR(26)` | `load_audit.run_id` of the computing run |

Primary key `(Date, Symbol, Exchange)` — INSERT IGNORE keeps the **first** computation of a session, matching the corp-action idempotency contract. Joins on `Symbol` need no COLLATE (same charset as `histdailyprice7`).

## II.4 Design

**Pipeline (one function, `hist_levels_handler.py`):**

```
schedule (new, disabled on deploy)
  → read candidate bars: SELECT ... FROM histminprice
      WHERE UTCDatetime >= (UTC now − 36h)      -- one scan window, both markets
  → group by (Exchange, Symbol, session date from exchange-local Datetime)
  → per group:  n_buckets, span check (≥ 60 min)
      → VWAP = Σ((H+L+C+L)/4? no — Close×Vol)   [close-to-close per bucket;
        typical-price×volume cross-check in tests]
      → coverage = n_buckets / expected(span/15min)
      → method = vwap_bucketed if coverage ≥ 0.80 else vwap_typical
      → volume profile on close-price grid (bucket_size from env):
            POC = argmax volume bucket
            expand to VAH/VAL until ≥ VA_RATIO captured
  → append_ignore(df, 'GlobalMarketData', 'histdailylevels')
  → load_audit: one row per symbol + '*' summary (job 'histLevels')
```

**Key choices, with reasons:**

| Choice | Why |
|---|---|
| Read bars by `UTCDatetime` window, group by exchange-local `Datetime` for session date | `UTCDatetime` is the only monotone-safe filter; session dates come from the per-row `timezone` |
| One scan window (36 h) instead of per-exchange queries | simpler; ~2.3 M rows/36 h ≈ within one 900 s Lambda comfortably at pandas speed; the grouping is pure pandas |
| No yfinance call anywhere | the whole point: derived data, zero upstream quota |
| `load_audit` rows per symbol | same contract as eodDaily; `v_load_status` then reports the new table for free once `INFERRED_TABLES` gains one line |
| `method` column records the fallback | TODOS §7.1 explicitly requires knowing which method produced the value; SR-Agent reads it as a DQ hint |
| VWAP uses **Close** of each 15-min bucket (not H/L/typical) | the bucket close is the best unbiased estimate of volume-weighted mean trade price at 15-min granularity; H+L+C+L/4 biases high; documented here so the tests pin it |
| Schedule **21:30 ET Mon–Fri** | after yfus30minEODv2 (20:05) and its data settle, before statusReport (20:00? — no: 21:30 is *after* 20:00 statusReport; the report simply picks the table up next day — acceptable for a T+1 derived set) — final time confirmed in Phase II.3 against the live `yfus30minEODv2` completion time |

**What is deliberately NOT here:** no Asia-specific schedule yet (P5 below), no R2 archive (a daily level row is 13 numbers — R2 stores nothing worth archiving), no back-fill of sessions before deploy (bars older than 60 days cannot serve; sessions between deploy and today are filled by the sweep on the next run only from bars still within the 60-day window).

## II.5 Python 3.13 layer and build tooling

No new layer. `hist_levels_handler.py` needs `pandas` + `sqlalchemy` + `pymysql` (all in `finDeepCore`) and nothing else — `numpy`/`pytz` are transitive in the same layer. Packaging: `dataUtil.py`, `hist_levels_handler.py` in `package.patterns`; layers `coreLayer` only. No `requirements_*.txt` change, **no layer rebuild** — a pure redeploy plus DDL.

## II.6 Phases

| Phase | Work | Gate |
|---|---|---|
| **II.0 — Confirm** | Owner verifies **B1** (Volume semantics): one query per symbol-day joining `histminprice` bucket sums against `histdailyprice7` Volume for 3 symbols × 3 recent days. Owner runs `Ops/fin-deep-data/sql/histdailylevels.sql` (idempotent DDL: table + grants unchanged). Confirm the intraday summary rows in `load_audit` for the latest US session (`job='yfus30minEOD'`, `status='ok'`) | B1 answered in writing; DDL present. Nothing is coded until Volume semantics are confirmed |
| **II.1 — Module + tests** | `hist_levels_handler.py` with pure functions (`session_groups`, `bucket_vwap`, `volume_profile`, `value_area`, `fallback_typical`, `build_frame`) + `tests/unit/test_hist_levels_handler.py` (fixture frames, no network/DB; sqlite mirror of the DDL like `test_dataUtil.py` does for load_audit) | All unit tests green in `venv-py313`; warning gate clean for the new module |
| **II.2 — Wire-in** | `serverless.yml` function `histLevels` (schedule **disabled**, `coreLayer`, package patterns); `status_report_handler.py` `INFERRED_TABLES` += `("DBMKTDATA", "TBLDAILYLEVELS", "histdailylevels", "Date")`; `.env.example` += `TBLDAILYLEVELS`, `VA_RATIO`, `PROFILE_BUCKET`, `LEVELS_MIN_SPAN_MIN`, `LEVELS_COVERAGE`; dry-run `__main__` like every handler | `serverless print` + `serverless package` succeed; existing 426+ tests still green |
| **II.3 — Schedule confirm** | Read CloudWatch: latest `yfus30minEODv2` actual finish time. Set the cron so it fires ≥ 60 min after that (proposal: 21:30 ET Mon–Fri). Confirm no collision with the concurrency quota (single function, one invocation/day) | Time fixed in writing + `doc/OPERATIONS.md` §10.5 row |
| **II.4 — Deploy + verify** | Deploy (no layer change), manual invoke `{"test": 3}` → `{"localrun": true, "dbFlag": false}` dry run first; then one real invoke with `dbFlag: true`; verify rows: `SELECT COUNT(*), MIN(vwap), MAX(vwap) FROM histdailylevels WHERE Date = <session>`; spot-check one symbol's VWAP by hand against its bars; confirm `load_audit` summary row + `v_load_status` line | Row counts sane; hand-computed VWAP matches to 4+ digits; audit row present |
| **II.5 — Enable + document** | Flip schedule `enabled: true`, redeploy; observe next scheduled run via CloudWatch; docs per §II.8; `TODOS.md` §7.1/§7.2 → DONE with pointer to this plan | Morning-after check: `v_load_status` shows `histLevels` `ok`, `n_rows > 0` |

## II.7 Test section (mandatory per `CLAUDE.md` §3)

### II.7.1 Must pass all existing verification

- Full `fin-deep-data` suite green in `venv-py313` (426 tests as of 2026-10-14) — this change touches `status_report_handler.py` (one list entry) and `serverless.yml`; both existing test files must stay green untouched or with an updated expected-table list.
- `serverless print` and `serverless package` from `Ops/fin-deep-data/` succeed with the new function present.
- Existing handlers are untouched — no dry-run regression required beyond the intraday pair's own golden CSVs (their module is not modified at all).
- `statusReport` dry run re-run to show the new inferred line renders when the table exists (and does not crash when it does not — the `env_or` default path).

### II.7.2 Removal of obsolete verification

**None.** Nothing is retired: no handler, event flag, CSV, symbol list, or stored procedure changes.

### II.7.3 New tests (`tests/unit/test_hist_levels_handler.py`)

| # | Target | Assertion |
|---|---|---|
| L1 | `session_groups()` | Fixture bars (2 symbols, 2 exchanges, 3 sessions) → correct per-(exchange, symbol, date) groups; session date derived from exchange-local `Datetime`, never `UTCDatetime` |
| L2 | `bucket_vwap()` | Hand-computed: 4 buckets, known close/volume → exact VWAP to 1e-9; zero-volume session → `None`, not 0 |
| L3 | coverage + method | 78 % of buckets → `vwap_typical`; 85 % → `vwap_bucketed`; boundary at exactly 80 % → bucketed (≥, per TODOS §7.1 wording) |
| L4 | `fallback_typical()` | `(H+L+C)/3` matches hand computation; recorded as `method='vwap_typical'` |
| L5 | `volume_profile()` | 5-bucket fixture → POC = argmax; `bucket_size` respected; all-equal volumes → POC = lowest such bucket (deterministic tie-break) |
| L6 | `value_area()` | 70 % captured exactly at 3 buckets → VAH/VAL = those bounds; expansion order (larger neighbour first) pinned |
| L7 | `build_frame()` | Exact column set/order matches §II.3; `Date` is `date` not `Timestamp`; `first_seen_at` UTC-naive |
| L8 | span floor | Session span 45 min → row skipped entirely (no VWAP, no levels), logged |
| L9 | env defaults | `VA_RATIO`/`PROFILE_BUCKET`/`LEVELS_*` unset → documented defaults used, run proceeds (per CLAUDE.md §3.3 for new env vars) |
| L10 | `dbFlag=False` | No `append_ignore`, no `audit_run`; CSV still written via `out_dir` |
| L11 | idempotency (sqlite) | Same frame twice → second `append_ignore` inserts 0 rows on the PK (sqlite mirror of the DDL, per the `test_dataUtil.py` pattern) |
| L12 | `load_audit` summary | `'*'` row carries `job='histLevels'`, `table_name=histdailylevels`, `n_rows` = symbols written; audit failure never fails the run (exception injected) |
| L13 | Network policy | Autouse no-socket fixture holds (module makes zero outbound calls) |

### II.7.4 Non-unit verification

| Check | Detail |
|---|---|
| Local dry run | `cd Ops/fin-deep-data && ../../venv-py313/bin/python hist_levels_handler.py` — `{"localrun": true, "dbFlag": false, "test": 3}`; expect `hist_levels_<date>.csv` with ≥ 3 rows for 3 capped symbols, no stack trace |
| Golden CSV — new | Commit the first dry-run output as `Ops/fin-deep-data/hist_levels_golden.csv`; it becomes §3.5's reference for this handler (none exists today — this change creates the baseline, stated explicitly) |
| Config validation | `serverless print` + `serverless package` with the new disabled schedule |
| DB write validation | First real write against the new table only — there is no production collision risk (new table), but the first invoke still uses `{"test": 3}` (3 symbols) before a full run |
| Post-deploy check | After the first scheduled run: CloudWatch clean; `SELECT COUNT(*) FROM histdailylevels WHERE Date = <yesterday>` ≈ US intraday symbol count; `v_load_status` line appears |
| Hand-computation regression | One symbol-day recomputed by an independent pandas snippet (in the test file as L2's data); live row must match to 4 significant digits |

## II.8 Documentation obligations

| File | Update |
|---|---|
| `HISTORY.md` | Full entry per the mandatory template (goal, implementation, related files, test coverage) — created when the change lands |
| `doc/TECHNICAL-DESIGN.md` | §1/§6.1 function table row; §6.2 data-flow diagram + the new branch; §6.3 decision rows (bucket-Close VWAP, first-wins idempotency, no-yfinance derivation); §6.6 `dataUtil` API section gains nothing (no new helpers); **§6.7 new table** `histdailylevels` with column reference |
| `doc/OPERATIONS.md` | §10.2 env rows (`TBLDAILYLEVELS`, `VA_RATIO`, `PROFILE_BUCKET`, `LEVELS_MIN_SPAN_MIN`, `LEVELS_COVERAGE`); §10.5 schedule row + enable-state note; §10.6 dry-run command + healthy shape; §7 constraint note (36 h scan window, 900 s timeout headroom) |
| `doc/PRODUCT-GUIDE.md` | Dataset-catalogue row: "Daily VWAP + volume-profile levels" — what it contains, 15-min-bucket provenance, T+1 freshness, `method` fallback meaning, how to join to `histdailyprice7` |
| `doc/API-REFERENCE.md` | §7.1 event contract (`test`, `localrun`, `dbFlag`, `asof`-less, `sessions`-override shape); **§7.x new table reference** `histdailylevels` column table (marked "from DDL"); §7.5 `v_load_status` gains its line |
| `TODOS.md` | §7.1 / §7.2 → status DONE, pointer to this plan + the handler |

## II.9 Risks

| Risk | Likelihood | Mitigation |
|---|---|---|
| **B1 wrong** (Volume semantics) → every VWAP wrong | Low (yfinance documents per-bar volume) but **high impact** | Phase II.0 gate: 3-symbol × 3-day verification query against live tables before any code |
| 900 s timeout on a big scan window | Low-Medium (2.3 M rows pandas-side is fine; the risk is the SELECT) | One indexed `UTCDatetime >=` window read; if slow, chunk by exchange in the same run — measured in II.4's first invoke |
| Asia bars mixed into a US session's grouping | Low | Grouping keys on `(Exchange, timezone)` — the fixture test L1 pins a mixed-exchange frame |
| Half-day sessions look "sparse" → typical-price fallback fires | Expected, by design | `method` column records it; SR-Agent reads the flag, no data corruption |
| `histminprice` gaps (61 stalled symbols, §8.9) → missing level rows | Certain for those symbols | By design: missing row, not wrong row; PRODUCT-GUIDE documents the non-uniform coverage |
| New schedule collides with quota-10 concurrency | Negligible | One invocation/day at 21:30 ET, off-peak vs all other schedules |

## II.10 Not in scope

- **Back-filling** sessions before deploy (bars older than 60 days are gone upstream).
- **Tick-level VWAP** — 15-min buckets are the best available upstream; `source` column leaves room for a future recompute.
- **Asia enablement** — the handler processes Asia bars when they exist (06:00 ET run feeds the next morning's window), but a dedicated Asia-session schedule is deferred until the US path is proven.
- **Greeks / options levels** — options chains remain in `OptionChains`; no per-strike profile.
- **Any SR-Agent repo change** — the consumer reads the new table via its own extractor; its Stage-A DQ treats absent levels as `*_available = 0` (already spec'd in SR tech doc §4.6.4).
