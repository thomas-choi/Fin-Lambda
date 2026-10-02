# fin-deep-data

The python3.13 Serverless service: the PLAN-SR-UPSTREAM Phase A–F collectors.

Separate from `Ops/fin-cron-data` on purpose — own `serverless.yml`, `.env`,
layers, `dataUtil.py` and pytest root. Nothing here can change a function or a
layer of that service, and a `serverless deploy` from this folder only ever
touches the eight functions below.

## Functions

| Function | Handler | Schedule (ET) | Writes |
|---|---|---|---|
| `eodDaily` | `eod_daily_handler.run` | 18:30 Mon–Fri, sweep 19:00 | `$EOD_WRITE_TBL`, `corp_action_daily` |
| `optChainEOD` | `optchain_eod_handler.run` | dispatch 17:40, sweeps 18:40 / 19:40 | `$OPT_WRITE_TBL`, raw chains to R2 |
| `statusReport` | `status_report_handler.run` | 20:00 Mon–Fri | SNS e-mail + R2 JSON |
| `portAssetsHandlerv2` | `port_assets_handler.run` | 18:30 | `Trading.portfolio_assets_info` |
| `usrateHandlerv2` | `usrate_handler.run` | 17:05 | `$TBLUSRATES` |
| `FXHistHandlerv2` | `fxeod_handler.run` | 17:10 | `$TBLHISTFX` |
| `yfus30minEODv2` | `intraday_min_handler.run_us` | 20:05 | `$TBLMINUTEPRICE` |
| `yfasia30minEODv2` | `intraday_min_handler.run_asia` | 06:00 | `$TBLMINUTEPRICE` |

All eight also write `GlobalMarketData.load_audit`. The schedule column is when
each fires **once enabled**.

**Every schedule ships `enabled: false`.** A deploy creates all eight functions
and all eleven schedules and runs nothing. Verify one by invoking it, then set
`enabled: true` on its schedule(s) and redeploy — `doc/OPERATIONS.md` §10.4.1:

```bash
serverless invoke -f eodDaily -d '{"dbFlag":false,"test":5}' --log
```

The five `v2` functions carry a second condition: each writes a table its
still-live `fin-cron-data` counterpart writes, so enable one only as part of the
per-data-set cutover in `doc/OPERATIONS.md` §11.2.

`optChainEOD` carries a third: its `reservedConcurrency` is commented out because
the account's total Lambda concurrency quota is 10 and reserving needs 100
unreserved left over. Until that quota is raised nothing caps the shard fan-out,
and the shards share those 10 slots with the live python3.10 functions — raise the
quota and restore the line before enabling it (`doc/OPERATIONS.md` §8.4).

First deployed 2026-10-01: eight functions on python3.13, eleven schedules all
`DISABLED`, SNS subscription awaiting confirmation.

Do not turn `enabled` into an `${env:...}` lookup — only the literal
`false`/`true` work, and `0`, `yes`, `True` or an empty value all render
`ENABLED` with the warning suppressed.

## Layers

Build core first — the other two are de-duplicated against it and will not
import without it.

```bash
make finDeep            # from the repo root; or ./build_layers.sh [core|yf|web]
```

| Layer | Contents | Unzipped | Zipped |
|---|---|---|---|
| `finDeepCore` | pandas, numpy, SQLAlchemy, PyMySQL, python-dotenv, pytz, requests | 101 MB | 29 MB |
| `finDeepYf` | yfinance 0.2.58 + deps | 28 MB | 10 MB |
| `finDeepWeb` | lxml, beautifulsoup4, openpyxl | 14 MB | 6 MB |

Publish each with `aws lambda publish-layer-version --compatible-runtimes
python3.13` and put the versioned ARNs in `.env` as `FINDEEPCORE_LAYER_ARN`,
`FINDEEPYF_LAYER_ARN`, `FINDEEPWEB_LAYER_ARN`. There are no defaults: a wrong
layer deploys fine and fails on the first import.

That one step needs the **AWS CLI** (`doc/OPERATIONS.md` §10.1 has a user-local
install); `serverless deploy` does not — it reads `~/.aws/credentials` itself.

## Local use

```bash
cp .env.example .env                 # then fill it in
npm install                          # serverless-dotenv-plugin

# tests -- from THIS folder (two pytest roots; see pytest.ini)
../../venv-py313/bin/python -m pytest tests/unit -v

# dry runs -- dbFlag=False everywhere, output CSVs are gitignored
../../venv-py313/bin/python eod_daily_handler.py
../../venv-py313/bin/python intraday_min_handler.py asia

serverless print && serverless package
```

`dbFlag` is the off-switch, not `localrun`. Every `__main__` here passes
`dbFlag: False`; keep it that way — on 2026-10-01 a `__main__` with
`dbFlag: True` wrote four index membership sets to production.

## Files

| File | Role |
|---|---|
| `dataUtil.py` | the only DB layer. A **fork** of `fin-cron-data/dataUtil.py` at `fa1d6ac` plus the Phase A helpers; changes do not propagate either way |
| `intraday_min_handler.py` | one module for both intraday functions; the market difference is in `MARKETS` |
| `build_layers.sh` | cross-builds, prunes and de-duplicates the three layers; enforces the 80 MB zip ceiling |
| `sql/upstream_tables.sql` | idempotent DDL for `load_audit`, `corp_action_daily`, `v_load_status` and the two shadow tables |
| `stock_exchange.csv`, `Exchange_timezone.csv`, `intra_blacklist.csv` | packaged symbol → exchange → time zone maps, read through `dataUtil.list_dir()` |
| `requirements_deep_{core,yf,web}.txt` | one per layer; also what the dev venv installs |

Full documentation: `doc/TECHNICAL-DESIGN.md` §6, `doc/OPERATIONS.md` §10–11,
`doc/API-REFERENCE.md` §7, `doc/PRODUCT-GUIDE.md`.
