# NOTES — porting the last five functions to fin-deep-data (paused 2026-10-11)

Plan: `/home/thomas/.claude/plans/create-a-plan-to-agile-micali.md`.
Full record of the work: the `HISTORY.md` entry *2026-10-11 — the last five
python3.10 functions ported* (inside the stash, see below).

## Where the work is right now — read before touching anything

The change is split in two, so **neither half works without the other**:

| Half | Where | What |
|---|---|---|
| Tracked edits (14 files) | **`stash@{0}`** — "other half of functions migrate to Python 3.13" | `dataUtil.py` (`replace_snapshot`, `r2_client`/`r2_exists`/`r2_put_json`), `optchain_eod_handler.py`, `serverless.yml` (5 functions), `.env.example`, `tests/conftest.py`, `tests/unit/test_dataUtil.py`, `HISTORY.md`, `CLAUDE.md`, `TODOS.md`, `PLAN-SR-UPSTREAM.md`, the four `doc/*.md` |
| New files (9) | **working tree, untracked** | `fx_snapshot_handler.py`, `fff_handler.py`, `news_handler.py`, `opt_snapshot_handler.py`, `stock_snapshot_handler.py`, `tests/unit/test_snapshot_handlers.py`, `test_fff_handler.py`, `test_news_handler.py`, `tests/fixtures/ff3_factors_sample.csv` (all under `Ops/fin-deep-data/`) |
| Local config | `Ops/fin-deep-data/.env` (gitignored, **not** stashed) | already has the 8 new keys |

The untracked handlers call `DU.replace_snapshot` / `DU.r2_client`, which exist
only in the stash, so the suite fails until the stash is back.

**To resume:**

```bash
cd /home/thomas/projects/Fin-Lambda
git stash list                      # confirm stash@{0} is "other half of functions migrate..."
git stash pop stash@{0}             # NOT stash@{1}, which is an older, unrelated stash
cd Ops/fin-deep-data && ../../venv-py313/bin/python -m pytest tests/unit -q   # expect 416 passed, 1 skipped
```

If anything is committed on `main` in the meantime, the pop can conflict in
`HISTORY.md` and the four `doc/*.md` files (all edited near their top).

## What is done

All five written, unit-tested, and dry-run against live data with writes off.
Every new schedule is `enabled: false`. **Nothing deployed, nothing committed,
nothing in `Ops/fin-cron-data` changed.**

| New function | Module | Dry-run result (2026-10-11) |
|---|---|---|
| `FXrateHandlerv2` | `fx_snapshot_handler.py` | 15 of 17 currencies (INR, IDR had no bars yet on a Sunday evening) |
| `fffHandlerv2` | `fff_handler.py` | 34 new months, 2023-11 … 2026-08 (`famaFrench` stops at 2023-10-31) |
| `yfNewshandlerv2` | `news_handler.py` | 241 articles for 5 US tickers in 34 s; second run 0 new in 0.4 s |
| `optHandlerv2` | `opt_snapshot_handler.py` | the same 71 contracts as live `options_snapshot`; 23 s vs 111 s |
| `cronHandlerv2` | `stock_snapshot_handler.py` | 795 rows ⊇ all 787 live symbols; 88 s vs 517 s |

Checks passed: 416 tests (was 343), warning gate clean for every new/changed
module, `serverless package` renders the 5 new schedules `DISABLED`, layer
import test passes on core+yf and core (no layer rebuild needed).

## Departures from the plan / from the old behaviour

- **Stock snapshot:** the planned batched `yf.download` was slower (110–155 s at
  any thread count; Yahoo caps the request rate, ~8–10/s). Now one `Ticker.info`
  per symbol on 8 threads, since `info` carries the day's OHLCV.
- **Snapshot writes:** DELETE + insert in one transaction; an empty/failed fetch
  keeps the previous snapshot. Side effect: a currency with no trade yet that day
  is absent from `FX_snapshot` until it trades.
- **Fama-French:** Ken French has revised history (2023-10 `Mkt-RF` −3.15 % now
  vs −3.18 % stored). Only months after the stored max are appended.
- **News:** keys are dated by UTC day (as before), so each day's first runs
  re-fetch everything — ~3,000 s of work, spread over a few hourly runs by the
  timeout resume.

## Next steps

1. Pop the stash (above), re-run the suite, commit.
2. `cd Ops/fin-deep-data && serverless deploy`, then invoke each new function
   once by hand with `dbFlag: True`; check its `load_audit` row and the table.
3. Enable one at a time (`enabled: true`, redeploy):
   - first `FXrateHandlerv2`, `fffHandlerv2`, `yfNewshandlerv2` — their old
     versions are disabled or fail every run, so nothing collides;
   - then `optHandlerv2` and `cronHandlerv2` — **disable the old
     `optHandler` / `cronHandler` rule first** (`doc/OPERATIONS.md` §11.2).
4. Once all five are flipped, retire the old functions (TODOS §5.11).

## Open problems found along the way (not part of the port)

- **Possible double writer on `FX_histdaily`:** old `FXHistHandler` rule is
  still ENABLED in AWS while `FXHistHandlerv2` is `enabled: true` in the repo.
  Meanwhile `FXrateHandler` was the one disabled on 2026-10-07 — looks swapped.
  Check with `aws events list-rules --name-prefix fin-cron-data` /
  `aws scheduler list-schedules`.
- **Rotate the `thomas` DB password:** it is printed in clear text in the
  `fin-cron-data` CloudWatch logs (the frozen `dataUtil` logs the full URL).
  Same for `deepops`, whose password was in a pasted log on 2026-10-11.
- **`CLAUDE.md` vs the repo:** CLAUDE.md says every `fin-deep-data` schedule
  ships `enabled: false`; commit `e1f72f1` turned the existing eleven on.
  Update one of them.
- Account Lambda concurrency quota is still **10** (TODOS §2.13).
