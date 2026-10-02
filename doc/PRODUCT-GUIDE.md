# PRODUCT-GUIDE.md

What data Fin-Lambda collects, how fresh it is, and where to read it. Written
for data consumers — no handler names, no SQLAlchemy, no event flags.

> **Scope note.** Created alongside the index-membership dataset, which is
> documented in full below. The other datasets have summary entries; expanding
> them is tracked in `TODOS.md` §6.

## Changelog

- 2026-10-01 | Added | *Daily prices and option chains — collection in trial*: a **Which symbols are covered** subsection — delisted symbols leave the universe, option chains narrow to symbols flagged as having options, and the figure is 863 symbols, not 857.
- 2026-10-01 | Modified | Dataset catalogue and *Corporate actions*: the universe is ~863 symbols; both now point at the coverage rules rather than quoting a bare count.
- 2026-08-01 | Added | Initial file: dataset catalogue, full entry for index membership (S&P 500 / NASDAQ-100), as-of query recipe, coverage caveats.
- 2026-10-01 | Added | Three datasets: corporate actions, the per-dataset load record, and the daily load-status report. Entries for DJIA and Hang Seng membership. A *Freshness you can rely on* section explaining `available_at`.
- 2026-10-01 | Modified | Dataset catalogue: daily prices and EOD option chains gain a coverage/lag note and their shadow-table status while the new collectors are in trial.

---

## Dataset catalogue

| Dataset | Where to read it | Frequency | Expected lag |
|---|---|---|---|
| **Index membership (S&P 500, NASDAQ-100, DJIA, Hang Seng)** | `Trading.portfolio_assets_info` | checked daily, **stored only when it changes** | ≤ 1 business day |
| Stock/ETF snapshot | `GlobalMarketData.snapshot` | every 10 min during US hours | minutes; **latest snapshot only, no history** |
| Options snapshot | `GlobalMarketData.options_snapshot` | every 10 min during US hours | minutes; latest only |
| Intraday bars (15 min) | `GlobalMarketData.histminprice` | nightly per market | 1 day |
| Daily prices | `GlobalMarketData.histdailyprice7` | daily, after the US close | same evening. ~863 symbols across US, HK, CN, KR, TW, JP and crypto — see *Which symbols are covered* |
| **EOD option chains** | `GlobalMarketData.OptionChains` | daily, after the US close | same evening; filtered, see below |
| **Corporate actions** | `GlobalMarketData.corp_action_daily` | daily, only when there is one | ≤ 1 business day |
| **Load record (per dataset, per symbol)** | `GlobalMarketData.load_audit`, summarised by `GlobalMarketData.v_load_status` | one row per symbol per collection run | minutes after the run |
| **Daily load-status report** | e-mail, and `status/latest.json` in Cloudflare R2 | weekdays 20:00 ET | — |
| FX spot | `GlobalMarketData.FX_snapshot` | hourly | ~1 hour; latest only |
| FX daily history | `GlobalMarketData.FX_histdaily` | daily | 1 day |
| US interest rates | `GlobalMarketData.USRates` | daily, weekdays | 1 day |
| Fama-French factors | `GlobalMarketData.famaFrench` | monthly | **currently not updating** — see `TODOS.md` §4.1 |
| News articles | S3 / Cloudflare R2 | hourly | ~1 hour |

Snapshot tables hold **only the newest snapshot** — they are cleared and
rewritten on every run. If you need history, use the daily or intraday tables.

---

## Index membership — `Trading.portfolio_assets_info`

### What it contains

A dated, point-in-time record of which companies were in an index, with each
company's sector, asset class and currency.

Two index portfolios are collected automatically:

| `Port_name` | Index | Typical size |
|---|---|---|
| `SP500` | S&P 500 | ~503 |
| `NDX100` | NASDAQ-100 | ~103 |

The same table also holds seven manually-loaded portfolios that predate this —
`DJI`, `HSI`, `IAM`, `TM-ASIA`, `TM-CHINA`, `US-ETF`, `US-Top20`. Those are
**not** refreshed automatically; most carry a single date of 2024-12-30.

### Columns

| Column | Meaning |
|---|---|
| `Date` | The date this membership set was effective |
| `Port_name` | Which portfolio / index |
| `Symbol` | Ticker |
| `Class` | Asset class — `Equity` for both indices |
| `Sector` | See the caveat below |
| `Type` | Instrument type — `Stock` for both indices |
| `Currency` | Trading currency — `USD` for both indices |
| `Rate` | **FX rate to USD**, not an index weight. `1.0` for both indices |

> `Rate` is a currency conversion factor. In the older portfolios it carries
> real values — HKD rows are `0.1282`, CNY `0.14`, KRW `0.00073`. It is **not**
> a portfolio weight, and this table does not carry index weights at all.

### Cadence — stored only when it changes

A new dated set is written **only when the membership or a constituent's
attributes actually differ** from the most recently stored set. On a typical
day nothing is written.

That means **`Date` is not a daily series.** Consecutive rows can be weeks or
months apart. To ask "who was in the S&P 500 on 15 March?", take the latest set
*on or before* that date:

```sql
SELECT p.*
FROM Trading.portfolio_assets_info p
WHERE p.Port_name = 'SP500'
  AND p.Date = (
      SELECT MAX(Date)
      FROM Trading.portfolio_assets_info
      WHERE Port_name = 'SP500' AND Date <= '2027-03-15'
  );
```

Each set is complete — the full membership list is rewritten every time, never
a delta — so no reconstruction from change events is needed.

### Coverage and freshness

- Checked every weekday at 22:30 UTC, after the US close.
- Index committees announce changes to take effect at a market open, so a
  change is reflected **within one business day**.
- There is **no history before the first run.** The table is not backfilled;
  the earliest `SP500` / `NDX100` set is whenever collection started.
- `SP500` is dated with the collection day. `NDX100` is dated with the date
  Nasdaq itself publishes on the source list, so the two can differ by a day or
  two on the same run. This is expected and does not affect as-of queries.

### Caveat — `Sector` is populated for `SP500` only

| `Port_name` | `Sector` |
|---|---|
| `SP500` | Real **GICS** sector — `Information Technology`, `Health Care`, … |
| `NDX100` | Always the literal **`General`** |

The NASDAQ-100 source publishes no sector. The only free alternative classifies
companies under a *different* taxonomy (ICB), and mixing the two would make the
column meaningless across portfolios — `AAPL` would read
`Information Technology` under `SP500` and `Technology` under `NDX100`. Rather
than that, `NDX100` carries a sentinel, matching what `DJI` and `HSI` already
use.

**If you need a sector for a NASDAQ-100 name**, join to the `SP500` rows —
roughly 90 % of NASDAQ-100 constituents are also in the S&P 500:

```sql
SELECT n.Symbol, COALESCE(s.Sector, 'General') AS Sector
FROM Trading.portfolio_assets_info n
LEFT JOIN Trading.portfolio_assets_info s
       ON s.Symbol = n.Symbol
      AND s.Port_name = 'SP500'
      AND s.Date = (SELECT MAX(Date) FROM Trading.portfolio_assets_info
                    WHERE Port_name = 'SP500')
WHERE n.Port_name = 'NDX100'
  AND n.Date = (SELECT MAX(Date) FROM Trading.portfolio_assets_info
                WHERE Port_name = 'NDX100');
```

Be aware the older portfolios use their own sector vocabularies — `US-Top20`
mixes real sectors with `NoSector` and has values with trailing whitespace.
Don't assume one taxonomy across the whole table.

### Caveat — ticker format

Symbols use the **dashed** form for share classes: `BRK-B`, `BF-B` — not
`BRK.B`. This matches the price tables, so joins to price history work directly:

```sql
SELECT m.Symbol, m.Sector, p.Close
FROM Trading.portfolio_assets_info m
JOIN GlobalMarketData.histdailyprice7 p ON p.Symbol = m.Symbol
WHERE m.Port_name = 'SP500' AND m.Date = '2026-08-03' AND p.Date = '2026-08-03';
```

Note that the daily price table covers ~450 symbols, not the full index, so a
join will not return every constituent.

### Not covered

- **Index weights.** Not collected, and not derivable from this table.
- **Non-US indices** beyond the manually-loaded `HSI` / `TM-*` portfolios.
- **Historical membership** before collection started.
- **Intraday changes.** One set per day at most.

---

## Freshness you can rely on

Until 2026 the only way to tell whether a day's data had finished loading was to
look at the data. Three datasets now answer that directly.

**The load record** (`GlobalMarketData.load_audit`) has one row per symbol per
collection run, with the UTC time that symbol's rows were committed
(`finished_at`), the date range written, and a status: `ok`, `empty` (the source
had nothing — a holiday, a symbol with no options), `error`, or `skipped` (the
run ran out of time; a later sweep picks it up). A symbol may have several rows
for one day — a first attempt and a sweep — and it counts as collected when
**any** of them is `ok` or `empty`.

Use `finished_at` as the "as of" time for point-in-time work: it is when the
rows actually became readable, which is strictly later than the trade date and
is what keeps a backtest honest.

**`v_load_status`** condenses that to one row per dataset: last data date, when
the last run started and finished, how many symbols succeeded out of how many
were expected, and the row count.

**The daily report** turns the same view into one line per dataset, mailed every
weekday at 20:00 ET and written to `status/latest.json` in R2. Each line reads
`ok`, `partial` (some symbols did not finish), `error`, or `stale` (the last data
is older than the previous weekday). Two caveats: holidays are not modelled, so
the day after a US holiday can read `stale`; and datasets that only write when
something changes — corporate actions, index membership — are judged by their
last *run*, not their last data date.

---

## Corporate actions — `GlobalMarketData.corp_action_daily`

Cash dividends and stock splits, one row per `(ex-date, symbol, exchange)`:

| Column | Meaning |
|---|---|
| `Date` | ex-date, in the exchange's local calendar |
| `Symbol`, `Exchange` | as in the price tables |
| `Dividends` | cash per share, in the traded currency and units of that date |
| `StockSplits` | ratio, new over old (`2.0` = a 2-for-1 split) |
| `first_seen_at` | UTC time this action was **first** observed |

Only non-zero actions are stored. The collector re-reads a rolling ~14-session
window every evening, so an action posted late is still picked up — and because
the first sighting wins, `first_seen_at` tells you when the information became
available rather than when it was last re-read. That makes it usable as the
`available_at` of the action itself.

Coverage follows the daily-price universe, so it is as wide as that list (~863
symbols, see *Which symbols are covered*) and no wider. Rights issues, spin-offs, symbol changes and delistings
are **not** collected.

---

## Daily prices and option chains — collection in trial

The daily bars and the EOD option chains have been collected by a host cron job
on a separate machine. New collectors now run in AWS alongside it, and during
the trial they write to shadow copies of the tables
(`histdailyprice7_shadow`, `OptionChains_shadow`). Nothing to change on the
consumer side: `histdailyprice7` and `OptionChains` keep being written as before,
and at the cutover the new collectors simply start writing them instead. Rows
written before the cutover are never modified.

What changes for consumers at that point:

- bars are collected at **18:30 ET**, at least an hour after the close in every
  season. The old job ran ten minutes after the close, which in winter could
  capture a bar before it was final;
- every symbol gets a load record, so `available_at` becomes exact;
- option chains are captured at **17:40 ET year-round** rather than drifting an
  hour with daylight saving.

### Which symbols are covered

The universe is assembled on the server each run: every symbol with daily price
history, plus the underlyings of the stock- and ETF-options lists, plus the
members of the four index-membership portfolios, plus anything flagged in the
symbol master as a stock, ETF, crypto or benchmark. That is **863 symbols** on
2026-10-01.

Two exclusions are being introduced:

- **delisted symbols leave the universe** (863 → 838). They keep the history
  already stored; nothing new is collected for them, and no load record is
  written, so a delisted symbol stops showing as a gap;
- **option chains narrow further** (838 → 814) by also dropping symbols the
  symbol master records as having no options. Only symbols the master actually
  lists are affected — a symbol the master does not mention is still attempted,
  so no underlying you hold chains for today disappears.

Both take effect at the same time, with the collectors that write the shadow
tables. Daily prices for a live symbol are unaffected either way.

**Option chains are filtered, not complete.** Per underlying and expiry, only
contracts with a last price above $0.05 **and** open interest above the chain's
25th percentile are stored. This is unchanged from the previous collector; the
unfiltered chain is archived as CSV in Cloudflare R2 under
`raw/optchain/<date>/` if you need it. Each row carries `Date`, `Section` (`PM`,
the post-close capture), the underlying and its price at capture, strike,
expiry, call/put, and the usual quote fields.

---

## Index membership — DJIA and Hang Seng

Two more indices are collected on the same only-when-it-changes basis as the S&P
500 and NASDAQ-100:

| `Port_name` | Index | Members | Symbol format | Currency |
|---|---|---|---|---|
| `DJI` | Dow Jones Industrial Average | 30 | US tickers, dashed (`BRK-B`) | `USD`, rate `1.0` |
| `HSI` | Hang Seng Index | 50–110 (85 on 2026-10-01) | `0700.HK` — four digits, `.HK` suffix | `HKD`, with a rate to USD |

The `HSI` rate is copied from the newest stored `HSI` row rather than re-fetched,
so a change in the FX rate alone never registers as a membership change. Both
carry `Sector = 'General'`: the DJIA source gives no sector, and the Hang Seng
sub-index classification is not GICS, so putting it in the same column as the
S&P's GICS sectors would make the column meaningless.

DJIA membership comes from the holdings file of the DIA ETF, which tracks the
index exactly; the Wikipedia DJIA page stopped carrying a ticker table. Hang Seng
membership comes from Wikipedia.

Because these members also enter the symbol list used for price and options
collection, a new DJIA or Hang Seng constituent starts being collected
automatically.
