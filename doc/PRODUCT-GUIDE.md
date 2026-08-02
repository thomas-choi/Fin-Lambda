# PRODUCT-GUIDE.md

What data Fin-Lambda collects, how fresh it is, and where to read it. Written
for data consumers — no handler names, no SQLAlchemy, no event flags.

> **Scope note.** Created alongside the index-membership dataset, which is
> documented in full below. The other datasets have summary entries; expanding
> them is tracked in `TODOS.md` §6.

## Changelog

- 2026-08-01 | Added | Initial file: dataset catalogue, full entry for index membership (S&P 500 / NASDAQ-100), as-of query recipe, coverage caveats.

---

## Dataset catalogue

| Dataset | Where to read it | Frequency | Expected lag |
|---|---|---|---|
| **Index membership (S&P 500, NASDAQ-100)** | `Trading.portfolio_assets_info` | checked daily, **stored only when it changes** | ≤ 1 business day |
| Stock/ETF snapshot | `GlobalMarketData.snapshot` | every 10 min during US hours | minutes; **latest snapshot only, no history** |
| Options snapshot | `GlobalMarketData.options_snapshot` | every 10 min during US hours | minutes; latest only |
| Intraday bars (15 min) | `GlobalMarketData.histminprice` | nightly per market | 1 day |
| Daily prices | `GlobalMarketData.histdailyprice7` | daily | 1 day |
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
