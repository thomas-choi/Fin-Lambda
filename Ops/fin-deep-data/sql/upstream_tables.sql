-- =============================================================================
-- upstream_tables.sql -- PLAN-SR-UPSTREAM.md Pre-Phase P4
--
-- Creates, in GlobalMarketData:
--   load_audit              per-symbol + per-run load record (SR contract)
--   corp_action_daily       dividends / splits with first-seen time (SR contract)
--   v_load_status           latest summary row per data set (statusReport, `sr status`)
--   histdailyprice7_shadow  shadow target for eodDaily during the U5 shadow run
--   OptionChains_shadow     shadow target for optChainEOD during the U5 shadow run
--
-- Source: Support-Resistance-Agent docs/SR_Technical_Document.md 4.5.7 (at
-- 8ab0591), with ONE deliberate deviation -- see "load_audit primary key" below.
--
-- Run by the owner, as a user with CREATE / CREATE VIEW on GlobalMarketData:
--   mysql -h $DBHOST -P $DBPORT -u $DBUSER -p --ssl-mode=REQUIRED < Ops/fin-cron-data/sql/upstream_tables.sql
--
-- Server facts this file was written against (read 2026-09-25):
--   * MySQL 8.0.45 -- ROW_NUMBER() in the view needs >= 8.0.
--   * sql_require_primary_key=ON -- every table below declares a PK; to_sql()
--     can never create them.
--   * sql_mode includes ANSI (ANSI_QUOTES, PIPES_AS_CONCAT) and STRICT_ALL_TABLES:
--     "..." quotes identifiers, so only '...' is used for strings here. Strict
--     mode rejects an over-long value instead of truncating it, so writers must
--     cut `error` to 512 chars (INSERT IGNORE downgrades it to a warning, but
--     do not rely on that).
--   * Server time zone UTC; DATETIME(3) columns hold UTC as written.
--   * Default engine InnoDB, utf8mb4 / utf8mb4_0900_ai_ci -- the same as
--     histdailyprice7 and OptionChains, so joins on Symbol need no COLLATE.
--
-- Idempotent: CREATE TABLE IF NOT EXISTS / CREATE OR REPLACE VIEW. Re-running
-- never drops data. It also never alters an existing table -- if a definition
-- here changes after the first run, write an explicit ALTER.
-- =============================================================================

USE GlobalMarketData;

-- -----------------------------------------------------------------------------
-- load_audit
--
-- One row per (run, data set, symbol) plus one summary row per (run, data set)
-- with Symbol = '*' and Exchange = ''. Written with INSERT IGNORE via
-- dataUtil.append_ignore(); rows are never updated.
--
-- load_audit primary key -- DEVIATION from SR 4.5.7.
--   SR:   PRIMARY KEY (run_id, Symbol, Exchange)
--   here: PRIMARY KEY (run_id, table_name, Symbol, Exchange)
--   Reason: one eodDaily invocation writes two data sets (histdailyprice7 and
--   corp_action_daily) and needs a summary row for each, because v_load_status
--   partitions by (table_name, job). Under SR's key both summary rows are
--   (run_id, '*', ''), so the second is silently dropped by INSERT IGNORE and
--   corp_action_daily never appears in the status report. SR's docs and its
--   test_upstream_contract.py PK assertion must be amended to match (SR-repo
--   follow-up, same as the OPT_SHARDS correction).
--
-- Column notes beyond SR's comments:
--   job         also the U10 handlers: usrateHandler, FXHistHandler,
--               yfus30minEOD, yfasia30minEOD, portAssetsHandler.
--   Exchange    '' on summary rows and for handlers with no exchange concept.
--   yf_version  '' for handlers that do not use yfinance (usrateHandler).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS load_audit (
  run_id        CHAR(26)     NOT NULL,          -- ULID of the invocation
  job           VARCHAR(32)  NOT NULL,          -- 'eodDaily' | 'optChainEOD' | U10 handler names
  Symbol        VARCHAR(45)  NOT NULL,          -- '*' = run summary row
  Exchange      VARCHAR(45)  NOT NULL,          -- '' on summary rows
  date_lo       DATE         NULL,              -- first and last trade date written
  date_hi       DATE         NULL,
  table_name    VARCHAR(64)  NOT NULL,          -- DataName in the status report, e.g. 'histdailyprice7'
  segment       VARCHAR(8)   NOT NULL,          -- 'first' | 'append' | 'prepend' | 'summary'
  n_rows        INT          NOT NULL,
  n_ok          INT          NULL,              -- summary rows: symbols with status ok
  n_expected    INT          NULL,              -- summary rows: symbols this shard was given
  status        VARCHAR(16)  NOT NULL,          -- ok | empty | error | skipped
  error         VARCHAR(512) NULL,
  started_at    DATETIME(3)  NOT NULL,          -- UTC
  finished_at   DATETIME(3)  NOT NULL,          -- UTC, commit of this symbol's rows
  yf_version    VARCHAR(16)  NOT NULL,          -- '' when the handler does not use yfinance
  host          VARCHAR(64)  NOT NULL,          -- 'lambda:eodDaily' or hostname
  PRIMARY KEY (run_id, table_name, Symbol, Exchange),
  KEY k_job_date (job, date_hi),                 -- missing_for_sweep(job, date, ...)
  KEY k_table_finished (table_name, finished_at) -- v_load_status, SR's available_at lookup
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- -----------------------------------------------------------------------------
-- corp_action_daily
--
-- Non-zero Dividends / Stock Splits seen in eodDaily's 14-session window.
-- INSERT IGNORE on the PK keeps the FIRST sighting, so first_seen_at is an
-- honest available_at for the action. Verbatim from SR 4.5.7.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS corp_action_daily (
  Date          DATE         NOT NULL,          -- ex-date, exchange-local
  Symbol        VARCHAR(45)  NOT NULL,
  Exchange      VARCHAR(45)  NOT NULL,
  Dividends     DOUBLE       NULL,              -- cash per share, traded units of that date
  StockSplits   DOUBLE       NULL,              -- r_u, new/old
  first_seen_at DATETIME(3)  NOT NULL,          -- UTC; INSERT IGNORE keeps the first sighting
  run_id        CHAR(26)     NOT NULL,          -- load_audit.run_id of the sighting run
  PRIMARY KEY (Date, Symbol, Exchange)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- -----------------------------------------------------------------------------
-- v_load_status
--
-- Latest summary row per (table_name, job). statusReport and SR's `sr status`
-- read this. Verbatim from SR 4.5.7. The view runs with its definer's rights
-- (MySQL default), so a read-only consumer needs SELECT on the view only.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_load_status AS
SELECT table_name, job, date_hi AS last_data_date, started_at, finished_at,
       status, n_ok, n_expected, n_rows, error
FROM (SELECT a.*, ROW_NUMBER() OVER (PARTITION BY table_name, job ORDER BY finished_at DESC) AS rn
      FROM load_audit a WHERE a.segment = 'summary') s
WHERE rn = 1;

-- -----------------------------------------------------------------------------
-- Shadow targets for U5.
--
-- CREATE TABLE ... LIKE copies columns, PK, indexes, engine and collation
-- exactly. Created EMPTY here; seeding with the last 30 days of production
-- rows happens at Phase G1, right before the shadow deploy, so every symbol's
-- watermark matches production on the first shadow night.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS histdailyprice7_shadow LIKE histdailyprice7;
CREATE TABLE IF NOT EXISTS OptionChains_shadow    LIKE OptionChains;

-- -----------------------------------------------------------------------------
-- Grants
--
-- Today the Lambda functions connect as the same user that owns
-- GlobalMarketData (ALL PRIVILEGES on the schema), so no GRANT is needed.
-- If SR gets its own read-only user, grant it:
--   GRANT SELECT ON GlobalMarketData.load_audit        TO '<sr_reader>'@'%';
--   GRANT SELECT ON GlobalMarketData.corp_action_daily TO '<sr_reader>'@'%';
--   GRANT SELECT ON GlobalMarketData.v_load_status     TO '<sr_reader>'@'%';
-- -----------------------------------------------------------------------------

-- -----------------------------------------------------------------------------
-- Readiness checks (P4). Each should succeed; the last two should return
-- identical column lists for each pair.
-- -----------------------------------------------------------------------------
SHOW CREATE TABLE load_audit;
SHOW CREATE TABLE corp_action_daily;
SHOW CREATE VIEW  v_load_status;
SELECT * FROM v_load_status LIMIT 1;           -- empty result, no error

SELECT TABLE_NAME, COUNT(*) AS n_cols,
       GROUP_CONCAT(COLUMN_NAME ORDER BY ORDINAL_POSITION) AS cols
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = 'GlobalMarketData'
  AND TABLE_NAME IN ('histdailyprice7', 'histdailyprice7_shadow',
                     'OptionChains',    'OptionChains_shadow')
GROUP BY TABLE_NAME
ORDER BY TABLE_NAME;

SELECT TABLE_NAME, GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS pk
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = 'GlobalMarketData' AND INDEX_NAME = 'PRIMARY'
  AND TABLE_NAME IN ('load_audit', 'corp_action_daily',
                     'histdailyprice7', 'histdailyprice7_shadow',
                     'OptionChains',    'OptionChains_shadow')
GROUP BY TABLE_NAME
ORDER BY TABLE_NAME;

-- -----------------------------------------------------------------------------
-- Rollback (manual; only before any production run has written to them).
-- -----------------------------------------------------------------------------
-- DROP VIEW  IF EXISTS v_load_status;
-- DROP TABLE IF EXISTS load_audit;
-- DROP TABLE IF EXISTS corp_action_daily;
-- DROP TABLE IF EXISTS histdailyprice7_shadow;
-- DROP TABLE IF EXISTS OptionChains_shadow;
