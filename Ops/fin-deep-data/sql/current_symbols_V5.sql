-- =============================================================================
-- current_symbols_V5.sql
--
-- GlobalMarketData.current_symbols_V5(IN p_type CHAR(1))
--
-- The symbol list the fin-deep-data collectors load (dataUtil.load_symbols_db).
-- V5 = V4's union, minus an exclusion list taken from SymbolMaster, selected by
-- @type:
--
--   'a'  (all; the default)   exclude delisted symbols
--   'o'  (optionable)         exclude delisted symbols and symbols with no options
--
-- Callers: eodDaily passes 'a', optChainEOD passes 'o'
-- (Ops/fin-deep-data/eod_daily_handler.py, optchain_eod_handler.py).
--
-- MySQL has no default argument values, so "default 'a'" is enforced twice:
-- dataUtil sends 'a' unless told otherwise, and the body below maps NULL, '' and
-- any unrecognised value to 'a' as well. `CALL current_symbols_V5()` with no
-- argument is still an error -- the Python caller always sends one.
--
-- Run by the owner, as a user with CREATE ROUTINE on GlobalMarketData:
--   mysql -h $DBHOST -P $DBPORT -u $DBUSER -p --ssl-mode=REQUIRED \
--         < Ops/fin-deep-data/sql/current_symbols_V5.sql
--
-- Then grant the Lambda DB user EXECUTE on it:
--   GRANT EXECUTE ON PROCEDURE GlobalMarketData.current_symbols_V5 TO '<lambda user>'@'%';
--
-- V4 is left in place and keeps working for fin-cron-data and anything else that
-- calls it; it returns the bare union, 863 symbols, no exclusions.
--
-- Watch the `options` spelling. A V4 that read SymbolMaster as `option` was live
-- for a few minutes on 2026-10-01 (2026-10-02 UTC) and every call failed with
--   ERROR 1054 Unknown column 'option' in 'where clause'
-- which reaches a handler as an empty symbol list, not as an error
-- (doc/OPERATIONS.md 8.7). The column is `options`.
--
-- Server facts this file was written against (doc/OPERATIONS.md 8.3):
--   * MySQL 8.0.45, server time zone UTC, default engine InnoDB.
--   * sql_mode includes ANSI (ANSI_QUOTES), so "..." quotes an identifier --
--     every string literal below is single-quoted. No identifier here needs
--     quoting: `options` is not a reserved word, `option` would have been.
--   * sql_require_primary_key=ON -- it applies to CREATE TEMPORARY TABLE as
--     well, hence the surrogate `id` PK on the scratch table.
--   * GlobalMarketData.SymbolMaster (read 2026-10-01) is 215 rows, columns
--     Symbol, stock, crypto, options, brenchmark, fund, delisted -- `options`,
--     not `option`, and no `fund` in any clause below because neither the V4
--     body nor the exclusion list mentions it.
--
-- Idempotent: DROP PROCEDURE IF EXISTS then CREATE. Re-running is safe; it
-- touches no data.
-- =============================================================================

USE GlobalMarketData;

DROP PROCEDURE IF EXISTS current_symbols_V5;

DELIMITER $$

CREATE PROCEDURE current_symbols_V5(IN p_type CHAR(1))
BEGIN
    DECLARE v_type CHAR(1);

    -- "default 'a'": NULL, '' and anything that is not 'o' all mean "all".
    SET v_type = LOWER(COALESCE(NULLIF(TRIM(p_type), ''), 'a'));
    IF v_type <> 'o' THEN
        SET v_type = 'a';
    END IF;

    DROP TEMPORARY TABLE IF EXISTS GlobalMarketData.current_symbols_V5_tmp;
    CREATE TEMPORARY TABLE GlobalMarketData.current_symbols_V5_tmp (
        id     BIGINT NOT NULL AUTO_INCREMENT,
        Symbol VARCHAR(20),
        PRIMARY KEY (id)
    );

    -- ---- the union (unchanged from V4) --------------------------------------
    INSERT INTO GlobalMarketData.current_symbols_V5_tmp (Symbol)
        (SELECT DISTINCT Symbol FROM Trading.Stock_Options);
    INSERT INTO GlobalMarketData.current_symbols_V5_tmp (Symbol)
        (SELECT DISTINCT Symbol FROM Trading.ETF_Options);
    INSERT INTO GlobalMarketData.current_symbols_V5_tmp (Symbol)
        (SELECT DISTINCT Symbol FROM GlobalMarketData.histdailyprice7);
    INSERT INTO GlobalMarketData.current_symbols_V5_tmp (Symbol)
        (SELECT DISTINCT Symbol FROM Trading.portfolio_assets_info);
    INSERT INTO GlobalMarketData.current_symbols_V5_tmp (Symbol)
        (SELECT DISTINCT Symbol
           FROM GlobalMarketData.SymbolMaster
          WHERE stock       > 0
             OR crypto      > 0
             OR options     > 0
             OR brenchmark  > 0);

    -- ---- the exclusion list (new in V5) ------------------------------------
    -- A symbol is dropped only when SymbolMaster has a row for it saying so;
    -- a symbol absent from SymbolMaster is kept, exactly as the two SELECTs
    -- this was derived from imply:
    --   'o' -> SELECT * FROM SymbolMaster WHERE (options = 0) OR (delisted = 1)
    --   'a' -> SELECT * FROM SymbolMaster WHERE (delisted = 1)
    --
    -- A symbol that SymbolMaster does not list is kept on purpose, including for
    -- 'o': 648 of the 863 union symbols have no SymbolMaster row, so 'o' is an
    -- exclusion, not a whitelist. Measured 2026-10-01:
    --   union 863 -> 'a' 838 (25 delisted) -> 'o' 814 (49 delisted or options=0).
    -- Making 'o' a whitelist instead (SymbolMaster.options > 0) would return 166
    -- and would drop 38 underlyings that Stock_Options / ETF_Options do hold.
    IF v_type = 'o' THEN
        DELETE t
          FROM GlobalMarketData.current_symbols_V5_tmp t
          JOIN GlobalMarketData.SymbolMaster m ON m.Symbol = t.Symbol
         WHERE (m.options = 0) OR (m.delisted = 1);
    ELSE
        DELETE t
          FROM GlobalMarketData.current_symbols_V5_tmp t
          JOIN GlobalMarketData.SymbolMaster m ON m.Symbol = t.Symbol
         WHERE (m.delisted = 1);
    END IF;

    SELECT DISTINCT Symbol
      FROM GlobalMarketData.current_symbols_V5_tmp
     ORDER BY Symbol;
END$$

DELIMITER ;

-- -----------------------------------------------------------------------------
-- Verification (run as the Lambda DB user, which is what the handlers use).
-- Expect: 'o' <= 'a' <= 863, column name `Symbol` in all three. V4 is not in
-- the list because it errors today -- see the header.
-- -----------------------------------------------------------------------------
-- CALL GlobalMarketData.current_symbols_V5('a');
-- CALL GlobalMarketData.current_symbols_V5('o');
-- CALL GlobalMarketData.current_symbols_V5(NULL);   -- same result as 'a'

-- Expected counts on 2026-10-01 data: 'a' 838, 'o' 814.

-- -----------------------------------------------------------------------------
-- Rollback: set SYMBOL_PROC_VER back to V4 in Ops/fin-deep-data/.env, redeploy,
-- then optionally
--   DROP PROCEDURE IF EXISTS GlobalMarketData.current_symbols_V5;
-- -----------------------------------------------------------------------------
