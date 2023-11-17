from datetime import datetime, date
import logging
import yfinance as yf
from dotenv import load_dotenv
import dataUtil as DU
import json
from os import environ
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)

localrun = False

def getOptions(ticker, PnC, strike, expiration):
    print('getOptions:', ticker, PnC, strike, expiration)
    exp_dt = datetime.strptime(expiration, "%Y-%m-%d")
    today = date.today()
    logging.debug(f'-->getOptions({ticker},{PnC},{strike},{expiration},{exp_dt},{today})')
    op_price = None
    if (today > exp_dt.date()):
        logging.error(f'  expiration: {exp_dt} is old\n')
        return None
    
    asset = yf.Ticker(ticker)
    histdata = asset.history()
    if len(histdata)>0:
        pclose = asset.history().iloc[-1].Close
        pclose = float("{:.2f}".format(pclose))
        try:
            opts = asset.option_chain(expiration)
            if PnC == 'P':
                op = opts.puts[opts.puts['strike'] == strike]
                # print(opts.puts)
            else:
                op = opts.calls[opts.calls['strike'] == strike]
                # print(opts.calls)
            if len(op)>0:
                opt = op.head(1).reset_index()
                op_price = op.iloc[0]
            else:
                logging.error(f'{ticker} {strike}{PnC} not found\n')
        except Exception as error:
            logging.error(error)
    return op_price

def run(event, context):
    logging.debug("In handler.run()\n")
    current_time = datetime.now()
    timestr = str(current_time)
    logger.info("Your cron function handler.run " + " ran at " + timestr)

    load_dotenv("Prod_config/Stk_eodfetch.env") 

    opt_cols = ["Symbol","PnC","Strike","Expiration"]
    opt_df = pd.DataFrame(columns=opt_cols)
    logging.debug(opt_df.head())
    df = DU.load_df_SQL(f'call Trading.sp_stock_trades_V3;')
    df['Date'] = df['Date'].astype(str)
    df['Expiration'] = df['Expiration'].astype(str)
    print(df.head(2))
    for ix, row in df.iterrows():
        opt_df.loc[len(opt_df)] = [row.Symbol, row.PnC, row.Strike, row.Expiration]
    df = DU.load_df_SQL(f'call Trading.sp_etf_trades_v2;')
    df['Date'] = df['Date'].astype(str)
    df['Expiration'] = df['Expiration'].astype(str)
    print(df.head(2))
    print(df.info())
    for ix, row in df.iterrows():
        opt_df.loc[len(opt_df)] = [row.Symbol, row.PnC, row.H_Strike, row.Expiration]
    print(opt_df.info())
    if localrun:
        opt_df.to_csv("options_list.csv", index=False)

    options_columns=opt_cols + ["contractSymbol", "lastTradeDate","strike","lastPrice","bid","ask","change","percentChange",
                     "volume","openInterest","impliedVolatility","inTheMoney","contractSize","currency"]
    snapshots = pd.DataFrame(columns=options_columns)
    for ix, row in opt_df.iterrows():
        options = getOptions(row.Symbol, row.PnC, row.Strike, row.Expiration)
        # print(options)
        if options is not None:
            print(options.tolist())
            snapshots.loc[len(snapshots)] = [row.Symbol, row.PnC, row.Strike, row.Expiration] + options.tolist()

    DBMKTDATA=environ.get("DBMKTDATA")
    TBLSNAPSHOOT="options_snapshot"
    snapshots = snapshots[opt_cols + ["lastPrice","bid","ask","change","percentChange","volume","openInterest","impliedVolatility","inTheMoney","contractSize","currency"]]
    if localrun:
        snapshots.to_csv(f"{TBLSNAPSHOOT}.csv", index=False)
    print(snapshots)
    DU.ExecSQL(f"DELETE FROM {DBMKTDATA}.{TBLSNAPSHOOT} where (Symbol != \'1\');")
    DU.StoreEOD(snapshots, DBMKTDATA, TBLSNAPSHOOT)

if __name__ == '__main__':
    localrun = True
    run(0, 0)
