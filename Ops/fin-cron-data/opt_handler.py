from datetime import datetime, date
import logging
import yfinance as yf
from dotenv import load_dotenv
import dataUtil as DU
import json
from os import environ
import pandas as pd
import sys
import pytz
from FOC_data import retreive_options

logger = logging.getLogger()
logger.setLevel(logging.INFO)
load_dotenv() 

localrun = False

def getOptions(ticker, PnC, strike, expiration):
    exp_dt = datetime.strptime(expiration, "%Y-%m-%d")
    today = date.today()
    logging.info(f'-->getOptions({ticker},{PnC},{strike},{expiration},{exp_dt},{today})')
    op_price = None
    pclose = -0.001
    OptDataEngine = environ.get("OptDataEngine")
    if (today > exp_dt.date()):
        logging.error(f'  expiration: {exp_dt} is old\n')
        return pclose, op_price
    
    try:
        asset = yf.Ticker(ticker)

        histdata = asset.history()

        if len(histdata)>0:
            # logging.info(f'asset.history:\n {histdata}')
            pclose = histdata.iloc[-1].Close
            pclose = float("{:.2f}".format(pclose))
            logging.debug(f"pclose is {pclose}")
            ex_dates = asset.options
            logging.info(f'  {ticker}.Options expiration dates is {ex_dates}')
            try:
                opts = asset.option_chain(expiration)
                logging.info(' opts found:\n')
                if PnC == 'P':
                    op = opts.puts[opts.puts['strike'] == strike]
                else:
                    op = opts.calls[opts.calls['strike'] == strike]
                if len(op)>0:
                    opt = op.head(1).reset_index()
                    op_price = op.iloc[0]
                    
            except Exception as error:
                logging.error(f"option-chains: {error}")
    except Exception as error:
        logging.error(f"yf_ticker: {error}")

    if op_price is None:
        logging.error(f'{ticker} {strike}-{PnC} not found\n')
        if OptDataEngine == "FOC":
            op_price, ts = retreive_options(ticker, expiration, PnC, strike)

    logging.info("-->getOptions() return")
    return pclose, op_price

def run(event, context):
    logging.info(f"** ==> opt_handler.run(event: {event}, context: {context})")
    ny_time = datetime.now().astimezone( pytz.timezone('US/Eastern'))
    logging.info(f"Current opt_handler: NY Time: {ny_time}")

    opt_cols = ["Symbol","PnC","Strike","Expiration"]
    opt_df = pd.DataFrame(columns=opt_cols)
    logging.debug(opt_df.head())
    df = DU.load_df_SQL(f'call Trading.sp_stock_trades_V3;')
    df['Date'] = df['Date'].astype(str)
    df['Expiration'] = df['Expiration'].astype(str)
    logging.debug(df.head(2))
    for ix, row in df.iterrows():
        opt_df.loc[len(opt_df)] = [row.Symbol, row.PnC, row.Strike, row.Expiration]
    df = DU.load_df_SQL(f'call Trading.sp_etf_trades_v2;')
    df['Date'] = df['Date'].astype(str)
    df['Expiration'] = df['Expiration'].astype(str)
    logging.debug(df.head(2))
    logging.debug(df.info())
    for ix, row in df.iterrows():
        opt_df.loc[len(opt_df)] = [row.Symbol, row.PnC, row.H_Strike, row.Expiration]
        
    def keyformat(sym, pnc, strike, expire):
        return f'{sym}-{pnc}-{strike:.2f}-{expire}'
    
    opt_df['KEY'] = opt_df.apply(lambda row: keyformat(row['Symbol'],row['PnC'],
                                               row['Strike'],row['Expiration']), axis=1)
    opt_df.sort_values(by=['Symbol','PnC','Strike','Expiration'], inplace=True)
    dup_values = opt_df['KEY'].duplicated()
    logging.debug(opt_df[dup_values])
    logging.debug("============")
    opt_df = opt_df[~dup_values]
    if localrun:
        opt_df.to_csv("options_list.csv", index=False)
        
    options_columns=opt_cols + ["contractSymbol", "lastTradeDate","strike","lastPrice","bid","ask","change","percentChange",
                     "volume","openInterest","impliedVolatility","inTheMoney","contractSize","currency", "PClose", "timestamp"]
    snapshots = pd.DataFrame(columns=options_columns)
    for ix, row in opt_df.iterrows():
        pclose, options = getOptions(row.Symbol, row.PnC, row.Strike, row.Expiration)
        # assume short options, therefore use bid price
        if options is not None:
            logging.debug(f"** pclose:{pclose}, opt:", options)
            opt_values = options.tolist()
            logging.debug(opt_values)
            snapshots.loc[len(snapshots)] = [row.Symbol, row.PnC, row.Strike, row.Expiration] + opt_values + [pclose, ny_time]

    DBMKTDATA=environ.get("DBMKTDATA")
    TBLSNAPSHOOT="options_snapshot"
    snapshots = snapshots[opt_cols + ["lastPrice","bid","ask","change","percentChange","volume","openInterest","impliedVolatility",
                                      "inTheMoney","contractSize","currency","PClose",'timestamp']]

    snapshots['lastPrice'] = snapshots['lastPrice'].astype(float)
    snapshots['bid'] = snapshots['bid'].astype(float)
    snapshots['ask'] = snapshots['ask'].astype(float)
    snapshots['volume'] = snapshots['volume'].astype(float)
    snapshots['openInterest'] = snapshots['openInterest'].astype(float)
    snapshots['impliedVolatility'] = snapshots['impliedVolatility'].astype(float)
    snapshots['PClose'] = snapshots['PClose'].astype(float)
    if localrun:
        snapshots.to_csv(f"{TBLSNAPSHOOT}.csv", index=False)
    else:
        DU.ExecSQL(f"DELETE FROM {DBMKTDATA}.{TBLSNAPSHOOT} where (Symbol != \'1\');")
        DU.StoreEOD(snapshots, DBMKTDATA, TBLSNAPSHOOT)

if __name__ == '__main__':
    # logging.basicConfig(filename="opt_handler.log", encoding='utf-8', level=logging.DEBUG)
    localrun = False
    run(0, 0)
