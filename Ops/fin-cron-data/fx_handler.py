import logging
import dataUtil as DU
import pytz

import yfinance as yf
import time
import datetime
import pandas as pd
from dotenv import load_dotenv
import os
import ast

# Create logger
logger = logging.getLogger()

load_dotenv() 
    

localrun = False

# Function to fetch the latest exchange rates for multiple tickers
def fetch_exchange_rates(tickers, base):
    data = yf.download(tickers, period="1d", interval="5m")
    lastrow = data['Close'].ffill().iloc[-1]
    print(lastrow)
    uptime = lastrow.name
    print(uptime)
    ts = lastrow.index.to_list()
    print(ts)
    records = []
    for y in ts:
        rec = {'base_cur': base, 'target_cur': y.split("=")[0], 'last_updated': uptime, 'rate':lastrow[y]}
        records.append(rec)
    rec = {'base_cur': base, 'target_cur': base, 'last_updated': uptime, 'rate':1.0}
    records.append(rec)    
    df = pd.DataFrame(records)
    return df

def fx_run(event, context):
    if "NYTIME" in event:
        current_time = event["NYTIME"]
    else:
        current_time = datetime.now().strftime("%Y/%m/%d-%H:%M:%S")
    current_time = current_time.strftime("%Y/%m/%d-%H:%M:%S")
    logger.info("Your cron function fx_handler " + " ran at " + current_time)

    base_cur = "USD"
    # Get the string representation of the list from .env
    my_list_str = os.getenv("FX_TICKERS")
    tickers = ast.literal_eval(my_list_str)
    tickers.sort()
    logging.debug(tickers)

    fx_df = fetch_exchange_rates(tickers, base_cur)
    fx_df['server_time'] = current_time
    logging.debug(fx_df)

    DBMKTDATA=os.environ.get("DBMKTDATA")
    TBLFXSNAPSHOT=os.environ.get("TBLFXSNAPSHOT") 
    if localrun:
        fx_df.to_csv(f"{base_cur}_FX.csv", index=False)
    else:
        DU.ExecSQL(f"DELETE FROM {DBMKTDATA}.{TBLFXSNAPSHOT};")
        DU.StoreEOD(fx_df, DBMKTDATA, TBLFXSNAPSHOT)

def run(event, context):
    if ('test' in event):
        logger.setLevel(logging.DEBUG)  # Set the logger to handle DEBUG level messages
    else:
        logger.setLevel(logging.INFO)  # Set the logger to handle DEBUG level messages

    logging.info(f"** ==> fx_handler.run(event: {event}, context: {context}")
    # Get the current time in New York
    ny_time = datetime.datetime.now().astimezone( pytz.timezone('US/Eastern'))
    logging.info(f"Current NY Time: {ny_time}")
    event["NYTIME"] = ny_time
    fx_run(event, context)     

if __name__ == '__main__':
    localrun = True
    event={"test": "True"}
    run(event, 0)
