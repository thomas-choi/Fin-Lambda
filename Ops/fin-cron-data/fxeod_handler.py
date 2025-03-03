import logging
import dataUtil as DU
import pytz

import yfinance as yf
import time
from datetime import datetime, timedelta
import pandas as pd
from dotenv import load_dotenv
import os
import ast

# Create logger
logger = logging.getLogger()

load_dotenv() 
    

localrun = False

# Function to fetch the latest exchange rates for multiple tickers
def fetch_exchange_rates(start_dt, end_dt, tickers, base):
    cols = ['base_cur',	'target_cur', 'Open','High','Low','Close','Adj Close','Volume']
    data = {}
    for ticker in tickers:
        try:
            ddf = yf.download(ticker, start=start_dt, end=end_dt, auto_adjust=False)
            if len(ddf)>0:
                ddf.columns = [col[0] for col in ddf.columns]
                # logging.debug(f'Reshape column of {ticker} to {ddf.head(2)}')
                ddf['base_cur'] = base
                ddf['target_cur'] = ticker.split('=')[0]
                data[ticker] = ddf[cols]
                logging.debug(f'Reshape column of {ticker} to {data[ticker].head(3)}')
        except Exception as e:
            logging.error("Exception occurred at fetch historical FX", exc_info=True)           
        
    # Combine all data into a single DataFrame for easier analysis (optional)
    if len(data)>0:
        combined_data = pd.concat(data.values())
    else:
        combined_data = pd.DataFrame()
    return combined_data

def fx_run(event, context):
    if "NYTIME" in event:
        current_time = event["NYTIME"]
    else:
        current_time = datetime.now().strftime("%Y/%m/%d-%H:%M:%S")
    mToday = current_time.date()
    today5PM = current_time.replace(hour=17, minute=0, second=0, microsecond=0)
    if current_time < today5PM:
        mToday = mToday - timedelta(days=1)
    current_time = current_time.strftime("%Y/%m/%d-%H:%M:%S")
    logger.info("Your cron function fxeod_handler " + " ran at " + current_time)

    base_cur = "USD"
    # Get the string representation of the list from .env
    my_list_str = os.getenv("FX_TICKERS")
    tickers = ast.literal_eval(my_list_str)
    tickers.sort()
    logging.debug(tickers)
    DBMKTDATA=os.environ.get("DBMKTDATA")
    TBLHISTFX=os.environ.get("TBLHISTFX") 
    FIRSTTRAINDTE = datetime.strptime(os.getenv("FIRSTTRAINDTE"), "%Y/%m/%d").date()
    mktdate = DU.get_Max_date(f'{DBMKTDATA}.{TBLHISTFX}')
    if mktdate is None:
        Sdate = FIRSTTRAINDTE
    else:
        Sdate = mktdate + timedelta(days=1)

    logging.info(f"Start_dt = {Sdate}   ---  end_dt = {mToday} ")
    fx_df = fetch_exchange_rates(Sdate, mToday, tickers, base_cur)
    fx_df['server_time'] = current_time 

    logging.debug(fx_df)

    if localrun:
        fx_df.reset_index().to_csv(f"{base_cur}_dailyFX.csv", index=False)
    elif len(fx_df)>0 :
        DU.StoreEOD(fx_df.reset_index(), DBMKTDATA, TBLHISTFX)

def run(event, context):
    if ('test' in event):
        logger.setLevel(logging.DEBUG)  # Set the logger to handle DEBUG level messages
    else:
        logger.setLevel(logging.INFO)  # Set the logger to handle DEBUG level messages

    logging.info(f"** ==> fx_handler.run(event: {event}, context: {context}")
    # Get the current time in New York
    ny_time = datetime.now().astimezone( pytz.timezone('US/Eastern'))
    logging.info(f"Current NY Time: {ny_time}")
    event["NYTIME"] = ny_time
    fx_run(event, context)     

if __name__ == '__main__':
    localrun = True
    event={"test": "True"}
    run(event, 0)
