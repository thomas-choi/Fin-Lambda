import os
from os import path
from os import environ
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import logging
from dateutil.rrule import rrule, DAILY
import yfinance as yf
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pytz
import time
import argparse
import dataUtil as DU


load_dotenv() 
DEBUG=environ.get("DEBUG")
print(f"DEBUG is {DEBUG}")
if DEBUG == "debug":
    logging.basicConfig(level=logging.DEBUG)
    print("Logging is DEBUG.")
else:
    logging.basicConfig(level=logging.INFO)
    print("Logging is INFO.")
logger = logging.getLogger(__name__)
    
tformats = '%Y-%m-%d %H:%M:%S'

def yf_get_max_datetime(localnow, sym=None):
    DBMKTDATA = environ.get("DBMKTDATA")
    TBLMINUTEPRICE = environ.get("TBLMINUTEPRICE", "minute_price")

    mkt_datetime = DU.get_Max_datetime(f'{DBMKTDATA}.{TBLMINUTEPRICE}', sym)
    logging.info(f"Max date of {sym} at {DBMKTDATA}.{TBLMINUTEPRICE} is {mkt_datetime}")
    if mkt_datetime is None:
        Sdatetime = localnow - timedelta(days=59)
    else:
        Sdatetime = mkt_datetime
    logging.debug(f"maxdate : {Sdatetime} for {DBMKTDATA}.{TBLMINUTEPRICE} : {sym}")
    return Sdatetime

def yf_download(sym, sdatetime, edatetime):
    logging.debug(f"yf_download({sym}, {sdatetime} - {edatetime})")
    sDF = yf.download(sym, start=sdatetime, end=edatetime, interval='30m', auto_adjust=False)
    if len(sDF) > 0:
        sDF = sDF.reset_index()
        if 'Adj Close' in sDF.columns:
            sDF = sDF.rename(columns={'Adj Close':'AdjClose'})
    return sDF

def check_exchange_from_ticker(ticker):
    st = ticker.split(".")
    if len(st)>1:
        return st[-1]
    else:
        return None

def yf_exchange_code(exdict, sym):
    if sym in exdict:
        return exdict[sym]
    else:
        return check_exchange_from_ticker(sym)

def load_us_symbols():
    symbol_list = []
    sql=f"call GlobalMarketData.get_us_symbol;"
    logging.info(f"load_asia_symbols({sql})")
    df = DU.load_df_SQL(sql)
    logging.info(f"load from intra_blacklist.csv")
    blacklist = pd.read_csv("intra_blacklist.csv", encoding='utf-8')
    blacklist = sorted(blacklist.Symbol.unique())
    symbol_list = sorted(list(set(sorted(df.Symbol.unique()))- set(blacklist)))
    logging.debug(f'{symbol_list}')
    return symbol_list

def common_fetch_eod(sdatetime, tdatetime, list_name, localrun, dbFlag=True): 

    logging.info(f'common_fetch_eod handle {sdatetime} UPTO {tdatetime}, list_name={list_name}, dbFlag={dbFlag}')

    symbol_list = load_us_symbols()
    if len(symbol_list) <= 0:
        return
    exch_dict = DU.load_symbols_dict()
    tzlist = DU.load_exchange_tz()
    DBMKTDATA = environ.get("DBMKTDATA")
    TBLMINUTEPRICE = environ.get("TBLMINUTEPRICE", "minute_price")
    savColumns = ['Datetime','Symbol','Exchange','Close','Open','High','Low','Volume','AdjClose','UTCDatetime','timezone']
    datallist = list()
    logging.debug(f"symbol_list: {symbol_list}")
     
    for sym in symbol_list:
        exchange = yf_exchange_code(exch_dict, sym)
        logging.debug(f"sym: {sym},   Exchange: {exchange}")
        if exchange is None:
            # Skip symbols without exchange info
            logging.warning(f"Exchange not found for symbol {sym}, skipping...")
            continue
        tz = tzlist[exchange]
        localnow = tdatetime.astimezone(pytz.timezone(tz))
        sdatetime = yf_get_max_datetime(localnow, sym)
        if sdatetime.tzinfo is None:
            sdatetime = pytz.timezone(tz).localize(sdatetime)
        if localnow >= sdatetime:
            edatetime = localnow + timedelta(minutes=1)
            logging.debug(f'Loading {sym} minute OHLC from Yahoo {sdatetime} to {edatetime}!   localnow={localnow}')

            sDF = yf_download(sym, sdatetime, edatetime)
            if localrun:
                sDF.to_csv(f"30min_{sym}.csv", index=False)
                   
            logging.info(f"{sym} is downloaded DF : {len(sDF)} records")
            logging.debug(sDF.info())
            if len(sDF) > 0:
                # Remove timezone info from 'Datetime' column
                if sDF.columns.nlevels > 1:
                    sDF.columns = [col[0] for col in sDF.columns]
                sDF = sDF.rename(columns={'Datetime': 'UTCDatetime'})
                sDF['Symbol'] = sym
                sDF['Exchange'] = exchange
                # store datetime in local timezone of the exchange
                sDF['Datetime'] = sDF['UTCDatetime'].dt.tz_convert (tz=tz)
                sDF['timezone'] = tz
                # sDF['Datetime'] = pd.to_datetime(sDF['Datetime'])
                logging.info(f'common_fetch_eod: downloaded {sym} from {sDF.Datetime.iloc[0]} to {sDF.Datetime.iloc[-1]}')
                sDF = sDF[(sDF['Datetime'] > sdatetime) & (sDF['Datetime'] <= edatetime)]
                sDF = sDF[savColumns]
                # Remove timezone information
                sDF['Datetime'] = sDF['Datetime'].dt.tz_localize(None)
            if len(sDF) > 0:
                if len(sDF) > 100 and dbFlag:
                    DU.StoreEOD(sDF, None, TBLMINUTEPRICE)
                else:
                    datallist.append(sDF)
    
    if len(datallist) > 0:
        totalDF = pd.concat(datallist)
        if localrun:
            totalDF.to_csv(f"30min_{list_name}.csv", index=False)
        # totalDF.Datetime = pd.to_datetime(totalDF.Datetime)
        if dbFlag:
            DU.StoreEOD(totalDF, None, TBLMINUTEPRICE)
    
    logging.info(f'common_fetch_eod finish the handle {list_name} UPTO {tdatetime}')

def minute_output_columns():
    return ["Datetime", "Symbol", "Exchange", "garch", "svr", "mlp", "LSTM", "prev_Close", "prediction", "volatility"]

def run(event, context):

    utcNow = datetime.now(pytz.utc)
    Sdatetime = yf_get_max_datetime(utcNow)
    localrun = False
    dbFlag = True
    if "localrun" in event:
        localrun = event["localrun"]
    if "dbFlag" in event:
        dbFlag = event["dbFlag"]
    list_N = ["stock_list", "etf_list", "crypto_list", "us-cn_stock_list"]
    SYMBOLLIST = environ.get("SYMBOLLIST")
    if SYMBOLLIST is not None:
        common_fetch_eod(Sdatetime, utcNow, list_name=SYMBOLLIST, localrun=localrun, dbFlag=dbFlag)
    else:
        for symN in list_N:
            common_fetch_eod(Sdatetime, utcNow, list_name=symN, localrun=localrun, dbFlag=dbFlag)

if __name__ == '__main__':

    event={"localrun":True, "dbFlag":True}
    run(event, 0)