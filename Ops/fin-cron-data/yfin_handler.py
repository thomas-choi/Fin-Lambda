import logging
import json
import DDSClient as DDS
from dotenv import load_dotenv
import dataUtil as DU
from os import environ
import pandas as pd
# import opt_handler as OPT
from datetime import datetime, timezone, timedelta
import pytz
import yfinance as yf
import sys

# Create logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # Set the logger to handle DEBUG level messages

# Create handler for logging to stdout
# handler = logging.StreamHandler(sys.stdout)
# handler.setLevel(logging.DEBUG)  # Ensure the handler captures DEBUG level messages

load_dotenv() 
    
TBLmap = {'33': 'timestamp', '37':'high', '133':'open','32':'low', '3':'last',
          '0':'Symbol','21':'name',
           '31':'pclose', '30':'30', '127':'close',
          '17':'volume', '1':'bid', '16':'bidvol', '2':'ask', '19':'askvol'}

localrun = False
testing = False

def record_info(rec):
    try:
        logging.info(f"Symbol:({rec['symbol']})")
        logging.info(f"Type:  {rec['quoteType']}")
        logging.info("************")
        info = dict()
        info['quoteType'] = rec['quoteType']
        info['name'] = rec['longName']
        info['Symbol'] = rec['symbol']
        if 'previousClose' in rec:
            info['pclose'] = rec['previousClose']
        else:
            info['pclose'] = 0.0
        if info['quoteType'] == 'EQUITY':
            info['last'] = rec['currentPrice']
            info['open'] = rec['open']
            info['high'] = rec['dayHigh']
            info['low'] = rec['dayLow']
            info['volume'] = rec['volume']
            if 'bid' in rec:
                info['bid'] = rec['bid']
            else:
                info['bid'] = 0.0            
            if 'ask' in rec:
                info['ask'] = rec['ask']
            else:
                info['ask'] = 0.0            
            if 'bidSize' in rec:
                info['bidvol'] = rec['bidSize']
                info['askvol'] = 0
            else:
                info['bidvol'] = 0
                info['askvol'] = 0
        else:
            info['last'] = info['pclose']
            info['open'] = 0.0
            info['high'] = 0.0
            info['low'] = 0.0
            info['volume'] = 0.0
            info['bid'] = 0.0
            info['bidvol'] = 0.0
            info['ask'] = 0.0
            info['askvol'] = 0.0
        return info
    except:
        logging.error(rec)
        return None

def stk_run(event, context):
    if "NYTIME" in event:
        current_time = event["NYTIME"]
    else:
        current_time = datetime.now().strftime("%Y/%m/%d-%H:%M:%S")
    current_time = current_time.strftime("%Y/%m/%d-%H:%M:%S")
    logger.info("Your cron function handler.stk_run " + " ran at " + current_time)

    list_N = ["system"]              # get symbol lists from the system database

    market = pd.DataFrame()
    records = []
    for lt in list_N:
        symbol_list = DU.load_symbols(lt, "V3")
        if testing:
            symbol_list = ['0002.HK','000270.KS','0P0001Q87U','6415.TW','601966.SS','300347.SZ','IHYU.L','OBEROIRLTY.NS','SIDO.JK','TSLA','SPY']
        num_s = len(symbol_list)
        logger.info(f'Process \'{lt}\' with {num_s} of  {symbol_list}')
        for sy in symbol_list:
            try:
                stock = yf.Ticker(sy)
                record = record_info(stock.info)
                if record is not None:
                    records.append(record)

            except Exception as error:
                logging.error(f"yf_ticker: {error}")

    DBheaders = ['Symbol','open','high','low','last','volume','bid','bidvol','ask','askvol','pclose','name','timestamp','quoteType']
    market = pd.DataFrame(records)
    market['timestamp'] = current_time
    market = market[DBheaders].dropna(axis=0, how='any')
    logging.info(f"Data Size: {market.shape}")

    # market['timestamp'] = market['timestamp'] + current_time
    # logging.info(market.info)
    out_symbols = market['Symbol'].to_list()
    difference = list(set(symbol_list) - set(out_symbols))
    if len(difference)>0:
        logging.error(f"{len(difference)} of symbols has no data: {difference}")
    DBMKTDATA=environ.get("DBMKTDATA")
    TBLSNAPSHOOT=environ.get("TBLSNAPSHOOT")
    if localrun:
        market.to_csv(f"{TBLSNAPSHOOT}.csv", index=False)
    else:
        DU.ExecSQL(f"DELETE FROM {DBMKTDATA}.{TBLSNAPSHOOT} where (Symbol != \'1\');")
        DU.StoreEOD(market, DBMKTDATA, TBLSNAPSHOOT)

def run(event, context):
    logging.info(f"** ==> yfin_handler.run(event: {event}, context: {context}")
    # Get the current time in New York
    ny_time = datetime.now().astimezone( pytz.timezone('US/Eastern'))
    logging.info(f"Current NY Time: {ny_time}")
    event["NYTIME"] = ny_time
    # Check if the current time is after 9:30 AM and before 4 PM
    if ny_time.time() >= datetime.strptime('09:30', '%H:%M').time() and ny_time.time() < datetime.strptime('16:00', '%H:%M').time():
        logging.info('The current time is between 9:30 AM and 4 PM in New York time.')
        stk_run(event, context)
        # OPT.run(event, context)       
    else:
        logging.info('The current time is not between 9:30 AM and 4 PM in New York time.')
        stk_run(event, context)
        # if "test" in event:
        #     OPT.run(event, context)

if __name__ == '__main__':
    # logging.basicConfig(filename="yfin_handler.log", encoding='utf-8', level=logging.DEBUG)
    localrun = False
    testing = False
    # OPT.localrun=localrun
    event={"test":"true"}
    run(event, 0)