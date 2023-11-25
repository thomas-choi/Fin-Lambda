import logging
import json
import DDSClient as DDS
from dotenv import load_dotenv
import dataUtil as DU
from os import environ
import pandas as pd
import opt_handler as OPT
from datetime import datetime, timezone, timedelta
import pytz

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
load_dotenv() 
    
TBLmap = {'33': 'timestamp', '37':'high', '133':'open','32':'low', '3':'last',
          '0':'Symbol','21':'name',
           '31':'pclose', '30':'30', '127':'close',
          '17':'volume', '1':'bid', '16':'bidvol', '2':'ask', '19':'askvol'}

localrun = False
testing = False

def stk_run(event, context):
    current_time = datetime.now()
    timestr = str(current_time)
    logger.info("Your cron function handler.stk_run " + " ran at " + timestr)

    # StockList = ['AMD','BAC','C','CSCO','DIS','DKNG','KO','MSFT','MU','NVDA','OXY','PYPL','TFC','TSLA','UBER','USB','VZ','WFC','XOM']
    # StockList = ['IBM']
    list_N = ["stock_list", "etf_list", "crypto_list", "us-cn_stock_list"]
    # list_N = ["test_list"]
    defaultIP=environ.get("defaultIP")
    defaultPort=int(environ.get("defaultPort"))

    market = pd.DataFrame(columns=TBLmap.values())
    DDSServer = DDS.TCPClient(defaultIP, defaultPort)
    for lt in list_N:
        symbol_list = DU.load_symbols(lt)
        logger.info(f'Process {lt} with {symbol_list}')
        for sy in symbol_list:   
            reply = DDSServer.snapshot(sy, TBLmap)
            # print('reply : ', reply)
            logging.info(reply.keys())
            logging.info(reply.values())
            market.loc[len(market), reply.keys()] = reply.values()
    del DDSServer
    DBheaders = ['Symbol','open','high','low','last','volume','bid','bidvol','ask','askvol','pclose','name','timestamp']
    market = market[DBheaders].dropna(axis=0, how='any')
    print(market.info)
    print(market.head(2))
    print(market.tail(2))
    DBMKTDATA=environ.get("DBMKTDATA")
    TBLSNAPSHOOT="snapshot"
    if localrun:
        market.to_csv(f"{TBLSNAPSHOOT}.csv", index=False)
    else:
        DU.ExecSQL(f"DELETE FROM {DBMKTDATA}.{TBLSNAPSHOOT} where (Symbol != \'1\');")
        DU.StoreEOD(market, DBMKTDATA, TBLSNAPSHOOT)

def run(event, context):
    # Get the current time in New York
    logging.info("** ==> run(event: {event}, context: {context}")
    ny_time = datetime.now().astimezone( pytz.timezone('US/Eastern'))
    logging.info(f"Current NY Time: {ny_time}")
    # Check if the current time is after 9:30 AM and before 4 PM
    if ny_time.time() >= datetime.strptime('09:30', '%H:%M').time() and ny_time.time() < datetime.strptime('16:00', '%H:%M').time():
        logging.info('The current time is between 9:30 AM and 4 PM in New York time.')
        stk_run(event, context)
        OPT.run(event, context)
    else:
        logging.info('The current time is not between 9:30 AM and 4 PM in New York time.')
        stk_run(event, context)
        if "test" in event:
            OPT.run(event, context)

if __name__ == '__main__':
    # logging.basicConfig(filename="handler.log", encoding='utf-8')
    localrun = True
    OPT.localrun=True
    event={"test":"true"}
    run(event, 0)