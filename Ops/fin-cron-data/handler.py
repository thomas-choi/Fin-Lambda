import logging
import json
import DDSClient as DDS
from dotenv import load_dotenv
import dataUtil as DU
from os import environ
import pandas as pd
from datetime import datetime, timezone, timedelta
import pytz
import yfinance as yf

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
load_dotenv() 
DEBUG=environ.get("DEBUG")
if DEBUG == "debug":
    logger.setLevel(logging.DEBUG)
    print("Logging is DEBUG.")
    
TBLmap = {'33': 'timestamp', '37':'high', '133':'open','32':'low', '3':'last',
          '0':'Symbol','21':'name',
           '31':'pclose', '30':'30', '127':'close',
          '17':'volume', '1':'bid', '16':'bidvol', '2':'ask', '19':'askvol'}

localrun = False
testing = False

def stk_run(event, context):
    global localrun

    if "NYTIME" in event:
        current_time = event["NYTIME"]
    else:
        current_time = datetime.now().strftime("-%Y/%m/%d-%H:%M:%S")
    current_time = current_time.strftime("-%Y/%m/%d-%H:%M:%S")
    logger.info("Your cron function handler.stk_run " + " ran at " + current_time)

    # StockList = ['AMD','BAC','C','CSCO','DIS','DKNG','KO','MSFT','MU','NVDA','OXY','PYPL','TFC','TSLA','UBER','USB','VZ','WFC','XOM']
    # StockList = ['IBM']
    # list_N = ["stock_list", "etf_list", "crypto_list", "us-cn_stock_list"]
    # list_N = ["full_list"]          # tge combined list from the above symbol list files.
    list_N = ["system"]              # get symbol lists from the system database
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
            logging.info('reply : ', reply)
            # logging.info(reply.keys())
            # logging.info(reply.values())
            market.loc[len(market), reply.keys()] = reply.values()
    del DDSServer
    DBheaders = ['Symbol','open','high','low','last','volume','bid','bidvol','ask','askvol','pclose','name','timestamp']
    market = market[DBheaders].dropna(axis=0, how='any')
    market['timestamp'] = market['timestamp'] + current_time
    logging.info(market.info)
    logging.info(market.head(2))
    logging.info(market.tail(2))
    DBMKTDATA=environ.get("DBMKTDATA")
    TBLSNAPSHOOT="snapshot"
    if localrun:
        market.to_csv(f"{TBLSNAPSHOOT}.csv", index=False)
    else:
        DU.ExecSQL(f"DELETE FROM {DBMKTDATA}.{TBLSNAPSHOOT} where (Symbol != \'1\');")
        DU.StoreEOD(market, DBMKTDATA, TBLSNAPSHOOT)

def yf_stk_run(event, context):
    """
    Retrieve stock market data using yfinance API instead of DDS client.
    Fetches the same fields as stk_run() for consistency.
    """
    global localrun

    if "NYTIME" in event:
        current_time = event["NYTIME"]
    else:
        current_time = datetime.now().astimezone(pytz.timezone('US/Eastern'))
    
    timestamp_str = current_time.strftime("-%Y/%m/%d-%H:%M:%S")
    logger.info(f"Your cron function handler.yf_stk_run ran at {current_time}")

    list_N = ["system"]              # get symbol lists from the system database
    
    market = pd.DataFrame(columns=['Symbol', 'open', 'high', 'low', 'last', 'volume', 
                                   'bid', 'bidvol', 'ask', 'askvol', 'pclose', 'name', 'timestamp'])
    
    for lt in list_N:
        symbol_list = DU.load_symbols(lt)
        logger.info(f'Process {lt} with {symbol_list}')
        
        for sy in symbol_list:
            try:
                # Fetch ticker data from yfinance
                ticker = yf.Ticker(sy)
                
                # Get the latest data
                info = ticker.info
                hist = ticker.history(period='1d')
                
                if hist.empty:
                    logger.warning(f'No data available for {sy}')
                    continue
                
                # Extract data from the most recent row
                latest = hist.iloc[-1]
                
                # Build market data dictionary
                market_data = {
                    'Symbol': sy,
                    'open': latest.get('Open', None),
                    'high': latest.get('High', None),
                    'low': latest.get('Low', None),
                    'last': latest.get('Close', None),  # Close price as 'last'
                    'volume': int(latest.get('Volume', 0)) if pd.notna(latest.get('Volume')) else 0,
                    'bid': info.get('bid', None),
                    'bidvol': info.get('bidSize', None),
                    'ask': info.get('ask', None),
                    'askvol': info.get('askSize', None),
                    'pclose': info.get('previousClose', None),
                    'name': info.get('longName', sy),
                    'timestamp': current_time.strftime("%Y/%m/%d-%H:%M:%S")
                }
                
                # Append to market dataframe
                market = pd.concat([market, pd.DataFrame([market_data])], ignore_index=True)
                logger.info(f'Successfully fetched data for {sy}')
                
            except Exception as e:
                logger.error(f'Error fetching data for {sy}: {str(e)}')
                continue
    
    # Clean and prepare data
    market = market.dropna(axis=0, how='any')
    
    logger.info(market.info())
    logger.info(market.head(2))
    logger.info(market.tail(2))
    
    DBMKTDATA = environ.get("DBMKTDATA")
    TBLSNAPSHOOT = "snapshot"
    
    if localrun:
        market.to_csv(f"{TBLSNAPSHOOT}_yf.csv", index=False)
        logger.info(f'Saved market data to {TBLSNAPSHOOT}_yf.csv')
    else:
        try:
            DU.ExecSQL(f"DELETE FROM {DBMKTDATA}.{TBLSNAPSHOOT} where (Symbol != \'1\');")
            DU.StoreEOD(market, DBMKTDATA, TBLSNAPSHOOT)
            logger.info(f'Successfully stored {len(market)} records to database')
        except Exception as e:
            logger.error(f'Error storing data to database: {str(e)}')

def run(event, context):
    logging.info(f"** ==> handler.run(event: {event}, context: {context}")
    # Get the current time in New York
    ny_time = datetime.now().astimezone( pytz.timezone('US/Eastern'))
    logging.info(f"Current NY Time: {ny_time}")
    event["NYTIME"] = ny_time
    # Check if the current time is after 9:30 AM and before 4 PM
    if ny_time.time() >= datetime.strptime('09:30', '%H:%M').time() and ny_time.time() < datetime.strptime('16:00', '%H:%M').time():
        logging.info('The current time is between 9:30 AM and 4 PM in New York time.')
        yf_stk_run(event, context)
        #  Cannot run opt_snapshot data from yfinance, use IB from local
        # OPT.run(event, context)       
    else:
        logging.info('The current time is not between 9:30 AM and 4 PM in New York time.')
        yf_stk_run(event, context)

if __name__ == '__main__':
    LOCALRUN = environ.get("LOCALRUN")
    if LOCALRUN == "localrun":
        localrun = True
        logging.basicConfig(filename="handler.log", encoding='utf-8')
        print("Set localrun True")
    event={"test":"false"}
    run(event, 0)
