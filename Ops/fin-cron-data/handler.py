import datetime
import logging
import json
import DDSClient as DDS

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# def run(event, context):
#     print("In handler.run()\n")
#     current_time = datetime.datetime.now()
#     name = context.function_name
#     timestr = str(current_time)
#     logger.info("Your cron function " + name + " ran at " + timestr)

#     market = dict()
#     bucket = "workingplace"
#     s3_client = boto3.client('s3')
#     data1 = {"bid":100.0, "ask":101.2, "volumn":20000.0, "time":timestr}
#     data2 = {"bid":9.35, "ask":9.40, "volumn":100.0, "time":timestr}
#     market['data1'] = data1
#     market['data2'] = data2

#     defaultIP = "47.106.136.162" 
#     defaultPort = 9945

#     DDSServer = DDS.TCPClient(defaultIP, defaultPort)
  
#     jstr = json.dumps(market)
#     print("Storing: ", jstr)
#     s3_client.put_object(Bucket=bucket, 
#                         Key="MarketData.json",
#                         Body=jstr)
    
TBLmap = {'33': 'timestamp', '37':'high', '133':'open','32':'low', '3':'last',
          '0':'Symbol','21':'name',
           '31':'pclose', '30':'30', '127':'close',
          '17':'volume', '1':'bid', '16':'bidvol', '2':'ask', '19':'askvol'}

localrun = False

def run(event, context):
    from dotenv import load_dotenv
    import dataUtil as DU
    import DDSClient as DDS
    import json
    from os import environ
    import pandas as pd

    print("In handler.run()\n")
    current_time = datetime.datetime.now()
    timestr = str(current_time)
    logger.info("Your cron function handler.run " + " ran at " + timestr)

    load_dotenv("Prod_config/Stk_eodfetch.env") 
    # StockList = ['AMD','BAC','C','CSCO','DIS','DKNG','KO','MSFT','MU','NVDA','OXY','PYPL','TFC','TSLA','UBER','USB','VZ','WFC','XOM']
    # StockList = ['IBM']
    list_N = ["stock_list", "etf_list", "crypto_list", "us-cn_stock_list"]
    # list_N = ["test_list"]
    defaultIP=environ.get("defaultIP")
    defaultPort=int(environ.get("defaultPort"))
    # defaultIP = "47.106.136.162" 
    # defaultPort = 9945

    market = pd.DataFrame(columns=TBLmap.values())
    DDSServer = DDS.TCPClient(defaultIP, defaultPort)
    for lt in list_N:
        symbol_list = DU.load_symbols(lt)
        logger.info(f'Process {lt} with {symbol_list}')
        for sy in symbol_list:   
            reply = DDSServer.snapshot(sy, TBLmap)
            # print('reply : ', reply)
            print(reply.keys())
            print(reply.values())
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
    # else:
    #     s3_client = boto3.client('s3')
    #     s3_client.put_object(Bucket="workingplace", 
    #                         Key="MarketData.json",
    #                         Body=jstr)   
        
    DU.ExecSQL(f"DELETE FROM {DBMKTDATA}.{TBLSNAPSHOOT} where (Symbol != \'1\');")
    DU.StoreEOD(market, DBMKTDATA, TBLSNAPSHOOT)

if __name__ == '__main__':
    localrun = True
    run(0, 0)