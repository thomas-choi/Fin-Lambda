import getFamaFrenchFactors as gff
import pandas as pd
import logging
from dotenv import load_dotenv
import dataUtil as DU
import datetime as dt

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
load_dotenv() 
    
def run(event, context):

    logging.info("** ==> run(event: {event}, context: {context}")
    ny_time = datetime.now().astimezone( pytz.timezone('US/Eastern'))
    logging.info(f"Current NY Time: {ny_time}")

    ff3_monthly = pd.DataFrame(gff.famaFrench3Factor(frequency='m'))
    ff3_monthly.rename(columns={'date_ff_factors':'Date'}, inplace=True)
    print(ff3_monthly)

    opt_tbl = "famaFrench"
    trd_DB = "GlobalMarketData"
    max_date = DU.get_Max_date(f'{trd_DB}.{opt_tbl}')
    logging.debug(f'ff3 max_date is {max_date}')

    if max_date is not None:
        ff3_monthly = ff3_monthly[ff3_monthly['Date'].dt.date > max_date]
    lrecord = len(ff3_monthly)
    logging.debug(f'{lrecord} new rows to ff3')
    if len(ff3_monthly) > 0:
        DU.StoreEOD(ff3_monthly, trd_DB, opt_tbl)

if __name__ == '__main__':
    # logging.basicConfig(filename="opt_handler.log", encoding='utf-8', level=logging.DEBUG)

    localrun = False
    localrun = True
    run(0, 0)
