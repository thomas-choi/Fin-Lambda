import requests
import pandas as pd
from bs4 import BeautifulSoup
import logging
from dotenv import load_dotenv
from datetime import datetime, timedelta
import argparse
from os import environ

import dataUtil as DU

def  HTML2DataFrame(_url):

    # Send a GET request to the webpage and store the response
    response = requests.get(_url)

    soup = BeautifulSoup(response.text, 'html.parser')
    logging.debug(soup.prettify())

    table = soup.find(id='h15table')
    logging.debug(table)

    data = pd.read_html(str(table))
    data[0]['Instruments'].values

    return data[0]

# DPCREDIT - Discount window primary credit
# 5YTIISNK - 5 years inflation indexed Treasury constant maturities
nInstruments=['Federal_funds', 'CP',
       'NF', 'CP_NF_1_month', 'CP_NF_2_month', 'CP_NF_3_month', 'Fi',
       'CP_Fi_1_month', 'CP_Fi_2_month', 'CP_Fi_3_month', 'Bank_prime_loan',
       'DPCREDIT', 'U.S.',
       'TBill', 'TBill_4_week', 'TBill_3_month',
       'TBill_6_month', 'TBill_1_year', 'TBond', 'Nominal',
       'TBond_1_month', 'TBond_3_month', 'TBond_6_month', 'TBond_1_year', 'TBond_2_year',
       'TBond_3_year',
       'TBond_5_year', 'TBond_7_year', 'TBond_10_year', 'TBond_20_year', 'TBond_30_year',
       'Inflation', '5YTIISNK', '7YTIISNK', '10YTIINK', '20YTIINK',
       '30YTIINK', 'Inf_average']



if __name__ == '__main__':
    load_dotenv("./Prod_config/Stk_eodfetch_DO.env") #Check path for env variables
    # logging.basicConfig(filename=f'logging/eod_USrates_{datetime.today().date()}.log', filemode='a', format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
    logging.getLogger().setLevel(logging.DEBUG)

    tzNow = DU.nowbyTZ('US/Eastern')

    parser = argparse.ArgumentParser()
    # parser.add_argument('-D', '--Date', dest='InDate', type=str)
    parser.add_argument('-S', '--SSHDB', dest='SSHDB', action='store_true', default=False)
    # parser.add_argument('-t', '--test', dest='test', action='store_true', default=False)
    parser.add_argument('-c', '--check', dest='checkFlag', action='store_true', default=False)
    parser.add_argument('-f', '--force', dest='forceFlag', action='store_true', default=False)

    args = parser.parse_args()

    logging.debug(f'argments: {args}')
    todt = tzNow.date()

    if not (todt.isoweekday() in range(1,6) or args.forceFlag):
        print('Jobs must be ran from Monday to Friday. use -f otherwise')
        quit()

    if args.SSHDB:
        DU.setDBSSH()

    if args.checkFlag:
        quit()

    # URL of the webpage to retrieve the data from
    url = 'https://www.federalreserve.gov/releases/h15/'

    df = HTML2DataFrame(url).copy()
    dropcols = []
    for col in df.columns.values:
        if col.find('*') > -1:
            dropcols.append(col)
    logging.debug(f'Need to drop columns: {dropcols}')
    df = df.drop(columns=dropcols)
    logging.debug(df.info())
    logging.debug(df)
    df['Instruments'] = nInstruments
    df = df.set_index('Instruments')
    logging.debug(df.info())
    df = df.drop(index=['CP','NF','Fi','U.S.','TBill','TBond', 'Nominal','Inflation'])
    logging.debug(df)

    rates = df.transpose().replace(regex={'n.a.':'nan'}).astype(float)
    rates.index = pd.to_datetime(rates.index)
    rates.index.name = 'Date'
    logging.debug(rates)

    datapath = "USrates.csv"
    logging.debug(f'Writing usrates to {datapath}')
    rates.reset_index().to_csv(datapath, index=False)
    retDF = pd.read_csv(datapath)
    retDF['Date'] = pd.to_datetime(retDF.Date)
    logging.debug(retDF)
    logging.debug(retDF.info())
    DB = environ.get("DBMKTDATA")
    TBL = environ.get("TBLUSRATES")
    maxdate = DU.get_Max_date(f'{DB}.{TBL}')
    if maxdate is None:
        maxdate = datetime(1800,1,1)
    maxdate = maxdate.strftime("%Y-%m-%d")
    logging.info(f'US Rates max date is {maxdate}')
    stDF = retDF[retDF['Date']>maxdate]
    if len(stDF)>0:
        logging.debug('Loading data to database ')
        # DU.StoreEOD(stDF, DB, TBL)
