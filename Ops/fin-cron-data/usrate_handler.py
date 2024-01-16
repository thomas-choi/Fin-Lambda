import pandas as pd
import logging
import sys
from dotenv import load_dotenv
import dataUtil as DU
import datetime as dt
from bs4 import BeautifulSoup
import pytz
import requests
from os import environ

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
load_dotenv() 

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


def run(event, context):

    logging.info("** ==> run(event: {event}, context: {context}")
    ny_time = dt.datetime.now().astimezone( pytz.timezone('US/Eastern'))
    logging.info(f"Current NY Time: {ny_time}")

    DB = environ.get("DBMKTDATA")
    TBL = environ.get("TBLUSRATES")
    data_name = f'{DB}.{TBL}'
    max_date = DU.get_Max_date(data_name)
    logging.debug(f'{data_name} max_date is {max_date}')
    maxdate = DU.get_Max_date(data_name)
    if maxdate is None:
        maxdate = datetime(1800,1,1)
    maxdate = maxdate.strftime("%Y-%m-%d")
    logging.info(f'US Rates max date is {maxdate}')

    # URL of the webpage to retrieve the data from
    url = 'https://www.federalreserve.gov/releases/h15/'

    df = HTML2DataFrame(url).copy()

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

    retDF = rates.reset_index()
    retDF['Date'] = pd.to_datetime(retDF.Date)
    logging.debug(retDF)
    logging.debug(retDF.info())

    stDF = retDF[retDF['Date']>maxdate]
    if len(stDF)>0:
        logging.debug('Loading data to database ')
        DU.StoreEOD(stDF, DB, TBL)

if __name__ == '__main__':
    # logging.basicConfig(filename="opt_handler.log", encoding='utf-8', level=logging.DEBUG)

    localrun = False
    localrun = True
    run(0, 0)
