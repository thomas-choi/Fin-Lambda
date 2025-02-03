# fin-cron-data 
This is a group of lambda functions that can be deployed AWS using Serverless Framework

## 1. The Lambda Functions which are executed daily

- cronHandler
  ** Replaced by yfin_handler.py
  snapshot data from yfinance.
  
- fffHandler
- usrateHandler
- fxHandler
  download a snapshot of current FX rate.
  Current FX Tickers is ["HKD=X", "JPY=X", "MXN=X", "KRW=X", "CNY=X", "INR=X", "IDR=X","EUR=X","GBP=X","SGD=X","TWD=X","CAD=X","AUD=X", "NZD=X","THB=X", "MYR=X"]
- fxeod_handler.py
  download historical FX rate daily.
  Current FX Tickers is ["HKD=X", "JPY=X", "MXN=X", "KRW=X", "CNY=X", "INR=X", "IDR=X","EUR=X","GBP=X","SGD=X","TWD=X","CAD=X","AUD=X", "NZD=X","THB=X", "MYR=X"]

The additional options data source is added in case there is some strike is not available in yfinance. 

## 2. The Jupyter Notebook for manual tasks that would be executed once a month.

- Train new update HMM models

- Train new update LSTM models

