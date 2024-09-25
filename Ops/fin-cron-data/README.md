# fin-cron-data 
This is a group of lambda functions that can be deployed AWS using Serverless Framework

## 1. The Lambda Functions which are executed daily

- cronHandler
- fffHandler
- usrateHandler

The additional options data source is added in case there is some strike is not available in yfinance. 

## 2. The Jupyter Notebook for manual tasks that would be executed once a month.

- Train new update HMM models

- Train new update LSTM models

