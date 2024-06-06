# Fin-Lambda
Migrate data processing to AWS Lambda. There are two key area would be implemented with AWS lambda.

### Docker environment to simulate AWS Lambda runtime

- mlupin/docker-lambda:python3.10-build
- create **LambdaShell** for python Lambda development


### Market Data during market open

- Last, bid, ask, update periodically update during market open
- The products updated includes US ETF, US Stock, US Stock options


### Daily closing pricess


### Serverless framework configure file Ops/fin-cron-data/serverless.yml
- iam: to default the permission required in the cron tasks
- cronb tasks
    - cronhandler : to capture snapshoot of stock/etf prices during market hour and also pre-market + post-market hours
        - opt_handler : to capture options snapshoot of last, open-interest, volume ONLY in market hour
    - fffHandler : to download Fama French Factors data
    - usrateHandler : to download daily US Fed interest

### Making layers of python packages for the cron jobs
- finCronLib  : required by cronhandler, opt_handler, usratehandler. 

  At the project folder, do 

  $ make finCron.zip 

  Then upload to AWS and install the finCron.zip as layer