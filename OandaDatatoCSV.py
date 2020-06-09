from oandapyV20 import  API
import oandapyV20.endpoints.instruments as instruments
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import mysql.connector
from datetime import datetime, timedelta
from pandas.io.json import json_normalize
from configparser import ConfigParser 

parser = ConfigParser()
parser.read('config.ini')

access_token = parser.get('keys','access_token')

#access requirements
client= API(access_token=access_token,environment="live")

#parameters for request
params = {
  "count" : "5000",
  "granularity": "S30"
}

#see test file for full doc - define instrument below
r = instruments.InstrumentsCandles(instrument="GBP_USD", params = params)
RawTable = client.request(r)
normalize = json_normalize(RawTable['candles'])
candles = pd.DataFrame(normalize, columns=['volume','time','mid.o','mid.h','mid.l','mid.c',]).set_index('time')
print(candles.head())

candles.index = pd.DatetimeIndex(candles.index)
print(candles.head())

#candles.to_excel(r"D:\OneDrive\David\src\Forex APIs Test Region\GBPUSD30sec.xlsx") 