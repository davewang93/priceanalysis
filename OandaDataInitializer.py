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

host = parser.get('pricedb','host')
user = parser.get('pricedb','user')
passwd = parser.get('pricedb','passwd')
database = parser.get('pricedb','database')

engine = parser.get('engines','pricedbengine')

access_token = parser.get('keys','access_token')

#connect to specific db w/ both mysql connector and sqlalchemy. sqlalchemy for pushing and mysql for pulling
mydb = mysql.connector.connect(
    host = host,
    user = user,
    passwd = passwd,
    database = database,
)

#connect to db using sqlalchemy for read/write
engine = create_engine(engine)

#access requirements
client= API(access_token=access_token,environment="live")

#parameters for request
params = {
  "count" : "1",
  "granularity": "D"
}

#see test file for full doc - define instrument below
r = instruments.InstrumentsCandles(instrument="EUR_NZD", params = params)
RawTable = client.request(r)
normalize = json_normalize(RawTable['candles'])
candles = pd.DataFrame(normalize, columns=['volume','time','mid.o','mid.h','mid.l','mid.c',]).set_index('time')
candles.index = pd.DatetimeIndex(candles.index)
#print(candles)
 
candles.to_sql("eurnzd",engine)