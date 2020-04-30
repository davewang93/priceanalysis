from oandapyV20 import  API
import oandapyV20.endpoints.instruments as instruments
import pandas_datareader as pdr
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import mysql.connector
import datetime
from datetime import timedelta
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

#connect to db using sqlalchemy
engine = create_engine(engine)

#access requirements
client= API(access_token=access_token,environment="live")

#todays date
today = datetime.date.today()
print(today)
#block below is used to update tables - should be turned into callable object to save on the copy paste

#create sql.connector cursor, often called "self"
my_cursor = mydb.cursor()

my_cursor.execute("SELECT TIME FROM oandaprices.usdjpy ORDER BY TIME DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
print(LastRecord)
LastDate = LastRecord[0][0]
print(LastDate)
StartDate = LastDate.date() + timedelta(days=2)
print(StartDate)
params = {
  "from" : StartDate,
  "to" : today,
  "granularity": "D"
}
r = instruments.InstrumentsCandles(instrument="USD_JPY", params = params)
RawTable = client.request(r)
normalize = json_normalize(RawTable['candles'])
candles = pd.DataFrame(normalize, columns=['volume','time','mid.o','mid.h','mid.l','mid.c',]).set_index('time')
candles.index = pd.DatetimeIndex(candles.index)
#print(candles)
candles.to_sql("test",engine,if_exists='append')

'''
my_cursor.execute("SELECT TIME FROM test2 ORDER BY TIME DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
print(LastRecord)
LastDate = LastRecord[0][0]
print(LastDate)
StartDate = LastDate.date() + timedelta(days=2)
print(StartDate)
params = {
  "from" : StartDate,
  "granularity": "D"
}
r = instruments.InstrumentsCandles(instrument="AUD_USD", params = params)
RawTable = client.request(r)
normalize = json_normalize(RawTable['candles'])
candles = pd.DataFrame(normalize, columns=['volume','time','mid.o','mid.h','mid.l','mid.c',]).set_index('time')
candles.index = pd.DatetimeIndex(candles.index)
#print(candles)
candles.to_sql("test2",engine,if_exists='append')

'''