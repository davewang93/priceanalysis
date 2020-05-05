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

#todays date - this is needed so we don't pull the current incomplete candle
today = datetime.date.today()
#print(today)

#block below is used to update tables - should be turned into callable object to save on the copy paste

#create sql.connector cursor, often called "self"
my_cursor = mydb.cursor()
#call last record of sql table - notice it outputs as list in list even when select specific column, ie "Date"
my_cursor.execute("SELECT TIME FROM gbpusd ORDER BY TIME DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
#print(LastRecord)

#extract date from list in list
LastDate = LastRecord[0][0]
#print(LastDate)

#Drop the time using .date() and add 1 day to get new start date
#for some reason oanda uses t+1 as t. So need to do T+2. Not sure if this will cause problems down the line, should monitor
StartDate = LastDate.date() + timedelta(days=2)
#print(StartDate)

#pull new values going forward from start date
params = {
  "from" : StartDate,
  "to" : today,
  "granularity": "D"
}

#data pull from previous files
r = instruments.InstrumentsCandles(instrument="GBP_USD", params = params)
RawTable = client.request(r)
normalize = json_normalize(RawTable['candles'])
candles = pd.DataFrame(normalize, columns=['volume','time','mid.o','mid.h','mid.l','mid.c',]).set_index('time')
candles.index = pd.DatetimeIndex(candles.index)
print(candles)

#append into existing sql table
candles.to_sql("gbpusd",engine,if_exists='append')

#updaters for each pair - currently i'm pulling candles in a t-1 date. So 1/12 in table is 1/13 local. 
my_cursor.execute("SELECT TIME FROM eurusd ORDER BY TIME DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=2)
params = {
  "from" : StartDate,
  "to" : today,
  "granularity": "D"
}
r = instruments.InstrumentsCandles(instrument="EUR_USD", params = params)
RawTable = client.request(r)
normalize = json_normalize(RawTable['candles'])
candles = pd.DataFrame(normalize, columns=['volume','time','mid.o','mid.h','mid.l','mid.c',]).set_index('time')
candles.index = pd.DatetimeIndex(candles.index)
#print(candles)
candles.to_sql("eurusd",engine,if_exists='append')

####
my_cursor.execute("SELECT TIME FROM usdjpy ORDER BY TIME DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=2)
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
candles.to_sql("usdjpy",engine,if_exists='append')

####
my_cursor.execute("SELECT TIME FROM audusd ORDER BY TIME DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=2)
params = {
  "from" : StartDate,
  "to" : today,
  "granularity": "D"
}
r = instruments.InstrumentsCandles(instrument="AUD_USD", params = params)
RawTable = client.request(r)
normalize = json_normalize(RawTable['candles'])
candles = pd.DataFrame(normalize, columns=['volume','time','mid.o','mid.h','mid.l','mid.c',]).set_index('time')
candles.index = pd.DatetimeIndex(candles.index)
#print(candles)
candles.to_sql("audusd",engine,if_exists='append')

####
my_cursor.execute("SELECT TIME FROM usdcad ORDER BY TIME DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=2)
params = {
  "from" : StartDate,
  "to" : today,
  "granularity": "D"
}
r = instruments.InstrumentsCandles(instrument="USD_CAD", params = params)
RawTable = client.request(r)
normalize = json_normalize(RawTable['candles'])
candles = pd.DataFrame(normalize, columns=['volume','time','mid.o','mid.h','mid.l','mid.c',]).set_index('time')
candles.index = pd.DatetimeIndex(candles.index)
#print(candles)
candles.to_sql("usdcad",engine,if_exists='append')

####
my_cursor.execute("SELECT TIME FROM usdchf ORDER BY TIME DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=2)
params = {
  "from" : StartDate,
  "to" : today,
  "granularity": "D"
}
r = instruments.InstrumentsCandles(instrument="USD_CHF", params = params)
RawTable = client.request(r)
normalize = json_normalize(RawTable['candles'])
candles = pd.DataFrame(normalize, columns=['volume','time','mid.o','mid.h','mid.l','mid.c',]).set_index('time')
candles.index = pd.DatetimeIndex(candles.index)
#print(candles)
candles.to_sql("usdchf",engine,if_exists='append')

####
my_cursor.execute("SELECT TIME FROM eurgbp ORDER BY TIME DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=2)
params = {
  "from" : StartDate,
  "to" : today,
  "granularity": "D"
}
r = instruments.InstrumentsCandles(instrument="EUR_GBP", params = params)
RawTable = client.request(r)
normalize = json_normalize(RawTable['candles'])
candles = pd.DataFrame(normalize, columns=['volume','time','mid.o','mid.h','mid.l','mid.c',]).set_index('time')
candles.index = pd.DatetimeIndex(candles.index)
#print(candles)
candles.to_sql("eurgbp",engine,if_exists='append')

####
my_cursor.execute("SELECT TIME FROM usdmxn ORDER BY TIME DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=2)
params = {
  "from" : StartDate,
  "to" : today,
  "granularity": "D"
}
r = instruments.InstrumentsCandles(instrument="USD_MXN", params = params)
RawTable = client.request(r)
normalize = json_normalize(RawTable['candles'])
candles = pd.DataFrame(normalize, columns=['volume','time','mid.o','mid.h','mid.l','mid.c',]).set_index('time')
candles.index = pd.DatetimeIndex(candles.index)
#print(candles)
candles.to_sql("usdmxn",engine,if_exists='append')

####
my_cursor.execute("SELECT TIME FROM gbpjpy ORDER BY TIME DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=2)
params = {
  "from" : StartDate,
  "to" : today,
  "granularity": "D"
}
r = instruments.InstrumentsCandles(instrument="GBP_JPY", params = params)
RawTable = client.request(r)
normalize = json_normalize(RawTable['candles'])
candles = pd.DataFrame(normalize, columns=['volume','time','mid.o','mid.h','mid.l','mid.c',]).set_index('time')
candles.index = pd.DatetimeIndex(candles.index)
#print(candles)
candles.to_sql("gbpjpy",engine,if_exists='append')

####
my_cursor.execute("SELECT TIME FROM gbpzar ORDER BY TIME DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=2)
params = {
  "from" : StartDate,
  "to" : today,
  "granularity": "D"
}
r = instruments.InstrumentsCandles(instrument="GBP_ZAR", params = params)
RawTable = client.request(r)
normalize = json_normalize(RawTable['candles'])
candles = pd.DataFrame(normalize, columns=['volume','time','mid.o','mid.h','mid.l','mid.c',]).set_index('time')
candles.index = pd.DatetimeIndex(candles.index)
#print(candles)
candles.to_sql("gbpzar",engine,if_exists='append')

####
my_cursor.execute("SELECT TIME FROM eursek ORDER BY TIME DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=2)
params = {
  "from" : StartDate,
  "to" : today,
  "granularity": "D"
}
r = instruments.InstrumentsCandles(instrument="EUR_SEK", params = params)
RawTable = client.request(r)
normalize = json_normalize(RawTable['candles'])
candles = pd.DataFrame(normalize, columns=['volume','time','mid.o','mid.h','mid.l','mid.c',]).set_index('time')
candles.index = pd.DatetimeIndex(candles.index)
#print(candles)
candles.to_sql("eursek",engine,if_exists='append')

####
my_cursor.execute("SELECT TIME FROM eurnzd ORDER BY TIME DESC LIMIT 1")
LastRecord = my_cursor.fetchall()
LastDate = LastRecord[0][0]
StartDate = LastDate.date() + timedelta(days=2)
params = {
  "from" : StartDate,
  "to" : today,
  "granularity": "D"
}
r = instruments.InstrumentsCandles(instrument="EUR_NZD, params = params)
RawTable = client.request(r)
normalize = json_normalize(RawTable['candles'])
candles = pd.DataFrame(normalize, columns=['volume','time','mid.o','mid.h','mid.l','mid.c',]).set_index('time')
candles.index = pd.DatetimeIndex(candles.index)
#print(candles)
candles.to_sql("eurnzd",engine,if_exists='append')


