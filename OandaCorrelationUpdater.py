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
import os

directory = os.path.dirname(os.path.abspath(__file__))
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

OandaList = os.path.join(directory, 'OandaListCorrelation.csv')
OandaDF = pd.read_csv(OandaList, engine='python')

my_cursor = mydb.cursor()

today = datetime.date.today()

maintable = pd.DataFrame()

for index,row in OandaDF.iterrows():

  #create sql command
  sqlcmd = "SELECT date FROM fxpricemaintable ORDER BY date DESC LIMIT 1"
  getID = row['ID']
  tableID = row['table']

  my_cursor.execute(sqlcmd)
  LastRecord = my_cursor.fetchall()
  LastDate = LastRecord[0][0]
  StartDate = LastDate.date() + timedelta(days=1)

  params = {
  "from" : StartDate,
  "to" : today,
  "granularity": "D"
  }

  r = instruments.InstrumentsCandles(instrument=getID, params = params)
  RawTable = client.request(r)
  normalize = json_normalize(RawTable['candles'])
  candles = pd.DataFrame(normalize, columns=['volume','time','mid.o','mid.h','mid.l','mid.c']).set_index('time')
  candles.index = pd.DatetimeIndex(candles.index) + pd.DateOffset(1)
  candles.index = candles.index.normalize()
  candles.index.name = 'date'
  candles = candles.drop(['volume','mid.o','mid.h','mid.l'],1)
  candles.rename(columns={'mid.c':tableID}, inplace = True)
  #candles['date'] = candles['time'] + + pd.Timedelta(days=1)
  #print(candles)
  maintable = pd.concat([candles, maintable],axis = 1)
  print("Appended " + tableID)

maintable.to_sql('fxpricemaintable', engine, if_exists='append')



