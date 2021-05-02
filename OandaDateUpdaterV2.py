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
configfile = os.path.join(directory, 'config.ini')
parser = ConfigParser()
parser.read(configfile)

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
    database = database
)

#connect to db using sqlalchemy
engine = create_engine(engine)

#access requirements
client= API(access_token=access_token,environment="live")

#see FredUpdaterV2 for detailed loop conversion comments

OandaList = os.path.join(directory, 'OandaList.csv')
OandaDF = pd.read_csv(OandaList, engine='python')

my_cursor = mydb.cursor()

today = datetime.date.today()

for index,row in OandaDF.iterrows():
    #create sql command
    sqlcmd = "SELECT TIME FROM " +row['table'] + " ORDER BY TIME DESC LIMIT 1"
    getID = row['ID']
    tableID = row['table']

    my_cursor.execute(sqlcmd)
    LastRecord = my_cursor.fetchall()
    LastDate = LastRecord[0][0]
    StartDate = LastDate.date() + timedelta(days=2)
    params = {
    "from" : StartDate,
    "to" : today,
    "granularity": "D"
    }
    r = instruments.InstrumentsCandles(instrument=getID, params = params)
    RawTable = client.request(r)
    normalize = json_normalize(RawTable['candles'])
    candles = pd.DataFrame(normalize, columns=['volume','time','mid.o','mid.h','mid.l','mid.c',]).set_index('time')
    candles.index = pd.DatetimeIndex(candles.index)
    print(candles)
    candles.to_sql(tableID, engine, if_exists='append')