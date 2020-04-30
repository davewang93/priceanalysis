from oandapyV20 import  API
import oandapyV20.endpoints.instruments as instruments
import oandapyV20.endpoints.accounts as accounts
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import mysql.connector
from datetime import datetime, timedelta, timezone
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


'''
# Create a new DB in mySQL w/ block below

mydb = mysql.connector.connect(
        host = host,
        user = user,
        passwd = passwd,
    )

#create cursor
cursor = mydb.cursor()

#create a db
cursor.execute("CREATE DATABASE 'database'")
'''


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

'''
#below is prelimary code to try and force the correct timezone
todaydatetime = datetime.now(timezone.utc).astimezone()
todaymindate = datetime.combine(todaydatetime, datetime.min.time())
print(todaymindate)

startdateraw = todaymindate -timedelta(days=30)
#converts to RC339F time format or whatever its called
startdate = startdateraw.isoformat()
print(startdate)
todate = todaymindate.isoformat()
print(todate)


#parameters for request
params = {
  "from" : startdate,
  "to" : todate,
  "granularity": "D"
}

#define request
r = instruments.InstrumentsCandles(instrument="AUD_USD", params = params)

#submit request and add to table - this is a dict by default, not json string
RawTable = client.request(r)

#flattens json response - for candle portion
normalize = json_normalize(RawTable['candles'])

#print(normalize)
#need to convert dict to dataframe object so we can push to sql. This also definted the columns we want. Finally, we sort 'time' column as the index for the table.
candles = pd.DataFrame(normalize, columns=['volume','time','mid.o','mid.h','mid.l','mid.c',]).set_index('time')

#this converts the index of table candles into a datetime instead of the string that Oanda provides
candles.index = pd.DatetimeIndex(candles.index)

#DataType = candles.dtypes
#print(candles)
#print(DataType)
#print(candles.index)
#RawTable = pd.DataFrame(r.response)

#print(RawTable)

#df = flatten(RawTable.to_dict())

#print(df)

#push dataframe to mysql table
candles.to_sql("test",engine)
'''

r = accounts.AccountInstruments(accountID)
client.request(r)
df = pd.DataFrame(r.response)
df.to_csv(r'D:\OneDrive\David\src\PriceAnalysis\Tradeable.csv',index = None, header = True)