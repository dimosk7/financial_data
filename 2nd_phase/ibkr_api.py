import requests
import json
import pandas as pd
import pySQL_functions
import mysql.connector
from datetime import datetime

# connect to MySQL database
mydb = mysql.connector.connect(
  host = "localhost",
  user = "root",
  password ="dimosk",
  database = "stocks_sample",
  auth_plugin = "mysql_native_password"
)

# API request to retrieve trades history
response = requests.get("https://localhost:5000/v1/api/iserver/account/trades", verify = False)
data = json.loads(response.content)
pd_data = pd.DataFrame(data)

# select the columns we need
col = ["execution_id", "account", "symbol", "trade_time", "side", "size", "price", "net_amount", "commission" ]
sql_data = pd_data[col]
sql_data.rename(columns = { "symbol" : "ticker", "trade_time" : "trade time", "net_amount": "net amount" }, inplace = True)

# trade_time should be converted to the appropriate format (from str to datetime)
sql_data["trade_time"] = sql_data["trade_time"].apply( lambda x : datetime.strptime(x, "%Y%m%d-%H:%M:%S"))


# create table "trades"
pySQL_functions.create_table(sql_data, "trades", mydb, time = True)
pySQL_functions.insert_to_sql(sql_data, "trades", mydb)


# API request to retrieve accounts info
response = requests.get("https://localhost:5000/v1/api/portfolio/accounts", verify = False)
data = json.loads(response.content)
pd_data = pd.DataFrame(data)

# select columns we need
col = ["accountId", "accountTitle", "currency", "tradingType"]
sql_data = pd_data[col]
sql_data.rename(columns = {"accountId" : "account id", "accountTitle" : "account title", "tradingType" : "trading type"}, inplace = True)

# create table "accounts"
pySQL_functions.create_table(sql_data, "accounts", mydb)
pySQL_functions.insert_to_sql(sql_data, "accounts", mydb)
