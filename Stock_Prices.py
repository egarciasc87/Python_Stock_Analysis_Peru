import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from selenium import webdriver
import yahoo_fin.stock_info as si
import yfinance as yf
import matplotlib.pyplot as plt
import math
import datetime

import requests
from itertools import cycle
from datetime import date
from tqdm import tqdm
import random
import time
import collections
import os
import pygsheets

import snowflake.connector
import json


pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
pd.options.display.float_format = '{:.2f}'.format


###--->>> Upload data to Snowflake
def load_database_snowflake(account,
                            user,
                            password,
                            database,
                            schema,
                            warehouse,
                            role,
                            df_temp):
    conn = snowflake.connector.connect(user=user,
                                       account=account,
                                       password=password,
                                       role=role)
    conn.cursor().execute(f"USE DATABASE {database}")
    conn.cursor().execute(f"USE SCHEMA {schema}")
    table_column = []
    table_schema = ""
    list_scripts = []
    script_temp = ""
    total_rows = len(df_temp)
    count = 1

    print("\nLoading data into Snowflake database: ")
    script_temp = "INSERT INTO BVL_STOCK_PRICE VALUES "
    
    for index, row in df_temp.iterrows():
        script_temp += " ('{}', '{}', '{}', {}, {})".format(row["Stock"], row["Date"], row["Year"], row["Month"], row["Price"])
        #print(f"Generating script: row {count} out of {total_rows}")
        
        if (count == total_rows):
            script_temp += ";"
        else:
            script_temp += ", "

        count += 1
    
    print(f"Total rows: {total_rows}")
    #print(script_temp)
    conn.cursor().execute(script_temp)
    print("Data successfully transfered into database...!!!")
    conn.close()


list_ticker_agro = ["casagrc1", "cartavc1", "laredoc1"]
list_ticker_industrial = ["cpacasc1", "unacemc1", "ferreyc1", "siderc1", "corarei1"] 
list_ticker_mining = ["poderc1", "cverdec1", "minsuri1", "scco"]
list_ticker_massive = ["alicorc1", "backusi1", "inretc1"]
list_ticker_utilities = ["lusurc1", "engepec1", "endispc1", "hidra2c1", "engiec1"]
list_ticker_finance = ["creditc1", "interbc1", "scotiac1", "bbvac1"]
list_tickers = list_ticker_agro + list_ticker_industrial + list_ticker_mining + list_ticker_massive + list_ticker_utilities + list_ticker_finance
frame = []
df_final = pd.DataFrame()
df_temp = pd.DataFrame()

for item in list_tickers:
    file_name = item.upper()
    file_name = file_name + " Historical Data.csv"
    df_price = pd.read_csv(file_name)
    
    df_price = df_price[["Date", "Price"]]
    df_price["Date"] = pd.to_datetime(df_price["Date"])
    df_price["Month"] = df_price["Date"].dt.month
    df_price["Year"] = df_price["Date"].dt.year
    #df_price = df_price.groupby("Year")["Price"].mean().reset_index()
    list_year = df_price["Year"].unique()
    print(f"Stock: {item}, # rows: {len(df_price)}")
    df_price = df_price.sort_values(by=["Year", "Month"], ascending=False)
    df_price["Stock"] = item.upper()
    frame.append(df_price)

    if len(frame) >= 1:
        df_final = pd.concat(frame)
        frame = [df_final]


year_ttm = datetime.date.today().year
df_final = df_final.reset_index()
df_final = df_final.drop(columns=["index"])
df_final["Year"] = df_final["Year"].apply(lambda x: "TTM" if x == year_ttm else str(x))
#print(df_final)


load_database_snowflake("klmonwu-tm58546",
                            "TEST_USER",
                            "Test2023",
                            "TEST_DB",
                            "PUBLIC",
                            "PYTHON_PROJECTS",
                            "ACCOUNTADMIN",
                            df_final)


