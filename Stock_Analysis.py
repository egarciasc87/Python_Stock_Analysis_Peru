import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from selenium import webdriver
import yahoo_fin.stock_info as si
import yfinance as yf
import matplotlib.pyplot as plt

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



""" URL_DATA_SOURCE = "https://www.marketwatch.com/investing/stock/"
URL_YEARLY_INCOME_STATEMENT = "/financials/income?countrycode=pe"
URL_YEARLY_BALANCE_SHEET = "/financials/balance-sheet?countrycode=pe"
URL_YEARLY_CASH_FLOW = "/financials/cash-flow?countrycode=pe"
 """

URL_DATA_SOURCE = "https://www.wsj.com/market-data/quotes/PE/XLIM/"
URL_YEARLY_INCOME_STATEMENT = "/financials/annual/income-statement"
URL_YEARLY_BALANCE_SHEET = "/financials/annual/balance-sheet"
URL_YEARLY_CASH_FLOW = "/financials/annual/cash-flow"

URL_QUARTERLY_INCOME_STATEMENT = "/financials/quarter/income-statement"
URL_QUARTERLY_BALANCE_SHEET = "/financials/quarter/balance-sheet"
URL_QUARTERLY_CASH_FLOW = "/financials/quarter/cash-flow"


pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
pd.options.display.float_format = '{:.2f}'.format


def getProxies(inURL):
    page = requests.get(inURL)
    soup = BeautifulSoup(page.text, 'html.parser')
    terms = soup.find_all('tr')
    IPs = []

    for x in range(len(terms)):

        term = str(terms[x])

        if '<tr><td>' in str(terms[x]):
            pos1 = term.find('d>') + 2
            pos2 = term.find('</td>')

            pos3 = term.find('</td><td>') + 9
            pos4 = term.find('</td><td>US<')

            IP = term[pos1:pos2]
            port = term[pos3:pos4]

            if '.' in IP and len(port) < 6:
                IPs.append(IP + ":" + port)

    return IPs


userAgentList = []
useragents = open("useragents.txt", "r")

for line in useragents:
    userAgentList.append(line.replace('\n', ''))

useragents.close()

proxyURL = "https://www.us-proxy.org/"
pxs = getProxies(proxyURL)
proxyPool = cycle(pxs)


###--->>> Get the tickers manually (input)
def get_ticker_list():
    ticker = "X"
    list = []
    index = 1

    while (ticker != ""):
        ticker = input("Enter ticker # {}: ".format(index))
        ticker = ticker.upper()
        index += 1

        if (ticker != ""):
            list.append(ticker)
    
    print("Tickers in the list: ", list)
    return list


def read_measure_currency(column):
    thousands = "Thousands"
    millions = "Millions"
    billions = "Billions"
    measure = ""
    currency = ""
    
    if column.find(billions) != -1:
        measure = "B"
    elif column.find(millions) != -1:
        measure = "M"
    elif column.find(thousands) != -1:
        measure = "K"  

    if column.find("PEN") != -1:
        currency = "PEN"
    else:
        currency = "USD"

    return measure, currency


def clean_numeric_column(variable): 
    #when de value is negative, add sign   
    if variable.find("(") != -1:
        variable = variable.replace("(", "")
        variable = variable.replace(")", "")
        variable = "-" + variable 

    #no comma to separate thousands -> "(1,232)"
    if variable.find(",") != -1:
        variable = variable.replace(",", "")
    
    #when the item has no value ("-"
    if variable.find("-K") != -1 or variable.find("-M") != -1 or variable.find("-B") != -1:
        variable = 0
    #percentage values
    elif variable.find("%B") != -1:
        variable = variable.replace("%B", "")
        variable = float(variable)
    elif variable.find("%M") != -1:
        variable = variable.replace("%M", "")
        variable = float(variable)
    elif variable.find("%K") != -1:
        variable = variable.replace("%K", "")
        variable = float(variable) 
    #add zeros to values   
    elif variable.find("B") != -1:
        variable = variable.replace("B", "")
        variable = float(variable) * 1000000000
    elif variable.find("M") != -1:
        variable = variable.replace("M", "")
        variable = float(variable) * 1000000
    elif variable.find("K") != -1:
        variable = variable.replace("K", "")
        variable = float(variable) * 1000     
    elif variable.find("%") != -1:
        variable = variable.replace("%", "") 
        variable = float(variable)
        
    return variable


def create_insert_script(item, year, quarter, value, stock, table_name):
    query = "INSERT INTO {} VALUES ('{}', {}, {}, {}, '{}')".format(item, year, quarter, value, stock)
    return query


def rename_quarterly_column_name(list_column):
    dict_month = {"MAR": "1T", "JUN": "2T", "SEP": "3T", "DEC": "4T"}
    new_list_column = ["Item"]
    list_column = list_column[1:]

    for item in list_column:
        new_list_column.append(dict_month[item[3:6].upper()] + item[-4:])

    return new_list_column


def rename_yearly_column_name(list_column):
    new_list_column = ["Item"]
    list_column = list_column[1:]

    for item in list_column:
        new_list_column.append(item)

    return new_list_column
    

def process_financial_data(table,
                           quarterly_data, 
                           type_data):
    
    currency = ""
    measure = ""
    list_columns = table.columns
    
    #1. drop unnecessary columnsprint("step 1")
    print("step 1")
    table = table.drop(columns=list_columns[-1])
    
    if quarterly_data == True:
        table = table.drop(columns=list_columns[-2])

    #2. drop rows without valid data (NaN) and unnecessary rows       
    print("step 2")
    drop_rows_balance_sheet = ["Cash & Short Term Investments Growth",
                 "Cash & ST Investments / Total Assets",
                 "Accounts Receivable Growth",
                 "Accounts Receivable Turnover",
                 "Assets - Total - Growth",
                 "Asset Turnover",
                 "Return On Average Assets",
                 "Accounts Payable Growth",
                 "Current Ratio",
                 "Quick Ratio",
                 "Cash Ratio",
                 "Total Liabilities / Total Assets",
                 "Common Equity / Total Assets",
                 "Total Shareholders' Equity / Total Assets",
                 "Net Income before Extraordinaries", 
                 "Total Current Assets FOR CALCULATION PURPOSES ONLY",
                 "Total Assets FOR CALCULATION PURPOSES ONLY",
                 "Inventories FOR CALCULATION PURPOSES ONLY",
                 "Cash & Short Term Investments FOR CALCULATION PURPOSES ONLY",
                 "Cumulative Translation Adjustment/Unrealized For. Exch. Gain",
                 "Preferred Stock (Carrying Value)",
                 "Redeemable Preferred Stock",
                 "Non-Redeemable Preferred Stock",
                 "Preferred Stock issues for ESOP",
                 "ESOP Guarantees - Preferred Stock"]
    list_columns = table.columns
    measure, currency = read_measure_currency(list_columns[0]) 
    table[list_columns[0]] = table[list_columns[0]].replace(np.nan, "DELETE")
    table[list_columns[0]] = table[list_columns[0]].apply(lambda x: "DELETE" if x.find("Growth") != -1 else x)
    table[list_columns[0]] = table[list_columns[0]].apply(lambda x: "DELETE" if x in drop_rows_balance_sheet else x)
    table = table.loc[table[list_columns[0]] != "DELETE"]
    
    #3. format numeric columns 
    print("step 3")       
    for item in list_columns[1:]:                
        table[item] = table[item].replace(np.nan, "0")
        table[item] = table[item] + measure
        table[item] = table[item].apply(clean_numeric_column)   
           
    #4. rename columns
    print("step 4") 
    if quarterly_data == True:
        list_columns = rename_quarterly_column_name(list_columns)
        table.columns = list_columns
        table["0TTM "] = table[list_columns[1]] + table[list_columns[2]] + table[list_columns[3]] + table[list_columns[4]]
        list_columns = table.columns
    else:
        list_columns = rename_yearly_column_name(list_columns)
        table.columns = list_columns

    #5. correct data related to Income Statement
    print("step 5")  
    if type_data == "IS":
        table = table.set_index("Item")

        if quarterly_data == True:
            table.loc["Basic Shares Outstanding", list_columns[-1]] = table.loc["Basic Shares Outstanding", list_columns[1]]
            table.loc["Diluted Shares Outstanding", list_columns[-1]] = table.loc["Diluted Shares Outstanding", list_columns[1]]
            
        table = table.transpose()
        table["EPS (Basic)"] = table["Net Income"] / table["Basic Shares Outstanding"]
        table["EPS (Diluted)"] = table["Net Income"] / table["Diluted Shares Outstanding"]
        table = table.transpose()
        table = table.reset_index()

    return table, currency, list_columns
                

def get_financial_data_from_web(list_ticker, 
                                link_financial_data, 
                                type_financial_data, 
                                quarterly_data,
                                type_data):    
    dataframes = []
    stock_data = pd.DataFrame()
    currency = ""
    measure = ""
    index = 1
    total_stocks = len(list_ticker)
    result = True
    print("\nGetting {} from the web...".format(type_financial_data))

    for ticker in list_ticker:
        tempURL = URL_DATA_SOURCE + ticker + link_financial_data
        agent = random.choice(userAgentList)
        headers = {'User-Agent': agent}
        page = requests.get(f"{tempURL}", headers=headers, proxies = {"http": next(proxyPool)})
        print("Stock {} out of {}: {}".format(index, total_stocks, ticker))
        print(tempURL)
        index += 1
        
        try:
            tables = pd.read_html(page.text)
            table = tables[0]
            list_columns = table.columns
            index_table = 0

            table, currency, list_columns = process_financial_data(table, 
                                                     quarterly_data, 
                                                     type_data)
            
            index_balance_sheet_liab_equity_yearly = 45
            index_balance_sheet_liab_equity_quarterly = 37

            if type_data == "BS":
                if quarterly_data == True:
                    index_table = index_balance_sheet_liab_equity_quarterly
                else:
                    index_table = index_balance_sheet_liab_equity_yearly

                table_ = tables[index_table]
                table_, currency, list_columns = process_financial_data(table_,
                                                quarterly_data, 
                                                type_data)
                frames = [table, table_]
                table = pd.concat(frames)
            
              
            #table["Item  Item"] = table["Item  Item"].apply(lambda x: x[:round(len(x)/2)])
            """ table = table.drop(columns=list_columns[-1])
            print("step 1")

            if quarterly_data == True:
                table = table.drop(columns=list_columns[-2])
            
            list_columns = table.columns
            measure, currency = read_measure_currency(list_columns[0])
            table[list_columns[0]] = table[list_columns[0]].replace(np.nan, "DELETE")
            table[list_columns[0]] = table[list_columns[0]].apply(lambda x: "DELETE" if x.find("Growth") != -1 else x)
            table = table.loc[table[list_columns[0]] != "DELETE"]
            print("step 2")
            
            for item in list_columns[1:]:                
                table[item] = table[item].replace(np.nan, "0")
                table[item] = table[item] + measure
                table[item] = table[item].apply(clean_numeric_column)   
            
            print("step 3")
            if quarterly_data == True:
                list_columns = rename_quarterly_column_name(list_columns)
                table.columns = list_columns
                table["0TTM "] = table[list_columns[1]] + table[list_columns[2]] + table[list_columns[3]] + table[list_columns[4]]
                list_columns = table.columns
            else:
                list_columns = rename_yearly_column_name(list_columns)
                table.columns = list_columns
          
            print("step 4")
            if is_income_statement == True:
                table = table.set_index("Item")

                if quarterly_data == True:
                    table.loc["Basic Shares Outstanding", list_columns[-1]] = table.loc["Basic Shares Outstanding", list_columns[1]]
                    table.loc["Diluted Shares Outstanding", list_columns[-1]] = table.loc["Diluted Shares Outstanding", list_columns[1]]
            
                print("step 4.1")
                table = table.transpose()
                table["EPS (Basic)"] = table["Net Income"] / table["Basic Shares Outstanding"]
                table["EPS (Diluted)"] = table["Net Income"] / table["Diluted Shares Outstanding"]
                table = table.transpose()
                table = table.reset_index()
                 """
            
            print("step 6")
            unpivot_df = pd.melt(table, id_vars=list_columns[0], value_vars=list_columns[1:])
            unpivot_df.columns = ["Item", "Year", "Value"]
            unpivot_df["Stock"] = ticker.upper()
            unpivot_df["Quarter"] = 0
            unpivot_df["Currency"] = currency

            print("step 7")
            if quarterly_data == True:
                unpivot_df["Quarter"] = unpivot_df["Year"].apply(lambda x: x[0])
                unpivot_df["Year"] = unpivot_df["Year"].apply(lambda x: x[-4:])
            
            dataframes.append(unpivot_df)
                
            #print(table)
            #print("Ticker: \n", unpivot_df)            
        except:
            result = False
            soup = BeautifulSoup(page.text, 'html.parser')
            print("PARSE ERROR")

    stock_data = pd.concat(dataframes)
    stock_data.columns = ["Item", "Year", "Value", "Stock", "Quarter", "Currency"]
    #stock_data["Value"] = stock_data["Value"].apply(clean_numeric_column)
    stock_data = stock_data.reset_index()
    stock_data = stock_data.drop(columns=["index"])
    #print(stock_data)

    return stock_data, result


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
    table_name = "BVL_INCOME_STATEMENT"
    table_column = []
    table_schema = ""
    list_scripts = []
    script_temp = ""
    total_rows = len(df_temp)
    count = 1

    print("\nLoading data into Snowflake database: ")
    script_temp = "INSERT INTO BVL_INCOME_STATEMENT VALUES "
    
    for index, row in df_temp.iterrows():
        script_temp += " ('{}', '{}', '{}', '{}', {}, '{}')".format(row["Stock"], row["Item"], row["Year"], row["Quarter"], row["Value"], row["Currency"])
        print(f"Generating script: row {count} out of {total_rows}")
        
        if (count == total_rows):
            script_temp += ";"
        else:
            script_temp += ", "

        count += 1

    #print(script_temp)
    conn.cursor().execute(script_temp)
    print("Data successfully transfered into database...!!!")
    conn.close()


###--->>> Execution code
list_tickers = []
df_income_statement = pd.DataFrame()
df_income_statement_quarterly = pd.DataFrame()
df_balance_sheet = pd.DataFrame()
df_cash_flow = pd.DataFrame()

#list_tickers = get_ticker_list()
list_ticker_agro = ["casagrc1", "cartavc1", "laredoc1"]
list_ticker_industrial = ["cpacasc1", "unacemc1", "ferreyc1", "siderc1", "corarei1"]
list_ticker_mining = ["poderc1", "cverdec1", "minsuri1", "scco"]
list_ticker_massive = ["alicorc1", "backusi1", "inretc1"]
list_ticker_utilities = ["lusurc1", "engepec1", "endispc1", "hidra2c1", "engiec1"]
list_ticker_finance = ["creditc1", "interbc1", "scotiac1", "bbvac1"]
list_tickers = list_ticker_agro + list_ticker_industrial #+ list_ticker_mining + list_ticker_massive + list_ticker_energy
#list_tickers = list_ticker_mining + list_ticker_massive
#list_tickers = list_ticker_utilities
#list_tickers = list_ticker_finance
list_tickers = ["casagrc1"]

df_income_statement, result = get_financial_data_from_web(
    list_tickers, 
    URL_YEARLY_INCOME_STATEMENT, 
    "YEARLY INCOME STATEMENT",
    False,
    "IS")

result = False
df_income_statement_quarterly, result = get_financial_data_from_web(
    list_tickers,
    URL_QUARTERLY_INCOME_STATEMENT,
    "QUARTERLY INCOME STATEMENTE",
    True,
    "IS")

df_balance_sheet, result = get_financial_data_from_web(
    list_tickers,
    URL_YEARLY_BALANCE_SHEET,
    "YEARLY BALANCE SHEET",
    False,
    "BS")

df_balance_sheet, result = get_financial_data_from_web(
    list_tickers,
    URL_QUARTERLY_BALANCE_SHEET,
    "QUARTERLY BALANCE SHEET",
    True,
    "BS")
#df_balance_sheet = get_financial_data_from_web(list_tickers, URL_YEARLY_BALANCE_SHEET, "BALANCE SHEET")
#df_cash_flow = get_financial_data_from_web(list_tickers, URL_YEARLY_CASH_FLOW, "CASH FLOW")


""" if (result == True):
    load_database_snowflake("wtynwlj-nn04581",
                            "PYTHON_USER",
                            "Python2023",
                            "TEST_DB",
                            "PUBLIC",
                            "PYTHON_PROJECTS",
                            "ACCOUNTADMIN",
                            df_income_statement)
    
    load_database_snowflake("wtynwlj-nn04581",
                            "PYTHON_USER",
                            "Python2023",
                            "TEST_DB",
                            "PUBLIC",
                            "PYTHON_PROJECTS",
                            "ACCOUNTADMIN",
                            df_income_statement_quarterly)


   """