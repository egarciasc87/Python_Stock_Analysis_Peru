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
    #print("step 1")
    table = table.drop(columns=list_columns[-1])
    
    if quarterly_data == True:
        table = table.drop(columns=list_columns[-2])

    #2. drop rows without valid data (NaN) and unnecessary rows       
    #print("step 2")
    drop_rows_income_statement = ['COGS excluding D&A', 
                                  'Depreciation', 
                                  'Amortization of Intangibles',
                                  'SG&A Expense', 'Other SG&A', 
                                  'Other Operating Expense', 
                                  'EBIT', 
                                  'Unusual Expense', 
                                  'Non Operating Income/Expense',
                                  'Non-Operating Interest Income', 
                                  'Gross Interest Expense', 
                                  'Income Tax - Current Domestic',
                                  'Income Tax - Deferred Domestic', 
                                  'Equity in Affiliates', 
                                  'Discontinued Operations',
                                  'Net Income After Extraordinaries', 
                                  'Net Income Available to Common', 
                                  'Pretax Income',
                                  'Extraordinaries & Discontinued Operations']
    drop_rows_cash_flow = ["Net Operating Cash Flow Growth",
                           "Net Operating Cash Flow / Sales",
                           "Capital Expenditures Growth",
                           "Capital Expenditures / Sales",
                           "Net Investing Cash Flow Growth",
                           "Net Investing Cash Flow / Sales",
                           "Net Financing Cash Flow Growth",
                           "Net Financing Cash Flow / Sales",
                           "Free Cash Flow Growth",
                           "Free Cash Flow Yield"]
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

    if type_data == "IS":
        table[list_columns[0]] = table[list_columns[0]].replace("Interest Income", "Sales/Revenue")
        table[list_columns[0]] = table[list_columns[0]].replace("Net Interest Income", "Gross Income")

    table[list_columns[0]] = table[list_columns[0]].replace(np.nan, "DELETE")
    #table[list_columns[0]] = table[list_columns[0]].replace("'", "")
    table[list_columns[0]] = table[list_columns[0]].apply(lambda x: "DELETE" if x.find("Growth") != -1 else x)
    table[list_columns[0]] = table[list_columns[0]].apply(lambda x: "DELETE" if x.find("Growth") != -1 else x)
    table[list_columns[0]] = table[list_columns[0]].apply(lambda x: "DELETE" if x in drop_rows_balance_sheet else x)
    table[list_columns[0]] = table[list_columns[0]].apply(lambda x: "DELETE" if x in drop_rows_cash_flow else x)
    table[list_columns[0]] = table[list_columns[0]].apply(lambda x: "DELETE" if x in drop_rows_income_statement else x)
    table = table.loc[table[list_columns[0]] != "DELETE"]

    #3. format numeric columns 
    #print("step 3")       
    for item in list_columns[1:]:                
        table[item] = table[item].replace(np.nan, "0")
        table[item] = table[item] + measure
        table[item] = table[item].apply(clean_numeric_column)   

    if quarterly_data == False:
        table["5YA"] = (table[list_columns[1]] + table[list_columns[2]] + table[list_columns[3]] + table[list_columns[4]] + table[list_columns[5]]) / 5
        #table["05YA "] = table[list_columns[1]]
        list_columns = table.columns

    #4. rename columns
    #print("step 4") 
    if quarterly_data == True:
        list_columns = rename_quarterly_column_name(list_columns)
        table.columns = list_columns

        if type_data == "IS":
            table["0TTM "] = table[list_columns[1]] + table[list_columns[2]] + table[list_columns[3]] + table[list_columns[4]]
        else:
            table["0TTM "] = table[list_columns[1]]
        
        list_columns = table.columns
    else:
        list_columns = rename_yearly_column_name(list_columns)
        table.columns = list_columns
    
    #print(table)

    #5. correct data related to Income Statement
    #print("step 5")  
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
    df_indexes = []
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
        #print(tempURL)
        index += 1
        df_indexes = []
        
        try:
            tables = pd.read_html(page.text)
            table = tables[0]
            list_columns = table.columns
            index_table = 0

            for item in tables:
                if len(item) > 20:
                    df_indexes.append(index_table)
                    #print(tables[index_table])
                
                index_table += 1

            table, currency, list_columns = process_financial_data(tables[df_indexes[0]], 
                                                     quarterly_data, 
                                                     type_data)
            
            if type_data == "BS":
                table_1 = tables[df_indexes[1]]
                table_1, currency, list_columns = process_financial_data(table_1,
                                                quarterly_data, 
                                                type_data)
                frames = [table, table_1]
                table = pd.concat(frames)
            elif type_data == "CF":
                table_1 = tables[df_indexes[1]]
                table_1, currency, list_columns = process_financial_data(table_1,
                                                quarterly_data, 
                                                type_data)
                frames = [table, table_1]
                table = pd.concat(frames)
                
                table_1 = tables[df_indexes[2]]
                table_1, currency, list_columns = process_financial_data(table_1,
                                                quarterly_data, 
                                                type_data)
                frames = [table, table_1]
                table = pd.concat(frames)
            
            #table["Item  Item"] = table["Item  Item"].apply(lambda x: x[:round(len(x)/2)])
            #print("step 6")
            unpivot_df = pd.melt(table, id_vars=list_columns[0], value_vars=list_columns[1:])
            unpivot_df.columns = ["Item", "Year", "Value"]
            unpivot_df["Stock"] = ticker.upper()
            unpivot_df["Quarter"] = 0
            unpivot_df["Currency"] = currency

            #print("step 7")
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
    
    #stock_data = stock_data.loc[stock_data[stock_data.columns[1]] == "5YA"]
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
                            df_temp,
                            table_name):
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
    script_temp = "INSERT INTO {} VALUES ".format(table_name)
    
    for index, row in df_temp.iterrows():
        item_value = row["Item"].replace("'", "")
        script_temp += " ('{}', '{}', '{}', '{}', {}, '{}')".format(row["Stock"], item_value, row["Year"], row["Quarter"], row["Value"], row["Currency"])
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



###--->>> Execution code
list_tickers = []
df_income_statement = pd.DataFrame()
df_income_statement_quarterly = pd.DataFrame()
df_balance_sheet = pd.DataFrame()
df_balance_sheet_quarterly = pd.DataFrame()
df_cash_flow = pd.DataFrame()
df_cash_flow_quarterly = pd.DataFrame()

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
#list_tickers = ["siderc1"]
dict_company_category = {"1": list_ticker_agro, 
                         "2": list_ticker_industrial,
                         "3": list_ticker_mining,
                         "4": list_ticker_massive,
                         "5": list_ticker_utilities,
                         "6": list_ticker_finance}


option = ""
table_name = ""

while (option != "X"):
    print("\nSELECT INDUSTRY OF THE COMPANIES: ")
    print("[1] Agro")
    print("[2] Industrial")
    print("[3] Mining")
    print("[4] Massive Consumption")
    print("[5] Utilitiees")
    print("[6] Finance")
    print("[X] Exit")
    option = input("Enter category: ")

    if option == "X":
        break
    
    list_tickers = dict_company_category[option]
    print(list_tickers)
    print("\nSELECT TYPE OF FINANCIAL DATA: ")
    print("[1] Income Statement")
    print("[2] Balance Sheet")
    print("[3] Cash Flow")
    print("[X] Go back to company industries")
    option = input("Enter type of data: ")

    if option == "X":
        option = ""
        continue

    if option == "1":
        url_yearly_data = URL_YEARLY_INCOME_STATEMENT
        url_quarterly_data = URL_QUARTERLY_INCOME_STATEMENT
        data_type_descrition = "INCOME STATEMENT"
        data_type_short = "IS"
        table_name = "BVL_INCOME_STATEMENT"
    elif option == "2":
        url_yearly_data = URL_YEARLY_BALANCE_SHEET
        url_quarterly_data = URL_QUARTERLY_BALANCE_SHEET
        data_type_descrition = "BALANCE SHEET"
        data_type_short = "BS"
        table_name = "BVL_BALANCE_SHEET"
    elif option == "3":
        url_yearly_data = URL_YEARLY_CASH_FLOW
        url_quarterly_data = URL_QUARTERLY_CASH_FLOW
        data_type_descrition = "CASH FLOW"
        data_type_short = "CF"
        table_name = "BVL_CASH_FLOW"
    
    df_data_yearly, result = get_financial_data_from_web(
        list_tickers,
        url_yearly_data,
        "YEARLY " + data_type_descrition,
        False,
        data_type_short)

    df_data_quarterly, result = get_financial_data_from_web(
        list_tickers,
        url_quarterly_data,
        "QUARTERLY " + data_type_descrition,
        True,
        data_type_short)
    
    frames = [df_data_yearly, df_data_quarterly]
    df_total_data = pd.concat(frames)
    option = ""
    
    if (result == True):
        load_database_snowflake("klmonwu-tm58546",
                                "TEST_USER",
                                "Test2023",
                                "TEST_DB",
                                "PUBLIC",
                                "COMPUTE_WH",
                                "ACCOUNTADMIN",
                                df_total_data,
                                table_name)


    

