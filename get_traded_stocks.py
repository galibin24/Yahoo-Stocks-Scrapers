"""
    Here we are getting all the traded stocks traded on NASDAQ
"""

"""
    https://www.nasdaq.com/api/v1/screener?marketCap=Mega&page=1&pageSize=1000
    # MarketCap Ranges - Mega, Large, Medium, Small
    # Max return - 300(starts with 0)

    # get the count for the tier and get all stock symbols 
    # get company name and industry 
    # get number of stock by taking last price and last marketCap
"""

import requests
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
import json
import math
import time
import pandas as pd
import numpy as np
import psycopg2

driver = webdriver.Chrome()
market_caps = ["Mega", "Large", "Medium", "Small"]
API = "https://www.nasdaq.com/api/v1/screener?marketCap={}&page={}&pageSize=1000"

connection = psycopg2.connect(
    user="galibin24",
    password="Nn240494",
    host="127.0.0.1",
    port="5432",
    database="local_stocks",
)


# getting the number of queries to get all stocks
def get_query_count(marketCap):
    driver.get(API.format(marketCap, "1"))
    data = driver.find_elements_by_tag_name("pre")
    data_json = json.loads(data[0].text)
    # number of stock pages
    query_number = math.ceil(int(data_json["count"]) / 300)
    # driver.close()
    return query_number


def execute_many(conn, df, table):
    """
    Using cursor.executemany() to insert the dataframe
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # print(tuples)
    # Comma-separated dataframe columns
    cols = ",".join(list(df.columns))
    # SQL quert to execute
    query = 'INSERT INTO "Stocks"(%s) VALUES(%%s,%%s,%%s,%%s,%%s,%%s,%%s)' % (cols,)
    cursor = conn.cursor()
    try:
        cursor.executemany(query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_many() done")
    cursor.close()


# getting the stock data, storing in pandas and saving to csv
def get_stocks(marketCap):
    query_count = get_query_count(marketCap)
    print(query_count)
    time.sleep(5)
    table = pd.DataFrame(
        columns=[
            "ticker",
            "sector",
            "company_name",
            "market_cap",
            "market_cap_group",
            "analyst_consensus",
            "stock_outstanding",
        ]
    )
    for i in range(query_count):
        page = i + 1
        driver.get(API.format(marketCap, page))
        page_data = driver.find_elements_by_tag_name("pre")
        data_json = json.loads(page_data[0].text)
        main_data = data_json["data"]
        for stock in main_data:
            ticker = stock["ticker"]
            sector = stock["sector"]
            company_name = stock["company"]
            market_cap = stock["marketCap"]
            market_cap_group = stock["marketCapGroup"]
            analyst_consensus = stock["analystConsensus"]
            try:
                stock_outstanding = float(market_cap) / float(
                    stock["priceChartSevenDay"][-1]["price"]
                )
            except:
                print("price not found")
                stock_outstanding = None
            table = table.append(
                {
                    "ticker": ticker,
                    "sector": sector,
                    "company_name": company_name,
                    "market_cap": market_cap,
                    "market_cap_group": market_cap_group,
                    "analyst_consensus": analyst_consensus,
                    "stock_outstanding": stock_outstanding,
                },
                ignore_index=True,
            )

        # print(dataJson)

    table = table.rename(
        columns={
            "ticker": "ticker",
            "stock_outstanding": "stock_outstanding",
            "sector": "sector",
            "market_cap_group": "market_category",
            "market_cap": "market_cap",
            "company_name": "company_name",
            "analyst_consensus": "analyst_consensus",
        }
    )
    table.to_csv(f"./stocks_info/{marketCap}.csv")
    # execute_many(connection, table, "Stocks")


if __name__ == "__main__":
    for cap in market_caps:
        get_stocks(cap)
