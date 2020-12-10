""" 
    This script is used to download historical data for each stock
"""

# some POSTGRES shortcuts
### sudo -i -u postgres (log in posgres account)
### sudo -i -u <username> (log in into specific db)
### sudo -u admin(galibin24) psql (log in the running db)
### sudo netstat -plunt |grep postgres (check runnning db)

# Inside the posgres account
### psql (get the inteface)
### \conninfo (check the connected dbs)
### createuser --interactive

# https://query1.finance.yahoo.com/v8/finance/chart/${params.ticker}?period1=${params.startDate}&period2=${params.endDate}&interval=1d

# load cvs with pandas
# get ranges for the stock, if stock not found delete from csv loaded file
#
import asyncio
import requests
import pandas as pd
import numpy as np
from aiohttp import ClientSession
import psycopg2

from dbFuncs import execute_many

# TODO Push data as one object after downloading
API = "https://query1.finance.yahoo.com/v8/finance/chart/{}?period1={}&period2={}&interval=1d"
market_caps = ["Mega", "Large", "Medium", "Small"]


main_db = pd.DataFrame(
    columns=[
        "ticker",
        "close_price",
        "open_price",
        "volume",
        "adj_close",
        "Datetime",
        "day_drop",
        "market_cap",
        "company_name",
    ]
)


class CapScraper(object):
    def __init__(self, df, marketCap):
        self.df = df
        self.marketCap = marketCap

    async def start(self):
        async with ClientSession() as session:

            await asyncio.gather(
                *[
                    self.getRanges(
                        self.df.loc[i, "ticker"],
                        self.df.loc[i, "stock_outstanding"],
                        self.df.loc[i, "company_name"],
                        i,
                        session,
                    )
                    for i in range(len(list(self.df.index)))
                ]
            )

        self.df.to_csv(f"./stocks_info/{self.marketCap}.csv")

    async def getRanges(self, stock, sharesOut, company_name, index, session):
        """
        get starting range(when stock had IPO) and ending range
        return if it doesn't exist

        """
        # if stock got a dot change to dash
        if "." in stock:
            stock = stock.replace(".", "-")

        request = await session.request(
            method="GET", url=API.format(stock, "1604793600", "1605571200")
        )
        dataJson = await request.json()
        try:
            startRange = dataJson["chart"]["result"][0]["meta"]["firstTradeDate"]
        except:
            error = dataJson["chart"]["error"]["description"]
            print(error)
            # self.df.drop(index=index, inplace=True)
            return

        endRange = dataJson["chart"]["result"][0]["meta"]["currentTradingPeriod"][
            "regular"
        ]["end"]
        if startRange == None or endRange == None:
            print("range was not found")
            return

        await self.getData(
            startRange, endRange, stock, sharesOut, company_name, session
        )

    async def getData(
        self, startRange, endRange, stock, sharesOut, company_name, session
    ):
        global main_db
        """
        get the data from yahoo provided a range and a ticker

        """

        request = await session.request(
            method="GET", url=API.format(stock, startRange, endRange)
        )
        json_data = await request.json()

        try:
            openPrices = json_data["chart"]["result"][0]["indicators"]["quote"][0][
                "open"
            ]
        except:
            print(API.format(stock, startRange, endRange))

        closePrices = json_data["chart"]["result"][0]["indicators"]["quote"][0]["close"]
        timeStamp = json_data["chart"]["result"][0]["timestamp"]
        volume = json_data["chart"]["result"][0]["indicators"]["quote"][0]["volume"]
        adjPrice = json_data["chart"]["result"][0]["indicators"]["adjclose"][0][
            "adjclose"
        ]

        if "-" in stock:
            stock = stock.replace("-", ".")
        data_for_frame = {
            "ticker": stock,
            "close_price": closePrices,
            "open_price": openPrices,
            "volume": volume,
            "adj_close": adjPrice,
            "Datetime": timeStamp,
            "company_name": company_name,
        }

        dataFrame = pd.DataFrame(data_for_frame)
        dataFrame["day_drop"] = dataFrame["close_price"] / dataFrame["open_price"] - 1
        dataFrame["market_cap"] = dataFrame["adj_close"] * float(sharesOut)

        main_db = main_db.append(dataFrame, ignore_index=True)
        # saveData(dataFrame, stock)


if __name__ == "__main__":
    stocks = pd.read_csv(f"./stocks_info/Mega.csv", index_col=0)
    Cap_Scraper = CapScraper(stocks, "Mega")
    asyncio.run(Cap_Scraper.start())

    execute_many(main_db)

    # 1.5429834e+09
