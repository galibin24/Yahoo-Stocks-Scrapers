# same as history but range will get the ranges for last range from Database

# one day is 86400 in timestamp
# yahoo updates the prices on market open at 9:30 am
# we will update the price in the evening to get closing price right

# When pusging production db with historical data do it in the evening

# queries last 5 days
# run it after 5 pm UCT

import asyncio
import requests
import pandas as pd
import numpy as np
from aiohttp import ClientSession
import psycopg2

from dbFuncs import execute_many


API = "https://query1.finance.yahoo.com/v8/finance/chart/{}?interval=1d&range=2d"
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
                    self.getData(
                        self.df.loc[i, "ticker"],
                        self.df.loc[i, "stock_outstanding"],
                        session,
                    )
                    for i in range(len(list(self.df.index)))
                ]
            )

        self.df.to_csv(f"./stocks_info/{self.marketCap}.csv")

    async def getData(self, stock, sharesOut, session):
        global main_db
        """
        get the data from yahoo provided a range and a ticker
        """
        request = await session.request(method="GET", url=API.format(stock))
        json_data = await request.json()

        try:
            openPrices = json_data["chart"]["result"][0]["indicators"]["quote"][0][
                "open"
            ]
        except:
            print("price not found")
            print(API.format(stock))
            return

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
        }

        dataFrame = pd.DataFrame(data_for_frame)
        dataFrame["day_drop"] = dataFrame["close_price"] / dataFrame["open_price"] - 1
        dataFrame["market_cap"] = dataFrame["adj_close"] * float(sharesOut)

        main_db = main_db.append(dataFrame, ignore_index=True)
        # saveData(dataFrame, stock)


if __name__ == "__main__":
    stocks = pd.read_csv(f"./stocks_info/Mega.csv", index_col=0)
    # print(stocks)
    Cap_Scraper = CapScraper(stocks, "Mega")
    asyncio.run(Cap_Scraper.start())

    print(main_db)
    execute_many(main_db)