import numpy as np
import pandas as pd

caps = ["Mega", "Large", "Medium", "Small"]


def process_stock(stock_df, outShares, ticker):
    stock_df["marketCap"] = stock_df["adjClose"] * float(outShares)
    stock_df["dailyPriceChange"] = stock_df["closePrice"] / stock_df["openPrice"] - 1
    stock_df["ticker"] = ticker
    stock_df.to_csv(f"singleStockPrices/{ticker}.csv")

    pass


def get_stock_df(ticker, outShares):
    if "." in ticker:
        ticker = ticker.replace(".", "-")
    stock_df = pd.read_csv(f"./singleStockPrices/{ticker}.csv", index_col=0)
    process_stock(stock_df, outShares, ticker)


def main():
    for cap in caps:
        CapDf = pd.read_csv(f"./stocks_info/{cap}.csv", index_col=0)

        for i in range(len(list(CapDf.index))):
            data = CapDf.loc[i, ["ticker", "stock_outstanding"]]
            ticker = data[0]
            outShares = data[1]
            get_stock_df(ticker, outShares)


if __name__ == "__main__":
    main()
