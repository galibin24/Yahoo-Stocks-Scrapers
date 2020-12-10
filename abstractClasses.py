# TODO
class Stock:
    market_category = ENUM
    ticker = string(Index, UniqueKey)
    comany_name = string
    prices = ticker


class StockPrice:
    id = int(uniqueKey, autoincrement=True)
    ticker = ForeignKey(stock)
    open_price = float
    close_price = float
    date = datetime
    market_cap = float
    day_drop = float

    # SElECT MEGA
    # WHERE DATE
    #

    # cosequite percentage drop = 10%, 5
    #

    # first day, second day
    def func(start_date, end_date, percentage_drop, in_days):
        numpy_array = [[date, price], ...]

        num_len = numpy_array.length
        current_row = 0
        changes_captured = []

        # for i in iter_num:
        # i number of days

        while current_row < num_len:
            comparison_price = numpy_array[current_row][1]

            for day in in_days:
                this_day_price = numpy_array[(current_row + day + 1)][1]
                change = this_day_price / comparison_price - 1
                if change == percentage_drop:
                    changes_captured.append(
                        [
                            numpy_array[current_row],
                            numpy_array[(current_row + day + 1)],
                        ]
                    )
                    current_row = current_row + day + 1

    # SELECT * FROM stoc
    # JOIN stock_price on stock_price.ticker = stock.ticker
    # WHERE market_category = $market_category
    # AND stock_price.date BETWEEN $date_after AND $date_before


# DELETE dubplicates query

# DELETE FROM "Stock Prices" T1
#     USING   "Stock Prices" T2
# WHERE T1.ctid < T2.ctid
#     AND T1.close_price = T2.close_price
#     AND T1.datetime  = T2.datetime;