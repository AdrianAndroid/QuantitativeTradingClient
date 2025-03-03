import pandas as pd

import const.const
from functions.tecent.stock import Stock


# callback(Stock)
def read_stock_csv(callback):
    filepath = const.const.TENCENT_STOCKS_FILE
    df = pd.read_csv(filepath, dtype={0: str})
    size = len(df)
    for index, row in df.iterrows():
        _code = row[0]
        _name = row[1]
        _type = row[2]
        callback(Stock(_code, _name, _type))
        print(size)
        size -= 1
