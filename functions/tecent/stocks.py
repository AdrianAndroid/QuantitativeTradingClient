import pandas as pd

import const.const as const


# callback(_code, _name, _type)
def read_stock_csv(callback):
    filepath = const.TENCENT_STOCKS_FILE
    df = pd.read_csv(filepath, dtype={0: str})
    size = len(df)
    for index, row in df.iterrows():
        _code = row[0]
        _name = row[1]
        _type = row[2]
        # print(_code, _name, _type)
        callback(_code, _name, _type)
        print(size)
        size -= 1
