import tools.filetool as filetool
import log
from func.day import Day
import pandas as pd
from func.stock import Stock


class CsvStockOperator:
    def __init__(self, csvPath):
        self._csv_path_ = csvPath
        if not filetool.is_file_exits(csvPath):
            _err = f'CsvDayOperator csv文件不存在, csvPath={csvPath}'
            log.error(_err)
            raise Exception(_err)
        self._listStock = []

    def csv_row_to_stock(self, row):
        _code = row[0]
        _name = row[1]
        _type = row[2]
        return Stock(_code, _name, _type)

    def read_csv_stock_to_list(self):
        df = pd.read_csv(self._csv_path_, dtype={0: str})
        self._listStock.clear()
        for index, row in df.iterrows():
            _rowToStock = self.csv_row_to_stock(row)
            self._listStock.append(Stock())
        return self._listStock

    def read_csv_stock_to_list_callback(self, stock_call_back):
        df = pd.read_csv(self._csv_path_, dtype={0: str})
        for index, row in df.iterrows():
            _rowToStock = self.csv_row_to_stock(row)
            stock_call_back(_rowToStock)

    def iter_list_stock(self, callback):
        """
        调用此方法时，必须先调用{self.read_csv_stock_to_list()}
        :param callback:
        :return:
        """
        count = len(self._listStock)
        for stock in self._listStock:
            count -= 1
            log.info(f'当前stock={stock} 剩余:{count}')
            callback(stock)


class CsvDayOperator:

    def __init__(self, csvPath):
        self._csv_path_ = csvPath
        if not filetool.is_file_exits(csvPath):
            _err = f'CsvDayOperator csv文件不存在, csvPath={csvPath}'
            log.error(_err)
            raise Exception(_err)
        self.dictDay = {}

    def csv_row_to_day(self, csv_row):
        _date = csv_row[0]
        _open_price = csv_row[1]
        _high = csv_row[2]
        _low = csv_row[3]
        _close = csv_row[4]
        _vol = csv_row[5]
        _day = Day(
            date=_date,
            open_price=_open_price,
            high=_high,
            low=_low,
            close=_close,
            vol=_vol
        )
        return _day

    def read_csv_days_to_dict(self):
        self.dictDay.clear()
        df = pd.read_csv(
            self._csv_path_,
            dtype={0: str, 1: str, 2: str, 3: str, 4: str, 5: str}
        )  # 将Vol列设为str类型以保留小数点后的0
        for index, row in df.iterrows():
            _day = self.csv_row_to_day(row)
            self.dictDay[f'{_day.get_date()}'] = _day
        return self.dictDay

    def write_days_dict_to_csv(self, day_dict: dict):
        """
        :param callback:
        #     with open(filename, mode='w', newline='') as file:
        #         writer = csv.writer(file)
        #         writer.writerow(day.day_csv_header())
        #         log.info(day.day_csv_header())
        #         for _date in sorted(_dict.keys()):
        #             log.info(_date)
        #             _day = _dict[_date]
        #             _row = day.day_csv_row(_day)
        #             writer.writerow(_row)
        #             log.info(_row)
        """
        # with open(self._csv_path_, mode='w', newline='') as file:
        #     writer = csv.writer(file)  # header
        #     writer.writerow(day_csv_header())
        #
        pass
