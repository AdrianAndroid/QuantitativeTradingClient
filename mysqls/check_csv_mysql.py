import mysqls.ms_db as ms_db
import mysqls.ms_op as ms_op
import func.tecent.stocks as stockapp
from func.tecent.day import Day, DAY_HEADER_DATE, DAY_HEADER_OPEN_PRICE, DAY_HEADER_LOW, DAY_HEADER_VOL, \
    DAY_HEADER_HIGH, DAY_HEADER_CLOSE, day_csv_header, day_csv_row
from func.tecent.stock import Stock
import const.const
import log
import func.tecent.days_collect as days_collect
import tools.filetool as filetool
from mysqls.ms_db import MsDbOperator
import csv


class CsvMysqlComparator:
    def __init__(self):
        self.db_operator = MsDbOperator(db_name=const.const.DB_DAY_COLLECT)

    def check_table_empty(self, stock):
        code = stock.read_code()
        name = stock.read_name()
        stock_type = stock.read_type()
        log.info(f'{code}, {name}, {stock_type}')
        table_name = f'{stock_type}{code}'
        query_day_list = self.db_operator.query_rows(table_name)
        if len(query_day_list) == 0:
            raise Exception(f'{table_name} 数据为空。')
        else:
            log.info(f'{table_name} 正常.')



    def process_stock(self, stock):
        code = stock.read_code()
        name = stock.read_name()
        stock_type = stock.read_type()
        log.info(f'{code}, {name}, {stock_type}')

        table_name = f'{stock_type}{code}'
        stock_type_code = f'{stock_type}{code}'
        log.info(f'tableName={table_name}')

        # Read CSV data
        filename = f'{const.const.TENCENT_DAYS_COLLECT}/{stock_type_code}.csv'
        csv_list_days = self.read_csv_to_list(filename)

        query_day_list = self.db_operator.query_rows(table_name)
        query_day_dict = {day[0]: Day(day[0], day[1], day[2], day[3], day[4], day[5]) for day in query_day_list}
        query_day_keys = query_day_dict.keys()

        if len(csv_list_days) != len(query_day_keys):
            raise Exception(f'{table_name} 数据不一致. csvsize={len(csv_list_days)} tablesize={len(query_day_keys)}')

        for _day in csv_list_days:
            day_date = _day.get_date()
            if day_date in query_day_keys:
                query_day = query_day_dict[day_date]
                if not _day.equalsBean(query_day):
                    raise Exception(f'{table_name} 数据不一致: {_day} ; {query_day}')
            else:
                raise Exception(f'{table_name} 没有该数据')
        log.info(f'{table_name} 所有数据正常')

    def read_csv_to_list(self, filename):
        _listDays = []
        with open(filename, mode='r', newline='') as file:
            reader = csv.reader(file)
            next(reader)
            for _item in reader:
                _date = _item[0]
                _open_price = _item[1]
                _high = _item[2]
                _low = _item[3]
                _close = _item[4]
                _vol = _item[5]
                _day = Day(_date, _open_price, _high, _low, _close, _vol)
                _listDays.append(_day)
        return _listDays

    def compare_all_csv_files(self):
        stockapp.read_stock_csv(self.process_stock)


def check_callback(stock):
    code = stock.read_code()
    # print(type(code))
    # if f'{code}' not in ('001282', '603227', '000982', '000956', '000877', '000780', '000667'):
    #     return
    comparator = CsvMysqlComparator()
    # comparator.process_stock(stock)
    comparator.check_table_empty(stock)


def check_csv_to_db():
    stockapp.read_stock_csv(check_callback)
    # comparator.process_stock(Stock('002230', '科大讯飞', 'sz'))
