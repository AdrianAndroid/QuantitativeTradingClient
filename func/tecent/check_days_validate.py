from const.const import TENCENT_STOCKS_FILE
from const.const import TENCENT_DAYS_COLLECT
from const.const import DB_DAY_COLLECT
from mysqls.ms_db import MsDbDayOperator
import func.tecent.stocks as stocksapp
from func.tecent.csv_day_op import CsvDayOperator
from func.tecent.csv_day_op import CsvStockOperator
import tools.filetool as filetool
import log
from func.tecent.stock import Stock
from func.tecent.day import Day


class CheckDaysValid:
    def __init__(self):
        self._local_dir = f'{TENCENT_DAYS_COLLECT}'
        self.msDbOperator = MsDbDayOperator(db_name=DB_DAY_COLLECT)

    def query_data_dict_from_sql(self, stock):
        _code = stock.read_code()
        _type = stock.read_type()
        tableName = f'{_type}{_code}'
        return self.msDbOperator.query_day_rows_to_dict(tableName)  # dayDict

    def read_days_from_csv(self, stock):
        _typeCode = f'{stock.read_type()}{stock.read_code()}'
        csv_filename = f'{_typeCode}.csv'
        _csvDayOperator = CsvDayOperator(csvPath=filetool.join_path(self._local_dir, csv_filename))
        _dictDay = _csvDayOperator.read_csv_days_to_dict()
        return _dictDay

    def callback(self, stock):
        _queryDayDict = self.query_data_dict_from_sql(stock)
        _csvDayDict = self.read_days_from_csv(stock)
        if len(_queryDayDict) != len(_csvDayDict):
            log.error(f'数据库和csv数据不一致, _queryDayDict={len(_queryDayDict)}, _csvDayDict={len(_csvDayDict)}')

        if len(_queryDayDict) > len(_csvDayDict):
            log.info(f'数据库中的数据更多')
            _keys = _csvDayDict.keys()
        elif len(_queryDayDict) < len(_csvDayDict):
            log.info(f'csv中的数据更多')
            _keys = _queryDayDict.keys()
        else:
            log.info(f'数据库中的数据一致')
            _keys = _csvDayDict.keys()
        for _k in _keys:
            _queryDay = _queryDayDict[_k]
            _csvDay = _csvDayDict[_k]
            if not _queryDay.equalsBean(_csvDay):
                raise Exception(f'数据不一致 _queryDay={_queryDay} _csvDay={_csvDay}')

    def startOneWork(self):
        self.callback(Stock('002230', '科大讯飞', 'sz'))

    def startWork(self):
        # self.startOneWork()
        _csvStockOperator = CsvStockOperator(TENCENT_STOCKS_FILE)
        _csvStockOperator.read_csv_stock_to_list()
        _csvStockOperator.iter_list_stock(callback=lambda stock: self.callback(stock))
