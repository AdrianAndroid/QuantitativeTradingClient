import time
import pandas as pd
import log
from mysqls.ms_db import MsDbDayOperator
from func.stocks import read_stock_csv
from func.stock import Stock
from func.day import Day
from func.tecent.csv_day_op import CsvDayOperator
from func.tecent.csv_day_op import CsvStockOperator
from const.const import TENCENT_STOCKS_FILE
from const.const import UPDATE_DAY_COLLECT_DIR_CSV
import tools.filetool as filetool


class DbDayUpdater:
    def __init__(self, is_can_update=False):
        self._is_can_update_ = is_can_update
        self.db_operator = MsDbDayOperator()

    def _uploadStockDays(self, stock):
        _code = stock.read_code()
        _type = stock.read_type()
        _name = stock.read_name()
        _typeCode = f"{_type}{_code}"
        tableName = _typeCode

        if not self.db_operator.create_day_table(tableName):
            err = f'create_table {tableName} 不成功'
            log.error(f'create_table {tableName} 不成功')
            raise Exception(err)
            # return

        _queryDayDict = self.db_operator.query_day_rows_to_dict(table=tableName)

        _csvPath = filetool.join_path(filetool.join_path(UPDATE_DAY_COLLECT_DIR_CSV, f'{tableName}.csv'))
        if not filetool.is_file_exits(_csvPath):
            log.error(f"csv文件不存在 : {_csvPath} ")
            return
        _csvDayOp = CsvDayOperator(_csvPath)
        _csvDayDict = _csvDayOp.read_csv_days_to_dict()

        _needUpdateDays = []
        _needInsertDays = []
        for _k in _csvDayDict.keys():
            _csvDay = _csvDayDict[_k]
            if _k in _queryDayDict:
                # 需要更新
                _queryDay = _queryDayDict[_k]
                if not _queryDay.equalsBean(_csvDay):
                    _needUpdateDays.append(_csvDay)
                    if not self._is_can_update_:
                        raise Exception('数据不一致，请自查!')
                # else:
                #     log.info(f'数据一致! {_queryDay}')
            else:
                # 需要插入
                _needInsertDays.append(_csvDay)
        if len(_needUpdateDays) > 0:
            log.info('DbDayUpdater 更新数据')
            self.db_operator.update_day_list_rows(
                table=tableName,
                listDayRows=_needUpdateDays
            )
        if len(_needInsertDays) > 0:
            log.info('DbDayUpdater 插入新数据')
            self.db_operator.insert_day_rows(
                table=tableName,
                listDayRows=_needInsertDays
            )

    def callback(self, stock: Stock):
        self._uploadStockDays(stock)

    def startWork(self):
        # self.callback(Stock('002230', '科大讯飞', 'sz'))
        _csvStockStockOp = CsvStockOperator(TENCENT_STOCKS_FILE)
        # _list = _csvStockStockOp.read_csv_stock_to_list()
        # _csvStockStockOp.iter_list_stock(lambda stock: self.callback(stock))

        _csvStockStockOp.read_csv_stock_to_list_callback(lambda stock: self.callback(stock))
