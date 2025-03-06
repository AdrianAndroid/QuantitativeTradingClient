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


# def _create_table_sql(table):
#     sqlCreateTable = f'CREATE TABLE IF NOT EXISTS {table}   (Date VARCHAR(255) PRIMARY KEY, Open VARCHAR(255), High VARCHAR(255), Low VARCHAR(255), Close VARCHAR(255), Vol VARCHAR(255));'
#     log.info(f'_create_table_sql = {sqlCreateTable}')
#     return sqlCreateTable


# def dict_to_csv(stock, _dict):
#     _type = stock.read_type()
#     _name = stock.read_name()
#     _code = stock.read_code()
#     filename = f'{const.TENCENT_DAYS_COLLECT}/{_type}{_code}.csv'
#     if filetool.is_file_exits(filename):
#         filetool.remove_file(filename)
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

# def _query_all_list(table: str, where=''):
#     if where:
#         sql = f"SELECT * FROM {table} where {where}"
#     else:
#         sql = f"SELECT * FROM {table}"
#     log.info(f'query_all_list sql=\'{sql}\'')
#     if cursor is None:
#         raise Exception('cursor 是None')
#     cursor.execute(sql)
#     rList = cursor.fetchall()
#     log.info(f"{len(rList)} 记录查询成功。")
#     return rList


# def _insert_rows(table: str, _listDays):
#     keys = ",".join(
#         [
#             DAY_HEADER_DATE,
#             DAY_HEADER_OPEN_PRICE,
#             DAY_HEADER_HIGH,
#             DAY_HEADER_LOW,
#             DAY_HEADER_CLOSE,
#             DAY_HEADER_VOL
#         ]
#     )
#     values = ",".join(['%s', '%s', '%s', '%s', '%s', '%s'])
#     sql = f"INSERT INTO {table} ({keys}) VALUES ({values})"
#     for _day in _listDays:
#         _tuple = (
#             _day.get_date(),
#             _day.get_open_price(),
#             _day.get_high(),
#             _day.get_low(),
#             _day.get_close(),
#             _day.get_vol()
#         )
#         cursor.execute(sql, _tuple)
#     db.commit()  # 数据表内容有更新，必须使用到该语句
#     log.info(f"cursor.rowcount  记录插入成功。")


# def _update_row(db_name: str, table: str, where: str, **kwargs):
#     list1 = []
#     for key, value in kwargs.items():
#         list1.append(f'{str(key)}={str(value)}')
#     sets = ",".join(list1)
#     sql = f"UPDATE {table} SET {sets} WHERE {where}"
#     cursor.execute(sql)
#     db.commit()


def update_or_insert_day(table, _listDay):
    # _listDbDays = _query_all_list(table)
    # _my_dict = {}
    # for _day in _listDbDays:
    #     _my_dict[]
    # _insert_rows(table, _listDay)
    pass


def read_csv_to_list(filename):
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


def callback(stock):
    _code = stock.read_code()
    _name = stock.read_name()
    _type = stock.read_type()
    log.info(f'{_code}, {_name}, {_type}')
    tableName = f'{_type}{_code}'
    log.info(f'tableName={tableName}')
    msDbOperator = MsDbOperator(db_name=const.const.DB_DAY_COLLECT)
    fields = [
        'Date VARCHAR(255) PRIMARY KEY',
        'Open VARCHAR(255)',
        'High VARCHAR(255)',
        'Low VARCHAR(255)',
        'Close VARCHAR(255)',
        'Vol VARCHAR(255)'
    ]
    if not msDbOperator.create_table(tableName, fields):
        err = f'create_table {tableName} 不成功'
        log.error(f'create_table {tableName} 不成功')
        raise Exception(err)
        return

    # 读出csv数据
    stockTypeCode = f'{_type}{_code}'
    filename = f'{const.const.TENCENT_DAYS_COLLECT}/{stockTypeCode}.csv'
    _listDays = read_csv_to_list(filename)
    # update_or_insert_day(stockTypeCode, _listDays)

    _listDates = [f'\'{day.get_date()}\'' for day in _listDays]
    _queryDays = msDbOperator.query_rows(tableName, lambda: f'{DAY_HEADER_DATE} in ({",".join(_listDates)})' if len(_listDates) != 0 else None)
    _queryDayDict = {day[0]: Day(day[0], day[1], day[2], day[3], day[4], day[5]) for day in _queryDays}
    _keys = _queryDayDict.keys()

    _needUpdateDays = []
    _needInsertDays = []
    for day in _listDays:
        if day.get_date() in _keys:
            # 需要更新
            if not day.equalsBean(_queryDayDict[day.get_date()]):
                _needUpdateDays.append(day)
                raise Exception('数据不一致，请自查!')
            # else:
            #     log.info(f"数据一致.{day}")
        else:
            _needInsertDays.append(day)

    if len(_needUpdateDays) > 0:
        msDbOperator.update_list_rows(
            tableName, _needUpdateDays, day_csv_header(),
            whereProcess=lambda day: f'{DAY_HEADER_DATE}=\'{day.get_date()}\'',
            process=lambda day: day.row_tuple()
        )
    if len(_needInsertDays) > 0:
        msDbOperator.insert_list_rows(
            tableName, _needInsertDays, day_csv_header(),
            lambda day: day.row_tuple())

    _queryAfterInsert = msDbOperator.query_rows(tableName)
    if len(set(_queryAfterInsert)) != len(_queryAfterInsert):
        _errMsg = f'完成插入的数据有重复的日期 {tableName}'
        log.error(_errMsg)
        raise Exception(_errMsg)
    else:
        log.error(f'{tableName} 所有数据唯一,数据正常')

    # msDbOperator.update_list_rows(
    #     tableName, _listDays, day_csv_header(),
    #     whereProcess=lambda day: f'{DAY_HEADER_DATE}=\'{day.get_date()}\'',
    #     process=lambda day: day.row_tuple()
    # )

    # msDbOperator.insert_list_rows(
    #     tableName, _listDays, day_csv_header(),
    #     lambda day: day.row_tuple())

    # historyFileName = history_un_collect.history_filename(stock)
    # _year = datetime.date.today().year
    # deal_day_to_dict(stock, _year)
    # _loadJsonHistoryList = history_un_collect.load_json_history_list(historyFileName)
    # print(_loadJsonHistoryList)
    # for _year in _loadJsonHistoryList:
    #     _dayCollectJsonFileName = day_collect_json_filename(stock)
    #     deal_day_to_dict(stock, _year)
    #     print('未整理数据路径', _dayCollectJsonFileName, filetool.is_json_file_validate(_dayCollectJsonFileName))
    # dict_to_csv.dict_to_csv(stock, _gDayDict)


def _test_stock():
    # callback(Stock('002230', '科大讯飞', 'sz'))sh603271
    # callback(Stock('000789', '', 'sz'))
    # callback(Stock('600529', '', 'sh'))
    callback(Stock('603271', '', 'sh'))
    # callback(Stock('301275', '', 'sz'))
    # callback(Stock('301501', '', 'sz'))
    # callback(Stock('301275', '', 'sz'))
    # callback(Stock('301501', '', 'sz'))
    # callback(Stock('301479', '', 'sz'))
    # callback(Stock('301275', '', 'sz'))
    # callback(Stock('000789', '', 'sz'))


def convert_csv_to_db():
    msDbOperator = MsDbOperator(db_name='')
    msDbOperator.create_db(const.const.DB_DAY_COLLECT)
    # 创建数据
    # stockapp.read_stock_csv(callback)
    _test_stock()
