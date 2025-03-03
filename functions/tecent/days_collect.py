import datetime

import const.const as const
import csvs.csv_tool as dict_to_csv
import functions.tecent.history_un_collect as history_un_collect
import functions.tecent.stocks as stockapp
import jsons.jsontool as jsontool
from functions.tecent.day import Day
from functions.tecent.stock import Stock

_gDayDict = {}


# 未整理的日数据
def day_un_collect_json_filename(stock, _year):
    _code = stock.read_code()
    _name = stock.read_name()
    _type = stock.read_type()
    return f'{const.TENCENT_DAYS_UN_COLLECT}/{_type}{_code}_{_year}.json'


# 处理后的日文件
def day_collect_json_filename(stock):
    _code = stock.read_code()
    _name = stock.read_name()
    _type = stock.read_type()
    return f'{const.TENCENT_DAYS_COLLECT}/{_type}{_code}.json'


def deal_day_to_dict(stock, _year):
    _code = stock.read_code()
    _name = stock.read_name()
    _type = stock.read_type()
    _dayUnCollectJsonFileName = day_un_collect_json_filename(stock, _year)
    if jsontool.is_json_file_validate(_dayUnCollectJsonFileName):
        json_data = jsontool.read_json_file(_dayUnCollectJsonFileName)
        print(json_data)
        if 'data' in json_data:
            data = json_data['data']
            stockCode = f'{_type}{_code}'
            if stockCode in data:
                json_stockcode = data[stockCode]
                if 'day' in json_stockcode:
                    _days = json_stockcode['day']
                    for _day_item in _days:
                        print(_day_item)
                        _date = _day_item[0]
                        _open_price = _day_item[1]
                        _high = _day_item[3]
                        _low = _day_item[4]
                        _close = _day_item[2]
                        _vol = _day_item[5]
                        _dayBean = Day(_date, _open_price, _high, _low, _close, _vol)
                        print(_dayBean)
                        _add_to_dict(_dayBean)


def _add_to_dict(day):
    _date = day.get_date()
    if _date in _gDayDict:
        # 验证比较是否相同
        _oldDay = _gDayDict[_date]
        if not day.equalsBean(_oldDay):
            raise ValueError('两个Date的输入不同。')
        else:
            print('000000000000000-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0-0')

    else:
        _gDayDict[_date] = day


def _read_item_to_day(_day_item):
    _date = _day_item[0]
    _open_price = _day_item[1]
    _high = _day_item[3]
    _low = _day_item[4]
    _close = _day_item[2]
    _vol = _day_item[5]
    _dayBean = Day(_date, _open_price, _high, _low, _close, _vol)
    return _dayBean


def _test_stock():
    callback(Stock('002230', '科大讯飞', 'sz'))


def callback(stock):
    _gDayDict.clear()
    _code = stock.read_code()
    _name = stock.read_name()
    _type = stock.read_type()
    print(_code, _name, _type)
    historyFileName = history_un_collect.history_filename(stock)
    _year = datetime.date.today().year
    deal_day_to_dict(stock, _year)
    _loadJsonHistoryList = history_un_collect.load_json_history_list(historyFileName)
    print(_loadJsonHistoryList)
    for _year in _loadJsonHistoryList:
        _dayCollectJsonFileName = day_collect_json_filename(stock)
        deal_day_to_dict(stock, _year)
        # print('未整理数据路径', _dayCollectJsonFileName, filetool.is_json_file_validate(_dayCollectJsonFileName))
    dict_to_csv.dict_to_csv(stock, _gDayDict)


def collect_days():
    print('collect days')
    stockapp.read_stock_csv(callback)
    # _test_stock()
