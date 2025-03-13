# 显示的列表
# https://proxy.finance.qq.com/ifzqgtimg/appstock/news/noticeList/search?page=1&symbol=sz300750&n=51&_var=finance_notice&noticeType=0103&from=web&_=1741762379841
# 需要下载的pdf内容
# https://proxy.finance.qq.com/ifzqgtimg/appstock/news/content/content?_var=notice_detail&id=nos1221432744&_=1741762380022
# 具体的哪个pdf
# https://file.finance.qq.com/finance/hs/pdf/2024/10/19/1221432744.PDF

from func.stock import Stock
from tools.urltools import read_url
from tools.timetool import timestamp13
import tools.filetool as filetool
import tools.jsontool as jsontool
import const.const as const
import json
import log


class ReportListItem:
    # "id": "nos1209729000",
    # "symbol": "sz002230",
    # "title": "科大讯飞：2021年第一季度报告全文",
    # "time": "2021-04-19 21:34:35",
    # "type": "0",
    # "url": "",
    # "newstype": "01010503,010112,01030501",
    # "update_time": "2021-04-19 21:34:35",
    # "Ftranslate": "0",
    # "noticeTypeDesc": "一季度报告正文"
    def __init__(self, itemDict):
        _id = itemDict['id']  # "id": "nos1209729000",
        _symbol = itemDict['symbol']  # "symbol": "sz002230",
        _title = itemDict['title']  # "title": "科大讯飞：2021年第一季度报告全文",
        _time = itemDict['time']  # "time": "2021-04-19 21:34:35",
        _type = itemDict['type']  # "type": "0",
        _url = itemDict['url']  # "url": "",
        _newstype = itemDict['newstype']  # "newstype": "01010503,010112,01030501",
        _update_time = itemDict['update_time']  # "update_time": "2021-04-19 21:34:35",
        _Ftranslate = itemDict['Ftranslate']  # "Ftranslate": "0",
        _noticeTypeDesc = itemDict['noticeTypeDesc']  # "noticeTypeDesc": "一季度报告正文"


class ReportList:
    # {code:x, mag:x, data:[]}
    def __init__(self, jsonData):
        self._total_num = 0
        self._total_page = 0
        self._reportList = []
        if 'data' in jsonData:
            _data = jsonData['data']
            self._total_num = _data['total_num']
            self.total_page = _data['total_page']
            for _item in _data['data']:
                _reportItem = ReportListItem(_item)
                self._reportList.append(_reportItem)

    def get_report_list(self):
        return self._reportList



class DownloadNoticeList:

    def __init__(self, stock: Stock):
        self._reportList = None
        self._stock = stock

    def read_notice_list(self):
        _code = self._stock.read_code()
        _type = self._stock.read_type()
        _name = self._stock.read_name()
        _typeCode = f'{_type}{_code}'
        _timestamp13 = timestamp13()
        url = f'https://proxy.finance.qq.com/ifzqgtimg/appstock/news/noticeList/search?page=1&symbol={_typeCode}&n=51&_var=finance_notice&noticeType=0103&from=web&_={_timestamp13}'
        _decode_data = read_url(url)

        str_json_data = _decode_data.split('=', 1)[1]
        json_data = json.loads(str_json_data)
        if not jsontool.is_json_validate(json_data):
            log.error(f'非法的json数据. url={url}, json:{json_data}')
            return
        _reportList = ReportList(json_data)
        return _reportList.get_report_list()


class DownloadReports:

    def startDownload(self):
        _stock = Stock(_code='002230', _name='科大讯飞', _type='sz')

        # 下载年报的工作目录
        _code = _stock.read_code()
        _type = _stock.read_type()
        _name = _stock.read_name()
        _work_dir_name = f'{_type}{_code}{_name}'
        _report_dir = filetool.join_path(const.DOWNLOAD_REPORT_PATH, _work_dir_name)
        filetool.create_folder_if_not_exists(_report_dir)

        # 获取第一页的年报列表
        _download_notice_list = DownloadNoticeList(_stock)
        _report_list = _download_notice_list.read_notice_list()
        log.info(_report_list)
