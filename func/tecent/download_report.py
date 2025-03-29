# 显示的列表
# https://proxy.finance.qq.com/ifzqgtimg/appstock/news/noticeList/search?page=1&symbol=sz300750&n=51&_var=finance_notice&noticeType=0103&from=web&_=1741762379841
# 需要下载的pdf内容
# https://proxy.finance.qq.com/ifzqgtimg/appstock/news/content/content?_var=notice_detail&id=nos1221432744&_=1741762380022
# 具体的哪个pdf
# https://file.finance.qq.com/finance/hs/pdf/2024/10/19/1221432744.PDF

from func.stock import Stock
from tools.urltools import read_url
from tools.urltools import download_pdf
from tools.urltools import is_url
from tools.timetool import timestamp13
import tools.filetool as filetool
import tools.jsontool as jsontool
import const.const as const
import json
import log
from func.stocks import read_stock_csv_no_thread_to_dict_by_name_key


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
        self._id = itemDict['id']  # "id": "nos1209729000",
        self._symbol = itemDict['symbol']  # "symbol": "sz002230",
        self._title = itemDict['title']  # "title": "科大讯飞：2021年第一季度报告全文",
        self._time = itemDict['time']  # "time": "2021-04-19 21:34:35",
        self._type = itemDict['type']  # "type": "0",
        self._url = itemDict['url']  # "url": "",
        self._newstype = itemDict['newstype']  # "newstype": "01010503,010112,01030501",
        self._update_time = itemDict['update_time']  # "update_time": "2021-04-19 21:34:35",
        self._Ftranslate = itemDict['Ftranslate']  # "Ftranslate": "0",
        self._noticeTypeDesc = itemDict['noticeTypeDesc']  # "noticeTypeDesc": "一季度报告正文"

    def __repr__(self):
        return (f"ReportListItem(id='{self._id}', "
                f"symbol='{self._symbol}', "
                f"title='{self._title}', "
                f"time='{self._time}', "
                f"type='{self._type}', "
                f"noticeTypeDesc='{self._noticeTypeDesc}')")

    @property
    def id(self):
        return self._id


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

    def __repr__(self):
        return (f"ReportList(total_num={self._total_num}, "
                f"total_page={self.total_page}, "
                f"reports_count={len(self._reportList)}, "
                f"reports={self._reportList})")


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


class DownloadNosItem:
    def __init__(self, itemDict):
        self._id = self.dictRead(itemDict, 'id')  # "id": "1221432679",
        self._time = self.dictRead(itemDict, 'time')  # "time": "2024-10-18 18:44:10",
        self._type = self.dictRead(itemDict, 'type')  # "type": 0,
        self._title = self.dictRead(itemDict, 'title')  # "title": "科大讯飞：2024年三季度报告",
        self._url = self.dictRead(itemDict, 'url')  # "url": "http://stockhtm.finance.qq.com/sstock/quot",
        self._pdf = self.dictRead(itemDict, 'pdf')  # "pdf": "http://file.finance.qq.com/finance/hs/pdf/2024/10/19/1221432679.PDF"

    def dictRead(self, _dict, _key):
        if _key in _dict:
            return _dict[_key]
        else:
            return ''

    def __repr__(self):
        return (f"DownloadNosItem(id='{self._id}', "
                f"time='{self._time}', "
                f"type='{self._type}', "
                f"title='{self._title}', "
                f"pdf='{self._pdf}')")


class DownloadNosReport:

    def __init__(self):
        self.nosItem = None

    def download_nos(self, report_item: ReportListItem):
        _nosId = report_item.id
        url = f'https://proxy.finance.qq.com/ifzqgtimg/appstock/news/content/content?_var=notice_detail&id={_nosId}&_={timestamp13()}'
        log.info(url)
        _decode_data = read_url(url)

        str_json_data = _decode_data.split('=', 1)[1]
        json_data = json.loads(str_json_data)
        if not jsontool.is_json_validate(json_data):
            log.error(f'非法的json数据. url={url}, json:{json_data}')
            return
        # log.info(json_data)
        if 'data' in json_data:
            _data = json_data['data']
            if len(_data) > 0:
                self.nosItem = DownloadNosItem(_data[0])
        log.info(self.nosItem)


class DownloadReports:

    def _download(self, stock):
        # 下载年报的工作目录
        _code = stock.read_code()
        _type = stock.read_type()
        _name = stock.read_name()
        _work_dir_name = f'{_type}{_code}{_name}'
        _report_dir = filetool.join_path(const.DOWNLOAD_REPORT_PATH, _work_dir_name)
        filetool.create_folder_if_not_exists(_report_dir)

        # 获取第一页的年报列表
        _download_notice_list = DownloadNoticeList(stock)
        _report_list = _download_notice_list.read_notice_list()

        _download_nos_report = DownloadNosReport()
        for _item in _report_list:
            log.info(_item)
            _title = _item._title
            _filepath = filetool.join_path(_report_dir, f'{_title}.pdf')

            if not filetool.is_file_exits(_filepath):
                log.info(f'文件不存在 filepath={_filepath}')
                _download_nos_report.download_nos(_item)
                _nosItem = _download_nos_report.nosItem
                _pdfUrl = _nosItem._pdf
                if is_url(_pdfUrl):
                    download_pdf(_pdfUrl, _filepath)
            else:
                log.info(f'文件已经存在存在 filepath={_filepath}')

    def startDownload(self):
        # _stock = Stock(_code='002230', _name='科大讯飞', _type='sz')
        # _stock = Stock(_code='002594', _name='比亚迪', _type='sz')
        # _stock = Stock(_code='600126', _name='杭钢股份', _type='sh')
        # _stock = Stock(_code='600580', _name='卧龙电驱', _type='sh')
        # _stock = Stock(_code='603881', _name='数据港', _type='sh')
        # _stock = Stock(_code='002102', _name='能特科技', _type='sz')
        # _stock = Stock(_code='002570', _name='贝因美', _type='sz')
        # _stock = Stock(_code='300611', _name='美力科技', _type='sz')
        # _stock = Stock(_code='600186', _name='莲花控股', _type='sh')
        # _stock = Stock(_code='600143', _name='金发科技', _type='sh')
        _list_stocks = [
            '宁波华翔',
            '重庆百货',
            '紫江企业',
            '志邦家居',
            '永利股份',
            '泸州老窖',
            '老板电器',
            '鲍斯股份',
            '欧派家居',
            '赛轮轮胎',
            '兔 宝 宝',
            '海信家电',
            '顾家家居',
            '南钢股份',
            '索菲亚',
            '比音勒芬',
            '承德露露',
            '华域汽车',
            '太阳纸业',
            '中信银行',
            '红旗连锁',
            '上海银行',
            '海尔智家',
            '北京人力',
            '水星家纺',
            '报 喜 鸟',
            '远兴能源',
            '锦泓集团',
            '鸿路钢构',
            '兴业银行',
            '江苏银行',
            '金牌家居',
            '嘉化能源',
            '苏盐井神',
            '招商银行',
            '新奥股份',
            '科思股份',
            '福建高速',
            '华旺科技',
            '雅戈尔',
            '氯碱化工',
            '江苏国泰',
            '亚翔集成',
            '东航物流',
            '洽洽食品',
            '长虹美菱',
            '富安娜',
            '中新集团',
            '新澳股份',
            '齐鲁银行',
            '潍柴动力',
            '嘉曼服饰',
            '招商公路',
            '中创物流',
            '哈尔斯',
            '宁波银行',
            '中炬高新',
            '成都银行',
            '杭州银行',
            '四川路桥',
            '北新建材',
            '华鲁恒升',
            '葵花药业',
            '口子窖',
            '招商轮船',
            '渝农商行',
            '格力电器',
            '济川药业',
            '广日股份',
            '旗滨集团',
            '梅花生物',
            '华润双鹤',
            '楚天高速',
            '健盛集团',
            '中国石油',
            '大商股份',
            '申能股份',
            '中国海油',
            '海兴电力',
            '国药一致',
            '光大银行',
            '华贸物流',
            '青岛港',
            '南京银行',
            '中粮糖业',
            '华帝股份',
            '周大生',
            '元祖股份',
            '沪农商行',
            '平安银行',
            '中国石化',
            '北京银行',
            '永艺股份',
            '新宝股份',
            '万华化学',
            '交通银行',
            '华夏银行',
            '长沙银行',
            '川恒股份',
            '菜百股份',
            '同德化工',
            '农业银行',
            '江山欧派',
            '航民股份',
            '洋河股份',
            '工商银行',
            '上港集团',
            '奥普科技',
            '辰欣药业',
            '欧普照明',
            '莱克电气',
            '英特集团',
            '弘亚数控',
            '城发环境',
            '南京医药',
            '浦发银行',
            '物产中大',
            '景津装备',
            '鲁阳节能',
            '重庆银行',
            '赣粤高速',
            '邮储银行',
            '中国神华',
            '西藏药业',
            '中国交建',
            '嘉友国际',
            '天健集团',
            '雪峰科技',
            '青岛银行',
            '国电电力',
            '现代投资',
            '建设银行',
            '大秦铁路',
            '常熟银行',
            '天津港',
            '兰州银行',
            '森林包装',
            '华新水泥',
            '康尼机电',
            '海油工程',
            '福元医药',
            '瑞丰银行',
            '三七互娱',
            '中国银行',
            '紫金银行',
            '厦门银行',
            '浙能电力',
            '力聚热能',
            '中远海能',
            '新洋丰',
            '同济科技',
            '浦东建设',
            '蓝焰控股',
            '建霖家居',
            '华能国际',
            '建发股份',
            '南京高科',
            '苏州银行',
            '招商证券',
            '模塑科技',
            '中原高速',
            '民生银行',
            '江阴银行',
            '皖天然气',
            '龙佰集团',
            '长春高新',
            '贵州轮胎',
            '苏农银行',
            '江南水务',
            '创力集团',
            '中国中铁',
            '深圳燃气',
            '汉钟精机',
            '威孚高科',
            '冠农股份',
            '韵达股份',
            '皖能电力',
            '森麒麟',
            '上海电力',
            '新天然气',
            '南方传媒',
            '东方电气',
            '三角轮胎',
            '神火股份',
            '中曼石油',
            '洪通燃气',
            '柳药集团',
            '贵阳银行',
            '浙商银行',
            '中国中冶',
            '中国外运',
            '吉林高速',
            '秦港股份',
            '中国平安',
            '四川成渝',
            '江盐集团',
            '宁波港',
            '唐山港',
            '九州通',
            '无锡银行',
            '通宝能源',
            '中远海特',
            '外运发展',
            '日照港',
            '地铁设计',
            '青农商行',
            '张家港行',
            '鹭燕医药',
            '双箭股份',
            '华昌化工',
            '陕天然气',
            '广东建工',
            '招商港口',
            '甘肃能化',
            '中原环保',
            '美的电器',
            '健之佳',
            '常熟汽饰',
            '国药股份',
            '渤海轮渡',
            '双环科技',
            '云天化',
            '中国人保',
            '鲁  泰Ａ',
            '中国太保',
            '中煤能源',
            '京基智农',
            '设计总院',
            '众望布艺',
            '淮北矿业',
            '老凤祥',
            '宝新能源',
            '中国铁建',
            '物产环能',
            '恒源煤电',
            '时代出版',
            '中文传媒',
            '内蒙华电',
            '金杯电工',
            '陕国投Ａ',
            '玲珑轮胎',
            '凤凰传媒',
            '兖矿能源',
            '嘉泽新能',
            '横店东磁',
            '浙商中拓',
            '洪城环境',
            '华泰证券',
            '节能风电',
            '中交设计',
            '冀中能源',
            '海容冷链',
            '中国建筑',
            '圆通速递',
            '郑煤机',
            '常宝股份',
            '云图控股',
            '中国电建',
            '孚日股份',
            '川仪股份',
            '安徽建工',
            '中山公用',
            '兴蓉环境',
            '陕西能源',
            '兴发集团',
            '广州发展',
            '厦门象屿',
            '中远海控',
            '浙江交科',
            '创业环保',
            '江河集团',
            '福能股份',
            '平煤股份',
            '长江传媒',
            '建业股份',
            '新媒股份',
            '江苏金租',
            '兰花科创',
            '一汽富维',
            '江铃汽车',
            '首创环保',
            '华特达因',
            '长虹华意',
            '云铝股份',
            '中国海诚',
            '昊华能源',
            '金钼股份',
            '陕建股份',
            '乐歌股份',
            '新华文轩',
            '吉林敖东',
            '陕西煤业',
            '电投能源',
            '上海能源',
            '华阳股份',
            '新集能源',
            '华光环能',
            '甬金股份',
            '潞安环能',
            '中国化学',
            '金 螳 螂',
            '林洋能源',
            '牧原股份',
            '新华保险',
            '太阳能',
            '华电国际',
            '凌霄泵业',
            '新华医疗',
            '星湖科技',
            '中原传媒',
            '三峰环境',
            '晋控煤业',
            '江西铜业',
            '中信特钢',
            '风神股份',
            '威奥股份',
            '山东路桥',
            '开滦股份',
            '南山铝业',
            '鄂尔多斯',
            '山东出版',
            '宁波能源',
            '金卡智能',
            '中谷物流',
            '晶盛机电',
            '隧道股份',
            '西部矿业',
            '天山铝业',
            '海天股份',
            '洛阳钼业',
            '海油发展',
            '正泰电器',
            '豫光金铅',
            '四方科技',
            '瀚蓝环境',
            '天地科技',
            '炬华科技',
            '常润股份',
            '山西焦煤',
            '上海建工',
            '九丰能源',
            '兆驰股份',
            '诺力股份',
            '山煤国际',
            '中钢国际',
            '中材国际',
            '河钢资源',
            '武进不锈',
            '高测股份',
            '金开新能',
            '捷佳伟创',
            '中国铝业',
            '大华股份',
            '荣晟环保',
            '华设集团',
            '安徽合力',
            '博威合金',
            '富春环保',
            '奥特维',
            '旺能环境',
            '聚合顺',
            '苏美达'
        ]
        stockDict = read_stock_csv_no_thread_to_dict_by_name_key()

        for stockName in _list_stocks:
            log.info(stockDict[stockName])
            self._download(stockDict[stockName])
