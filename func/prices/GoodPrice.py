# 中国十年期国债收益率
# https://cn.investing.com/rates-bonds/china-10-year-bond-yield

# 深证平均市盈率走势图
# https://legulegu.com/stockdata/shenzhenPE

# 海尔智家
# https://xueqiu.com/S/SH600690?md5__1038=n4IxR70%3Di%3DDQitDkbD%2F8WiQoD%3DeQxD5%2BvvmewQx
# 海天味业
# https://xueqiu.com/S/SH603288?md5__1038=n4Ix2D9G0%3DDQD%3DG8YD%2F8WuS1zA5iKGCi%2F%2B0eD
# https://stock.xueqiu.com/v5/stock/quote.json?symbol=SH603288&extend=detail

# 沪深平均市盈率走势图
# http://value500.com/Pe.asp

from func.stocks import Stock
import tools.urltools as urltools
import log
import json
from decimal import Decimal, InvalidOperation
from func.stocks import read_stock_csv_no_thread_to_dict_by_name_key


class XueQiuDetail:
    """
    解析的容器
    """

    def __init__(self, jsonDict):
        dataDict = self.dictRead(jsonDict, 'data')
        quoteDict = self.dictRead(dataDict, 'quote')
        self.name = self.dictRead(quoteDict, 'name')  # 海天味业
        self.symbol = self.dictRead(quoteDict, 'symbol')  # SH603288
        self.exchange = self.dictRead(quoteDict, 'exchange')  # SH
        self.code = self.dictRead(quoteDict, 'code')  # 603288
        self.current = self.dictRead(quoteDict, 'current')  # 当前价：40.98
        self.chg = self.dictRead(quoteDict, 'chg')  # -0.35
        self.percent = self.dictRead(quoteDict, 'percent')  # -0.85
        self.high = self.dictRead(quoteDict, 'high')  # 最高：41.57
        self.low = self.dictRead(quoteDict, 'low')  # 最低：40.85
        self.volume_ratio = self.dictRead(quoteDict, 'volume_ratio')  # 量比：0.7
        # 委比 另外字段
        self.eps = self.dictRead(quoteDict, 'eps')  # 每股收益：1.1
        self.navps = self.dictRead(quoteDict, 'navps')  # 每股净资产：5.2822
        self.high52w = self.dictRead(quoteDict, 'high52w')  # 52周最高：52.99
        self.open = self.dictRead(quoteDict, 'open')  # 今开：41.33
        self.last_close = self.dictRead(quoteDict, 'last_close')  # 今开：41.33
        self.turnover_rate = self.dictRead(quoteDict, 'turnover_rate')  # 换手：0.13
        self.amplitude = self.dictRead(quoteDict, 'amplitude')  # 振幅：1.74
        self.dividend = self.dictRead(quoteDict, 'dividend')  # 股息（TTM）0.66
        self.dividend_yield = self.dictRead(quoteDict, 'dividend_yield')  # 股息率（TTM）1.611
        self.low52w = self.dictRead(quoteDict, 'low52w')  # 52周最低:33.08
        self.limit_up = self.dictRead(quoteDict, 'limit_up')  # 涨停：45.46
        self.limit_down = self.dictRead(quoteDict, 'limit_up')  # 跌停：37.2
        self.pe_forecast = self.dictRead(quoteDict, 'pe_forecast')  # 市盈率(动)：35.495
        self.pe_lyr = self.dictRead(quoteDict, 'pe_lyr')  # 市盈率(静)：40.499
        self.total_shares = self.dictRead(quoteDict, 'total_shares')  # 总股本：5560600544
        self.float_shares = self.dictRead(quoteDict, 'float_shares')  # 流通股：5560600544
        self.is_registration_desc = self.dictRead(quoteDict, 'is_registration_desc')  # 注册制:否
        self.volume = self.dictRead(quoteDict, 'volume')  # 成交量：7355452
        self.amount = self.dictRead(quoteDict, 'amount')  # 成交额：302448442
        self.pe_ttm = self.dictRead(quoteDict, 'pe_ttm')  # 市盈率（TTM）37.278
        self.pb = self.dictRead(quoteDict, 'pb')  # 市净率：7.758
        self.market_capital = self.dictRead(quoteDict, 'market_capital')  # 总市值：227873410293
        self.float_market_capital = self.dictRead(quoteDict, 'float_market_capital')  # 流通值：227873410293
        self.currency = self.dictRead(quoteDict, 'float_market_capital')  # 货币单位:CNY

    def dictRead(self, _dict, _key):
        if _dict is None:
            return None
        elif _key in _dict:
            return _dict[_key]
        else:
            return None

    def __repr__(self):
        # 检查必要的字段是否为 None，如果是则提供默认值
        def safe_format(value, format_spec=""):
            if value is None:
                return "N/A"
            try:
                if isinstance(format_spec, str) and format_spec.endswith('f'):
                    return format(float(value), format_spec)
                elif isinstance(format_spec, str) and ',' in format_spec:
                    return format(int(value), format_spec)
                return str(value)
            except (ValueError, TypeError):
                return "N/A"

        return (
            f"XueQiuDetail(\n"
            f"    名称: {safe_format(self.name)} ({safe_format(self.symbol)})\n"
            f"    当前价: {safe_format(self.current, '.2f')} ({safe_format(self.percent, '+.2f')}%)\n"
            f"    今日: 开盘{safe_format(self.open, '.2f')} "
            f"最高{safe_format(self.high, '.2f')} "
            f"最低{safe_format(self.low, '.2f')} "
            f"昨收{safe_format(self.last_close, '.2f')}\n"
            f"    成交: 量{safe_format(self.volume, ',d')} "
            f"额{safe_format(self.amount, ',.2f')} "
            f"换手{safe_format(self.turnover_rate, '.2f')}%\n"
            f"    估值: PE(TTM){safe_format(self.pe_ttm, '.2f')} "
            f"PE(动){safe_format(self.pe_forecast, '.2f')} "
            f"PB{safe_format(self.pb, '.2f')}\n"
            f"    股息: {safe_format(self.dividend, '.3f')}元 "
            f"股息率{safe_format(self.dividend_yield, '.2f')}%\n"
            f"    52周: 最高{safe_format(self.high52w, '.2f')} "
            f"最低{safe_format(self.low52w, '.2f')}\n"
            f"    市值: 总值{safe_format(self.market_capital / 100000000 if self.market_capital else None, '.2f')}亿 "
            f"流通{safe_format(self.float_market_capital / 100000000 if self.float_market_capital else None, '.2f')}亿\n"
            f"    每股: 收益{safe_format(self.eps, '.3f')} "
            f"净资产{safe_format(self.navps, '.3f')}\n"
            f"    其他: 量比{safe_format(self.volume_ratio, '.2f')} "
            f"振幅{safe_format(self.amplitude, '.2f')}%\n"
            f"    涨跌限: 涨停{safe_format(self.limit_up, '.2f')} "
            f"跌停{safe_format(self.limit_down, '.2f')}"
            f")"
        )


class DownloadXueQiuStockDetail:
    """
    下载数据
    """

    def __init__(self, stock: Stock):
        self.stock = stock
        pass

    def _header(self):
        _cookie = '''
            xq_a_token=cc9943aa6d41f0ae420f49b428f2f90a472b070a; 
            xqat=cc9943aa6d41f0ae420f49b428f2f90a472b070a; 
            xq_r_token=20869bd02083b2ef75d4d4b7654f827f00fdcd22; 
            xq_id_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJ1aWQiOi0xLCJpc3MiOiJ1YyIsImV4cCI6MTc0NDc2NTA3MCwiY3RtIjoxNzQzMDk2OTI0NTgyLCJjaWQiOiJkOWQwbjRBWnVwIn0.nR0-DZnypXOMxLWYKsQXxlr4jr6Em_c_CtXn70PVSJbs0U0qkfqYeZutt0IJDOz0gd8focdBoKMr3snUyIO7JhpzrHQ2iHZMzyxpfhPEZWqmCjNeDTtxw4gVvI3QsVB4EoHJZlbpeavkWgiFYyH52nfXdirJki6ZQDmhxvd5RxUIglCEqHClX7klDlwj_hA__TbiTl2ckVWSTlnDGzbb8IsF6Hh36Llfh8tZaY33iQs-LVse57RXrPuHw4id-3jc1v-b2spYhKvA-rwikL3d_VdvAJxRd1hvU9szt5O_z4RrWlfHiz2tIVEGQUuNJipfRRWqN3Lb_GC_8lfeKnrdkA; 
            cookiesu=211743096974887; 
            u=211743096974887; 
            Hm_lvt_1db88642e346389874251b5a1eded6e3=1743096975; 
            Hm_lpvt_1db88642e346389874251b5a1eded6e3=1743146544;
            HMACCOUNT=8659CD8CE466AC4E;
            device_id=7ddd79ad90a00c36653873729e92989a; 
            s=al13wkbg8m; 
            ssxmod_itna=eqGO7KiKBK0KAK4Y5i7qgqiKD58IDWIe07DzxC5YG7DupxjKidoDUWLatjDiuxR9t4e9Q7TE1k8mqDsqzW4GzDidKGhDBWE7YomL244NKCEoqoq317H0ILonjrw=py67gtodNlF=MxUuEeD8++rDYoDCqDSDxD93roD4S3Dt4DIDAYDDxDWbm2KxGP=vlR=PeePEAr=xi3iiQqiDit8xi5=VnrtkPGWneGfDDoDYbT3vp4DGT3DbkeFnjhZKP7eOrOiKEODQLOiRiTDj+FF8KGyijypAlluxBjKxoHe5hItc62zdoh1FfVbnpe7m5nesjxoemqnG1mqoYegQGx3hn0iljv=lGqeA=9pUDDARhxGnrtRDWbMGvlAvZ3hrSvxkiT8RCSRxFi48i7mu4A74zi4RxNlr18iV+THzGemDq2oiqADD; 
            ssxmod_itna2=eqGO7KiKBK0KAK4Y5i7qgqiKD58IDWIe07DzxC5YG7DupxjKidoDUWLatjDiuxR9t4e9Q7TE1k8W4DWpWCYym1DFryqoTTYOLm=DBd9GfarSp0Bqsqv18i0+dI=2Gy6kTDC+2UNs/2hlKdxyFMjqm0jElM+eNwi2tLxKNTjq9OxlSwj5bjhubYhhbCG59AfqYAhrkwt3o=dQFA2E87xOcCvFaj6etD0c085KbnRE9YuTcuye/+RElEg1Pz9=bLt5r92A=mU4O1cCLkGKnLy5WaToy/kvriaXX0GOyghPb7/zxvX1Ni97FYdWOxpb==AxTOK+oCVw3uDd54QZDhQMtggQh2WeSGVL+LOKsuwa4rhj+HWhYk=Nb8BrN27qkiNtBN4k5zgD5jxOmpDQYNqr5GF/eCSbmLS7zgxToEhqWOxkzODWEDaCCNdwcQf36=D4d20TWPhdDAUrQ205Sr5DfjkoENBb9jjDlxyqxocjBeNbCKR1mp4hSeQbifN1KoPxKqrtqax4wGQxkW04phYnXdeTTeGFRdHRNa2GfbdD1tNusbGDDQQW2bQG5zh+bGvnxdkY9XrcPaTZ+tqAp2O5wjpw6ki8LxRi/uWzaR2oQ5Ch/4hHbRyehmu2AU7injpa5bziYQ7qXpdPNI=jyEehS2rE3eGSEh3fHaqDtUVG0s3dDrT6DZwnK9Z/4SLZlX/xD2wE8qU+jAsQPs2xmDcbQWijAHFDTOd3x475QYcSD+DuYfD+6DfWG1BYn0WQxILCG5lhdhCBiNoPoDvDP5DxD
        '''.replace('\n', '').replace(' ', '')

        typeCode = self.stock.read_type_code().upper()
        headers = {
            'authority': 'stock.xueqiu.com',
            'method': 'GET',
            'path': f'/v5/stock/quote.json?symbol={typeCode}&extend=detail',
            'scheme': 'https',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-encoding': 'gzip, deflate, br, zstd',
            'accept-language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
            'cache-control': 'max-age=0',
            'cookie': _cookie,
            'priority': 'u=0, i',
            'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36'
        }
        return headers

    def download(self):
        typeCode = self.stock.read_type_code().upper()
        url = f'https://stock.xueqiu.com/v5/stock/quote.json?symbol={typeCode}&extend=detail'
        resp = urltools.read_url_get(url=url, headers=self._header(), _time=0)
        data = json.loads(resp)
        xueQiuDetail = XueQiuDetail(data)
        log.info(xueQiuDetail)
        return xueQiuDetail


class GoodPriceCalculator:
    def __init__(self, xqDetail: XueQiuDetail, tenYearTreasuryYield=0.01884, averagePriceEarningsRatio=15):
        self.xqDetail = xqDetail
        self.tenYearTreasuryYield = tenYearTreasuryYield
        self.averagePriceEarningsRatio = averagePriceEarningsRatio
        self.peRatioGoodPrice = 0.0
        self.dividendYieldGoodPrice = 0.0
        self.goodPrice = 0.0

    def _calculate_good_price_by_pe_ratio(self):
        """
        市盈率法好价格 = 15 x (当前股价➗TTM市盈率)
        """
        pe = Decimal(self.averagePriceEarningsRatio)
        current = Decimal(str(self.xqDetail.current))
        ttm = Decimal(str(self.xqDetail.pe_ttm))
        self.peRatioGoodPrice = pe * (current / ttm)
        return self.peRatioGoodPrice

    def _calculate_good_price_by_dividend_yield(self):
        """
        股息率法好价格 = TTM股息➗中国十年国债收益率
        """
        ttm = Decimal(str(self.xqDetail.dividend))
        pe = Decimal(str(self.tenYearTreasuryYield))
        self.dividendYieldGoodPrice = ttm / pe
        return self.dividendYieldGoodPrice

    def calculate_good_price(self):
        self._calculate_good_price_by_pe_ratio()
        self._calculate_good_price_by_dividend_yield()
        self.goodPrice = min(Decimal(str(self.peRatioGoodPrice)), Decimal(str(self.dividendYieldGoodPrice)))
        return self.goodPrice

    def get_pe_good_price(self):
        """
        市盈率法好价格, 调用之前，先调用good_price()方法计算
        """
        return self.peRatioGoodPrice

    def get_dividend_good_price(self):
        """
        股息率法好价格, 调用之前，先调用good_price()方法计算
        """
        return self.dividendYieldGoodPrice

    def get_good_price(self):
        """
        两种好价格的最小值
        """
        return self.goodPrice


class GoodPriceResult:
    def __init__(self, stock: Stock, xqDetail: XueQiuDetail, goodPrice, peGood, divideGood):
        self.stock = stock
        self.xqDetail = xqDetail
        self.goodPrice = goodPrice
        self.peGood = peGood
        self.divideGood = divideGood

    def result_msg(self):
        _stock = self.stock
        _name = f'{_stock.read_name()}:{_stock.read_type_code().upper()}'
        _current = f'{self.xqDetail.current:0.2f}'
        _good = f'{self.goodPrice:0.2f}'
        _peGood = f'{self.peGood:0.2f}'
        _divideGood = f'{self.divideGood:0.2f}'
        return f'股票名称：{_name}, 当前价格：{_current}, 好价格:{_good}, 市盈率法好价格：{_peGood}, 股息率法好价格：{_divideGood}'

    def __repr__(self):
        return self.result_msg()

    def is_can_buy_stock(self):
        good = Decimal(str(self.goodPrice))
        current = Decimal(str(self.xqDetail.current))
        return good > 0 and good >= current


class CalculateGoodPrice:

    def start_calculate(self, stock: Stock):
        _stock = stock
        xq = DownloadXueQiuStockDetail(_stock)
        xqDetail = xq.download()
        goodPrice = GoodPriceCalculator(xqDetail=xqDetail, tenYearTreasuryYield=0.01884, averagePriceEarningsRatio=15)
        goodPrice.calculate_good_price()
        return GoodPriceResult(
            stock=_stock,
            xqDetail=xqDetail,
            goodPrice=goodPrice.get_good_price(),
            peGood=goodPrice.get_pe_good_price(),
            divideGood=goodPrice.get_dividend_good_price()
        )


if __name__ == '__main__':
    stockDict = read_stock_csv_no_thread_to_dict_by_name_key()
    calculateGoodPrice = CalculateGoodPrice()
    msgList = []
    for stock in stockDict.values():
        log.info(stock)
        if stock.contains_S_or_T():
            continue
        try:
            goodPriceResult = calculateGoodPrice.start_calculate(stock)
            if goodPriceResult.is_can_buy_stock():
                log.info(goodPriceResult.result_msg())
                msgList.append(goodPriceResult.result_msg())
        except:
            log.error(stock)

    log.info('输出所有结果：')
    for _msg in msgList:
        log.info(_msg)
# _list_stocks = [
#     '美的集团',
#     '同花顺',
#     '海尔智家',
#     '海天味业',
#     '格力电器'
# ]
# stockDict = read_stock_csv_no_thread_to_dict_by_name_key()
# calculateGoodPrice = CalculateGoodPrice()
#
# msgList = []
# for stockName in _list_stocks:
#     log.info(stockDict[stockName])
#     _stock = stockDict[stockName]
#     msg = calculateGoodPrice.start_calculate(_stock)
#     msgList.append(msg)
#
# log.info('输出所有结果：')
# for _msg in msgList:
#     log.info(_msg)
