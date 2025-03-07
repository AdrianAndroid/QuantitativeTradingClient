DAY_HEADER_DATE = 'Date'
DAY_HEADER_OPEN_PRICE = 'Open'
DAY_HEADER_HIGH = 'High'
DAY_HEADER_LOW = 'Low'
DAY_HEADER_CLOSE = 'Close'
DAY_HEADER_VOL = 'Vol'


class Day:
    def __init__(self, date='', open_price='', high='', low='', close='', vol=''):
        self.date = date
        self.open_price = open_price
        self.high = high
        self.low = low
        self.close = close
        self.vol = vol

    def get_date(self):
        return self.date

    def set_date(self, date):
        self.date = date

    def get_open_price(self):
        return self.open_price

    def set_open_price(self, open_price):
        self.open_price = open_price

    def get_high(self):
        return self.high

    def set_high(self, high):
        self.high = high

    def get_low(self):
        return self.low

    def set_low(self, low):
        self.low = low

    def get_close(self):
        return self.close

    def set_close(self, close):
        self.close = close

    def get_vol(self):
        return self.vol

    def set_vol(self, vol):
        self.vol = vol

    def equalsBean(self, _day):
        return (self.date == _day.date and
                self.open_price == _day.open_price and
                self.high == _day.high and
                self.low == _day.low and
                self.close == _day.close and
                self.vol == _day.vol)

    def __repr__(self):
        return (f"Day(date={self.date}, open_price={self.open_price}, "
                f"high={self.high}, low={self.low}, close={self.close}, vol={self.vol})")

    def row_tuple(self):
        return (
            self.get_date(),
            self.get_open_price(),
            self.get_high(),
            self.get_low(),
            self.get_close(),
            self.get_vol()
        )

    def self_day_csv_header(self):
        return day_csv_header()

    def self_day_csv_row(self):
        return day_csv_row(self)

    def self_day_csv_to_day(self, csv_row):
        _date = csv_row[0]
        _open_price = csv_row[1]
        _high = csv_row[2]
        _low = csv_row[3]
        _close = csv_row[4]
        _vol = csv_row[5]
        self.date = _date
        self.open_price = _open_price
        self.high = _high
        self.low = _low
        self.close = _close
        self.vol = _vol

    def self_kline_json_to_day(self, json_day_data):
        _date = json_day_data[0]
        _open_price = json_day_data[1]
        _high = json_day_data[3]
        _low = json_day_data[4]
        _close = json_day_data[2]
        _vol = json_day_data[5]
        self.set_date(_date)
        self.set_open_price(_open_price)
        self.set_high(_high)
        self.set_low(_low)
        self.set_close(_close)
        self.set_vol(_vol)


def day_csv_header():
    return [
        DAY_HEADER_DATE,
        DAY_HEADER_OPEN_PRICE,
        DAY_HEADER_HIGH,
        DAY_HEADER_LOW,
        DAY_HEADER_CLOSE,
        DAY_HEADER_VOL
    ]


def day_csv_row(day):
    return [
        day.get_date(),
        day.get_open_price(),
        day.get_high(),
        day.get_low(),
        day.get_close(),
        day.get_vol()
    ]
