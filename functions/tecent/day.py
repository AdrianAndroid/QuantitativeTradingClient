DAY_HEADER_DATE = 'Date'
DAY_HEADER_OPEN_PRICE = 'Open'
DAY_HEADER_HIGH = 'High'
DAY_HEADER_LOW = 'Low'
DAY_HEADER_CLOSE = 'Close'
DAY_HEADER_VOL = 'Vol'


class Day:
    def __init__(self, date, open_price, high, low, close, vol):
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
