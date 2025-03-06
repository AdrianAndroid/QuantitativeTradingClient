import os
import time
from datetime import datetime, timedelta
import pandas as pd
import log
from mysqls.ms_db import MsDbOperator
from func.tecent.stocks import read_stock_csv
from func.tecent.stock import Stock


class StockUpdater:
    def __init__(self):
        self.db_operator = MsDbOperator("stock_data")
        self.fields = [
            "Date VARCHAR(255) PRIMARY KEY",
            "Open DECIMAL(10,2)",
            "High DECIMAL(10,2)",
            "Low DECIMAL(10,2)",
            "Close DECIMAL(10,2)",
            "Volume BIGINT",
            "Amount DECIMAL(16,2)"
        ]

    def _get_table_name(self, stock: Stock):
        """生成股票对应的表名"""
        _code = stock.read_code()
        _type = stock.read_type()
        _name = stock.read_name()
        return f"{_type}{_code}"

    def _download_stock_data(self, stock: Stock):
        """下载指定股票的历史数据"""
        try:
            # 这里可以根据实际情况调用腾讯或其他数据源的API
            # 示例使用pandas_datareader (需要自行实现具体的下载逻辑)
            log.info(f"Downloading data for {stock.code} - {stock.name}")
            # TODO: 实现实际的数据下载逻辑
            pass
        except Exception as e:
            log.error(f"Error downloading data for {stock.code}: {str(e)}")
            return None

    def _process_data(self, df):
        """处理下载的数据，进行清洗和格式化"""
        if df is None or df.empty:
            return None

        # 数据清洗和格式化
        df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
        # 确保所有数值列都是数值类型
        numeric_columns = ['Open', 'High', 'Low', 'Close', 'Volume', 'Amount']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        return df

    def update_single_stock(self, stock: Stock):
        """更新单个股票的数据"""
        table_name = self._get_table_name(stock)

        # 创建表（如果不存在）
        self.db_operator.create_table(table_name, self.fields)

        # 获取最新数据
        latest_data = self._download_stock_data(stock)
        if latest_data is None:
            return False

        # 处理数据
        processed_data = self._process_data(latest_data)
        if processed_data is None:
            return False

        # 更新数据库
        try:
            # 转换为数据库可接受的格式
            data_tuples = [tuple(x) for x in processed_data.values]
            headers = list(processed_data.columns)

            # 插入新数据
            self.db_operator.insert_list_rows(
                table_name,
                data_tuples,
                headers,
                lambda x: x
            )
            return True
        except Exception as e:
            log.error(f"Error updating {stock.code}: {str(e)}")
            return False

    def update_all_stocks(self):
        """更新所有股票的数据"""
        success_count = 0
        fail_count = 0

        def update_callback(stock: Stock):
            nonlocal success_count, fail_count
            log.info(f"Updating {stock.code} - {stock.name}")
            if self.update_single_stock(stock):
                success_count += 1
            else:
                fail_count += 1
            # 添加延时避免请求过快
            time.sleep(1)

        # 读取股票列表并更新
        read_stock_csv(update_callback)

        log.info(f"Update completed. Success: {success_count}, Failed: {fail_count}")
        return success_count, fail_count

    def filter_stocks(self, conditions):
        """根据条件筛选股票
        conditions: 筛选条件的字典，例如：
        {
            'price_range': (10, 50),  # 价格区间
            'volume_min': 1000000,    # 最小成交量
            'ma_cross': True,         # 均线交叉
        }
        """
        filtered_stocks = []

        def filter_callback(stock: Stock):
            table_name = self._get_table_name(stock)
            # 获取该股票的历史数据
            data = self.db_operator.query_rows(table_name)
            if not data:
                return

            # 转换为DataFrame进行分析
            df = pd.DataFrame(data, columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Amount'])

            # 应用筛选条件
            if self._apply_filters(df, conditions):
                filtered_stocks.append(stock)

        read_stock_csv(filter_callback)
        return filtered_stocks

    def _apply_filters(self, df, conditions):
        """应用筛选条件"""
        if df.empty:
            return False

        latest = df.iloc[-1]

        # 价格区间过滤
        if 'price_range' in conditions:
            min_price, max_price = conditions['price_range']
            if not (min_price <= latest['Close'] <= max_price):
                return False

        # 成交量过滤
        if 'volume_min' in conditions:
            if latest['Volume'] < conditions['volume_min']:
                return False

        # 均线交叉判断
        if 'ma_cross' in conditions and conditions['ma_cross']:
            df['MA5'] = df['Close'].rolling(window=5).mean()
            df['MA10'] = df['Close'].rolling(window=10).mean()
            if len(df) < 10:
                return False
            # 判断5日均线是否上穿10日均线
            last_two = df.iloc[-2:]
            if last_two['MA5'].iloc[0] <= last_two['MA10'].iloc[0] and \
                    last_two['MA5'].iloc[1] > last_two['MA10'].iloc[1]:
                return True
            return False

        return True


def update_stock_data():
    """主更新函数"""
    updater = StockUpdater()
    success, failed = updater.update_all_stocks()
    return success, failed


def filter_stock_data(conditions):
    """主筛选函数"""
    updater = StockUpdater()
    return updater.filter_stocks(conditions)


if __name__ == "__main__":
    # 更新所有股票数据
    success, failed = update_stock_data()
    log.info(f"Updated stocks - Success: {success}, Failed: {failed}")

    # 示例：筛选股票
    conditions = {
        'price_range': (10, 50),
        'volume_min': 1000000,
        'ma_cross': True
    }
    filtered_stocks = filter_stock_data(conditions)
    for stock in filtered_stocks:
        log.info(f"Filtered stock: {stock.code} - {stock.name}")
