import pandas as pd
import log
import const.const
from func.tecent.stock import Stock
from queue import Queue, Empty
from threading import Event
from concurrent.futures import ThreadPoolExecutor


# callback(Stock)
def read_stock_csv(callback, num_consumers=200, max_workers=300, queue_size=1000):
    """使用生产者消费者模式读取股票CSV文件
    Args:
        callback: 处理每个Stock对象的回调函数
        num_consumers: 消费者线程数量
        max_workers: 线程池最大工作线程数
        queue_size: 队列大小
    """
    processor = StockProcessor(max_workers=max_workers, queue_size=queue_size)
    processor.set_callback(callback)
    processor.start_consumers(num_consumers)

    try:
        filepath = const.const.TENCENT_STOCKS_FILE
        df = pd.read_csv(filepath, dtype={0: str})
        size = len(df)
        for index, row in df.iterrows():
            _code = row[0]
            _name = row[1]
            _type = row[2]
            processor.add_stock(Stock(_code, _name, _type))
            log.info(f'read_stock_csv剩余个数: {size}')
            size -= 1
    finally:
        log.info(f'processor.stop()')
        processor.stop()


def read_stock_csv_no_thread(callback):
    filepath = const.const.TENCENT_STOCKS_FILE
    df = pd.read_csv(filepath, dtype={0: str})
    size = len(df)
    for index, row in df.iterrows():
        _code = row[0]
        _name = row[1]
        _type = row[2]
        callback(Stock(_code, _name, _type))
        log.info(f'read_stock_csv剩余个数: {size}')
        size -= 1


class StockProcessor:
    def __init__(self, max_workers=4, queue_size=100):
        self.thread_pool = ThreadPoolExecutor(max_workers=max_workers)
        self.stock_queue = Queue(maxsize=queue_size)
        self.stop_event = Event()
        self.consumers = []
        self.callback = None

    def set_callback(self, callback):
        """设置回调函数"""
        self.callback = callback

    def start_consumers(self, num_consumers=2):
        """启动消费者线程"""
        for _ in range(num_consumers):
            future = self.thread_pool.submit(self._consumer_worker)
            self.consumers.append(future)

    def add_stock(self, stock):
        """生产者方法：添加stock到队列"""
        if not self.stop_event.is_set():
            self.stock_queue.put(stock)

    def _consumer_worker(self):
        """消费者工作线程"""
        while not self.stop_event.is_set() or not self.stock_queue.empty():
            try:
                stock = self.stock_queue.get(timeout=1)  # 1秒超时
                if stock:
                    self._process_stock(stock)
                self.stock_queue.task_done()
            except Empty:
                continue
            except Exception as e:
                log.error(f"处理stock时发生错误: {str(e)}")
                # 继续处理下一个，不中断整个处理流程
                continue

    def _process_stock(self, stock):
        """处理单个stock的方法"""
        try:
            if self.callback:
                self.callback(stock)
        except Exception as e:
            log.error(f"处理stock {stock.read_code()} 时发生错误: {str(e)}")

    def stop(self):
        """停止处理并等待所有任务完成"""
        self.stop_event.set()
        self.stock_queue.join()  # 等待所有任务处理完成
        for future in self.consumers:
            future.result()  # 等待所有消费者线程结束
        self.thread_pool.shutdown(wait=True)
