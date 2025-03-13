import time


def timestamp13():
    # 获取当前时间的秒级时间戳
    timestamp_seconds = time.time()
    # 将秒级时间戳转换为毫秒级时间戳并转换为整数
    timestamp_milliseconds = int(timestamp_seconds * 1000)
    return timestamp_milliseconds
