import socket  # Import socket to handle timeout exceptions
import time
from urllib import request
from urllib.error import URLError, HTTPError
import log


def read_url(url, _time=3):
    try:
        resp = request.urlopen(url, timeout=10)
        data = resp.read()
        decode_data = data.decode('utf-8')
    except HTTPError as e:
        return _read_url_retry(url, _time, f"HTTP 错误: {e.code}, URL: {url}")
    except URLError as e:
        return _read_url_retry(url, _time, f"URL 错误: {e.reason}, URL: {url}")
    except socket.timeout as e:
        return _read_url_retry(url, _time, f"请求超时:{e}, URL:{url}")
    except Exception as e:
        return _read_url_retry(url, _time, f"出现错误:{e}, URL:{url}")
    return decode_data


def _read_url_retry(url, _time, msg):
    if _time > 0:
        log.error(f'重试次数:{_time}, {msg}, url={url}')
        if _time > 2:
            time.sleep(5)
        elif _time > 1:
            time.sleep(10)
        else:
            time.sleep(20)
        return read_url(url, _time - 1)
    else:
        return None
