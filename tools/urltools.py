import socket  # Import socket to handle timeout exceptions
import time
from urllib import request
from urllib.error import URLError, HTTPError
import log
import requests


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


def download_pdf(url, save_path):
    try:
        # 发送 HTTP 请求获取 PDF 文件内容
        response = requests.get(url, stream=True)
        # 检查响应状态码，200 表示请求成功
        response.raise_for_status()

        # 以二进制写入模式打开文件
        with open(save_path, 'wb') as file:
            # 遍历响应内容的每个数据块
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    # 将数据块写入文件
                    file.write(chunk)
        print(f"PDF 文件下载成功，保存路径为: {save_path}")
    except requests.exceptions.RequestException as e:
        print(f"请求出错: {e}")
    except Exception as e:
        print(f"下载过程中出现错误: {e}")


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
