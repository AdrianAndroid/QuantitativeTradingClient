import pprint


def d(msg):
    print(msg)


def log(*args):
    print(args)


def log_dict(_dict):
    pprint.pprint(_dict)


class Colors:
    DEBUG = '\033[96m'  # 青色
    INFO = '\033[92m'  # 绿色
    WARNING = '\033[93m'  # 黄色
    ERROR = '\033[91m'  # 红色
    CRITICAL = '\033[1;31;47m'  # 红色文字，白色背景
    END = '\033[0m'


def debug(message):
    print(f"{Colors.DEBUG}{message}{Colors.END}")


def info(message):
    print(f"{Colors.INFO}{message}{Colors.END}")


def warning(message):
    print(f"{Colors.WARNING}{message}{Colors.END}")


def error(message):
    print(f"{Colors.ERROR}{message}{Colors.END}")


def critical(message):
    print(f"{Colors.CRITICAL}{message}{Colors.END}")


# 测试不同级别的日志输出
# debug('This is a debug message')
# info('This is an info message')
# warning('This is a warning message')
# error('This is an error message')
# critical('This is a critical message')
