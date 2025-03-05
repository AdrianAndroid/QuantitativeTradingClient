import psutil
import os
import threading


def get_process_name():
    return psutil.Process(os.getpid()).name()


def get_thread_name():
    return threading.current_thread().name
