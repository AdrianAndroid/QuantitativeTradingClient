import json
import os


def is_file_exits(filepath):
    return os.path.exists(filepath) and os.path.isfile(filepath)


def is_dic_validate(dict_data):
    return isinstance(dict_data, dict)

