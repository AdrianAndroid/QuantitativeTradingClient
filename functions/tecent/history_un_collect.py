import json

import const.const as const
import files.filetool as filetool
import jsons.jsontool as jsontool


def history_filename(stock):
    _code = stock.read_code()
    _name = stock.read_name()
    _type = stock.read_type()
    return f'{const.TENCENT_HISTORY_YEAR_UN_COLLECT}/{_type}{_code}.json'


def load_json_history_list(history_file_path):
    isFileExits = filetool.is_file_exits(history_file_path)
    if not isFileExits:
        return list()
    if not jsontool.is_json_file_validate(history_file_path):
        print(f'{history_file_path} 不是json文件.')
        return list()
    with open(history_file_path, 'r', encoding='utf-8') as file:
        json_data = json.load(file)
    if 'data' not in json_data:
        return list()
    data = json_data['data']
    if 'history' not in data:
        return list()
    return data['history']
