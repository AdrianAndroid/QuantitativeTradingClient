import json

import tools.filetool as _filetool


def is_json_file_validate(filepath):
    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            content = file.read()
            data = json.loads(content)
            if isinstance(data, (list, dict)) and len(data) == 0:
                return False
            return True
    except json.JSONDecodeError:
        return False
    except FileNotFoundError:
        print(f"文件 {filepath} 未找到。")
        return False


def is_json_validate(json_data):
    try:
        return isinstance(json_data, (list, dict)) and len(json_data) == 0
    except json.JSONDecodeError:
        return False


def read_json_file(filename):
    if not is_json_file_validate(filename):
        return None
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            json_data = json.load(file)
    except FileNotFoundError:
        return None
    except json.decoder.JSONDecodeError:
        return None
    return json_data
