import json
import os


def is_file_exits(filepath):
    return os.path.exists(filepath) and os.path.isfile(filepath)


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
