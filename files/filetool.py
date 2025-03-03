import os


def is_file_exits(filepath):
    return os.path.exists(filepath) and os.path.isfile(filepath)


def is_dic_validate(dict_data):
    return isinstance(dict_data, dict)


def remove_file(file_path):
    try:
        # 使用 remove() 函数删除文件
        os.remove(file_path)
        print(f"文件 {file_path} 已成功删除。")
    except FileNotFoundError:
        print(f"文件 {file_path} 未找到。")
    except PermissionError:
        print(f"没有权限删除文件 {file_path}。")
    except Exception as e:
        print(f"删除文件 {file_path} 时出现其他错误: {e}")
