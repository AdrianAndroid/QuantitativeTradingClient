import os
import json
from threading import Lock
import log


def join_path(*paths):
    return os.path.join(paths)


def is_file_exits(filepath):
    return os.path.exists(filepath) and os.path.isfile(filepath)


def is_dic_validate(dict_data):
    return isinstance(dict_data, dict)


# 创建一个全局锁对象用于保存JSON数据
_json_lock = Lock()


def save_json_data(__filepath, jsonData):
    with _json_lock:
        try:
            with open(__filepath, 'w', encoding='utf-8') as file:
                json.dump(jsonData, file, ensure_ascii=False, indent=4)
            return True
        except FileNotFoundError:
            log.error(f"文件路径 {__filepath} 不存在或无法创建。")
        except PermissionError:
            log.error(f"没有权限写入文件 {__filepath}。")
        except TypeError:
            log.error(f"JSON数据格式错误，无法序列化。")
        except Exception as e:
            log.error(f"保存JSON数据时出现未知错误: {e}")
        return False


def remove_file(file_path):
    try:
        if not is_file_exits(file_path):
            # 使用 remove() 函数删除文件
            os.remove(file_path)
            log.info(f"文件 {file_path} 已成功删除。")
        else:
            log.info(f"文件 {file_path} 已不存在，不用删除。")
    except FileNotFoundError:
        log.info(f"remove_file 文件 {file_path} 未找到。")
        pass
    except PermissionError:
        log.error(f"remove_file 没有权限删除文件 {file_path}。")
    except Exception as e:
        log.error(f"remove_file 删除文件 {file_path} 时出现其他错误: {e}")


# 创建一个全局锁对象
_folder_lock = Lock()


def create_folder_if_not_exists(folder_path):
    """
    检查文件夹是否存在，不存在则递归创建文件夹
    :param folder_path: 要检查和创建的文件夹路径 
    :return: 如果文件夹创建成功或已存在返回 True，创建失败返回 False
    """
    with _folder_lock:
        if not os.path.exists(folder_path):
            try:
                os.makedirs(folder_path)
                log.info(f"文件夹 {folder_path} 创建成功")
                return True
            except FileExistsError:
                return True
            except PermissionError:
                log.error(f"没有权限创建文件夹 {folder_path}")
                return False
            except Exception as e:
                log.error(f"创建文件夹 {folder_path} 时出现未知错误: {e}")
                return False
        else:
            return True


def delete_files_in_folder(folder_path):
    try:
        # 检查文件夹是否存在
        if os.path.exists(folder_path):
            # 遍历文件夹中的所有文件和子文件夹
            for root, dirs, files in os.walk(folder_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    try:
                        # 删除文件
                        os.remove(file_path)
                        log.info(f"已删除文件: {file_path}")
                    except Exception as e:
                        log.error(f"删除文件 {file_path} 时出错: {e}")
        else:
            log.info(f"文件夹 {folder_path} 不存在。")
    except Exception as e:
        log.error(f"处理文件夹 {folder_path} 时出现错误: {e}")
