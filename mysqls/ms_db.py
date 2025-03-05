import mysql.connector
import log

_HOST = "180.76.52.226"
_USER = "root"
_PASSWORD = "123456"


class MsDbOperator:
    def __init__(self, db_name, table=''):
        if not db_name:
            raise Exception(f'数据库名称不为空!!! db_name={db_name}')
        self.db_name = db_name
        self.table = table

    def _close(self, cursor=None, db=None):
        cursor.close()
        db.close()

    def init_db(self):
        if self.db_name:
            mydb = mysql.connector.connect(
                host=_HOST,
                user=_USER,
                passwd=_PASSWORD,
                database=self.db_name
            )
        else:
            mydb = mysql.connector.connect(
                host=_HOST,
                user=_USER,
                passwd=_PASSWORD
            )
        return mydb

    def create_db(self):
        if not self.db_name:
            raise Exception(f'数据库名称不为空!!! db_name={self.db_name}')
        try:
            db = self.init_db()
            cursor = db.cursor()
            sql = f'CREATE DATABASE IF NOT EXISTS {self.db_name}'
            log.info(f'create_db sql={sql}')
            cursor.execute(sql)
            return True
        except mysql.connector.Error as err:
            log.error(f"创建数据库 {self.db_name} 时出错: {err}")
            return False
        finally:
            self._close(db, cursor)

    # table 表名称
    # fields = list(Date VARCHAR(255) PRIMARY KEY, Open VARCHAR(255), High VARCHAR(255), Low VARCHAR(255), Close VARCHAR(255), Vol VARCHAR(255))
    def create_table(self, table, fields):
        try:
            db = self.init_db()
            cursor = db.cursor()
            _fields = ",".join(fields)
            sql = f'CREATE TABLE IF NOT EXISTS {table}   ({_fields});'
            log.info(f'create_table sql={sql}')
            cursor.execute(sql)
            log.info(f'create_table success.')
            return True
        except mysql.connector.Error as err:
            log.error(f"操作数据库 {self.db_name} 时出错: {err}")
            return False
        finally:
            self._close(db, cursor)

    # process = lambda x: tuple(x.x1, x.x2)
    def insert_list_rows(self, table, listItems, listHeader, process):
        try:
            db = self.init_db()
            cursor = db.cursor()
            keys = ",".join(listHeader)
            values = ",".join(['%s'] * len(listHeader))
            sql = f"INSERT INTO {table} ({keys}) VALUES ({values})"
            log.info(f'insert_list_rows sql={sql}')
            for _item in listItems:
                _tuple = process(_item)
                log.info(f'insert tuple={_tuple}')
                cursor.execute(sql, _tuple)
            db.commit()  # 数据表内容有更新，必须使用到该语句
            log.info(f"{cursor.rowcount} 记录插入成功。")
            return True
        except mysql.connector.Error as err:
            log.error(f"插入List数据 {self.db_name} 时出错: {err}")
            return False
        finally:
            self._close(db, cursor)

    # lisItems
    # listHeader = ['Date', 'xxxx']
    # whereProcess = lambda x: f'Date=x.get_date()'
    # process = lambda x : (x.x1, x.x2)
    def update_list_rows(self, table, listItems, listHeader, whereProcess, process):
        try:
            db = self.init_db()
            cursor = db.cursor()
            _listTemp = []
            for header in listHeader:
                _listTemp.append(f'{header}=%s')
            sets = ','.join(_listTemp)
            for _item in listItems:
                sql = f"UPDATE {table} SET {sets} WHERE {whereProcess(_item)}"
                log.info(f'update_list_rows sql={sql}')
                _tuple = process(_item)
                cursor.execute(sql, _tuple)
            db.commit()  # 数据表内容有更新，必须使用到该语句
            log.info(f"记录修改成功。")
            return True
        except mysql.connector.Error as err:
            log.error(f"插入List数据 {self.db_name} 时出错: {err}")
            return False
        finally:
            self._close(db, cursor)

    # whereProcess=lambda:op
    def query_rows(self, table, whereProcess=None):
        try:
            db = self.init_db()
            cursor = db.cursor()
            if whereProcess:
                _whereProcess = whereProcess()
                sql = f'SELECT * FROM {table} WHERE {_whereProcess}'
            else:
                sql = f'SELECT * FROM {table}'
            log.info(f'query_rows sql={sql}')
            cursor.execute(sql)
            return cursor.fetchall()
        except mysql.connector.Error as err:
            log.error(f"query_rows {self.db_name} 时出错: {err}")
            return list()
        finally:
            self._close(db, cursor)


def open_db(db_name=''):
    if db_name:
        mydb = mysql.connector.connect(
            host=_HOST,
            user=_USER,
            passwd=_PASSWORD,
            database=db_name
        )
    else:
        mydb = mysql.connector.connect(
            host=_HOST,
            user=_USER,
            passwd=_PASSWORD
        )
    # mycursor = mydb.cursor()
    # op(mycursor)
    # mycursor.close()
    # mydb.close()
    return mydb


def create_db(cursor, db_name):
    if not db_name:
        raise Exception(f'数据库名称不为空!!! db_name={db_name}')
    cursor.execute(f'CREATE DATABASE IF NOT EXISTS {db_name}')


def create_table(cursor, sql):
    cursor.execute(sql)

# def drop_table(cursor, table):
#     sql = f"DROP TABLE IF EXISTS {table}"  # 删除数据表 sites
#     cursor.execute(sql)
