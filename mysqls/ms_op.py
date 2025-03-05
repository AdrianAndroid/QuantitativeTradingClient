import mysql.connector
from mysqls.ms_db import open_db
import log


def once_create_db(db_name):
    if not db_name:
        raise Exception('db_name 不能为空!!!')
    db = open_db(db_name='')
    cursor = db.cursor()
    res = cursor.execute(f'CREATE DATABASE IF NOT EXISTS {db_name}')
    log.info(f'创建数据库 {db_name} 成功! result={res}')
    cursor.close()
    db.close()


def once_query_all_list(db_name: str, table: str, where=''):
    log.info('query all list')
    db = open_db(db_name=db_name)
    if where:
        sql = f"SELECT * FROM {table} where {where}"
    else:
        sql = f"SELECT * FROM {table}"
    log.info(f'query_all_list sql=\'{sql}\'')
    cursor = db.cursor()
    cursor.execute(sql)
    rList = cursor.fetchall()
    log.info(f"{len(rList)} 记录查询成功。")
    cursor.close()
    db.close()
    return rList


# date=date, open_price=open_price, high=high, low=low, close=close, vol=vol
def once_insert_row(db_name: str, table: str, **kwargs):
    list1 = []
    list2 = []
    for key, value in kwargs.items():
        list1.append(str(key))
        list2.append(str(value))
    keys = ",".join(list1)
    values = ",".join(list2)
    sql = f"INSERT INTO {table} ({keys}) VALUES ({values})"
    db = open_db(db_name=db_name)
    cursor = db.cursor()
    cursor.execute(sql)
    log.info(f"{db.rowcount} 记录插入成功。")
    cursor.close()
    db.close()
    db.commit()  # 数据表内容有更新，必须使用到该语句


def _is_list_dict_validate(_list_dict):
    return len(_list_dict) > 0 and isinstance(_list_dict[0], dict) and len(_list_dict[0]) > 0


# [
#   {date=date, open_price=open_price, high=high, low=low, close=close, vol=vol},
#   {date=date, open_price=open_price, high=high, low=low, close=close, vol=vol},
#   {date=date, open_price=open_price, high=high, low=low, close=close, vol=vol},
#   {date=date, open_price=open_price, high=high, low=low, close=close, vol=vol}
# ]
def once_insert_dict(db_name: str, table: str, _list_dict: list):
    if not _is_list_dict_validate():
        log.error(f'once_insert_dict _list_dict 不正确!!!!! _list_dict={_list_dict}')
        return
    list1 = []
    list2 = []
    _ks = _list_dict[0].keys()
    for k in _ks:
        v = _list_dict[k]
        list1.append(str(k))
        list2.append(str('%s'))
    keys = ",".join(_ks)
    values = ",".join(list2)
    sql = f"INSERT INTO {table} ({keys}) VALUES ({values})"
    log.info(f'once_insert_dict dict sql = {sql}')

    listTuple = []
    for itemDict in _list_dict:
        if not isinstance(itemDict):
            raise Exception(f'传入的数据不正确：{itemDict}')
        list3 = []
        for k in _ks:
            list3.append(itemDict[k])
        listTuple.append(tuple(list3))
        list3.clear()
    db = open_db(db_name=db_name)
    cursor = db.cursor()
    cursor.execute(sql, listTuple)
    log.info(f"{cursor.rowcount} 记录插入成功。")
    db.commit()  # 数据表内容有更新，必须使用到该语句
    cursor.close()
    db.close()


def once_update_row(db_name: str, table: str, where: str, **kwargs):
    db = open_db(db_name=db_name)
    list1 = []
    for key, value in kwargs.items():
        list1.append(f'{str(key)}={str(value)}')
    sets = ",".join(list1)
    sql = f"UPDATE {table} SET {sets} WHERE {where}"
    cursor = db.cursor()
    cursor.execute(sql)
    db.commit()
    log.info(f'{cursor.rowcount} 条记录被修改, {cursor.lastrowid}')
    cursor.close()
    db.close()


def once_update_dict(db_name: str, table: str, primary_key: str, _list_dict: list):
    if not _is_list_dict_validate(_list_dict):
        log.error(f'once_insert_dict _list_dict 不正确!!!!! _list_dict={_list_dict}')
        return
    list_s = []  # [name=name, name2=name2, %s]
    _ks = _list_dict[0].keys()
    for k in _ks:
        list_s.append(f'{str(k)}=%s')
    keys_s = ",".join(list_s)
    where = f'{primary_key}=%s'
    sql = f"UPDATE {table} SET {keys_s} WHERE {where}"

    listTuple = []
    for itemDict in _list_dict:
        if not isinstance(itemDict):
            raise Exception(f'once_update_dict 传入的数据不正确：{itemDict}')
        if primary_key not in itemDict:
            raise Exception(f'once_update_dict 不包含主键:{itemDict}')
        list3 = []
        for k in _ks:
            list3.append(itemDict[k])
        list3.append(itemDict[primary_key])
        listTuple.append(tuple(list3))
        list3.clear()
    db = open_db(db_name=db_name)
    cursor = db.cursor()
    cursor.execute(sql, listTuple)
    log.info(f"{cursor.rowcount} 记录插入成功。")
    db.commit()  # 数据表内容有更新，必须使用到该语句
    cursor.close()
    db.close()


def once_drop_table(db_name: str, table):
    db = open_db(db_name=db_name)
    cursor = db.cursor()
    sql = "DROP TABLE IF EXISTS sites"  # 删除数据表 sites
    cursor.execute(sql)
    cursor.close()
    db.close()


def test_kwargs(**kwargs):
    list1 = []
    list2 = []
    for key, value in kwargs.items():
        print(f'{key}:{value}')
        list1.append(str(key))
        list2.append(str(value))
    print(",".join(list1))
    print(",".join(list2))


def _test_sql():
    test_kwargs(name='Alice', age=25, city='New York')

    # for item in query_all_list('sites'):
    #     print(item)

    # for item in query_all_list('sites', where="name LIKE '%INS_%'"):
    #     print(item)


# 查询大数据
def _test_sql_3():
    mydb = mysql.connector.connect(
        host="180.76.52.226",
        user="root",
        passwd="123456",
        database="newdb1"
    )
    mycursor = mydb.cursor()
    mycursor.execute("SELECT * FROM sites")
    myresult = mycursor.fetchall()  # fetchall() 获取所有记录
    print(type(myresult))
    # for x in myresult:
    #     print(type(x), x)
    mycursor.close()
    mydb.close()


# 多次执行sql
def _test_sql_2():
    mydb = mysql.connector.connect(
        host="180.76.52.226",
        user="root",
        passwd="123456",
        database="newdb1"
    )
    mycursor = mydb.cursor()
    # numbers = [i for i in range(10)]
    # for num in numbers:
    #     print(num)
    #     sql = "INSERT INTO sites (name, url) VALUES (%s, %s)"
    #     val = (f"RUNOOB{num}{num}", "https://www.runoob.com")
    #     mycursor.execute(sql, val)
    #     mydb.commit()  # 数据表内容有更新，必须使用到该语句
    #     print(mycursor.rowcount, "记录插入成功。")

    sql = "INSERT INTO sites (name, url) VALUES (%s, %s)"
    val = [
        ('Google', 'https://www.google.com'),
        ('Github', 'https://www.github.com'),
        ('Taobao', 'https://www.taobao.com'),
        ('stackoverflow', 'https://www.stackoverflow.com/')
    ]
    numbers = [i for i in range(10000)]
    for num in numbers:
        print(num)
        val.append((f"{num}INS_{num}", f'https.www.insert-{num}.com'))
    mycursor.executemany(sql, val)
    mydb.commit()  # 数据表内容有更新，必须使用到该语句
    print(mycursor.rowcount, "记录插入成功。")


def _test_sql_1():
    mydb = mysql.connector.connect(
        host="180.76.52.226",
        user="root",
        passwd="123456",
        database="newdb1"
    )
    mycursor = mydb.cursor()

    # mycursor.execute("CREATE TABLE sites (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), url VARCHAR(255))")
    # result = mycursor.execute("CREATE TABLE IF NOT EXISTS sites (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), url VARCHAR(255))")
    # print(result)

    # 插入数据
    # sql = "INSERT INTO sites (name, url) VALUES (%s, %s)"
    # val = ("RUNOOB", "https://www.runoob.com")
    # mycursor.execute(sql, val)
    # mydb.commit()  # 数据表内容有更新，必须使用到该语句
    # print(mycursor.rowcount, "记录插入成功。")

    # 批量插入
    # sql = "INSERT INTO sites (name, url) VALUES (%s, %s)"
    # val = [
    #     ('Google', 'https://www.google.com'),
    #     ('Github', 'https://www.github.com'),
    #     ('Taobao', 'https://www.taobao.com'),
    #     ('stackoverflow', 'https://www.stackoverflow.com/')
    # ]
    # mycursor.executemany(sql, val)
    # mydb.commit()  # 数据表内容有更新，必须使用到该语句
    # print(mycursor.rowcount, "记录插入成功。")

    # 如果我们想在数据记录插入后，获取该记录的 ID ，可以使用以下代码：
    # sql = "INSERT INTO sites (name, url) VALUES (%s, %s)"
    # val = ("Zhihu", "https://www.zhihu.com")
    # mycursor.execute(sql, val)
    # mydb.commit()
    # print("1 条记录已插入, ID:", mycursor.lastrowid)

    # 查询数据
    # mycursor.execute("SELECT * FROM sites")
    # myresult = mycursor.fetchall()  # fetchall() 获取所有记录
    # for x in myresult:
    #     print(type(x), x)

    # 也可以读取指定的字段数据：
    # mycursor.execute("SELECT name, url FROM sites")
    # myresult = mycursor.fetchall()
    # for x in myresult:
    #     print(x)

    # 只想读取一条数据，可以使用 fetchone() 方法：
    # mycursor.execute("SELECT * FROM sites")
    # myresult = mycursor.fetchone()
    # print(myresult)

    # where 条件语句
    # sql = "SELECT * FROM sites WHERE name ='RUNOOB'"
    # mycursor.execute(sql)
    # myresult = mycursor.fetchall()
    # for x in myresult:
    #     print(x)

    # 也可以使用通配符 %：
    # sql = "SELECT * FROM sites WHERE url LIKE '%oo%'"
    # mycursor.execute(sql)
    # myresult = mycursor.fetchall()
    # for x in myresult:
    #     print(x)

    # 为了防止数据库查询发生 SQL 注入的攻击，我们可以使用 %s 占位符来转义查询的条件：
    # sql = "SELECT * FROM sites WHERE name = %s"
    # na = ("RUNOOB",)
    # mycursor.execute(sql, na)
    # myresult = mycursor.fetchall()
    # for x in myresult:
    #     print(x)

    # 排序
    # sql = "SELECT * FROM sites ORDER BY name"
    # mycursor.execute(sql)
    # myresult = mycursor.fetchall()
    # for x in myresult:
    #     print(x)

    # 降序排序实例：
    # sql = "SELECT * FROM sites ORDER BY name DESC"
    # mycursor.execute(sql)
    # myresult = mycursor.fetchall()
    # for x in myresult:
    #     print(x)

    # Limit
    # mycursor.execute("SELECT * FROM sites LIMIT 3")
    # myresult = mycursor.fetchall()
    # for x in myresult:
    #     print(x)

    # 也可以指定起始位置，使用的关键字是OFFSET：
    # mycursor.execute("SELECT * FROM sites LIMIT 3 OFFSET 1")  # 0 为 第一条，1 为第二条，以此类推
    # myresult = mycursor.fetchall()
    # for x in myresult:
    #     print(x)

    # 删除记录
    # sql = "DELETE FROM sites WHERE name = 'stackoverflow'"
    # mycursor.execute(sql)
    # mydb.commit()
    # print(mycursor.rowcount, " 条记录删除")

    # 为了防止数据库查询发生 SQL 注入的攻击，我们可以使用 %s 占位符来转义删除语句的条件：
    # sql = "DELETE FROM sites WHERE name = %s"
    # na = ("stackoverflow",)
    # mycursor.execute(sql, na)
    # mydb.commit()
    # print(mycursor.rowcount, " 条记录删除")

    # 更新表数据
    # sql = "UPDATE sites SET name = 'ZH001' WHERE name = 'ZH'"
    # mycursor.execute(sql)
    # mydb.commit()
    # print(mycursor.rowcount, " 条记录被修改", mycursor.lastrowid)

    # 为了防止数据库查询发生 SQL 注入的攻击，我们可以使用 %s 占位符来转义更新语句的条件：
    # sql = "UPDATE sites SET name = %s WHERE name = %s"
    # val = ("Zhihu", "ZH")
    # mycursor.execute(sql, val)
    # mydb.commit()
    # print(mycursor.rowcount, " 条记录被修改")

    # 删除表
    # sql = "DROP TABLE IF EXISTS sites"  # 删除数据表 sites
    # mycursor.execute(sql)
