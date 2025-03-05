import mysql.connector


def create_table():
    f"CREATE TABLE DayData (Date VARCHAR(255) PRIMARY KEY,Open VARCHAR(255),High VARCHAR(255),Low VARCHAR(255), Close VARCHAR(255), Vol VARCHAR(255));"
    pass
