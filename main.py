from func.tecent.check_days_validate import CheckDaysValid
from func.tecent.update_days import DbDayUpdater

if __name__ == "__main__":
    print('开始运行')
    # days_collect.collect_days()

    # ms_convert_csv_to_db.convert_csv_to_db()
    # check_csv_mysql.check_csv_to_db()
    # DownloadDayOnStock().do_work(Stock(_code='002230', _name='科大讯飞', _type='sz'))
    # DownloadDays().startWorkMainThread()
    # ConvertJsonToCsv().startOneWork()
    # ConvertJsonToCsv().startWork()
    # CheckDaysValid().startWork()
    DbDayUpdater().startWork()
    print('结束运行')
