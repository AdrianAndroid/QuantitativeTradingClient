import func.tecent.days_collect as days_collect
import mysqls.ms_op
import mysqls.ms_convert_csv_to_db as ms_convert_csv_to_db
from func.tecent.stocks import StockProcessor
import mysqls.check_csv_mysql as check_csv_mysql
from func.tecent.download_day import DownloadDayOnStock
from func.tecent.download_day import DownloadDays
from func.tecent.download_day import ConvertJsonToCsv
from func.tecent.stock import Stock
from func.tecent.check_days_validate import CheckDaysValid

if __name__ == "__main__":
    print('开始运行')
    # days_collect.collect_days()

    # ms_convert_csv_to_db.convert_csv_to_db()
    # check_csv_mysql.check_csv_to_db()
    # DownloadDayOnStock().do_work(Stock(_code='002230', _name='科大讯飞', _type='sz'))
    # DownloadDays().startWorkMainThread()
    # ConvertJsonToCsv().startOneWork()
    # ConvertJsonToCsv().startWork()
    CheckDaysValid().startWork()
    print('结束运行')
