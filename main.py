import functions.tecent.days_collect as days_collect
import mysqls.ms_op
import mysqls.ms_convert_csv_to_db as ms_convert_csv_to_db
from functions.tecent.stocks import StockProcessor

if __name__ == "__main__":
    print('开始运行')
    # days_collect.collect_days()

    ms_convert_csv_to_db.convert_csv_to_db()

    print('结束运行')
