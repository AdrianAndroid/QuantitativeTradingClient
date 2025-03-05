import csv

import const.const as const
import tools.filetool as filetool
import functions.tecent.day as day
import log


def dict_to_csv(stock, _dict):
    _type = stock.read_type()
    _name = stock.read_name()
    _code = stock.read_code()
    filename = f'{const.TENCENT_DAYS_COLLECT}/{_type}{_code}.csv'
    if filetool.is_file_exits(filename):
        filetool.remove_file(filename)
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(day.day_csv_header())
        log.info(day.day_csv_header())
        for _date in sorted(_dict.keys()):
            log.info(_date)
            _day = _dict[_date]
            _row = day.day_csv_row(_day)
            writer.writerow(_row)
            log.info(_row)
