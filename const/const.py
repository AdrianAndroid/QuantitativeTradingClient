import tools.filetool as filetool

# 工作目录
WORK_PATH = '/Users/zhaojian/code/trading/lday2csv'

# 未整理的日数据
TENCENT_DAYS_UN_COLLECT = f'{WORK_PATH}/tencent/days_un_collect'
# 整理好的日数据
TENCENT_DAYS_COLLECT = f'{WORK_PATH}/tencent/days_collect'
TENCENT_HISTORY_YEAR_UN_COLLECT = f'{WORK_PATH}/tencent/history_year_un_collect'
# 所有的公司股票
TENCENT_STOCKS_FILE = f'{WORK_PATH}/tencent/stocks.csv'

DB_DAY_COLLECT = 'days_collect'

UPDATE_DAY_COLLECT_DIR_JSON = filetool.join_path(WORK_PATH, 'update_days_collect', 'jsons')
UPDATE_DAY_COLLECT_DIR_CSV = filetool.join_path(WORK_PATH, 'update_days_collect', 'csv')

DOWNLOAD_REPORT_PATH = '/Volumes/wd/公司分析'
