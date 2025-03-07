from mysqls.ms_db import MsDbOperator

class UploadDayCsvToDb:
    def __init__(self, db_name, csv_local_dir):
        self._csv_local_dir = csv_local_dir
        self.msDbOperator = MsDbOperator(db_name=db_name)
        self.msDbOperator.create_db(db_name)

    def _convert_day_list_to_dic(self, list):
        return dict()

    def upload(self, stock):
        pass