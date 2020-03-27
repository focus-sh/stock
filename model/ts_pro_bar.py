import logging

from MySQLdb._exceptions import ProgrammingError
from lib.pandas import pandas


class TsProBar:

    def __init__(self):
        self.table_name = 'ts_pro_bar'

        self.select_sql = f'''
SELECT 
    `ts_code`,
    `trade_date`,
    `open`,
    `high`,
    `low`,
    `close`,
    `pre_close`,
    `change`,
    `pct_chg`,
    `vol`,
    `amount`
FROM
    {self.table_name}'''

        self.select_latest_record_where_clause = '''
WHERE
    ts_code = %s
ORDER BY trade_date DESC
LIMIT 1'''

    def select_latest_record(self, ts_code):
        try:
            return pandas.create_data_frame_by_sql(
                sql=self.select_sql + self.select_latest_record_where_clause,
                params=[ts_code],
                subset='ts_code',
                keep='last'
            )
        except Exception:
            logging.exception(f'Query table{self.table_name} Failed')
            return None


ts_pro_bar = TsProBar()
