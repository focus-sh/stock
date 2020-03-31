import logging

from model.model import Model


class TsProBar(Model):

    def __init__(self):
        self.table_name = 'ts_pro_bar'
        self.primary_keys = ["ts_code", "trade_date"]
        self.indexes = ['trade_date']

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

        self.select_ts_code_sql = f'''
SELECT 
    ts_code, trade_date
FROM
    {self.table_name}'''

        self.select_latest_record_where_clause = '''
WHERE
    ts_code = %s
ORDER BY trade_date DESC
LIMIT 1'''

        self.select_all_valid_record_where_clause = '''
WHERE
    ts_code = %s
        AND open IS NOT NULL
ORDER BY trade_date'''

        self.select_valid_record_between_date_where_clause = '''
WHERE
    open IS NOT NULL
        AND trade_date >= %s
        AND trade_date <= %s
ORDER BY trade_date'''

    def select_latest_record(self, ts_code):
        try:
            sql = self.select_sql + self.select_latest_record_where_clause
            return self.select(sql, [ts_code])
        except Exception:
            logging.exception(f'Query table{self.table_name} Failed')
            return None

    def select_all_valid_record(self, ts_code):
        sql = self.select_sql + self.select_all_valid_record_where_clause
        return self.select(sql, [ts_code])

    def select_valid_record_between_date(self, begin_date, end_date):
        sql = self.select_ts_code_sql + self.select_valid_record_between_date_where_clause
        return self.select(sql, [begin_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")])


ts_pro_bar = TsProBar()
