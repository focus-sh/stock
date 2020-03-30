from lib.pandas import pandas


class ProTradeCal:

    def __init__(self):
        self.table_name = 'pro_trade_cal'

        self.select_sql = f"""
SELECT 
    exchange, cal_date, is_open
FROM
    stock_data.{self.table_name} """

        self.select_open_cal_where_clause = f'''
WHERE
    is_open = 1 AND cal_date >= %s AND cal_date <= %s
ORDER BY cal_date'''

    def select_open_cal(self, begin, end):
        return pandas.create_data_frame_by_sql(
            sql=self.select_sql + self.select_open_cal_where_clause,
            params=[begin.strftime("%Y%m%d"), end.strftime("%Y%m%d")],
            subset=['exchange', 'cal_date'],
            keep='last'
        )

pro_trade_cal = ProTradeCal()
