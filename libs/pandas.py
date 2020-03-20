import logging

import pandas as pd

from libs.mysql import engine


class Pandas:

    @staticmethod
    def create_data_frame_by_sql(sql, params, subset, keep):
        logging.info(f"Create DataFrame with sql[{sql}] and params[{params}]")
        data = pd.read_sql(sql=sql, con=engine(), params=params)
        logging.info(f"Drop duplicate records in DataFrame by subset=[{subset}] and keep[{keep}]")
        return data.drop_duplicates(subset, keep)
