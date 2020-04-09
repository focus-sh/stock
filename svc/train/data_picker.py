import random

from pandas import DataFrame

from model.stock_mark import stock_mark
from model.ts_pro_bar import ts_pro_bar


def select_valid_record(date, total) -> DataFrame:
    all_records = ts_pro_bar.select_valid_record_by_date(date)
    select_indexes = random.sample(range(0, all_records.shape[0]), min(all_records.shape[0], total))
    return all_records.iloc[select_indexes].copy()


def select_30d_up_50p_true_record(date, total) -> DataFrame:
    return select_30d_up_50p_record(date=date, total=total, mark=True)


def select_30d_up_50p_false_record(date, total) -> DataFrame:
    return select_30d_up_50p_record(date=date, total=total, mark=False)


def select_30d_up_50p_record(date, total, mark) -> DataFrame:
    all_records = ts_pro_bar.select_valid_record_by_date(date)
    filter_records = stock_mark.select_by_data_and_type_and_mark(
        date=date,
        type='d20_max',
        mark=mark
    )
    valid_records = all_records[all_records['ts_code'].isin(filter_records['ts_code'])]
    select_indexes = random.sample(range(0, valid_records.shape[0]), min(valid_records.shape[0], total))
    return valid_records.iloc[select_indexes].copy().reset_index(drop=True)
