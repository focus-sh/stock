
"""
填充函数，根据股票编码和采样日期列表，生成数据矩阵
"""
import logging

import pandas as pd

from lib.executor import executor
from model.stock_mark import stock_mark
from model.ts_pro_bar import ts_pro_bar


def default_filling(ts_code: str, date_list: list) -> pd.DataFrame:
    """
    默认填充函数，依次序返回每个采样日期下的open（开盘价）、high（最高价）、low（最低价）、close（收盘价）、vol（成交量）、amount（成交额）
    这些指标的变量名称分别是x0, x1, x2, x3, x4, x5，y值默认设置为0
    :param ts_code: 股票编码
    :param date_list: 日期列表
    :return: 包含<ts_code, trade_date, x0, x1, x2, x3, x4, x5, y>的DataFrame对象
    """
    stock_data = ts_pro_bar.select_all_valid_record(ts_code)
    result = pd.DataFrame({})
    for _, trade_date in enumerate(date_list):
        stock = stock_data.loc[stock_data['trade_date'] == trade_date].iloc[0]
        sample_row = pd.Series({'ts_code': ts_code, 'trade_date': trade_date})
        sample_row['x0'] = stock['open']
        sample_row['x1'] = stock['high']
        sample_row['x2'] = stock['low']
        sample_row['x3'] = stock['close']
        sample_row['x4'] = stock['vol']
        sample_row['x5'] = stock['amount']
        sample_row['y'] = 0
        result = result.append(sample_row, ignore_index=True)
    return result


def fill_30d_up_50p(ts_code: str, date_list: list) -> (str, pd.DataFrame):
    # 已经是按照日期排序了
    stock_data = ts_pro_bar.select_all_valid_record(ts_code)
    mark_value = stock_mark.select_by_stock_and_type(ts_code, 'd20_max')

    result = pd.DataFrame({})

    key_list = ['open', 'high', 'low', 'close', 'vol', 'amount']
    for index, trade_date in enumerate(date_list):
        logging.info(f'Fill data of stock[{ts_code}] for date[{trade_date}], progress: {index+1}/{len(date_list)}')
        stock_data_slice = get_slice(ts_code, trade_date, stock_data)
        if stock_data_slice is None:
            continue

        sample_row = pd.Series({'ts_code': ts_code, 'trade_date': trade_date})

        # 构建xi值
        x_index = -1
        for _, row in stock_data_slice.iterrows():
            for _, key in enumerate(key_list):
                x_index += 1
                sample_row['x' + str(x_index).zfill(4)] = row[key]
        sample_row['y'] = mark_value[mark_value['trade_date'] == trade_date]['mark'].values[0]
        result = result.append(sample_row, ignore_index=True)

    return ts_code, result


def get_slice(ts_code, trade_date, stock_data) -> pd.DataFrame:
    index = stock_data[stock_data['trade_date'] == trade_date].index.values[0]
    if index < 60:
        logging.warning(f'股票{ts_code}在日期{trade_date}之前，不存在60个交易日数据！')
        return None

    return stock_data.iloc[index-59:index+1, :]


if __name__ == '__main__':
    executor

    data = fill_30d_up_50p('000001.SZ', ['20031230', '20070418'])
    print(data)
