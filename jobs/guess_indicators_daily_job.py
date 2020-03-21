import datetime
import logging
import math

import numpy as np
import pandas as pd
import stockstats

from libs.executor import executor
from libs.mysql import mysql
from libs.pandas import pandas

batch_size = 100

stock_column = ['adx', 'adxr', 'boll', 'boll_lb', 'boll_ub', 'cci', 'cci_20', 'close_-1_r',
                'close_-2_r', 'code', 'cr', 'cr-ma1', 'cr-ma2', 'cr-ma3', 'date', 'dma', 'dx',
                'kdjd', 'kdjj', 'kdjk', 'macd', 'macdh', 'macds', 'mdi', 'pdi',
                'rsi_12', 'rsi_6', 'trix', 'trix_9_sma', 'vr', 'vr_6_sma', 'wr_10', 'wr_6']


def stat_all_lite(date):
    sql_1 = """
            SELECT `date`, `code`, `name`, `changepercent`, `trade`, `open`, `high`, `low`, 
                            `settlement`, `volume`, `turnoverratio`, `amount`, `per`, `pb`, `mktcap`,
                             `nmc` ,`kdjj`,`rsi_6`,`cci`
                        FROM stock_data.guess_indicators_daily WHERE `date` = %s 
                        and kdjk >= 80 and kdjd >= 70 and kdjj >= 90  and rsi_6 >= 50  and cci >= 100
    """

    try:
        del_sql = " DELETE FROM `stock_data`.`guess_indicators_lite_daily` WHERE `date`= '%s' " % date.strftime("%Y%m%d")
        mysql.insert(del_sql)
    except Exception as e:
        print("error :", e)

    try:
        data = pd.read_sql(sql=sql_1, con=mysql.engine(), params=[date.strftime("%Y%m%d")])
        data = data.drop_duplicates(subset="code", keep="last")
    except Exception as e:
        print('error :', e)

    try:
        mysql.insert_db(data, "guess_indicators_lite_daily", "`date`,`code`", False)
    except Exception as e:
        print("error :", e)


# 批处理数据。
def stat_all_batch(date):
    mysql.del_by_date('guess_indicators_daily', date.strftime("%Y%m%d"))

    count = mysql.count_with_where_clause(
        'ts_today_all',
        params=[(date.strftime("%Y%m%d")), '300%', '%st%'],
        clause="`date` = %s and `trade` > 0 and `open` > 0 "
               "and trade <= 20 and `code` not like %s "
               "and `name` not like %s"
    )
    end = int(math.ceil(float(count) / batch_size) * batch_size)

    for i in range(0, end, batch_size):
        data = pandas.create_data_frame_by_sql(sql="""
                SELECT `date`, `code`, `name`, `changepercent`, `trade`, `open`, `high`, `low`, 
                  `settlement`, `volume`, `turnoverratio`, `amount`, `per`, `pb`, `mktcap`, `nmc` 
                FROM stock_data.ts_today_all WHERE `date` = %s and `trade` > 0 and `open` > 0 and trade <= 20 
                  and `code` not like %s and `name` not like %s limit %s , %s
                """, params=[(date.strftime("%Y%m%d")), '300%', '%st%', i, batch_size], subset='code', keep='last')

        stat_index_all(data)


def stat_index_all(data):
    try:
        mysql.insert_db(data=concat_guess_data(data).round(2), table_name="guess_indicators_daily",
                        primary_keys="`date`,`code`", write_index=False)
    except Exception:
        logging.exception(f'Executing function[stat_index_all] failed')


# 链接guess 数据。
def concat_guess_data(data):
    tmp_dic = {}
    for col in stock_column:
        if col == 'date':
            tmp_dic[col] = data["date"]
        elif col == 'code':
            tmp_dic[col] = data["code"]
        else:
            tmp_dic[col] = data["trade"]
    stock_guess = pd.DataFrame(tmp_dic, index=data.index.values).apply(apply_guess, axis=1)
    stock_guess.drop('date', axis=1, inplace=True)

    return pd.merge(data, stock_guess, on=['code'], how='left')


# 带参数透传。
def apply_guess(tmp):
    date = tmp["date"]
    code = tmp["code"]
    date_end = datetime.datetime.strptime(date, "%Y%m%d")
    date_start = (date_end + datetime.timedelta(days=-300)).strftime("%Y-%m-%d")
    date_end = date_end.strftime("%Y-%m-%d")

    stock = pandas.get_hist_data_cache(code, date_start, date_end)
    # 设置返回数组。
    stock_data_list = []
    stock_name_list = []
    # 增加空判断，如果是空返回 0 数据。
    if stock is None:
        for col in stock_column:
            if col == 'date':
                stock_data_list.append(date)
                stock_name_list.append('date')
            elif col == 'code':
                stock_data_list.append(code)
                stock_name_list.append('code')
            else:
                stock_data_list.append(0)
                stock_name_list.append(col)
        return pd.Series(stock_data_list, index=stock_name_list)

    stock = stock.sort_index(0)  # 将数据按照日期排序下。

    stock["date"] = stock.index.values  # 增加日期列。
    stock = stock.sort_index(0)  # 将数据按照日期排序下。

    stock_stat = stockstats.StockDataFrame.retype(stock)

    for col in stock_column:
        if col == 'date':
            stock_data_list.append(date)
            stock_name_list.append('date')
        elif col == 'code':
            stock_data_list.append(code)
            stock_name_list.append('code')
        else:
            # 将数据的最后一个返回。
            tmp_val = stock_stat[col].tail(1).values[0]
            if np.isinf(tmp_val):  # 解决值中存在INF问题。
                tmp_val = 0
            if np.isnan(tmp_val):  # 解决值中存在NaN问题。
                tmp_val = 0
            stock_data_list.append(tmp_val)
            stock_name_list.append(col)
    return pd.Series(stock_data_list, index=stock_name_list)


if __name__ == '__main__':
    executor.run_with_args(stat_all_batch)
    executor.run_with_args(stat_all_lite)
