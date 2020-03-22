import datetime

import stockstats as ss

from lib.pandas import pandas
import pandas as pd
import numpy as np


class StockStats:

    def __init__(self):
        self.stock_column = [
            'adx',  # Average Directional Movement Index
            'adxr',  # Smoothed Moving Average of ADX
            # bolling, including upper band and lower band
            'boll',
            'boll_lb',
            'boll_ub',
            # CCI, default to 14 days
            'cci',
            'cci_20',
            # close price change (in percent) between today and yesterday.
            'close_-1_r',
            # close price change (in percent) between today and the day before yesterday.
            'close_-2_r',
            'code',
            # CR indicator, including 5, 10, 20 days moving average
            'cr',
            'cr-ma1',  # default to 5
            'cr-ma2',  # default to 10
            'cr-ma3',  # default to 20
            'date',
            'dma',  # Different of Moving Average (10, 50)
            'dx',  # DX, default to 14 days of +DI and -DI
            # KDJ, default to 9 days
            'kdjd',
            'kdjj',
            'kdjk',
            # moving average convergence divergence. Including signal and histogram.
            'macd',
            'macdh',
            'macds',
            # Directional Moving Index, including
            'mdi',  # -DI, default to 14 days
            'pdi',  # +DI, default to 14 days
            # relative strength index
            'rsi_12',  # 12 days RSI
            'rsi_6',  # 6 days RSI
            # Triple Exponential Moving Average
            'trix',  # TRIX, default to 12 days
            'trix_9_sma',  # MATRIX is the simple moving average of TRIX
            # Volatility Volume Ratio
            'vr',  # VR, default to 26 days
            'vr_6_sma',  # MAVR is the simple moving average of VR
            # Williams Overbought/Oversold index
            'wr_10',  # 10 days WR
            'wr_6',  # 6 days WR
        ]

    def calculate_statistics(self, code, date):
        date_end = datetime.datetime.strptime(date, "%Y%m%d")
        date_start = (date_end + datetime.timedelta(days=-300)).strftime("%Y-%m-%d")
        date_end = date_end.strftime("%Y-%m-%d")

        stock = pandas.get_hist_data_cache(code, date_start, date_end)
        # 设置返回数组。
        stock_data_list = []
        stock_name_list = []
        # 增加空判断，如果是空返回 0 数据。
        if stock is None:
            for col in self.stock_column:
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

        stock_stat = ss.StockDataFrame.retype(stock)

        for col in self.stock_column:
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


stockstats = StockStats()
