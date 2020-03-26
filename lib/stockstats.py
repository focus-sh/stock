import numpy as np
import pandas as pd
import stockstats as ss

from lib.numpy import numpy
from lib.pandas import pandas


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
            # CR indicator, including 5, 10, 20 days moving average
            'cr',
            'cr-ma1',  # default to 5
            'cr-ma2',  # default to 10
            'cr-ma3',  # default to 20
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
        stock = pandas.get_stock_hist_data_cache(code, date)

        stock_name_list = ['code', 'date']
        stock_data_list = [code, date]

        stock_stats = ss.StockDataFrame.retype(stock)

        for col in self.stock_column:
            stock_name_list.append(col)
            val = self.get_stock_stats_val(stock_stats, col)
            stock_data_list.append(val)
        return pd.Series(stock_data_list, index=stock_name_list)

    @staticmethod
    def calculate_wave(code, date, max_point=3):
        stock_name_list = ['date', 'code', 'wave_mean', 'wave_crest', 'wave_base']
        stock_data_list = [date, code]

        stock = pandas.get_stock_hist_data_cache(code, date)
        price_list = pd.Series(stock["close"].values).to_list()
        stock_data_list.append(numpy.get_valid_mean(price_list))

        stock_data_list.append(numpy.get_valid_nlargest_mean(max_point, price_list))
        stock_data_list.append(numpy.get_valid_nsmallest_mean(max_point, price_list))

        return pd.Series(stock_data_list, index=stock_name_list)

    @staticmethod
    def get_stock_stats_val(stock_stats, name):
        values = stock_stats[name].tail(1)
        if values.empty:
            return 0

        return numpy.get_valid_val(values[0])

    @staticmethod
    def calculate_up_rate(wave_mean, trade, wave_crest):
        return wave_mean.sub(trade).div(wave_crest).mul(100)


stockstats = StockStats()
