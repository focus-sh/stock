#!/usr/local/bin/python3
# -*- coding: utf-8 -*-


import math

import pandas as pd
from sklearn import linear_model

from lib.executor import executor
from lib.pandas import pandas
from model.stock_sklearn_index import stock_sklearn_index
from model.ts_today_all import ts_today_all


class CalculateStockSklearnIndex:

    def __init__(self):
        self.batch_size = 100

    def run(self, date):
        stock_sklearn_index.delete(date)

        count = ts_today_all.count(date)
        end = int(math.ceil(float(count) / self.batch_size) * self.batch_size)

        for i in range(0, end, self.batch_size):
            data = ts_today_all.paged_select(date, i, self.batch_size)

            statistics = data.apply(
                lambda row: self.linear_regression(
                    code=row['code'],
                    date=row['date'],
                ),
                axis=1
            )
            statistics.drop('date', axis=1, inplace=True)

            statistics = pd.merge(data, statistics, on=['code'], how='left')
            statistics["up_rate"] = (statistics["next_close"] - statistics["trade"]) * 100 / statistics["trade"]

            stock_sklearn_index.insert(statistics)

    def linear_regression(self, code, date):
        stock_name_list = ['code', 'date', 'next_close', 'sklearn_score']

        stock_x = pandas.get_stock_hist_data_cache(code, date)
        if stock_x is None or stock_x.empty:
            return pd.Series([code, date, 0.0, 0.0], index=stock_name_list)

        stock_y = pd.Series(stock_x["close"].values)

        stock_x_next = stock_x.iloc[len(stock_x) - 1]
        stock_x = stock_x.drop(stock_x.index[len(stock_x) - 1])
        stock_y = stock_y.drop(stock_y.index[0])

        stock_x.drop('close', axis=1, inplace=True)
        stock_x_next.drop(labels=['close'], inplace=True)

        model = linear_model.LinearRegression()
        model.fit(stock_x.values, stock_y)

        next_close = model.predict([stock_x_next.values])
        if len(next_close) == 1:
            next_close = next_close[0]

        sklearn_score = model.score(stock_x.values, stock_y)
        return pd.Series([code, date, next_close, sklearn_score * 100], index=stock_name_list)


calculate_stock_sklearn_index = CalculateStockSklearnIndex()

if __name__ == '__main__':
    tmp_datetime = executor.run_with_args(calculate_stock_sklearn_index.run)
