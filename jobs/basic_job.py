#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
import tushare as ts

from libs.executor import executor
from libs.mysql import mysql


def stat_all(datetime):
    # 存款利率
    data = ts.get_deposit_rate()
    mysql.insert_db(data, "ts_deposit_rate", False, "`date`,`deposit_type`")

    # 贷款利率
    data = ts.get_loan_rate()
    mysql.insert_db(data, "ts_loan_rate", False, "`date`,`loan_type`")

    # 存款准备金率
    data = ts.get_rrr()
    mysql.insert_db(data, "ts_rrr", False, "`date`")

    # 货币供应量
    data = ts.get_money_supply()
    mysql.insert_db(data, "ts_money_supply", False, "`month`")

    # 货币供应量(年底余额)
    data = ts.get_money_supply_bal()
    mysql.insert_db(data, "ts_money_supply_bal", False, "`year`")

    # 国内生产总值(年度)
    data = ts.get_gdp_year()
    mysql.insert_db(data, "ts_gdp_year", False, "`year`")

    # 国内生产总值(季度)
    data = ts.get_gdp_quarter()
    mysql.insert_db(data, "ts_get_gdp_quarter", False, "`quarter`")

    # 三大需求对GDP贡献
    data = ts.get_gdp_for()
    mysql.insert_db(data, "ts_gdp_for", False, "`year`")

    # 三大产业对GDP拉动
    data = ts.get_gdp_pull()
    mysql.insert_db(data, "ts_gdp_pull", False, "`year`")

    # 三大产业贡献率
    data = ts.get_gdp_contrib()
    mysql.insert_db(data, "ts_gdp_contrib", False, "`year`")

    # 居民消费价格指数
    data = ts.get_cpi()
    mysql.insert_db(data, "ts_cpi", False, "`month`")

    # 工业品出厂价格指数
    data = ts.get_ppi()
    mysql.insert_db(data, "ts_ppi", False, "`month`")

    #############################基本面数据 http://tushare.org/fundamental.html
    # 股票列表
    data = ts.get_stock_basics()
    mysql.insert_db(data, "ts_stock_basics", True, "`code`")


# main函数入口
if __name__ == '__main__':
    mysql.create_new_schema_if_necessary()
    executor.run_with_args(stat_all)
