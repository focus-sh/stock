#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
import tushare as ts

from libs.executor import executor
from libs.mysql import mysql
from libs.tushare import tushare as ts


class BasicJob:

    @staticmethod
    def stat_all(datetime):
        ts.download_data(
            svc_name='get_deposit_rate',
            table_name="ts_deposit_rate",
            primary_keys="`date`,`deposit_type`"
        )

        ts.download_data(
            svc_name='get_loan_rate',
            table_name="ts_loan_rate",
            primary_keys="`date`,`loan_type`"
        )

        ts.download_data(
            svc_name='get_rrr',
            table_name="ts_rrr",
            primary_keys="`date`"
        )

        ts.download_data(
            svc_name='get_money_supply',
            table_name="ts_money_supply",
            primary_keys="`month`"
        )

        ts.download_data(
            svc_name='get_money_supply_bal',
            table_name="ts_money_supply_bal",
            primary_keys="`year`")

        ts.download_data(
            svc_name='get_gdp_year',
            table_name="ts_gdp_year",
            primary_keys="`year`"
        )

        ts.download_data(
            svc_name='get_gdp_quarter',
            table_name="ts_get_gdp_quarter",
            primary_keys="`quarter`"
        )

        ts.download_data(
            svc_name='get_gdp_for',
            table_name="ts_gdp_for",
            primary_keys="`year`"
        )

        ts.download_data(
            svc_name='get_gdp_pull',
            table_name="ts_gdp_pull",
            primary_keys="`year`"
        )

        ts.download_data(
            svc_name='get_gdp_contrib',
            table_name="ts_gdp_contrib",
            primary_keys="`year`"
        )

        ts.download_data(
            svc_name='get_cpi',
            table_name="ts_cpi",
            primary_keys="`month`"
        )

        ts.download_data(
            svc_name='get_ppi',
            table_name="ts_ppi",
            primary_keys="`month`"
        )

        ts.download_data(
            svc_name='get_stock_basics',
            table_name="ts_stock_basics",
            primary_keys="`code`",
            write_index=True
        )


# main函数入口
if __name__ == '__main__':
    mysql.create_new_schema_if_necessary()
    executor.run_with_args(BasicJob().stat_all)
