#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

from lib.executor import executor
from lib.mysql import mysql
from lib.tushare import tushare


class DownloadDepositRate:
    @staticmethod
    def run(date):
        tushare.download_data(
            svc_name='get_deposit_rate', 
            primary_keys=["date", "deposit_type"]
        )


class DownloadLoanRate:
    @staticmethod
    def run(date):
        tushare.download_data(
            svc_name='get_loan_rate', 
            primary_keys=["date", "loan_type"]
        )


class DownloadRrr:
    @staticmethod
    def run(date):
        tushare.download_data(
            svc_name='get_rrr', 
            primary_keys="date"
        )


class DownloadMoneySupply:
    @staticmethod
    def run(date):
        tushare.download_data(
            svc_name='get_money_supply', 
            primary_keys="month"
        )


class DownloadMoneySupplyBal:
    @staticmethod
    def run(date):
        tushare.download_data(
            svc_name='get_money_supply_bal', 
            primary_keys="year"
        )


class DownloadGdpYear:
    @staticmethod
    def run(date):
        tushare.download_data(
            svc_name='get_gdp_year', 
            primary_keys="year"
        )

        
class DownloadGdpQuarter:
    @staticmethod
    def run(date):
        tushare.download_data(
            svc_name='get_gdp_quarter', 
            primary_keys="quarter"
        )

        
class DownloadGdpFor:
    @staticmethod
    def run(date):
        tushare.download_data(
            svc_name='get_gdp_for', 
            primary_keys="year"
        )

        
class DownloadGdpPull:
    @staticmethod
    def run(date):
        tushare.download_data(
            svc_name='get_gdp_pull', 
            primary_keys="year"
        )


class DownloadGdpContrib:
    @staticmethod
    def run(date):
        tushare.download_data(
            svc_name='get_gdp_contrib', 
            primary_keys="year"
        )

        
class DownloadCpi:
    @staticmethod
    def run(date):
        tushare.download_data(
            svc_name='get_cpi', 
            primary_keys="month"
        )

        
class DownloadPpi:
    @staticmethod
    def run(date):
        tushare.download_data(
            svc_name='get_ppi', 
            primary_keys="month"
        )


class DownloadStockBasics:
    @staticmethod
    def run(date):
        tushare.download_data(
            svc_name='get_stock_basics', 
            primary_keys="code"
        )


class DownloadStockBasicPro:
    @staticmethod
    def run(date):
        tushare.download_data_pro(
            svc_name='stock_basic',
            params={
                'kwargs': {'exchange': '', 'list_status': 'L'}
            },
            primary_keys="ts_code"
        )


download_deposit_rate = DownloadDepositRate()
download_loan_rate = DownloadLoanRate()
download_rrr = DownloadRrr()
download_money_supply = DownloadMoneySupply()
download_money_supply_bal = DownloadMoneySupplyBal()
download_gdp_year = DownloadGdpYear()
download_gdp_quarter = DownloadGdpQuarter()
download_gdp_for = DownloadGdpFor()
download_gdp_pull = DownloadGdpPull()
download_gdp_contrib = DownloadGdpContrib()
download_cpi = DownloadCpi()
download_ppi = DownloadPpi()
download_stock_basics = DownloadStockBasics()
download_stock_basic_pro = DownloadStockBasicPro()

if __name__ == '__main__':
    mysql.create_new_schema_if_necessary()
    executor.run_with_args(download_deposit_rate.run)
    executor.run_with_args(download_loan_rate.run)
    executor.run_with_args(download_rrr.run)
    executor.run_with_args(download_money_supply.run)
    executor.run_with_args(download_money_supply_bal.run)
    executor.run_with_args(download_gdp_year.run)
    executor.run_with_args(download_gdp_quarter.run)
    executor.run_with_args(download_gdp_for.run)
    executor.run_with_args(download_gdp_pull.run)
    executor.run_with_args(download_gdp_contrib.run)
    executor.run_with_args(download_cpi.run)
    executor.run_with_args(download_ppi.run)
    executor.run_with_args(download_stock_basics.run)

    executor.run_with_args(download_stock_basic_pro.run)
