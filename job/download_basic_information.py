#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

from lib.executor import executor
from lib.mysql import mysql
from lib.tushare import tushare


class DownloadDepositRate:
    @staticmethod
    def run():
        tushare.download_data(
            svc_name='get_deposit_rate', 
            primary_keys=["date", "deposit_type"]
        )


class DownloadLoanRate:
    @staticmethod
    def run():
        tushare.download_data(
            svc_name='get_loan_rate', 
            primary_keys=["date", "loan_type"]
        )


class DownloadRrr:
    @staticmethod
    def run():
        tushare.download_data(
            svc_name='get_rrr', 
            primary_keys="date"
        )


class DownloadMoneySupply:
    @staticmethod
    def run():
        tushare.download_data(
            svc_name='get_money_supply', 
            primary_keys="month"
        )


class DownloadMoneySupplyBal:
    @staticmethod
    def run():
        tushare.download_data(
            svc_name='get_money_supply_bal', 
            primary_keys="year"
        )


class DownloadGdpYear:
    @staticmethod
    def run():
        tushare.download_data(
            svc_name='get_gdp_year', 
            primary_keys="year"
        )

        
class DownloadGdpQuarter:
    @staticmethod
    def run():
        tushare.download_data(
            svc_name='get_gdp_quarter', 
            primary_keys="quarter"
        )

        
class DownloadGdpFor:
    @staticmethod
    def run():
        tushare.download_data(
            svc_name='get_gdp_for', 
            primary_keys="year"
        )

        
class DownloadGdpPull:
    @staticmethod
    def run():
        tushare.download_data(
            svc_name='get_gdp_pull', 
            primary_keys="year"
        )


class DownloadGdpContrib:
    @staticmethod
    def run():
        tushare.download_data(
            svc_name='get_gdp_contrib', 
            primary_keys="year"
        )

        
class DownloadCpi:
    @staticmethod
    def run():
        tushare.download_data(
            svc_name='get_cpi', 
            primary_keys="month"
        )

        
class DownloadPpi:
    @staticmethod
    def run():
        tushare.download_data(
            svc_name='get_ppi', 
            primary_keys="month"
        )


class DownloadStockBasics:
    @staticmethod
    def run():
        tushare.download_data(
            svc_name='get_stock_basics', 
            primary_keys="code"
        )


class DownloadStockBasicPro:
    @staticmethod
    def run():
        tushare.download_data_pro(
            svc_name='stock_basic',
            params={
                'kwargs': {'exchange': '', 'list_status': 'L'}
            },
            primary_keys="ts_code"
        )


if __name__ == '__main__':
    mysql.create_new_schema_if_necessary()
    DownloadDepositRate().run()
    DownloadLoanRate().run()
    DownloadRrr().run()
    DownloadMoneySupply().run()
    DownloadMoneySupplyBal().run()
    DownloadGdpYear().run()
    DownloadGdpQuarter().run()
    DownloadGdpFor().run()
    DownloadGdpPull().run()
    DownloadGdpContrib().run()
    DownloadCpi().run()
    DownloadPpi().run()
    DownloadStockBasics().run()

    DownloadStockBasicPro().run()
