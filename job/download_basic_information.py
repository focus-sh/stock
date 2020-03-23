#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
import tushare as ts

from lib.executor import executor
from lib.mysql import mysql
from lib.tushare import tushare as ts


class DownloadDepositRateJob:
    @staticmethod
    def run(date):
        ts.download_data(svc_name='get_deposit_rate', primary_keys=["date", "deposit_type"])


class DownloadLoanRateJob:
    @staticmethod
    def run(date):
        ts.download_data(svc_name='get_loan_rate', primary_keys=["date", "loan_type"])


class DownloadRrrJob:
    @staticmethod
    def run(date):
        ts.download_data(svc_name='get_rrr', primary_keys="date")


class DownloadMoneySupplyJob:
    @staticmethod
    def run(date):
        ts.download_data(svc_name='get_money_supply', primary_keys="month")


class DownloadMoneySupplyBalJob:
    @staticmethod
    def run(date):
        ts.download_data(svc_name='get_money_supply_bal', primary_keys="year")


class DownloadGdpYearJob:
    @staticmethod
    def run(date):
        ts.download_data(svc_name='get_gdp_year', primary_keys="year")

        
class DownloadGdpQuarterJob:
    @staticmethod
    def run(date):
        ts.download_data(svc_name='get_gdp_quarter', primary_keys="quarter")

        
class DownloadGdpForJob:
    @staticmethod
    def run(date):
        ts.download_data(svc_name='get_gdp_for', primary_keys="year")

        
class DownloadGdpPullJob:
    @staticmethod
    def run(date):
        ts.download_data(svc_name='get_gdp_pull', primary_keys="year")


class DownloadGdpContribJob:
    @staticmethod
    def run(date):
        ts.download_data(svc_name='get_gdp_contrib', primary_keys="year")

        
class DownloadCpiJob:
    @staticmethod
    def run(date):
        ts.download_data(svc_name='get_cpi', primary_keys="month")

        
class DownloadPpiJob:
    @staticmethod
    def run(date):
        ts.download_data(svc_name='get_ppi', primary_keys="month")


class DownloadStockBasicsJob:
    @staticmethod
    def run(date):
        ts.download_data(svc_name='get_stock_basics', primary_keys="code")


download_deposit_rate_job = DownloadDepositRateJob()
download_loan_rate_job = DownloadLoanRateJob()
download_rrr_job = DownloadRrrJob()
download_money_supply_job = DownloadMoneySupplyJob()
download_money_supply_bal_job = DownloadMoneySupplyBalJob()
download_gdp_year_job = DownloadGdpYearJob()
download_gdp_quarter_job = DownloadGdpQuarterJob()
download_gdp_for_job = DownloadGdpForJob()
download_gdp_pull_job = DownloadGdpPullJob()
download_gdp_contrib_job = DownloadGdpContribJob()
download_cpi_job = DownloadCpiJob()
download_ppi_job = DownloadPpiJob()
download_stock_basics_job = DownloadStockBasicsJob()


if __name__ == '__main__':
    mysql.create_new_schema_if_necessary()
    executor.run_with_args(download_deposit_rate_job.run)
    executor.run_with_args(download_loan_rate_job.run)
    executor.run_with_args(download_rrr_job.run)
    executor.run_with_args(download_money_supply_job.run)
    executor.run_with_args(download_money_supply_bal_job.run)
    executor.run_with_args(download_gdp_year_job.run)
    executor.run_with_args(download_gdp_quarter_job.run)
    executor.run_with_args(download_gdp_for_job.run)
    executor.run_with_args(download_gdp_pull_job.run)
    executor.run_with_args(download_gdp_contrib_job.run)
    executor.run_with_args(download_cpi_job.run)
    executor.run_with_args(download_ppi_job.run)
    executor.run_with_args(download_stock_basics_job.run)
