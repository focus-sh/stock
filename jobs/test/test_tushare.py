import unittest

import tushare as ts


# https://tushare.pro/document/2?doc_id=14
class TestTushareMethods(unittest.TestCase):

    def setUp(self):
        self.pro = ts.pro_api('133a09e917c71347aabc97a2b0fc3a4837a2487b643537c5d29f75a6')

    def test_ts_version(self):
        version = ts.__version__
        print(version)
        self.assertTrue(version, '1.2.54')

    def test_stock_basic_service(self):
        data = self.pro.stock_basic(exchange='', list_status='L')
        print(data)
        self.assertIsNotNone(data)

    def test_trade_cal_service(self):
        df = self.pro.trade_cal(exchange='')
        print(df)
        self.assertIsNotNone(df)

    def test_name_change_service(self):
        df = self.pro.namechange(ts_code='600848.SH')
        print(df)
        self.assertIsNotNone(df)

    def test_hs_const_service(self):
        # 获取沪股通成分
        df = self.pro.hs_const(hs_type='SH')
        print(df)
        self.assertIsNotNone(df)

        # 获取深股通成分
        df = self.pro.hs_const(hs_type='SZ')
        print(df)
        self.assertIsNotNone(df)

    def test_stock_company_service(self):
        df = self.pro.stock_company(exchange='SZSE',
                                    fields='ts_code,chairman,manager,secretary,reg_capital,setup_date,province')
        print(df)
        self.assertIsNotNone(df)

    def testDailyService(self):
        pro = ts.pro_api('133a09e917c71347aabc97a2b0fc3a4837a2487b643537c5d29f75a6')
        df = pro.daily(ts_code='000001.SZ', start_date='20180701', end_date='20180718')
        print(df)
