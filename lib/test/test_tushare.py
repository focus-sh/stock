import unittest

from lib.tushare import tushare


class TestTuShare(unittest.TestCase):
    def test_should_drop_duplicates(self):
        self.assertTrue(tushare.should_drop_duplicates(None, None))
        self.assertTrue(tushare.should_drop_duplicates(None, []))
        self.assertFalse(tushare.should_drop_duplicates("code", "code"))
        self.assertFalse(tushare.should_drop_duplicates("code", ["code", "date"]))
        self.assertTrue(tushare.should_drop_duplicates("code", ["id", "man"]))

    def test_call_remote(self):
        data = tushare.call_remote(svc_name='get_cpi')
        self.assertIsNotNone(data)
        self.assertFalse(data.empty)

    def test_call_remote_pro_api(self):
        data = tushare.call_remote(
            api=tushare.pro,
            svc_name='stock_basic',
            params={
                'kwargs': {'exchange': '', 'list_status': 'L'}
            },
        )
        self.assertIsNotNone(data)
        self.assertFalse(data.empty)

    def test_call_pro_bar(self):
        data = tushare.call_remote(
            svc_name='pro_bar',
            params={
                'kwargs': {
                    'ts_code': '000001.SZ',
                    'adj': 'qfq',
                    'start_date': '19910403'
                }
            },
        )
        self.assertIsNotNone(data)
        self.assertFalse(data.empty)
