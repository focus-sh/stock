import datetime
import time
import unittest

from job.download_today_information import download_top_list, del_his_cache_data, download_index, download_today_all


class TestDownloadTodayInformation(unittest.TestCase):
    @unittest.skip
    def test_download_today_information(self):
        del_his_cache_data.run(datetime.date(2019, 2, 11))
        download_top_list.run(datetime.date(2019, 2, 11))
        download_index.run(datetime.date(2019, 2, 11))
        time.sleep(5)  # 停止5秒
        download_today_all.run(datetime.date(2019, 2, 11))
