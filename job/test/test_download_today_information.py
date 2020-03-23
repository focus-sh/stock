import datetime
import unittest

from job.download_today_information import download_top_list


class TestDownloadTodayInformation(unittest.TestCase):
    @unittest.skip
    def test_download_top_list(self):
        download_top_list.run(datetime.date(2020, 3, 20))
