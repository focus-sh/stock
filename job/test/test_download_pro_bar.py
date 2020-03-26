import unittest

from job.download_pro_bar import download_pro_bar


class TestDownloadProBar(unittest.TestCase):

    def test_run(self):
        download_pro_bar.run()
