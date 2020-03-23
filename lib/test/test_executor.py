import unittest
import logging  # 引入logging模块
import os.path
import time
from lib.environment import environment as env


class TestExecutor(unittest.TestCase):

    def test_logging(self):
        logger = logging.getLogger()
        logger_level = getattr(logging, 'INFO')
        logger.setLevel(logger_level)
        rq = time.strftime('%Y%m%d%H%M', time.localtime(time.time()))
        log_path = env.home() + '/logs/'
        if not os.path.exists(log_path):
            os.makedirs(log_path)

        log_name = log_path + rq + '.log'
        logfile = log_name

        fh = logging.FileHandler(logfile, mode='w')
        fh.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
        fh.setFormatter(formatter)
        logger.addHandler(fh)

        logger.info("hello world")

