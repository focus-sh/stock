#!/usr/local/bin/python
# -*- coding: utf-8 -*-

import datetime
import logging
import os
import sys
import time
import traceback
from lib.environment import environment as env


class Executor:
    def __init__(self):
        self.logger_level = getattr(logging, env.get_env_with_def('LOGGING_LEVEL', 'INFO'))
        self.logger_path = env.get_env_with_def('LOGGING_PATH', f'{env.home()}/logs')

        timestamp = time.strftime('%Y%m%d%H%M', time.localtime(time.time()))
        self.logger_file = f'{self.logger_path}/stock-{timestamp}.log'

        self.logger_format = '%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s'
        self.set_up_logger()

    def set_up_logger(self):
        logger = logging.getLogger()
        logger.setLevel(self.logger_level)
        if not os.path.exists(self.logger_path):
            os.makedirs(self.logger_path)

        file_handler = logging.FileHandler(self.logger_file, mode='w')
        file_handler.setLevel(self.logger_level)

        file_handler.setFormatter(logging.Formatter(self.logger_format))
        logger.addHandler(file_handler)

        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(self.logger_level)
        stream_handler.setFormatter(logging.Formatter(self.logger_format))
        logger.addHandler(stream_handler)

    @staticmethod
    def run_with_args(run_fun):
        logging.info(f'Starting running func[{run_fun}] at {datetime.datetime.now()}')

        if len(sys.argv) == 3:
            tmp_year, tmp_month, tmp_day = sys.argv[1].split("-")
            loop = int(sys.argv[2])
            tmp_datetime = datetime.datetime(int(tmp_year), int(tmp_month), int(tmp_day))
            for i in range(0, loop):
                tmp_datetime_new = tmp_datetime + datetime.timedelta(days=i)
                try:
                    run_fun(tmp_datetime_new)
                except Exception as e:
                    print("error :", e)
                    traceback.print_exc()
        elif len(sys.argv) == 2:
            tmp_year, tmp_month, tmp_day = sys.argv[1].split("-")
            tmp_datetime = datetime.datetime(int(tmp_year), int(tmp_month), int(tmp_day))
            try:
                run_fun(tmp_datetime)
            except Exception as e:
                print("error :", e)
                traceback.print_exc()
        else:
            try:
                run_fun(datetime.datetime.now())  # 使用当前时间
            except Exception as e:
                print("error :", e)
                traceback.print_exc()

        logging.info(f'Finish running func[{run_fun}] at {datetime.datetime.now()}')


executor = Executor()
