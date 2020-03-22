#!/usr/local/bin/python
# -*- coding: utf-8 -*-

import datetime
import logging
import sys
import traceback


class Executor:
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
