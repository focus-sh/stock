import logging
import math

from model.pro_stock_basic import pro_stock_basic


class ProStockBasicIterator:
    def __init__(self):
        self.batch_size = 100

    def run(self, date):
        logging.info(f'Starting iterate each of pro_stock_basic data...')
        count = pro_stock_basic.count()
        end = int(math.ceil(float(count) / self.batch_size) * self.batch_size)
        for i in range(0, end, self.batch_size):
            data = pro_stock_basic.paged_select(i, self.batch_size)
            for _, row in data.iterrows():
                self.do_service(date=date, row=row)
        logging.info(f'Starting iterate all of pro_stock_basic data...')
