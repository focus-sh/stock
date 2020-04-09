from lib.datetime import datetime
from lib.file.tensorflow_slice import file_system
from lib.iterator import Iterator
from model.pro_trade_cal import pro_trade_cal
from model.ts_pro_bar import ts_pro_bar


class ProBarSliceCache:
    def __init__(self):
        self.file_name = 'index.gzip.pickle'

    def refresh_cache(self):
        for row in Iterator(pro_trade_cal):
            date = datetime.str_to_date(row['cal_date'])
            if not file_system.exist(segment=datetime.date_to_str(date), file_name=self.file_name):
                data = ts_pro_bar.select_valid_record_by_date(date=date)
                data.rename(columns={'trade_date': 'date'}, inplace=True)
                file_system.write(data=data, segment=datetime.date_to_str(date), file_name=self.file_name)


pro_bar_slice_cache = ProBarSliceCache()

if __name__ == '__main__':
    pro_bar_slice_cache.refresh_cache()
    data = file_system.read(date=datetime.str_to_date('20200107'), file_name=pro_bar_slice_cache.file_name)
    print(data.head(10))
