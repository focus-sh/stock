import datetime as dt


class Datetime:

    def next_day(self, date_str: str):
        date = self.str_to_date(date_str)
        if date is None:
            return None
        return self.date_to_str(date + dt.timedelta(days=1))

    def max(self, *args):
        if not args:
            return None

        max_date = self.str_to_date('10000101')
        for data_str in args:
            cur_date = self.str_to_date(data_str)
            if cur_date > max_date:
                max_date = cur_date

        return self.date_to_str(max_date)


    @staticmethod
    def str_to_date(date_str: str):
        if not date_str:
            return None

        return dt.datetime.strptime(date_str, "%Y%m%d")

    @staticmethod
    def date_to_str(date: dt):
        if not date:
            return None

        return date.strftime('%Y%m%d')


datetime = Datetime()
