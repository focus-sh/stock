from functools import reduce


class ListUtils:

    @staticmethod
    def as_list(data):
        if not data:
            return []
        if isinstance(data, list):
            return data
        return [data]

    @staticmethod
    def concat_list(lst):
        if isinstance(lst, str):
            return f'`{lst}`'

        return reduce(lambda x, y: f'{x}, `{y}`', lst, '')[2:]


list_utils = ListUtils()

