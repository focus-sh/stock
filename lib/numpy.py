import heapq

import numpy as np


class Numpy:

    def get_valid_nlargest_mean(self, n, lst):
        nlargest = heapq.nlargest(n, lst)
        return self.get_valid_mean(nlargest)

    def get_valid_nsmallest_mean(self, n, lst):
        nsmallest = heapq.nsmallest(n, lst)
        return self.get_valid_mean(nsmallest)

    def get_valid_mean(self, lst):
        return self.get_valid_val(np.mean(lst))

    def get_valid_ele(self, lst, index):
        if index < 0 or index >= len(lst):
            return 0
        return self.get_valid_val(lst[index])

    @staticmethod
    def get_valid_val(val):
        if val is None or np.isinf(val) or np.isnan(val):
            return 0
        return val

numpy = Numpy()
