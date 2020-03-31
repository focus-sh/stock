class Distribution:
    def calculate_distribute(self, x_max, y_total):
        distribute = []
        remain = 0.0
        for x in range(0, x_max+1):
            probability = self.probability(x_max, x)
            cnt_float = y_total * probability + remain
            distribute.append(int(cnt_float))
            remain = cnt_float - distribute[x]

        return distribute


class UniformDistribution(Distribution):
    """
    均值分布
    """
    @staticmethod
    def probability(x_max, x):
        return 1 / (x_max + 1)


uniform_distribution = UniformDistribution()