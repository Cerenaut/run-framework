import numpy


class ValueSeries:
    REPEAT_CHAR = "*"

    def __init__(self, series):
        self.idx = 0
        self.overflow = False
        self.series = series

    @classmethod
    def from_range(cls, minv, maxv, deltav):
        series = numpy.arange(minv, maxv, deltav)
        return cls(series)

    def value(self):
        return self.series[self.idx]

    def overflowed(self):
        return self.overflow

    def reset(self):
        self.idx = 0
        self.overflow = False

    def next_val(self):
        """
        Iterate through IF the value exceeds the maximum, then return false
        """

        if not self.overflow:
            next_idx = self.idx + 1

            if next_idx >= len(self.series):
                self.idx = 0
                self.overflow = True
            elif self.series[next_idx] == self.REPEAT_CHAR:
                # don't advance, stay on the last value if
                # it is followed by the repeat char (e.g. '*')
                pass
            else:
                self.idx = next_idx

        return self.overflow
