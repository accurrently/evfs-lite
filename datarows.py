
import datetime


class Row:
    def __init__(self, classname, value, timestamp):
        self.Class = classname
        self.Value = value
        self.Time = datetime.datetime.fromtimestamp(timestamp)

class Bracket:
    def __init__(self, name, minimum, maximum):
        self.Name = name
        self.Min = minimum
        self.Max = maximum

    def contains(self, value):
        if self.Min <= value <= self.Max:
            return True
        return False
