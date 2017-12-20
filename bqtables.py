import settings
import datetime
import re
from utils import load_json_list
from enum import Enum
from apache_beam.io.gcp.internal.clients import bigquery

def bq_table(table_dict_array):
    tbl = bigquery.TableSchema()
    for d in table_dict_array:
        fs = bigquery.TableFieldSchema()
        if 'name' in d.keys():
            fs.name = d['name']
        if 'type' in d.keys():
            fs.type = d['type']
        tbl.fields.append(fs)
    return tbl

class EVBQT:
    """
    EV Big Query Table base class
    """
    # schema = bigquery.TableSchema()
    # table_name = 'Base'

    def __init__(self, name, table = [{}], signal_strings = {}):
        self.name = name
        self.schema = table
        self.signal_strings = signal_strings

    def full_table_name(self):
        return "{a}.{b}".format(a = DATASET_NAME, b = self.name)

    def bq_schema(self):
        return bq_table(self.schema)

    def merge_signals(signal_dict):
        self.signal_strings.update(signal_dict)
        return self

    def set_signals(signal_dict):
        self.signal_strings = signal_dict
        return self

    def add_signal_string(s):
        d = {s}
        self.signal_strings.update(d)
        return self

    def parse(self, input_str):
        return input_str

    # Checks is val is a valid value for the table
    def validate(self, val):
        return True

    # In the base class, this method oes nothing
    def convert(self, val):
        return val

    def sql_convert(self, val):
        return "{value}".format(value = val)

class StringTable(EVBQT):
    def __init__(self, name, table = [{}], signal_strings = {}):
        super().__init__(name, table, signal_strings = signal_strings)
        self.value_type = ValueType.String

    def validate(self, val):
        if isinstance(val, str):
            return bool(re.match('^[A-Za-z0-9\_\ \-\+\.\,]+$', val))
        return False

    def convert(self, val):
        return str(val)

    def sql_convert(self, val):
        return "\"{value}\"".format(value = str(val))

class EnumTable(EVBQT):

    def __init__(self, name, enum_vals = '', table = [], signal_strings = {}):
        tbl = [
            {'name': 'VehicleID', 'type': 'string'},
            {'name': 'EventTime', 'type': 'timestamp'},
            {'name': 'Value', 'type': 'integer'}
        ]
        if len(table) > 0:
            tbl = table
        super().__init__(name, tbl, signal_strings)
        self.Values = IntEnum(name, enum_vals)

    def parse(self, input_str):
        if input_str in self.Values.__members__:
            return self.Values[input_str]
        return 0

    def validate(self, val):
        if isinstance(val, str):
            if val in self.Values.__members__:
                return True
        elif isinstance(val, int):
             return (any(val == item.value for item in self.Values))
        elif isinstance(val, Enum) or isinstance(val, IntEnum):
            return (any(val.value == item.value for item in self.Values))
        return False

    def convert(self, val):
        if isinstance(val, str):
            return self.lookup_enum(val)
        return int(val)

    def sql_convert(self, val):
        return "{value}".format(value = int(val))

class NumberTable(EVBQT):
    def __init__(self, name, table = [], signal_strings = {}, range_min = None, range_max = None):
        tbl = [
            {'name': 'VehicleID', 'type': 'string'},
            {'name': 'EventTime', 'type': 'timestamp'},
            {'name': 'Value', 'type': 'integer'}
        ]
        if len(table) > 0:
            tbl = table
        super().__init__(name, tbl, signal_strings)
        self.max = range_max
        self.min = range_min

class IntegerTable(NumberTable):

    def __init__(self, name, table = [], signal_strings = {}, range_min = None, range_max = None):
        tbl = [
            {'name': 'VehicleID', 'type': 'string'},
            {'name': 'EventTime', 'type': 'timestamp'},
            {'name': 'Value', 'type': 'integer'}
        ]
        if len(table) > 0:
            tbl = table
        super().__init__(name, tbl, signal_strings, range_min = range_min, range_max = range_max)

    def validate(self, val):
        if isinstance(val, int):
            if (self.max is None) and (self.min is None):
                return True
            elif (self.max is None) and (val >= self.min):
                return True
            elif (self.min is None) and (val <= self.max):
                return True
            elif self.min <= val <= self.max:
                return True
        return False

    def convert(self, val):
        return int(val)

    def sql_convert(self, val):
        return "{value}".format(value = int(val))

class FloatTable(NumberTable):

    def __init__(self, name, table = [], signal_strings = {}, range_min = None, range_max = None):
        tbl = [
            {'name': 'VehicleID', 'type': 'string'},
            {'name': 'EventTime', 'type': 'timestamp'},
            {'name': 'Value', 'type': 'float'}
        ]
        if len(table) > 0:
            tbl = table
        super().__init__(name, tbl, signal_strings, range_min = range_min, range_max = range_max)

    def validate(self, val):
        if isinstance(val, float):
            if (self.max is None) and (self.min is None):
                return True
            elif (self.max is None) and (val >= self.min):
                return True
            elif (self.min is None) and (val <= self.max):
                return True
            elif self.min <= val <= self.max:
                return True
        return False

    def convert(self, val):
        return float(val)

    def sql_convert(self, val):
        return "{value}".format(value = float(val))

class BooleanTable(EVBQT):

    def __init__(self, name, table = [], signal_strings = {}, true_strings = {}, false_strings = {}, default = False):
        tbl = [
            {'name': 'VehicleID', 'type': 'string'},
            {'name': 'EventTime', 'type': 'timestamp'},
            {'name': 'Value', 'type': 'boolean'}
        ]
        if len(table) > 0:
            tbl = table
        super().__init__(name, tbl, signal_strings)
        self.trues = true_strings
        self.falses = false_strings
        self.default = default

    def validate(self, val):
        return isinstance(val, bool):

    def parse(self, input_str):
        if input_str in self.trues:
            return True
        elif input_str in self.falses:
            return False
        return self.default

    def convert(self, val):
        return bool(val)

    def sql_convert(self, val):
        return "{value}".format(value = str(bool(val)).upper())

class CustomTable(EVBQT):

    def __init__(self, name, table = [], signal_strings = {}, parse_func = None, convert_func = None, sql_convert_func = None, valid_func = None):
        super().__init__(name, table, signal_strings)
        self.pf = super().parse
        self.cf = super().convert
        self.scf = super().sql_convert
        self.vld = super().valid
        if callable(parse_func):
            self.pf = parse_func
        if callable(convert_func):
            self.cf = convert_func
        if callable(sql_convert_func):
            self.scf = sql_convert_func
        if callable(sql_convert_func):
            self.vld = valid_func

    def validate(self, val):
        return self.vld(val)

    def parse(self, input_str):
        return self.pf(input_str)

    def convert(self, val):
        return self.cf(val)

    def sql_convert(self, val):
        return self.scf(val)
