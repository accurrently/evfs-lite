import settings
import datetime
import re
from utils import load_json_list
from google.cloud import bigquery

def bq_table(table_dict_array):
    from apache_beam.io.gcp.internal.clients import bigquery
    ts = bigquery.TableSchema()
    for d in table_dict_array:
        fs = bigquery.TableFieldSchema()
        if ('name' in d.keys()) and ('type' in d.keys()):
            fs.name = d['name']
            fs.type = d['type']
        fs.mode = 'nullable'
        if 'mode' in d.keys():
            fs.mode = d['mode']
        ts.fields.append(fs)
    return ts

class EVBQT(object):
    """
    EV Big Query Table base class
    """
    def __init__(self, name, table = [{}], signal_strings = {}):
        self.name = name
        self.schema = table
        self.signal_strings = signal_strings

    def full_table_name(self):
        return "{a}.{b}".format(a = settings.DATASET_NAME, b = self.name)

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
    def __init__(self, name, table = [], signal_strings = {}):
        tbl = [
            {'name': 'VehicleID', 'type': 'string'},
            {'name': 'EventTime', 'type': 'timestamp'},
            {'name': 'Value', 'type': 'string'}
        ]
        if len(table) > 0:
            tbl = table
        super(StringTable, self).__init__(name, table = tbl, signal_strings = signal_strings)

    def validate(self, val):
        if isinstance(val, str):
            return bool(re.match('^[A-Za-z0-9\_\ \-\+\.\,]+$', val))
        return False

    def convert(self, val):
        return str(val)

    def sql_convert(self, val):
        return "\"{value}\"".format(value = str(val))

class EnumTable(EVBQT):

    def __init__(self, name, enum_vals = '', table = [], signal_strings = {}, start_at_zero = False):
        tbl = [
            {'name': 'VehicleID', 'type': 'string'},
            {'name': 'EventTime', 'type': 'timestamp'},
            {'name': 'Value', 'type': 'integer'}
        ]
        if len(table) > 0:
            tbl = table
        super(EnumTable, self).__init__(name, tbl, signal_strings)
        self.Values = enum_vals.split(' ')
        self.z = 1
        if start_at_zero:
            self.z = 0

    def lookup(self, input_str):
        return self.parse(input_str)

    def get_ename(self, val):
        if 0 <= val - z <= len(self.Values):
            return self.Values[val - z]

    def parse(self, input_str):
        try:
            i = self.Values.index(input_str)
            return i + self.z
        except ValueError:
            return 0
        return 0

    def validate(self, val):
        if isinstance(val, str):
            try:
                i = self.Values.index(input_str)
            except ValueError:
                return False
            return True
        elif isinstance(val, int):
             return 0 <= val <= len(self.Values)
        return False

    def convert(self, val):
        if self.validate(val):
            if isinstance(val, str):
                return self.parse(val)
            return val
        return 0

    def sql_convert(self, val):
        return "{value}".format(value = self.convert(val))

class NumberTable(EVBQT):
    def __init__(self, name, table = [], signal_strings = {}, range_min = None, range_max = None):
        tbl = [
            {'name': 'VehicleID', 'type': 'string'},
            {'name': 'EventTime', 'type': 'timestamp'},
            {'name': 'Value', 'type': 'integer'}
        ]
        if len(table) > 0:
            tbl = table
        super(NumberTable, self).__init__(name, tbl, signal_strings)
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
        super(IntegerTable, self).__init__(name, tbl, signal_strings, range_min = range_min, range_max = range_max)

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
        super(FloatTable, self).__init__(name, tbl, signal_strings, range_min = range_min, range_max = range_max)

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
        super(BooleanTable, self).__init__(name, tbl, signal_strings)
        self.trues = true_strings
        self.falses = false_strings
        self.default = default

    def validate(self, val):
        return isinstance(val, bool)

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

class StringArray(StringTable):
    def __init__(self, name):
        tbl = [{'name': 'Value', 'type': 'string'}]
        super(StringArray, self).__init__(name, table = tbl)
class IntegerArray(IntegerTable):
    def __init__(self, name):
        tbl = [{'name': 'Value', 'type': 'integer'}]
        super(IntergerArray, self).__init__(name, table = tbl)
class FloatArray(FloatTable):
    def __init__(self, name):
        tbl = [{'name': 'Value', 'type': 'float'}]
        super(FloatArray, self).__init__(name, table = tbl)

class CustomTable(EVBQT):

    def __init__(self, name, table = [], signal_strings = {}, parse_func = None, convert_func = None, sql_convert_func = None, valid_func = None):
        super(CustomTable, self).__init__(name, table, signal_strings)
        self.pf = parse_func
        self.cf = convert_func
        self.scf = sql_convert_func
        self.vld = valid_func

        if callable(convert_func):
            self.cf = convert_func
        if callable(sql_convert_func):
            self.scf = sql_convert_func
        if callable(sql_convert_func):
            self.vld = valid_func

    def validate(self, val):
        if callable(self.cf):
            return self.vld(val)
        return super(CustomTable, self).valid(val)

    def parse(self, input_str):
        if callable(self.pf):
            return self.pf(input_str)
        return super(CustomTable, self).parse(input_str)

    def convert(self, val):
        if callable(self.cf):
            return self.cf(val)
        return super(CustomTable, self).convert(val)

    def sql_convert(self, val):
        if callable(self.scf):
            return self.scf(val)
        return super(CustomTable, self).sql_convert(val)
