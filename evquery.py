from google.cloud import bigquery

import settings
from schemas import ValueType, EVBQT
from schemas import Events, Reports

from bqtables import EVBQT, NumberTable, IntegerTable
from bqtables import FloatTable, CustomTable, EnumTable, StringTable

import statistics, uuid, logging, datetime, time

################################################################################
# Load up SQL strings
################################################################################
class SQLSTR:
    """
    A simple class to hold all the sql strings we might need for the functions.
    This is to load the strings into variables before they are dispached to
    workers, since workers may nt have local access to the .sql files.
    """
    # Bracketing querry for finding events within a certain time
    BRACKETS_RAW = settings.load_sql('brackets_raw.sql')
    BRACKETS = settings.load_sql('brackets.sql')
    # Returns all the signals of a given type
    SIGNALS = settings.load_sql('signals.sql')
    # Returns signals averaged over a specific time interval
    SIGNALS_INTERVAL_AVG = settings.load_sql('signals_interval_avg.sql')

    # Gets the difference between the min and max of the values in the sample
    VALUE_DELTA = settings.load_sql('value_delta.sql')
    # Returns the maximum value of a sample
    VALUE_MAX = settings.load_sql('value_max.sql')
    # Returns the minimum value of a sample
    VALUE_MIN = settings.load_sql('value_min.sql')
    # Returns the Minimum, Maximum, Average, standard deviation (sample and pop)
    VALUE_STATS = settings.load_sql('value_stats.sql')
    # Returns a specific statistic function (for speed)
    VALUE_STAT_FUNC = settings.load_sql('value_stat_func.sql')
    # Returns a simple count of values that match input value
    VALUE_COUNT = settings.load_sql('value_count.sql')

    # Get a GPS trace for a vehicle
    GPS_TRACE = settings.load_sql('gps_trace.sql')

    GET_ARRAY = settings.load_sql('get_array.sql')


# January 1, 2000
DEFAULT_DATETIME_START = datetime.datetime(2000, 1, 1, 0, 00)
# Now
DEFAULT_DATETIME_END = datetime.now()

def datetime_to_float(d):
    if isinstance(d,int) or isinstance(d,float):
        return d
    epoch = datetime.datetime.utcfromtimestamp(0)
    total_seconds =  (d - epoch).total_seconds()
    # total_seconds will be in decimals (millisecond precision)
    return total_seconds

def time_bracket_str(begin, end):
    begin_ts = datetime_to_float(begin)
    end_ts = datetime_to_float(end)
    q = 'UNIX_SECONDS(EventTime) >= {begin} AND UNIX_SECONDS(EventTime) <= {end}'
    return q.format(begin=begin_ts, end=end_ts)

class EVQuery(object):
    def __init__(self, vehicle_id, signal_class, sql = '', params = {},
        begin = DEFAULT_DATETIME_START, end = DEFAULT_DATETIME_END):

        self.params = {}

        if isinstance(vehicle_id, str):
            self.params['vehicle_id'] = vehicle_id
        else:
            raise TypeError('vehicle_id must be of type str')

        if isinstance(signal_class, EVBQT):
            self.params['table_name'] = signal_class.full_table_name()
        else:
            raise TypeError('signal_class must be a class or sublass of EVBQT')

        if isinstance(begin, datetime.datetime):
            self.params['bracket_begin'] = datetime_to_float(begin)
        else:
            raise TypeError('begin must be of type datetime.datetime')

        if isinstance(end, datetime.datetime):
            self.params['bracket_end'] = datetime_to_float(end)
        else:
            raise TypeError('end must be of type datetime.datetime')

        self.params['time_bracket'] = time_bracket_str(begin, end)

        if isinstance(sql, str):
            self.sql = sql
        else:
            raise TypeError('sql must be of type str')

        if isinstance(params, dict):
            self.params.update(params)
        else:
            raise TypeError('params must be of type dict')

    def set_bracket(self, begin, end):
        if not isinstance(begin, datetime.datetime):
            raise TypeError('begin must be of type datetime.datetime')
        if not isinstance(end, datetime.datetime):
            raise TypeError('end must be of type datetime.datetime')
        self.params['bracket_begin'] = datetime_to_float(begin)
        self.params['bracket_end'] = datetime_to_float(end)
        self.params['time_bracket'] = time_bracket_str(begin, end)
        return self

    def set_signal_class(self, signal_class):
        if isinstance(signal_class, EVBQT):
            self.params['table_name'] = signal_class.full_table_name
        else:
            raise TypeError('signal_class must be a class or sublass of EVBQT')

    def set_vid(self, vid):
        if not isinstance(begin, datetime.datetime):
            raise TypeError('begin must be of type datetime.datetime')
        self.params['vehicle_id'] = vid
        return self

    def set_params(self, parameters = None):
        if not isinstance(parameters, dict):
            raise TypeError('parameters must be of type dict')
        self.params.update(parameters)

    def query_string(self):
        return self.sql.format(**self.params)

    def __str__(self):
        return self.query_string()

    def run_async_query(self):
        query = self.query_string()
        client = bigquery.Client()
        job = client.run_async_query(str(uuid.uuid4()), query)
        job.begin()
        job.result()
        destination_table = job.destination
        destination_table.reload()
        return destination_table.fetch_data()

def get_bracket_query(vehicle_id, signal_class, value,
        prebracket_begin = DEFAULT_DATETIME_START, prebracket_end = DEFAULT_DATETIME_END):
    val = signal_class.sql_convert(value)
    return EVQuery(vehicle_id, signal_class, sql = SQLSTR.BRACKETS,
                    begin = prebracket_begin, end = prebracket_end,
                    params = { 'value' : val, 'order' : order})

def get_signals_query(vehicle_id, signal_class, order = 'ASC',
        bracket_begin = DEFAULT_DATETIME_START, bracket_end = DEFAULT_DATETIME_END):
    return EVQuery(vehicle_id, signal_class, sql = SQLSTR.SIGNALS,
                    begin = bracket_begin, end = bracket_end,
                    params = {'order' : order})

def get_signals_interval_avg_query(vehicle_id, signal_class, interval_sec = 60,
        bracket_begin = DEFAULT_DATETIME_START, bracket_end = DEFAULT_DATETIME_END):
    if isinstance(signal_class, NumberTable):
        return EVQuery(vehicle_id, signal_class, sql = SQLSTR.SIGNALS_INTERVAL_AVG,
                        begin = bracket_begin, end = bracket_end,
                        params = {'interval' : interval_sec})

def value_statfunc(vehicle_id, signal_class, function_name,
        bracket_begin = DEFAULT_DATETIME_START, bracket_end = DEFAULT_DATETIME_END):
    allowed_functions = {
        "AVG" : SQLSTR.VALUE_STAT_FUNC,
        "MIN" : SQLSTR.VALUE_STAT_FUNC,
        "MAX" : SQLSTR.VALUE_STAT_FUNC,
        "SUM" : SQLSTR.VALUE_STAT_FUNC,
        "VAR_SAMP" : SQLSTR.VALUE_STAT_FUNC,
        "VAR_POP" : SQLSTR.VALUE_STAT_FUNC,
        "STDDEV_POP" : SQLSTR.VALUE_STAT_FUNC,
        "STDDEV_SAMP" : SQLSTR.VALUE_STAT_FUNC,
        "COUNT" : SQLSTR.VALUE_STAT_FUNC,
        "DELTA" : SQLSTR.VALUE_DELTA,
    }
    x = None
    if isinstance(signal_class, NumberTable):
        if function_name in allowed_functions:
            q = EVQuery(vehicle_id, signal_class, sql = allowed_functions[function_name],
                    begin = bracket_begin, end = bracket_end, params = {'func' : function_name})
            r = q.run_async_query()
            for row in r:
                x = r['Result']
    return x

def count_values(vehicle_id, signal_class, value, bracket_begin = DEFAULT_DATETIME_START, bracket_end = DEFAULT_DATETIME_END):
    """
    Returns a SQL query string that finds a simple count of events where Value = value.
    """
    if isinstance(signal_class, NumberTable):
        if function_name in allowed_functions:
            q = EVQuery(vehicle_id, signal_class, sql = SQLSTR.VALUE_COUNT,
                    begin = bracket_begin, end = bracket_end, params ={'value': signal_class.sql_convert(value)})
            r = q.run_async_query()
            for row in r:
                x = r['Result']
    return x




def gps_trace(vehicle_id, interval_sec = 60,
    latitude_class = Events.Latitude, longitude_class = Events.Longitude,
    bracket_begin = DEFAULT_DATETIME_START, bracket_end = DEFAULT_DATETIME_END):
    """
    This function will return a GPS trace
    """
    q = EVQuery(vehicle_id, latitude_class, begin = bracket_begin, end = bracket_end,
                    sql = SQLSTR.GPS_TRACE,
                    params = {
                        'lat_table' : latitude_class.full_table_name(),
                        'lon_table' : longitude_class.full_table_name(),
                        'interval' : interval_sec
                    }
    )
    return q.run_async_query()

def gps_start_finish(vehicle_id, interval_sec = 60,
    latitude_class = Events.Latitude, longitude_class = Events.Longitude,
    bracket_begin = DEFAULT_DATETIME_START, bracket_end = DEFAULT_DATETIME_END):
    q = EVQuery(vehicle_id, latitude_class, begin = bracket_begin, end = bracket_end,
                    sql = SQLSTR.GPS_TRACE,
                    params = {
                        'lat_table' : latitude_class.full_table_name(),
                        'lon_table' : longitude_class.full_table_name(),
                        'interval' : interval_sec
                    }
    )
    r = q.run_async_query()
    gbegin = r[0]
    gend = r[-1]

    return {
        'VehicleID' : vehicle_id,
        'StartTime' : gbegin['TraceTime'],
        'StartLatitude' : gbegin['Latitude'],
        'StartLongitude' : gbegin['Longitude'],
        'EndTime' : gend['TraceTime'],
        'EndLatitude' : gend['Latitude'],
        'EndLongitude' : gend['Longitude']
    }


def run_async_query(query):
    client = bigquery.Client()
    job = client.run_async_query(str(uuid.uuid4()), query)
    job.begin()
    job.result()
    destination_table = job.destination
    destination_table.reload()
    return destination_table.fetch_data()

def get_array(signal_class):
    """
    Returns an array that's stored in BigQuery
    """
    q = SQLSTR.GET_ARRAY.format(table = signal_class.full_table_name())
    r = run_async_query(q)
    a = []
    for row in r:
        a.append(row['Value'])
    return a

def get_array_as_dicts(signal_class):
    """
    Returns an array that's stored in BigQuery as a list of dicts
    """
    q = SQLSTR.GET_ARRAY.format(table = signal_class.full_table_name())
    return run_async_query(q)
