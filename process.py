from google.cloud import bigquery
import settings
from schemas import ValueType, EVBQT
from schemas import Events

from bqtables import EVBQT, NumberTable, IntegerTable
from bqtables import FloatTable, CustomTable, EnumTable, StringTable

import statistics, uuid, logging, datetime, time

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from google.cloud import storage

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
            raise TypeError('signal_clas must be a class or sublass of EVBQT')

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

def value_statfunc_query(vehicle_id, signal_class, function_name,
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
    if isinstance(signal_class, NumberTable):
        if function_name in allowed_functions:
            return EVQuery(vehicle_id, signal_class, sql = allowed_functions[function_name],
                    begin = bracket_begin, end = bracket_end, params = {'func' : function_name})
    return None

def count_values_query(vehicle_id, signal_class, value, bracket_begin = DEFAULT_DATETIME_START, bracket_end = DEFAULT_DATETIME_END):
    """
    Returns a SQL query string that finds a simple count of events where Value = value.
    """
    return EVQuery(vehicle_id, signal_class, sql = SQLSTR.VALUE_COUNT,
            begin = bracket_begin, end = bracket_end, params ={'value': value})

def get_gps_trace_query(vehicle_id, interval_sec = 60
    latitude_class = Latitude, longitude_class = Longitude,
    bracket_begin = DEFAULT_DATETIME_START, bracket_end = DEFAULT_DATETIME_END):
    """
    This function will return a SQL query string that will look for GPS coordinates
    averaged over the interval time in seconds given in interval_sec.
    """
    return EVQuery(vehicle_id, latitude_class, begin = bracket_begin, end = bracket_end,
                    sql = SQLSTR.GPS_TRACE,
                    params = {
                        'lat_table' : latitude_class.full_table_name(),
                        'lon_table' : longitude_class.full_table_name(),
                        'interval' : interval_sec
                    }
            )

def run_async_query(query):
    client = bigquery.Client()
    job = client.run_async_query(str(uuid.uuid4()), query)
    job.begin()
    job.result()
    destination_table = job.destination
    destination_table.reload()
    return destination_table.fetch_data()

################################################################################
# Trips
################################################################################

def trip_distance(vehicle_id, trip_begin = DEFAULT_DATETIME_START, trip_end = DEFAULT_DATETIME_END):
    """
    Returns the distance traveled (by odometer) for a trip.
    """
    q = value_statfunc_query_str(vehicle_id, Odometer, 'DELTA',
        bracket_begin = trip_begin, bracket_end = trip_end)
    rows = q.run_async_query()
    dist = 0
    for row in rows:
        dist = row['Result']
    return dist

def trip_engine_starts(vehicle_id, trip_begin, trip_end):
    """
    Get the number of times the engine starts during the trip.
    """
    q = get_signals_query(vehicle_id, , bracket_begin = trip_begin, bracket_end = trip_end)
    rows = q.run_async_query()
    engine_running = False
    start_count = 0
    for row in rows:
        # Detect engine running as a start
        if row['Value']:
            if not engine_running:
                start_count += 1
                engine_running = True
        # Reset engine detector once it turns off
    elif row['Value'] == False:
            if engine_running:
                engine_running = False

    return start_count

def trip_fuel_consumed(vehicle_id, trip_begin, trip_end, engine_signal=FuelConsumed):
    """
    Get the amount of fuel used for the trip.
    """
    pass

def trip_electric_distance(vehicle_id, trip_begin, trip_end, engine_signal = EngineSpeed, engine_value = '0'):
    q = get_bracket_query(vehicle_id, engine_signal, engine_value, prebracket_begin=trip_begin, prebracket_end=trip_end)
    rows = q.run_async_query()
    dist = 0
    for row in rows:
        dist += trip_distance(vehicle_id, row['StartTime'], row['EndTime'])
    return dist


class TripProcessDoFn(beam.DoFn):
    def process(self, element):
        logging.info('Processing a trip...')

        logging.info('Looking up distance from odometer...')
        dist = element | "Distance traveled for trip" >> beam.Map(
            trip_distance,
            element['VehicleID'], element['StartTime'], element['EndTime'])

        logging.info('Looking for engine starts...')
        starts = element | "Engine starts on trip" >> beam.Map(
            trip_engine_starts,
            element['VehicleID'], element['StartTime'], element['EndTime'])

        logging.info('Calculating fuel consumption...')
        fuel_used = element | "Fuel used in trip" >> beam.Map(
            trip_fuel_consumed,
            element['VehicleID'], element['StartTime'], element['EndTime']
            )

        logging.info('Calculating electricity consumption...')
        elec_used = element | "Electricity used in trip" >> beam.Map(
            trip_fuel_used,
            element['VehicleID'], element['StartTime'], element['EndTime'],
            signal='electricity_flow_display'
            )

        logging.info('Get electric miles traveled...')
        elec_dist = element | "Get Electric (and fuel) distance" >> beam.Map(
            trip_electric_distance,
            element['VehicleID'], element['StartTime'], element['EndTime']
        )


        return {
            'VehicleID': element['VehicleID'],
            'StartTime': element['StartTime'],
            'EndTime': element['EndTime'],
            'DistanceTraveled': dist,
            'EngineStarts': starts,
            'ElectricDistance': elec_dist,
            'ElectricityUsed': elec_used,
            'FuelUsed': fuel_used,
            'FuelDistance': dist - elec_dist
        }

def get_vehicle_id(collection):
    return collection['VehicleID']

class ProcessTripData(beam.DoFn):
    """
    Processes all the trips.
    """
    def process(self, pcoll):

        trip_brackets = pcoll | "Read trip brackets" >> beam.io.Read(beam.io.BigQuerySource(
            get_bracket_query_str(
                vehicle=pcoll['VehicleID'],
                signal='ignition_status',
                value='run'
            )
        ))
        trips = trip_brackets | "Process brackets" >> beam.ParDo(TripProcessDoFn())

        trips | "Write rows to BigQuery" >> beam.io.Write(
            beam.io.BiqQuery(),
            Trips.full_table_name,
            schema = Trips.schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
        )

        return trips

################################################################################
# Stops
################################################################################

class ProcessStopsDoFn(beam.DoFn):
    """
    This DoFn will process the data for all stops
    """
    def process(self, element):

        logging.info('Calculating statistics for stop...')
        gps_traces = EVQuery( element['VehicleID'],
            EVBQT,
            sql = SQLSTR.GPS_TRACE,
            params = {
                'lat_table': Latitude.full_table_name,
                'lon_table': Longitude.full_table_name
            },
            begin = element['StartTime'],
            end = element['EndTime']
        ).run_async_query()

        plugged_in_query = """\
            SELECT
                COUNT(*) AS PluggedInSignals
            FROM
                {table_name}
            WHERE
                VehicleID = "{vehicle_id}"
                AND
                Signal = "charger_type"
                AND
                Value IN ("AC_Level1_120v", "AC_Level2_120v", "DC_Fast_Charging")
                AND
                EventTime >= {begin_time}
                AND
                EventTime <= {end_time}
        """.format(
            vehicle_id=element['VehicleID'],
            table_name=RawEvents.full_table_name,
            begin_time=element['StartTime'],
            end_time=element['EndTime']
        )

        charged_query = """
            SELECT
                COUNT(*) AS ChargeSignals
            FROM
                {table_name}
            WHERE
                VehicleID = "{vehicle_id}"
                AND
                Signal = "charge_ready_status"
                AND
                Value = "Charging"
                AND
                EventTime >= {begin_time}
                AND
                EventTime <= {end_time}
        """.format(
            vehicle_id=element['VehicleID'],
            table_name=RawEvents.full_table_name,
            begin_time=element['StartTime'],
            end_time=element['EndTime']
        )

        lat = run_async_query(latitudes_query)[0]['AvgValue']
        lon = run_async_query(longitude_query)[0]['AvgValue']
        charged = False
        if run_async_query(charged_query)[0]['ChargeSignals'] > 0:
            charged = True
        plugged_in = False
        if run_async_query(plugged_in_query)[0]['PluggedInSignals'] > 0:
            plugged_in = True

        return {
            'VehicleID': element['VehicleID'],
            'StartTime': element['StartTime'],
            'EndTime': element['EndTime'],
            'Latitude': lat,
            'Longitude': lon,
            'GPS': '{la}, {lo}'.format(la=lat, lo=lon),
            'PluggedIn': plugged_in,
            'ChargeEvent': charged,
        }




class ProcessStopData(beam.DoFn):
    """
    Processes all the stops.
    """
    def expand(self, pcoll):
        stop_brackets = pcoll | "Read stop brackets" >> beam.ion.Read(beam.io.BigQuerySource(
            get_bracket_query_str(
                vehicle=pcoll['VehicleID'],
                signal='ignition_status',
                value='off'
            )
        ))

        stops = stop_brackets | "Process stop brackets" >> beam.ParDo(ProcessStopsDoFn())

        stops | "Write stops to BigQuery" >> beam.io.Write(
            beam.io.BigQuery(),
            schema=Stops.schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
        )

        return stops



def run(argv=None):

    logging.info('Starting statistical processing job.')
    known_args = settings.ARGS
    thebucket = known_args.input

    opts = PipelineOptions(flags=argv)
    gopts = opts.view_as(GoogleCloudOptions)
   # gopts.runner = 'DataflowRunner'
    gopts.project = known_args.project
    gopts.temp_location = 'gs://' + known_args.input + known_args.tempfolder
    gopts.staging_location = 'gs://' + known_args.input + known_args.stagingfolder
    gopts.job_name = 'openxc-statistical-processing-' + datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    opts.view_as(StandardOptions).runner = known_args.runner

    logging.info('RawEvents table: {table}'.format(table=RawEvents.full_table_name))

    vehicle_query = """
    SELECT DISTINCT VehicleID FROM {table}
    """.format(table=RawEvents.full_table_name)

    with beam.Pipeline(options=opts) as p:

        vehicles = p | "Get List of Vehicle IDs" >> beam.io.Read(beam.io.BigQuerySource(
                                                        query=vehicle_query))

        trips = vehicles | "Process data for trips" >> beam.ParDo(ProcessTripData())

        stops = vehicles | "Process data for stops" >> beam.ParDo(ProcessStopData())

        result = p.run()
        result.wait_until_finish()

if __name__ == '__main__':
  #logging.basicConfig(filename='testing.log',level=logging.INFO)
  logging.getLogger().setLevel(logging.INFO)
  run()
