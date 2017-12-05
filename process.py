from google.cloud import bigquery
import settings
from schemas import ValueType
from schemas import RawEvents, FleetDailyStats, GPSTrace, Stops, Trips, DATASET_NAME
from schemas import ElectricRange, EngineSpeed, EngineRunStatus, IgnitionRunStatus
from schemas import Latitude, Longitude, FuelSinceRestart, Odometer
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


def get_bracket_query_str(vehicle_id, table_class, value, order='ASC',
        prebracket_begin = DEFAULT_DATETIME_START, prebracket_end = DEFAULT_DATETIME_END):
    val = value
    if isinstance(value, str):
        val = value.translate(None, "\"\'")
        newval = "\"{v}\"".format(v=val)
    return SQLSTR.BRACKETS.format(
        table_name = table_class.full_table_name,
        value = val,
        order = order,
        prebracket_begin = prebracket_begin,
        prebracket_end = prebracket_end
    )

def get_signals_query_str(vehicle_id, signal_class, order = 'ASC',
        bracket_begin = DEFAULT_DATETIME_START, bracket_end = DEFAULT_DATETIME_END):
    return SQLSTR.SIGNALS.format(
        table_name = signal_class.full_table_name,
        vehicle_id = vehicle_id,
        time_bracket = time_bracket_str(bracket_begin, bracket_end)
        order = order
    )

def get_signals_interval_avg_query_str(vehicle_id, signal_class, interval_sec = 60,
        bracket_begin = DEFAULT_DATETIME_START, bracket_end = DEFAULT_DATETIME_END):
    if signal_class.value_type == ValueType.Number:
        return SQLSTR.SIGNALS_INTERVAL_AVG.format(
            table_name = signal_class.full_table_name,
            vehicle_id = vehicle_id,
            interval = interval_sec,
            time_bracket = time_bracket_str(bracket_begin, bracket_end)
        )

def value_statfunc_query_str(vehicle_id, signal_class, function_name,
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
    if signal_class.value_type == ValueType.Number:
        if function_name in allowed_functions:
            return allowed_functions[function_name].format(
                table_name = signal_class.full_table_name,
                vehicle_id = vehicle_id,
                func = function_name,
                time_bracket = time_bracket_str(bracket_begin, bracket_end)
            )
    return None

def get_gps_trace_query_str(vehicle_id, interval_sec = 60
    latitude_class = Latitude, longitude_class = Longitude,
    bracket_begin = DEFAULT_DATETIME_START, bracket_end = DEFAULT_DATETIME_END):
    """
    This function will return a SQL query string that will look for GPS coordinates
    averaged over the interval time in seconds given in interval_sec.
    """
    return SQLSTR.GPS_TRACE.format(
        vehicle_id = vehicle_id,
        interval = interval_sec,
        lat_table = latitude_class.full_table_name,
        lon_table = longitude_class.full_table_name,
        time_bracket = time_bracket_str(bracket_begin, bracket_end)
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
    rows = run_async_query(q)
    dist = 0
    for row in rows:
        dist = row['Result']
    return dist

def trip_engine_starts(vehicle_id, trip_begin, trip_end):
    """
    Get the number of times the engine starts during the trip.
    """
    q = get_signals_query_str(vehicle_id, EngineRunStatus, bracket_begin = trip_begin, bracket_end = trip_end)
    rows = run_async_query(q)
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

def trip_fuel_consumed(vehicle_id, trip_begin, trip_end, engine_signal='fuel_consumed_since_restart'):
    """
    Get the amount of fuel used for the trip.
    """
    q ="""
    SELECT
        Value,
        EventTime
    FROM
        {table_name}
    WHERE (
        (EventTime >= {begin_bracket})
        AND (EventTime <= {end_bracket})
        AND (VehicleID = "{vehicle_id}")
        AND (Signal = "{engine_signal}")
    )
    ORDER BY
        EventTime
    DESC
    LIMIT 1
    """.format(
        vehicle_id = vehicle_id,
        begin_bracket=trip_begin,
        end_bracket=trip_end,
        engine_signal=engine_signal,
        table_name=RawEvents.full_table_name
    )
    client = bigquery.Client()
    job = client.run_async_query(str(uuid.uuid4()), q)
    job.begin()
    job.result()
    destination_table = job.destination
    destination_table.reload()
    rows = destination_table.fetch_data()
    fuel_used = 0
    for row in rows:
        fuel_used = float(row['Value'])*1000000
    return fuel_used

def trip_electric_distance(vehicle_id, trip_begin, trip_end, engine_signal='engine_speed', engine_value = '0'):
    electric_brackets = get_bracket_query_str(vehicle_id, engine_signal, engine_value, prebracket_begin=trip_begin, prebracket_end=trip_end)
    rows = run_async_query(electric_brackets)
    dist = 0
    for row in rows:
        dist += trip_distance(vehicle_id, rowp['StartTime'], row['EndTime'])
    return dist

def trip_fuel_used(vehicle_id, trip_begin, trip_end, signal='fuel_consumed_since_restart'):
    q = """
        SELECT
            Value
        FROM
            {table_name}
        WHERE (
            Signal = "{signal}"
            AND (EventTime >= {trip_begin})
            AND (EventTime <= {trip_end})
        )
        ORDER BY
            EventTime
        DESC
        LIMIT 1
    """.format(
        table_name = RawEvents.full_table_name,
        signal = signal,
        trip_begin = trip_begin,
        trip_end = trip_end
    )
    results = run_async_query(q)
    fuel = 0
    for result in results:
        fuel += float(result['Value'])

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
    def expand(self, pcoll):

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
        latitude_query = """
            SELECT
                AVG(FLOAT(Value)) AvgValue
            FROM
                {table_name}
            WHERE
                VehicleID="{vehicle_id}"
                AND
                Signal = "latitude"
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

        longitude_query = """\
            SELECT
                AVG(FLOAT(Value)) AvgValue
            FROM
                {table_name}
            WHERE
                VehicleID = "{vehicle_id}"
                AND
                Signal = "longitude"
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
