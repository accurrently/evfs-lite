# Google Cloud
from google.cloud import bigquery
from google.cloud import storage

# Apache Beam
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions

# Standard libs
import statistics, uuid, logging, datetime, time

# App settings
import settings

# Schemas
from schemas import ValueType, EVBQT
from schemas import Events, Reports, Arrays

# EVQuery
from evquery import DEFAULT_DATETIME_END, DEFAULT_DATETIME_START
from evquery import SQLSTR, EVQuery
from evquery import value_statfunc, run_async_query, gps_start_finish, gps_trace
from evquery import count_values, datetime_to_float, get_bracket_query, get_signals_query
from evquery import time_bracket_str, get_array, get_array_as_dicts

# Tables
from bqtables import EVBQT, NumberTable, IntegerTable
from bqtables import FloatTable, CustomTable, EnumTable, StringTable

################################################################################
# Stops
################################################################################
def do_stop(element):
    """
    Process a row for a stop
    """
    start = element['StartTime']
    end = element['EndTime']
    vid = element['VehicleID']

    result = {
        'VehicleID' : vid,
        'StartTime' : start,
        'Duration': (float(end) - float(start))/60 # Duration in minutes
        'EndTime' : end
    }

    l1_charges = count_values(vid, Events.ChargerType,
                                    Events.ChargerType.lookup('AC_Level1_120v'),
                                    bracket_begin = start, bracket_end = end)
    l2_charges = count_values(vid, Events.ChargerType,
                                    Events.ChargerType.lookup('AC_Level2_120v'),
                                    bracket_begin = start, bracket_end = end)
    dcfc_charges = count_values(vid, Events.ChargerType,
                                    Events.ChargerType.lookup('DC_Fast_Charging'),
                                    bracket_begin = start, bracket_end = end)

    result['Charged'] = False
    result['HighestChargeLevel'] = 0
    if l1_charges > 0:
        result['Charged'] = True
        result['HighestChargeLevel'] = 1
    if l2_charges > 0:
        result['Charged'] = True
        result['HighestChargeLevel'] = 2
    if dcfc_charges > 0:
        result['Charged'] = True
        result['HighestChargeLevel'] = 3

    result['ChargeCompleted'] = False
    complete_charges = count_values(vid, Events.ChargeStatus,
                                    Events.ChargeStatus.lookup('ChargeComplete'),
                                    bracket_begin = start, bracket_end = end)
    if complete_charges > 0:
        result['ChargeCompleted'] = True

    result['ChargeFault'] = False
    faults = count_values(vid, Events.ChargeStatus,
                                    Events.ChargeStatus.lookup('Faulted'),
                                    bracket_begin = start, bracket_end = end)
    if faults > 0:
        result['ChargeFault'] = True

    result['Latitude'] = value_statfunc(vid, Events.Latitude, 'AVG',
                                        bracket_begin = begin, bracket_end = end)
    result['Longitude'] = value_statfunc(vid, Events.Longitude, 'AVG',
                                        bracket_begin = begin, bracket_end = end)
    result['LatLong'] = "{},{}".format(result['Latitude'], result['Longitude'])

class ProcessStops(beam.DoFn):
    def process(self, element, p):
        """
        Parallelized processing for trips
        """
        q_brackets = get_bracket_query(element['Value'], Events.IngnitionStatus, Events.IngnitionStatus.lookup('off'))
        r_brackets = q_brackets.run_async_query()

        stop_brackets = p | "Reading stop brackets" >> beam.Create(r_brackets)

        res = (stop_brackets | "Finding stop data for" >> beam.Map(do_stop)
                    | "Write stops to BQ" >> beam.io.Write(beam.io.BigQuerySink(
                                Reports.Stops.full_table_name(),
                                schema = Reports.Stops.bq_schema(),
                                create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))




################################################################################
# Trips
################################################################################
def do_trip(element):
    """
    Process a row for a trip
    """
    start = element['StartTime']
    end = element['EndTime']
    vid = element['VehicleID']

    result = {
        'VehicleID' : vid,
        'StartTime' : start,
        'Duration': (float(end) - float(start))/60 # Duration in minutes
        'EndTime' : end
    }

    # Get fuel conumption
    result['FuelConsumed'] = value_statfunc(vid, Events.FuelSinceRestart, 'MAX',
                                            bracket_begin = start, bracket_end = end)
    # Fraction of fuel tank compacity consumed
    result['TankConsumed'] = value_statfunc(vid, Events.FuelLevel, 'DELTA',
                                            bracket_begin = start, bracket_end = end)
    # Battery SOC
    result['BatterySOCMax'] = value_statfunc(vid, Events.BatterySOC, 'MAX',
                                            bracket_begin = start, bracket_end = end)
    result['BatterySOCMin'] = value_statfunc(vid, Events.BatterySOC, 'MIN',
                                            bracket_begin = start, bracket_end = end)
    result['BatterySOCDelta'] = value_statfunc(vid, Events.BatterySOC, 'DELTA',
                                            bracket_begin = start, bracket_end = end)

    # Get Distance
    result['Distance'] = value_statfunc(vid, Events.Odometer, 'DELTA',
                                            bracket_begin = start, bracket_end = end)

    # Get Maximum and Average Speed
    result['SpeedMax'] = value_statfunc(vid, Events.VehicleSpeed, 'MAX',
                                            bracket_begin = start, bracket_end = end)
    result['SpeedAvg'] = value_statfunc(vid, Events.VehicleSpeed, 'AVG',
                                            bracket_begin = start, bracket_end = end)

    result['ElectricRangeConsumed'] = value_statfunc(vid, Events.ElectricRange, 'DELTA',
                                            bracket_begin = start, bracket_end = end)

    # GPS data
    gpsdat = gps_start_finish(vid, bracket_begin = start, bracket_end = end)

    slatlon = '{},{}'.format(gpsdat['StartLatitude'], gpsdat['StartLongitude'])
    elatlon = '{},{}'.format(gpsdat['EndLatitude'], gpsdat['EndLongitude'])

    result['StartLatitude'] = gpsdat['StartLatitude']
    result['StartLongitude'] = gpsdat['StartLongitude']
    result['StartLatLong'] = slatlon
    result['EndLatitude'] = gpsdat['EndLatitude']
    result['EndLongitude'] = gpsdat['EndLongitude']
    result['EndLatLong'] = elatlon

    return result

class ProcessTrips(beam.DoFn):
    def process(self, element, p):
        """
        Parallelized processing for trips
        """
        q_brackets = get_bracket_query(element['Value'], Events.IngnitionStatus, Events.IngnitionStatus.lookup('run'))
        r_brackets = q_brackets.run_async_query()

        trip_brackets = p | "Reading trip brackets" >> beam.Create(r_brackets)

        res = (trip_brackets | "Finding trip data for" >> beam.Map(do_trip)
                    | "Write trips to BQ" >> beam.io.Write(beam.io.BigQuerySink(
                                Reports.Trips.full_table_name(),
                                schema = Reports.Trips.bq_schema(),
                                create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))

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

        vlist = p | "Get List of Vehicle IDs" >> beam.Create(get_array_as_dicts(Arrays.VehicleIDs))

        (vlist | "Process trips" >> beam.ParDo(ProcessTrips(), p))
        (vlist | "Process stops" >> beam.ParDo(ProcessStops(), p))

        result = p.run()
        result.wait_until_finish()

if __name__ == '__main__':
  #logging.basicConfig(filename='testing.log',level=logging.INFO)
  logging.getLogger().setLevel(logging.INFO)
  run()
