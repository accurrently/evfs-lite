from google.cloud import bigquery
import settings
from schemas import RawEvents, FleetDailyStats, GPSTrace, Stops, Trips, DATASET_NAME
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


def get_bracket_query_str(vehicle, signal, value, order='ASC',
                            prebracket_begin = 0, prebracket_end = time.time() ):
    return settings.load_sql('brackets.sql').format(
        table_name=RawEvents.full_table_name,
        signal=signal,
        value=value,
        order=order,
        prebracket_begin=prebracket_begin,
        prebracket_end=prebracket_end
    )

def get_bracket_query(vehicle, signal, value, order='ASC'):
    q = get_bracket_query_str(vehicle, signal, value, order)
    client = bigquery.Client()
    job = client.run_async_query(str(uuid.uuid4()), q)
    job.begin()
    job.result()
    destination_table = job.destination
    destination_table.reload()
    return destination_table.fetch_data()

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

def trip_distance(vehicle_id, trip_begin, trip_end, odometer_signal='odometer'):
    """
    Returns the distance traveled (by odometer) for a trip.
    """
    q = """
        SELECT
            MAX(FLOAT(Value)) - MIN(FLOAT(Value)) AS Distance,
        FROM
            {the_table}
        WHERE (
            (EventTime >= {begin_bracket})
            AND (EventTime <= {end_bracket})
            AND (VehicleID = "{vehicle_id}")
            AND (Signal = "{odometer}")
        )
        LIMIT 1
        """.format(
        the_table=RawEvents.full_table_name,
        begin_bracket=trip_begin,
        vehicle_id=vehicle_id,
        end_bracket=trip_end,
        odometer=odometer_signal
    )
    client = bigquery.Client()
    job = client.run_async_query(str(uuid.uuid4()), q)
    job.begin()
    job.result()
    destination_table = job.destination
    destination_table.reload()
    rows = destination_table.fetch_data()
    dist = 0
    for row in rows:
        dist = row['Distance']
    return dist

def trip_engine_starts(vehicle_id, trip_begin, trip_end, engine_signal='engine_start'):
    """
    Get the number of times the engine starts during the trip.
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
    ASC
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
    engine_running = False
    start_count = 0
    for row in rows:
        # Detect engine running as a start
        if row['Value'] == 'running':
            if not engine_running:
                start_count += 1
                engine_running = True
        # Reset engine detector once it turns off
        elif row['Value'] == 'off':
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
