
import argparse
import logging
import re, json, datetime, os, hashlib
from pathlib import Path
import settings
from schemas import RawEvents, DATASET_NAME_NOPROJECT, DATASET_NAME
from schemas import EngineRunStatus, EngineSpeed, EVBQT, Longitude, ElectricRange
from schemas import Latitude, IgnitionRunStatus, Odometer, FuelSinceRestart
from schemas import VehicleSpeed
from xcstorage import get_vehicle_folders, move_xcfiles, get_xc_files
from google.cloud import bigquery

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

from collections import Counter


class PrepForBigQuery(beam.DoFn):
    """
    Formats data for insertion into BigQuery.
    """

    def __init__(self, xcfile_id, vehicle_id):
        self.xcfile_id = xcfile_id
        self.vehicle_id = vehicle_id

    def process(self, element):
        import datetime, json
        j = False
        try:
            j = json.loads(element)
        except:
            return None
        if j:
            ts = float(j['timestamp'])
            if ts >= 10000000000:
                ts = ts/1000
            return {
                'VehicleID' : self.vehicle_id,
                'EventTime' : ts,
                'Signal' : j['name'],
                'Value' : j['value']
            }
        return None

class FilterNull(beam.DoFn):
    """
    Filters out null xcrows
    """

    def process(self, element):
        if bool(element):
            yield element

def make_record(element, vid):
    import datetime, json
    j = False
    try:
        j = json.loads(element)
    except:
        return {}
    if j:
        ts = float(j['timestamp'])
        if ts >= 10000000000:
            ts = ts/1000
        return {
            'VehicleID' : vid,
            'EventTime' : ts,
            'Signal' : j['name'],
            'Value' : str(j['value'])
        }
    return {}

def make_ignition_status(element):
    if element['Signal'] == 'ignition_status':
        is_on = False
        if element['Value'] == 'run':
            is_on = True
        return {
            'VehicleID': element['VehicleID'],
            'EventTime': element['EventTime'],
            'Value': is_on
        }
    return {}

def make_boolean(element, signals, true_vals = {}, false_vals = {}, default = False):
    if element['Signal'] in signals:
        val = default
        if element['Value'] in false_vals:
            val = False
        if element['Value'] in true_vals:
            val = True
        return {
            'VehicleID': element['VehicleID'],
            'EventTime': element['EventTime'],
            'Value': val
        }
    return {}

def make_float(element, signals):
    if element['Signal'] in signals:
        try:
            val = float(element['Value'])
            return {
                'VehicleID': element['VehicleID'],
                'EventTime': element['EventTime'],
                'Value': val
                }
        except:
            return {}
    return {}

def make_str(element, signals):
    if element['Signal'] in signals:
        try:
            val = element['Value']
            return {
                'VehicleID': element['VehicleID'],
                'EventTime': element['EventTime'],
                'Value': val
                }
        except:
            return {}
    return {}

def make_int(element, signals):
    if element['Signal'] in signals:
        try:
            val = int(element['Value'])
            return {
                'VehicleID': element['VehicleID'],
                'EventTime': element['EventTime'],
                'Value': val
                }
        except:
            return {}
    return {}


def run(argv=None):
    """
    Main insertion point for starting jobs
    """

    known_args = settings.ARGS

#    tname = known_args.output + '.' + 'rawdata_' + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    thebucket = known_args.input

    logging.info('Looking for files in bucket: {}'.format(thebucket))

    xcfiles = get_xc_files(bucket_name=thebucket)

    logging.info('Preparing to process {} XC Files...'.format(len(xcfiles)))

    vehicle_folders = get_vehicle_folders(bucket_name=thebucket)

    logging.info('Creating dataset: {dsname}'.format(dsname=DATASET_NAME))

    bq_client = bigquery.Client(project=known_args.project)

    bq_client.dataset(DATASET_NAME_NOPROJECT).create()


    for folder in vehicle_folders:

        # Set up options for Apache Beam
        opts = PipelineOptions(flags=argv)
        gopts = opts.view_as(GoogleCloudOptions)
       # gopts.runner = 'DataflowRunner'
        gopts.project = known_args.project
        gopts.temp_location = 'gs://' + known_args.input + known_args.tempfolder
        gopts.staging_location = 'gs://' + known_args.input + known_args.stagingfolder
        gopts.job_name = 'openxc-processing-' + folder['vehicle_id'] + '-' + datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        opts.view_as(StandardOptions).runner = known_args.runner

        # Initialize Beam object
        p = beam.Pipeline(options=opts)

        # Read rows from XC Files
        xcrows = p | "Open XC Files " >> ReadFromText(folder['path'])

        # Reformat XC File rows into objects we like better
        xcevents = xcrows | "Map raw Event rows" >> beam.Map(make_record, folder['vehicle_id'])

        # Pull out the Odometer signals and write them to their own table
        xcevents    | "Find odometer readings" >> beam.Map(make_float, Odometer.signal_strings) \
                    | "Filter null Odometer" >> beam.ParDo(FilterNull()) \
                    | "Write Odometer" >> beam.io.Write(beam.io.BigQuerySink(
                                    Odometer.full_table_name,
                                    schema = Odometer.schema,
                                    create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))

        # Pull out Engine Run Status signals and write them to their own table
        xcevents    | "Find Engine Run Status readings" >> beam.Map(
                        make_boolean,
                        EngineRunStatus.signal_strings,
                        true_vals = {'engine_running', 'engine_start', ' engine_run_CSER' },
                        false_vals = {'off', 'engine_disabled'}) \
                    | "Filter null EngineRunStatus" >> beam.ParDo(FilterNull()) \
                    | "Write EngineRunStatus" >> beam.io.Write(beam.io.BigQuerySink(
                        EngineRunStatus.full_table_name,
                        schema = EngineRunStatus.schema,
                        create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))

        # Pull out Engine Speed signals and write them to their own table
        xcevents    | "Find Engine Speed readings" >> beam.Map(
                        make_float,
                        EngineSpeed.signal_strings) \
                    | "Filter null Engine Speed" >> beam.ParDo(FilterNull()) \
                    | "Write EngineSpeed" >> beam.io.Write(beam.io.BigQuerySink(
                        EngineSpeed.full_table_name,
                        schema = EngineSpeed.schema,
                        create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))

        # Pull out Ignition signals and write them to their own table
        xcevents    | "Find Ignition Status readings" >> beam.Map(
                        make_boolean,
                        IgnitionRunStatus.signal_strings,
                        true_vals = {'run', 'start'},
                        false_vals = {'off', 'acccessory'}) \
                    | "Filter null Ignition Status" >> beam.ParDo(FilterNull()) \
                    | "Write IgnitionRunStatus" >> beam.io.Write(beam.io.BigQuerySink(
                        IgnitionRunStatus.full_table_name,
                        schema = IgnitionRunStatus.schema,
                        create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))

        # Pull out Latitude and Longitude signals and write them to their own table
        xcevents    | "Find Latitude signals" >> beam.Map(
                        make_float,
                        Latitude.signal_strings) \
                    | "Filter null Latitude" >> beam.ParDo(FilterNull()) \
                    | "Write Latitude" >> beam.io.Write(beam.io.BigQuerySink(
                        Latitude.full_table_name,
                        schema = Latitude.schema,
                        create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))

        xcevents    | "Find Longitude signals" >> beam.Map(
                        make_float,
                        Longitude.signal_strings) \
                    | "Filter null Longitude" >> beam.ParDo(FilterNull()) \
                    | "Write Longitude" >> beam.io.Write(beam.io.BigQuerySink(
                        Longitude.full_table_name,
                        schema = Longitude.schema,
                        create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))

        # Get remaining EV range
        xcevents    | "Find EV range" >> beam.Map(
                        make_float,
                        ElectricRange.signal_strings) \
                    | "Filter null EV range" >> beam.ParDo(FilterNull()) \
                    | "Write ElectricRange" >> beam.io.Write(beam.io.BigQuerySink(
                        ElectricRange.full_table_name,
                        schema = ElectricRange.schema,
                        create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))

        # Get fuel used since restart
        xcevents    | "Fuel used since restart" >> beam.Map(
                        make_float,
                        FuelConsumption.signal_strings) \
                    | "Filter null Fuel" >> beam.ParDo(FilterNull()) \
                    | "Write FuelConsumption" >> beam.io.Write(beam.io.BigQuerySink(
                        FuelConsumption.full_table_name,
                        schema = FuelConsumption.schema,
                        create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))

        # Get vehicle speed information
        xcevents    | "Get vehicle speed" >> beam.Map(
                        make_float,
                        VehicleSpeed.signal_strings) \
                    | "Filter null VehicleSpeed" >> beam.ParDo(FilterNull()) \
                    | "Write VehicleSpeed" >> beam.io.Write(beam.io.BigQuerySink(
                                    VehicleSpeed.full_table_name,
                                    schema = VehicleSpeed.schema,
                                    create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))

        #lines | "Output data to BigQuery" >> beam.io.Write(beam.io.BigQuerySink(
        #                                        RawEvents.full_table_name,
        #                                        schema=RawEvents.schema,
        #                                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        #                                        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))

        result = p.run()
        result.wait_until_finish()

    move_xcfiles(xcfiles, bucket_name='dumbdata')

if __name__ == '__main__':
  #logging.basicConfig(filename='testing.log',level=logging.INFO)
  logging.getLogger().setLevel(logging.INFO)
  run()
