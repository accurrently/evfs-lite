
import argparse
import logging
import re, json, datetime, os, hashlib
from pathlib import Path
import settings
from schemas import Events, Reports, Arrays
#from bqtables import BooleanTable, CustomTable, EVBQT, EnumTable, FloatArray, FloatTable
#from bqtables import NumberTable, StringArray, StringTable
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

class SignalFilter(beam.DoFn):
    """
    Filters signals from a string
    """
    def process(self, element, filtered):
        el = Events.lookup_by_signal_string(element['Signal'])
        if el is not None:
            if el.name == filtered.name:
                yield element

def build_signal(element, signal_class):
    val = signal_class.convert(element['Value'])
    return {
        'VehicleID': element['VehicleID'],
        'EventTime': element['EventTime'],
        'Value': val
    }

def process_signal(pcoll, signal_class):
    (pcoll  | "Filter {}.".format(signal_class.name) >> beam.ParDo(SignalFilter(signal_class))
            | "Build {} events".format(signal_class.name) >> beam.Map(build_signal, signal_class)
            | "Write {} to BQ.".format(signal_class.name) >> beam.io.Write(beam.io.BigQuerySink(
                            signal_class.full_table_name(),
                            schema = signal_class.bq_schema(),
                            create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))

def record_vehicle_ids(vehicles, client, dataset):
    tr = dataset.table(Arrays.VehicleIDs.name)
    tbl = bigquery.Table( tr, schema = Arrays.VehicleIDs.bq_schema())
    table = client.create_table(tbl)
    client.create_rows(table, vehicles)

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

    logging.info('Creating dataset: {dsname}'.format(dsname=settings.DATASET_NAME))

    bq_client = bigquery.Client(project=known_args.project)

    bq_ds = bq_client.dataset(settings.REPORT_NAME).create()

    vehicles = []

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
        process_signal(xcevents, Events.Odometer)

        # Pull out Engine and Ignition statuses
        process_signal(xcevents, Events.EngineStatus)
        process_signal(xcevents, Events.IngnitionStatus)

        # Pull out Engine Speed signals and write them to their own table
        process_signal(xcevents, Events.EngineSpeed)
        # Get vehicle speed information
        process_signal(xcevents, Events.VehicleSpeed)

        # Pull out Latitude and Longitude signals and write them to their own table
        process_signal(xcevents, Events.Latitude)
        process_signal(xcevents, Events.Longitude)

        # Get remaining EV range
        process_signal(xcevents, Events.ElectricRange)

        # Get fuel information
        process_signal(xcevents, Events.FuelSinceRestart)
        process_signal(xcevents, Events.FuelLevel)


        result = p.run()
        result.wait_until_finish()

        vehicles.append(folder['vehicle_id'])

    record_vehicle_ids(vehicles, bq_client, bq_ds)


    #move_xcfiles(xcfiles, bucket_name='dumbdata')

if __name__ == '__main__':
  #logging.basicConfig(filename='testing.log',level=logging.INFO)
  logging.getLogger().setLevel(logging.INFO)
  run()
