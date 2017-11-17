
import argparse
import logging
import re, json, datetime, os, hashlib
from pathlib import Path
import settings
from schemas import RawEvents, DATASET_NAME_NOPROJECT, DATASET_NAME
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
def make_boolean(element, signal, true_vals = [], false_vals = [], default = False):
    if element['Signal'] == signal:
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
        
def make_float(element, signal):
    if element['Signal'] == signal:
        return {
            'VehicleID': element['VehicleID'],
            'EventTime': element['EventTime'],
            'Value': float(element['Value'])
        }





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

        opts = PipelineOptions(flags=argv)
        gopts = opts.view_as(GoogleCloudOptions)
       # gopts.runner = 'DataflowRunner'
        gopts.project = known_args.project
        gopts.temp_location = 'gs://' + known_args.input + known_args.tempfolder
        gopts.staging_location = 'gs://' + known_args.input + known_args.stagingfolder
        gopts.job_name = 'openxc-processing-' + folder['vehicle_id'] + '-' + datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        opts.view_as(StandardOptions).runner = known_args.runner

        p = beam.Pipeline(options=opts)

        xcrows = p | "Open XC Files " >> ReadFromText(folder['path'])

        lines = xcrows | "Map raw Event rows for BQ" >> beam.Map(make_record, folder['vehicle_id'])

        lines | "Output data to BigQuery" >> beam.io.Write(beam.io.BigQuerySink(
                                                RawEvents.full_table_name,
                                                schema=RawEvents.schema,
                                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))

        result = p.run()
        result.wait_until_finish()

    move_xcfiles(xcfiles, bucket_name='dumbdata')

if __name__ == '__main__':
  #logging.basicConfig(filename='testing.log',level=logging.INFO)
  logging.getLogger().setLevel(logging.INFO)
  run()
