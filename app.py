import logging
import settings, utils
from bqtables import BooleanTable, StringTable, EVBQT, IntegerTable, NumberTable,
from bqtables import CustomTable, EnumTable, FloatTable



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



if __name__ == '__main__':
  #logging.basicConfig(filename='testing.log',level=logging.INFO)
  logging.getLogger().setLevel(logging.INFO)
