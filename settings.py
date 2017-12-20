from __future__ import absolute_import

import argparse
import logging
import re, datetime, os



def get_args(argv=None):
    """
    Parse command line args with argparse.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
    	'--input',
    	required=True,
    	help=('The bucket we\'re going to use. (Without gs:// prefix.)')
    )
    parser.add_argument(
        '--project',
        required=True,
        help=('The GCP project to use')
    )
    parser.add_argument(
        '--runner',
        required=False,
        default='DataflowRunner',
        help=('The Runner to use for Apache Beam (defaults to GCP DataflowRunner)')
    )
    parser.add_argument(
        '--label',
        required=False,
        default=datetime.datetime.now().strftime("%Y%m%d%H%M"),
        help=('The label to use in the report. Defaults to timestamp.')
    )
    parser.add_argument(
        '--tempfolder',
        required=False,
        default='/dataflow/temp',
        help=('The temporary folder for Dataflow to use')
    )
    parser.add_argument(
        '--stagingfolder',
        required=False,
        default='/dataflow/staging',
        help=('The folder for Dataflow to use as staging')
    )


    return parser.parse_args()

ARGS = get_args()

DATASET_PREFIX = ARGS.project
REPORT_LABEL = ARGS.label
REPORT_NAME = 'Report_{}'.format(REPORT_LABEL)
DATASET_NAME = "{}:{}".format(DATASET_PREFIX, REPORT_NAME)
