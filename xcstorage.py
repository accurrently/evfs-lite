from google.cloud import storage
from pathlib import Path
import argparse
import logging
import re, json, datetime, os, hashlib

def get_vehicle_folders(bucket_name='dumbdata'):
    """get the vehicle ids"""
    client = storage.Client()
    logging.info(client)
    bucket = client.get_bucket(bucket_name)
    logging.info(bucket)
    blobs = bucket.list_blobs(prefix='ingest')
    l = []
    for item in blobs:
        path = item.name.strip()
        if Path(item.name.strip()).suffix == '.gz':
            vehicle_id = path.split('/')[1]
            if not any(d['vehicle_id'] == vehicle_id for d in l):
                logging.info('Added vehicle \'{}\'.'.format(vehicle_id))
                files_path = 'gs://' + bucket_name + '/ingest/' + vehicle_id + '/*.gz'
                l.append({'vehicle_id': vehicle_id, 'path': files_path})
    logging.info('Found {} vechicle(s)...'.format(len(l)))
    return l

def move_xcfiles(file_list, bucket_name='dumbdata'):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)

    for obj in file_list:
        oldpath = obj['rawpath']
        blob = bucket.blob(oldpath)
        pathparts = oldpath.split('/')[1:]
        newpath = 'digested'
        for part in pathparts:
            newpath += '/{}'.format(part)
        logging.info('Moving blob from \'{}\' to \'{}\''.format(oldpath, newpath))
        bucket.rename_blob(blob, newpath)

def get_xc_files(bucket_name='dumbdata'):
    """Lists all the blobs in the bucket."""
    coll = []
    client = storage.Client()
    logging.info(client)
    bucket = client.get_bucket(bucket_name)
    logging.info(bucket)
    blobs = bucket.list_blobs(prefix='ingest')
    l = []
    for item in blobs:
        l.append(str(item.name).strip())

    logging.info(l)
    logging.info('{} blobs found.'.format(len(l)))

    #xcfilelist = []
    for path in l:
        #logging.info(path)
    #    logging.info('suffix: {}'.format(Path(path.strip()).suffix))
#        logging.info('Suffix: \'{}\''.format(ext))
        if Path(path.strip()).suffix == '.gz':
            logging.info('XC File: {}'.format(path))
            car_id = path.split('/')[1]
            file_path = 'gs://' + bucket_name + '/' + path
            file_id = hashlib.sha256(path.split('/')[-1]).hexdigest()
            d = {'vehicle_id': car_id, 'file_path': file_path, 'xcfile_id': file_id, 'rawpath': path }
            coll.append(d)
    return coll
