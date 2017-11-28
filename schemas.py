import datetime
import json
import settings
from apache_beam.io.gcp.internal.clients import bigquery

DATASET_PREFIX = settings.ARGS.project + ':'
REPORT_TIMESTAMP_STR = settings.ARGS.label
DATASET_NAME_NOPROJECT = 'Report_{ts}'.format(ts=REPORT_TIMESTAMP_STR)
DATASET_NAME = DATASET_PREFIX + DATASET_NAME_NOPROJECT

def bq_table(table_dict_array):
    tbl = bigquery.TableSchema()
    for d in table_dict_array:
        fs = bigquery.TableFieldSchema()
        if 'name' in d.keys():
            fs.name = d['name']
        if 'type' in d.keys():
            fs.type = d['type']
        tbl.fields.append(fs)
    return tbl

def load_json_list(signal_name):
    return json.load(open("signalfiles/{sig}.json".format(sig=signal_name)))

class EVBQT:
    """
    EV Big Query Table base class
    """
    schema = bigquery.TableSchema()
    table_name = 'None'
    full_table_name = DATASET_NAME + '.' + table_name
    signal_strings = {}


class RawEvents(EVBQT):
    """
    Raw events that will be stored as strings. This is in case we want to do
    anyhting with the data that we haven't thought of yet.
    """
    table_name = 'RawEvents'
    full_table_name = DATASET_NAME + '.' + table_name
    schema =  bq_table([
        {'name': 'VehicleID', 'type': 'string'},
        {'name': 'EventTime', 'type': 'timestamp'},
        {'name': 'Signal', 'type': 'string'},
        {'name': 'Value', 'type': 'string'}
    ])

class IgnitionRunStatus(EVBQT):
    """
    Ignition (vehicle power) status signal table.
    Value will be True for ignition status set to "run", and False for "off".
    """
    table_name = 'IgnitionRunStatus'
    full_table_name = DATASET_NAME + '.' table_name
    schema = bq_table([
        {'name': 'VehicleID', 'type': 'string'},
        {'name': 'EventTime', 'type': 'timestamp'},
        {'name': 'Value', 'type': 'boolean'}
    ])
    signal_strings = load_json_list('IgnitionRunStatus')

class ElectricRange(EVBQT):
    """
    Remaining all-electric range of the vehicle
    """
    table_name = 'ElectricRange'
    full_table_name = DATASET_NAME + '.' table_name
    schema = bq_table([
        {'name': 'VehicleID', 'type': 'string'},
        {'name': 'EventTime', 'type': 'timestamp'},
        {'name': 'Value', 'type': 'float'}
    ])
    signal_strings = load_json_list('ElectricRange')

class EngineRunStatus(EVBQT):
    """
    Engine (ICE) status signal table.
    Value will be True for ignition status set to "run", and False for "off".
    """
    table_name = 'EngineRunStatus'
    full_table_name = DATASET_NAME + '.' table_name
    schema = bq_table([
        {'name': 'VehicleID', 'type': 'string'},
        {'name': 'EventTime', 'type': 'timestamp'},
        {'name': 'Value', 'type': 'boolean'}
    ])
    signal_strings = load_json_list('EngineRunStatus')

class EngineSpeed(EVBQT):
    """
    Engine speed reading in RPM.
    """
    table_name = 'EngineSpeed'
    full_table_name = DATASET_NAME + '.' table_name
    schema = bq_table([
        {'name': 'VehicleID', 'type': 'string'},
        {'name': 'EventTime', 'type': 'timestamp'},
        {'name': 'Value', 'type': 'float'}
    ])
    signal_strings = load_json_list('EngineSpeed')

class Odometer(EVBQT):
    """
    Odometer reading in km.
    """
    table_name = 'Odometer'
    full_table_name = DATASET_NAME + '.' table_name
    schema = bq_table([
        {'name': 'VehicleID', 'type': 'string'},
        {'name': 'EventTime', 'type': 'timestamp'},
        {'name': 'Value', 'type': 'float'}
    ])
    signal_strings = load_json_list('Odometer')

class Latitude(EVBQT):
    """
    Latitude in degrees
    """
    table_name = "Latitude"
    full_table_name = DATASET_NAME + '.' table_name
    schema = bq_table([
        {'name': 'VehicleID', 'type': 'string'},
        {'name': 'EventTime', 'type': 'timestamp'},
        {'name': 'Value', 'type': 'float'}
    ])
    signal_strings = load_json_list('Latitude')

class Longitude(EVBQT):
    """
    Longitude in degrees
    """
    table_name = "Longitude"
    full_table_name = DATASET_NAME + '.' table_name
    schema = bq_table([
        {'name': 'VehicleID', 'type': 'string'},
        {'name': 'EventTime', 'type': 'timestamp'},
        {'name': 'Value', 'type': 'float'}
    ])
    signal_strings = load_json_list('Longitude')

class FuelSinceRestart(EVBQT):
    """
    Fuel used since restart in l.
    """
    table_name = 'Odometer'
    full_table_name = DATASET_NAME + '.' table_name
    schema = bq_table([
        {'name': 'VehicleID', 'type': 'string'},
        {'name': 'EventTime', 'type': 'timestamp'},
        {'name': 'Value', 'type': 'float'}
    ])
    signal_strings = load_json_list('FuelSinceRestart')

class Trips(EVBQT):
    """
    Trips that are taken, as defined by the ignition status set to 'run'.
    """
    table_name = 'Trips'
    full_table_name = DATASET_NAME + '.' + table_name
    schema = bq_table([
        {'name': 'VehicleID', 'type': 'string'},
        {'name': 'StartTime', 'type': 'timestamp'},
        {'name': 'EndTime', 'type': 'timetamp'},
        {'name': 'DistanceTraveled', 'type': 'float'},
        {'name': 'EngineStarts', 'type': 'integer'},
        {'name': 'ElectricDistance', 'type': 'float'},
        {'name': 'ElectricityUsed', 'type': 'float'},
        {'name': 'FuelDistance', 'type': 'float'},
        {'name': 'FuelUsed', 'type': 'float'},
    ])


class Stops(EVBQT):
    """
    Stops that are taken as defined by the ignition status set to 'off'
    """
    table_name = 'Stops'
    full_table_name = DATASET_NAME + '.' + table_name
    schema = bq_table([
        {'name': 'VehicleID', 'type': 'string'},
        {'name': 'StartTime', 'type': 'timestamp'},
        {'name': 'EndTime', 'type': 'timestamp'},
        {'name': 'Latitude', 'type': 'float'},
        {'name': 'Longitude', 'type': 'float'},
        {'name': 'GPS', 'type': 'string'},
#        {'name': 'BattryChargeAdded', 'type': 'float'},
        {'name': 'PluggedIn', 'type': 'boolean'},
#        {'name': 'NearbyChargers', 'type': 'integer'},
        {'name': 'ChargeEvent', 'type': 'boolean'},
#        {'name': 'PotentialChargeEvent', 'type': 'boolean'}
    ])

class GPSTrace(EVBQT):
    """
    GPS Traces, averaged over 1 minute intervals
    """
    table_name = 'GPSTraces'
    full_table_name = DATASET_NAME + '.' + table_name
    schema = bq_table([
        {'name': 'VehicleID', 'type': 'string'},
        {'name': 'Time', 'type': 'timestamp'},
        {'name': 'Latitude', 'type': 'float'},
        {'name': 'Longitude', 'type': 'float'},
    ])

class FleetDailyStats(EVBQT):
    """
    Aggregation of daily statistics
    """
    table_name = 'FleetDailyStatistics'
    full_table_name = DATASET_NAME + '.' + table_name
    schema = bq_table([
        {'name': 'Date', 'type': 'timestamp'},
        {'name': 'TotalDistance', 'type': 'float'},
        {'name': 'TotalElectricDistance', 'type': 'float'},
        {'name': 'TotalFuelDistance', 'type': 'float'},
        {'name': 'ChargeEvents', 'type': 'integer'},
        {'name': 'MissedChargeEvents', 'type': 'integer'},
        {'name': 'PotentialChargeEvents', 'type': 'integer'},
        {'name': 'TotalFuelUsed', 'type': 'float'},
        {'name': 'TotalElectricityUsed', 'type': 'float'},
        {'name': 'TotalFuelAdded', 'type': 'float'},
        {'name': 'TotalElectricityAdded', 'type': 'float'}
    ])
