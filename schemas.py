import datetime
import settings
from enum import Enum, IntEnum
import os, json

from bqtables import StringArray, StringTable
from bqtables import FloatArray, FloatTable, NumberTable, IntegerArray, IntegerTable
from bqtables import EVBQT, CustomTable, EnumTable

class VehicleSchema:
    def __init__(self, fname):
        fullname = os.path.join("schemas", fname)
        try:
            j = json.load(fullname)
            self.Make = j['meta']['make']
            self.Model = j['meta']['model']
            self.Year = j['meta']['mod_year']
            self.Description = j['meta']['description']
            self.VType = j['meta']['vehicle_type']
            self.VClass = j['meta']['vehicle_class']
            self.Fuels = j['meta']['energy_fuels'].split(',')
            self.Ports = j['meta']['energy_ports'].split(',')

            self.signals = j['signals']

        except:
            pass # error

    def has_signal_string(self, signal, signal_str):
        if signal in self.signals:
            if signal_str in dict(self.signals[signal]['signal_strings'].split(",")):
                return True
        return False

    def supports_signal(self, signal):
        if signal in self.signals:
            return True
        return False

    def get_signal_str(self, signal):
        if signal in self.signals:
            return dict(self.signals[signal]['signal_strings'].split(","))
        return {}

    def get_table(self, signal):
        if signal in self.signals:
            if (self.signals[signal]['type'] is 'EnumTable') and ('enum_vals' in self.signals[signal]):
                return EnumTable(signal, enum_vals = self.signals[signal]['enum_vals'], signal_strings = self.get_signal_str(signal))
            elif self.signals[signal]['type'] is 'StringTable':
                return StringTable(signal, signal_strings = self.get_signal_str(signal))
            elif self.signals[signal]['type'] is 'IntegerTable':
                rmin = None
                rmax = None
                if 'range_max' in self.signals[signal]:
                    rmax = int(self.signals[signal]['range_max'])
                if 'range_min' in self.signals[signal]:
                    rmin = int(self.signals[signal]['range_min'])
                return IntegerTable(signal, range_min = rmin, range_max = rmax, signal_strings = self.get_signal_str(signal))
            elif self.signals[signal]['type'] is 'FloatTable':
                rmin = None
                rmax = None
                if 'range_max' in self.signals[signal]:
                    rmax = int(self.signals[signal]['range_max'])
                if 'range_min' in self.signals[signal]:
                    rmin = int(self.signals[signal]['range_min'])
                return FloatTable(signal, range_min = rmin, range_max = rmax, signal_strings = self.get_signal_str(signal))
            elif self.signals[signal]['type'] is 'BooleanTable':
                falses = {}
                trues = {}
                default = False
                if 'false_strings' in self.signals[signal]:
                    falses = dict(self.signals[signal]['false_strings'].split(","))
                if 'true_strings' in self.signals[signal]:
                    falses = dict(self.signals[signal]['true_strings'].split(","))
                if 'default' in self.signals[signal]:
                    default = bool(self.signals[signal]['default'])
                return BooleanTable(signal, true_strings = true, false_strings = falses, default = default, signal_strings = self.get_signal_str(signal))
        return None

    def table_list(self):
        lst = []
        for k, v in self.signals.iteritems():
            lst.append(self.get_table(k))
        return lst

class EventsClass:

    def __init__(self):

        self.RawEvent = EVBQT(
            'RawEvents',
            table = [
                {'name': 'VehicleID', 'type': 'string'},
                {'name': 'EventTime', 'type': 'timestamp'},
                {'name': 'Signal', 'type': 'string'},
                {'name': 'Value', 'type': 'string'}
            ]
        )
        self.IngnitionStatus = EnumTable( 'IgnitionRunStatus',
            enum_vals = 'off accessory run start'
        )
        self.EngineStatus = EnumTable( 'EngineStatus',
            enum_vals = 'off engine_start engine_run_CSER engine_running engine_disabled engine_start_cold_cat'
        )
        self.ChargerType = EnumTable( 'ChargerType',
            enum_vals = 'EVSE_Not_Detected AC_Level1_120v AC_Level2_120v DC_Fast_Charging'
        )
        self.ChargeStatus = EnumTable( 'ChargeStatus',
            enum_vals = 'NotReady ChargeWait BatteryChargeReady Charging ChargeComplete Faulted'
        )
        self.ElectricRange = FloatTable('ElectricRange')
        self.VehicleSpeed = FloatTable('VehicleSpeed')
        self.FuelSinceRestart = FloatTable('FuelSinceRestart')
        self.Odometer = FloatTable('Odometer')
        self.Latitude = FloatTable('Latitude')
        self.Longitude = FloatTable('Longitude')
        self.FuelLevel = FloatTable('FuelLevel')
        self.EngineSpeed = FloatTable('EngineSpeed')
        self.BatterySOC = FloatTable('BatterySOC')

    def lookup_by_signal_string(self, sig):
        d = {
            'ignition_status' : self.IngnitionStatus,
            'engine_start' : self.EngineStatus,
            'electric_range' : self.ElectricRange,
            'vehicle_speed' : self.VehicleSpeed,
            'odometer' : self.Odometer,
            'latitude' : self.Latitude,
            'longitude' : self.Longitude,
            'fuel_level' : self.FuelLevel,
            'fuel_consumed_since_restart' : self.FuelSinceRestart,
            'engine_speed' : self.EngineSpeed,
            'charger_type' : self.ChargerType,
            'charge_ready_status' : self.ChargeStatus,
            'range_per_charge_available': self.ElectricRange,
            'battery_level' : self.BatterySOC
        }
        if sig in d:
            return d[sig]
        return None

Events = EventsClass()

class ReportsClass:
    def __init__(self):
        self.Trips = CustomTable( 'Trips',
            table = [
                {'name': 'VehicleID', 'type': 'string'},
                {'name': 'StartTime', 'type': 'timestamp'},
                {'name': 'EndTime', 'type': 'timestamp'},
                {'name': 'Duration', 'type': 'float'},
                {'name': 'FuelConsumed', 'type': 'float'},
                {'name': 'TankConsumed', 'type': 'float'},
                {'name': 'BatterySOCMax', 'type': 'float'},
                {'name': 'BatterySOCMin', 'type': 'float'},
                {'name': 'BatterySOCDelta', 'type': 'float'},
                {'name': 'Distance', 'type': 'float'},
                {'name': 'SpeedMax', 'type': 'float'},
                {'name': 'SpeedAvg', 'type': 'float'},
                {'name': 'ElectricRangeConsumed', 'type': 'float'},
                {'name': 'StartLatitude', 'type': 'float'},
                {'name': 'StartLongitude', 'type': 'float'},
                {'name': 'EndLatitude', 'type': 'float'},
                {'name': 'EndLongitude', 'type': 'float'},
                {'name': 'StartLatong', 'type': 'string'},
                {'name': 'EndLatLong', 'type': 'string'}
            ]
        )
        self.Stops = CustomTable( 'Stops',
            table = [
                {'name': 'VehicleID', 'type': 'string'},
                {'name': 'StartTime', 'type': 'timestamp'},
                {'name': 'EndTime', 'type': 'timestamp'},
                {'name': 'Duration', 'type': 'float'},
                {'name': 'Charged', 'type': 'boolean'},
                {'name': 'ChargeCompleted', 'type': 'boolean'},
                {'name': 'HighestChargeLevel', 'type': 'integer'},
                {'name': 'ChargeFault', 'type': 'boolean'},
                {'name': 'Latitude', 'type': 'float'},
                {'name': 'Longitude', 'type': 'float'},
                {'name': 'LatLong', 'type': 'string'}
            ]
        )

Reports = ReportsClass()

class ArraysClass:
    def __init__(self):
        self.VehicleIDs = StringArray('VehicleIDs')

Arrays = ArraysClass()
