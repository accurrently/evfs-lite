import datetime
import settings
from enum import Enum, IntEnum
import os, json

from bqtables import EVBQT, EnumTable, BooleanTable, FloatTable, IntegerTable
from bqtables import StringTable, ValueType, CustomTable

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
            return dict(self.signals[signal]['signal_strings'].split(",")):
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

class Events:

    RawEvent = EVBQT(
        'RawEvents',
        table = [
            {'name': 'VehicleID', 'type': 'string'},
            {'name': 'EventTime', 'type': 'timestamp'},
            {'name': 'Signal', 'type': 'string'},
            {'name': 'Value', 'type': 'string'}
        ]
    )
    IngnitionStatus = EnumTable( 'IgnitionRunStatus',
        enum_vals = 'off accessory run start'
    )
    EngineStatus = EnumTable( 'EngineStatus',
        enum_vals = 'off engine_start engine_run_CSER engine_running engine_disabled engine_start_cold_cat'
    )
    ChargerType = EnumTable( 'ChargerType',
        enum_vals = 'EVSE_Not_Detected AC_Level1_120v AC_Level2_120v DC_Fast_Charging'
    )
    ChargeStatus = EnumTable( 'ChargeStatus',
        enum_vals = 'NotReady ChargeWait BatteryChargeReady Charging ChargeComplete Faulted'
    )
    ElectricRange = FloatTable('ElectricRange')
    VehicleSpeed = FloatTable('VehicleSpeed')
    FuelSinceRestart = FloatTable('FuelSinceRestart')
    Odometer = FloatTable('Odometer')
    Latitude = FloatTable('Latitude')
    Longitude = FloatTable('Longitude')
    FuelLevel = FloatTable('FuelLevel')
    EngineSpeed = FloatTable('EngineSpeed')

    def lookup_by_signal_string(self, sig):
        d = {
            'ignition_status' : Events.IngnitionStatus,
            'engine_start' : Events.EngineStatus,
            'electric_range' : Events.ElectricRange,
            'vehicle_speed' : Events.VehicleSpeed,
            'odometer' : Events.Odometer,
            'latitude' : Events.Latitude,
            'longitude' : Events.Longitude,
            'fuel_level' : Events.FuelLevel,
            'fuel_consumed_since_restart' : Events.FuelSinceRestart,
            'engine_speed' : Events.EngineSpeed,
            'charger_type' : Events.ChargerType,
            'charge_ready_status' : Events.ChargeStatus,
            'range_per_charge_available': Events.ElectricRange
        }
        if sig in d:
            return d[sig]
        return None
