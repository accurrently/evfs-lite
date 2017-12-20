import settings
import json, os

def load_json_list(signal_name):
    fname = "{sig}.json".format(sig=signal_name)
    return json.load(open(os.path.join('signalfiles', fname)))

def load_sql(filename):
    path = os.path.join('sql', filename)
    q = None
    with open(path, 'r') as thefile:
        q = thefile.read().replace('\n', '').replace('\t', ' ')
    return q

def correct_time(ts):
    if ts >= 10000000000:
        return ts/1000
    return ts
