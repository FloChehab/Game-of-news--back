import simplejson
import datetime
from numpy import int64

def other_types_handler(x):
    if isinstance(x, datetime.datetime):
        return x.isoformat() + "Z"
    if isinstance(x, int64):
        return int(x)
    raise TypeError("Unknown type")


JSON_CONFIG = dict(ignore_nan=True, default=other_types_handler)


def json_dumps(content):
    return simplejson.dumps(content, **JSON_CONFIG)


def dict_to_json_file(content, fp):
    with open(fp, 'w') as f:
        simplejson.dump(content, f, **JSON_CONFIG)


def json_to_dict(fp):
    with open(fp, 'r') as f:
        return simplejson.load(f)
