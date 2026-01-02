import json


def byte_string_array(x):
    return json.dumps(x).encode("utf-8")
