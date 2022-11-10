'''
Build tools to construct custom queries
'''
import datetime
import time 

def timestamp_tuple_to_unix(timestamp_tuple):
    if isinstance(timestamp_tuple, tuple):
        date_time = datetime.datetime(*timestamp_tuple)
    elif isinstance(timestamp_tuple, datetime.datetime):
        date_time = timestamp_tuple
    unix_ts = int(time.mktime(date_time.timetuple()))
    return unix_ts

class Query():

    def __init__(self):
        ...

    def get(self) -> str:
        return self.query   