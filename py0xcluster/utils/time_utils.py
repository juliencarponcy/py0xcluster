from datetime import datetime, timedelta
import time 

def timestamp_tuple_to_unix(timestamp_tuple):
    if isinstance(timestamp_tuple, tuple):
        date_time = datetime(*timestamp_tuple)
    elif isinstance(timestamp_tuple, datetime):
        date_time = timestamp_tuple
    unix_ts = int(time.mktime(date_time.timetuple()))
    return unix_ts

def batch(iterable, n:int=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]

def days_interval_tuples(
        start_date:tuple, 
        end_date:tuple, 
        days_batch_size:int):
    '''
    Construct a generator of date limits over an interval
    based on a batch size in days.
    Return a generator which outputs a list of tuples of two datetime
    (start and end of the batch)

    Example: 

    days_batch_lim = [dates_lim for dates_lim in days_interval(start_date, end_date, days_batch_size)]

    >> [(datetime(2022, 6, 10, 0, 0), datetime(2022, 6, 13, 0, 0)),
        (datetime(2022, 6, 13, 0, 0), datetime(2022, 6, 16, 0, 0)),
        (datetime(2022, 6, 16, 0, 0), datetime(2022, 6, 19, 0, 0)),
        (datetime(2022, 6, 19, 0, 0), datetime(2022, 6, 22, 0, 0)),
        (datetime(2022, 6, 22, 0, 0), datetime(2022, 6, 25, 0, 0)),
        (datetime(2022, 6, 25, 0, 0), datetime(2022, 6, 28, 0, 0)),
        (datetime(2022, 6, 28, 0, 0), datetime(2022, 6, 30, 0, 0))]
    '''
    start = datetime(*start_date)
    end = datetime(*end_date)
    curr = start
    while curr < end:
        yield (curr, min(end, curr + timedelta(days_batch_size)))
        curr += timedelta(days_batch_size)