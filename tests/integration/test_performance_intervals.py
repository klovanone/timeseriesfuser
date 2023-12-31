import itertools
from datetime import datetime, timedelta, timezone
from timeseriesfuser.classes import interval_string_to_milliseconds, get_next_interval


# function to increment the timestamp by 1 millisecond 100000 times using datetime
# quick and dirty just to confirm that the timeseriesfuser style is faster since it is simply
# integer addition (with less features!)

def increment_timestamp_py():
    start_time = datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    begin = datetime.now()
    for i in range(10000000):
        start_time += timedelta(milliseconds=1)
    end = datetime.now()
    return (end - begin).total_seconds()


# timeseriesfuserstyle
def increment_timestamp_tsf():
    #  approximate translation from batcher code
    start_time_ts = int(datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
    batch_interval_ms = interval_string_to_milliseconds('1l')
    begin = datetime.now()
    next_batch_ts = get_next_interval(start_time_ts, '1l', initialize=True)
    #  create a generator which will return the next batch interval timestamp forever
    next_batch_gen = itertools.count(start=next_batch_ts, step=batch_interval_ms)
    for i in range(10000000):
        dummy_ts = next(next_batch_gen)
    end = datetime.now()
    return (end - begin).total_seconds()


def test_basic_time_logic():
    python_time = increment_timestamp_py()
    tsf_time = increment_timestamp_tsf()
    #  calculate the percentage faster:
    times_faster = python_time / tsf_time
    print(f'python_time: {python_time} seconds, timeseriesfuster_time: {tsf_time} seconds. '
          f'TSF is {times_faster:.2f}x faster.')
    assert tsf_time < python_time
