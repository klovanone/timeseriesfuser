from timeseriesfuser.classes import get_next_interval
from timeseriesfuser.helpers.helpers import toutcisotime

ROOT_TIMESTAMP = 1627776123213  # 2021-08-01T00:02:03.213Z - random timestamp to test functions


def test_next_intervals_days():
    isotime_str = toutcisotime(get_next_interval(ROOT_TIMESTAMP, '1d'))
    assert isotime_str == '2021-08-02T00:02:03.213000+00:00'


def test_next_intervals_days_initialize():
    isotime_str = toutcisotime(get_next_interval(ROOT_TIMESTAMP, '1d', initialize=True))
    assert isotime_str == '2021-08-02T00:00:00+00:00'


def test_next_intervals_hours():
    isotime_str = toutcisotime(get_next_interval(ROOT_TIMESTAMP, '1h'))
    assert isotime_str == '2021-08-01T01:02:03.213000+00:00'


def test_next_intervals_hours_initialize():
    isotime_str = toutcisotime(get_next_interval(ROOT_TIMESTAMP, '1h', initialize=True))
    assert isotime_str == '2021-08-01T01:00:00+00:00'


def test_next_intervals_minutes():
    isotime_str = toutcisotime(get_next_interval(ROOT_TIMESTAMP, '1h'))
    assert isotime_str == '2021-08-01T01:02:03.213000+00:00'


def test_next_intervals_minutes_initialize():
    isotime_str = toutcisotime(get_next_interval(ROOT_TIMESTAMP, '1m', initialize=True))
    assert isotime_str == '2021-08-01T00:03:00+00:00'


def test_next_intervals_seconds():
    isotime_str = toutcisotime(get_next_interval(ROOT_TIMESTAMP, '1s'))
    assert isotime_str == '2021-08-01T00:02:04.213000+00:00'


def test_next_intervals_seconds_initialize():
    isotime_str = toutcisotime(get_next_interval(ROOT_TIMESTAMP, '1s', initialize=True))
    assert isotime_str == '2021-08-01T00:02:04+00:00'


def test_next_intervals_milliseconds():
    isotime_str = toutcisotime(get_next_interval(ROOT_TIMESTAMP, '1l'))
    assert isotime_str == '2021-08-01T00:02:03.214000+00:00'


def test_next_intervals_milliseconds_initialize():
    isotime_str = toutcisotime(get_next_interval(ROOT_TIMESTAMP, '1l', initialize=True))
    assert isotime_str == '2021-08-01T00:02:03.214000+00:00'
