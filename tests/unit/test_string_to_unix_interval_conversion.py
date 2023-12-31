from timeseriesfuser.classes import interval_string_to_milliseconds


def test_single_day():
    assert interval_string_to_milliseconds('1d') == 86400000


def test_multi_day():
    assert interval_string_to_milliseconds('3d') == 259200000


def test_single_hour():
    assert interval_string_to_milliseconds('1h') == 3600000


def test_multi_hour():
    assert interval_string_to_milliseconds('3h') == 10800000


def test_single_minute():
    assert interval_string_to_milliseconds('1m') == 60000


def test_multi_minute():
    assert interval_string_to_milliseconds('3m') == 180000


def test_single_second():
    assert interval_string_to_milliseconds('1s') == 1000


def test_multi_second():
    assert interval_string_to_milliseconds('3s') == 3000


def test_single_millisecond():
    assert interval_string_to_milliseconds('1l') == 1


def test_multi_millisecond():
    assert interval_string_to_milliseconds('3l') == 3
