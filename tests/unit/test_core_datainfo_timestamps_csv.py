"""
Copyright (C) 2023-2024 Anthony Sweeney - email: safsweeney@gmail.com

Please view the LICENSE file for the terms and conditions
associated with this software.
"""
import pytest
import polars as pl
from timeseriesfuser.classes import DataInfo, ExampleHandler
from timeseriesfuser.core import TimeSeriesFuser
from timeseriesfuser.datasources import CSVSrc
from timeseriesfuser.helpers.helpers import toutcisotime


@pytest.fixture
def setup_csv_tsf():
    hdlr = ExampleHandler()
    datar = CSVSrc(files_path='./data/timestamps_start_end/csv')
    data_info = DataInfo(
        descriptor='testdata',
        datareader=datar,
        datatypes=[pl.Int64, pl.Utf8],
        timestamp_col_name='timestamp')
    tsf = TimeSeriesFuser(datainfos=[data_info],
                          handler=hdlr
                          )
    tsf.start_tsf()
    return tsf


def test_core_datainfo_start_timestamp_csv(setup_csv_tsf):
    """
    Test that the start timestamp is identified correctly from the CSV file.
    """
    di = setup_csv_tsf.datainfos[0]
    start_timestamp = setup_csv_tsf._get_global_proc_start_end_from_files('start', datainfo=di)
    assert start_timestamp == 1577836800000
    # toutcisotime(start_timestamp): '2020-01-01T00:00:00+00:00'


def test_core_datainfo_end_timestamp_csv(setup_csv_tsf):
    """
        Test that the end timestamp is identified correctly from the CSV file.
        """
    di = setup_csv_tsf.datainfos[0]
    end_timestamp = setup_csv_tsf._get_global_proc_start_end_from_files('end', datainfo=di)
    assert end_timestamp == 1984994346000
    # toutcisotime(end_timestamp): '2032-11-25T11:19:06+00:00'
