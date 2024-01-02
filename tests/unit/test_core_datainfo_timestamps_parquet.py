"""
Copyright (C) 2023-2024 Anthony Sweeney - email: safsweeney@gmail.com

Please view the LICENSE file for the terms and conditions
associated with this software.
"""
import pytest
import polars as pl
from timeseriesfuser.classes import DataInfo, ExampleHandler
from timeseriesfuser.core import TimeSeriesFuser
from timeseriesfuser.datasources import ParquetSrc
from timeseriesfuser.helpers.helpers import toutcisotime


@pytest.fixture
def setup_parquet_tsf():
    hdlr = ExampleHandler()
    datar = ParquetSrc(files_path='./data/timestamps_start_end/parquet')
    data_info = DataInfo(
        descriptor='testdata',
        datareader=datar,
        datatypes=[pl.Int64, pl.Utf8],
        timestamp_col_name='timestamp')
    tsf = TimeSeriesFuser(datainfos=[data_info],
                          handler=hdlr
                          )
    return tsf


def test_core_datainfo_start_timestamp_parquet(setup_parquet_tsf):
    """
    Test that the start timestamp is identified correctly from the parquet file. While it is
    usually the case to deal with the public interface only when testing, due to the nature of
    the TimeSeriesFuser class, it is preferable to test the private method
    _forward_fill_dataframe directly.
    """
    di = setup_parquet_tsf.datainfos[0]
    start_timestamp = setup_parquet_tsf._get_global_proc_start_end_from_files('start', datainfo=di)
    assert start_timestamp == 1577836800000
    # toutcisotime(start_timestamp): '2020-01-01T00:00:00+00:00'


def test_core_datainfo_end_timestamp_parquet(setup_parquet_tsf):
    """
    Test that the end timestamp is identified correctly from the parquet file. While it is
    usually the case to deal with the public interface only when testing, due to the nature of
    the TimeSeriesFuser class, it is preferable to test the private method
    _forward_fill_dataframe directly.
    """
    di = setup_parquet_tsf.datainfos[0]
    end_timestamp = setup_parquet_tsf._get_global_proc_start_end_from_files('end', datainfo=di)
    assert end_timestamp == 1984994346000
    # toutcisotime(end_timestamp): '2032-11-25T11:19:06+00:00'
