"""
Copyright (C) 2023-2024 Anthony Sweeney - email: safsweeney@gmail.com

Please view the LICENSE file for the terms and conditions
associated with this software.
"""
import pytest
import polars as pl
from pathlib import Path
from timeseriesfuser.classes import DataInfo, ExampleHandler
from timeseriesfuser.core import TimeSeriesFuser
from timeseriesfuser.datasources import CSVSrc
from timeseriesfuser.helpers.helpers import toutcisotime


def convert_time_format(time_column: str) -> pl.Expr:
    #  this uses the rust chrono library to convert the timestamp column to a datetime column.
    #  In this instance, the chrono library is smart enough to automatically infer the format
    #  of the datetime string.
    return (
        pl.col(time_column).str.strptime(pl.Datetime).dt.epoch(time_unit="ms")
    )


@pytest.fixture
def setup_csv_tsf():
    hdlr = ExampleHandler()
    test_data_dir = Path(__file__).parent / "data/timestamps_start_end_fmt_conversion/csv"
    datar = CSVSrc(files_path=test_data_dir)
    data_info = DataInfo(
        descriptor='testdata_timestamp_conversion',
        datareader=datar,
        datatypes=[pl.Int64, pl.Utf8],
        timestamp_col_name='str_iso_timestamp',
        convert_timestamp_function=convert_time_format)
    tsf = TimeSeriesFuser(datainfos=[data_info],
                          handler=hdlr
                          )
    return tsf


def test_core_datainfo_start_timestamp_csv_conversion(setup_csv_tsf):
    """
    Test that the start timestamp is identified correctly from the CSV file.
    """
    di = setup_csv_tsf.datainfos[0]
    start_timestamp = setup_csv_tsf._get_global_proc_start_end_from_files('start', datainfo=di)
    assert start_timestamp == 1577836800000
    # toutcisotime(start_timestamp): '2020-01-01T00:00:00+00:00'


def test_core_datainfo_end_timestamp_csv_conversion(setup_csv_tsf):
    """
        Test that the end timestamp is identified correctly from the CSV file.
        """
    di = setup_csv_tsf.datainfos[0]
    end_timestamp = setup_csv_tsf._get_global_proc_start_end_from_files('end', datainfo=di)
    assert end_timestamp == 1984994346000
    # toutcisotime(end_timestamp): '2032-11-25T11:19:06+00:00'
