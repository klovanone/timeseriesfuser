"""
Copyright (C) 2023-2024 Anthony Sweeney - email: safsweeney@gmail.com

Please view the LICENSE file for the terms and conditions
associated with this software.
"""
from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from timeseriesfuser.classes import DataInfo
from timeseriesfuser.datasources import CSVSrc

# fixtures created for future testing re-use

@pytest.fixture
def datainfo_listfmt():
    path_list = Path(__file__).parent / 'data/datatypes_temperature_data'
    datar_listfmt = CSVSrc(files_path=path_list)
    data_info = DataInfo(
        descriptor='test_datainfo_datatypes',
        datareader=datar_listfmt,
        datatypes=[int, datetime, float, str, int, bool],
        timestamp_col_name='Timestamp')
    return data_info


@pytest.fixture
def datainfo_dataframe_list(datainfo_listfmt):
    df = pl.read_csv(datainfo_listfmt.files_path / 'temperature_data.csv',
                     dtypes=datainfo_listfmt.datatypes)
    return df


@pytest.fixture
def datainfo_dictfmt():
    datapath = Path(__file__).parent / 'data/datatypes_temperature_data'
    datar_dictfmt = CSVSrc(files_path=datapath)
    data_info = DataInfo(
        descriptor='test_datainfo_datatypes',
        datareader=datar_dictfmt,
        datatypes={'Timestamp': int,
                   'Date': datetime,
                   'DailyMinTemp': float,
                   'WeatherType': str,
                   'ReadingNum': int,
                   'IsHot': bool},
        timestamp_col_name='Timestamp')
    return data_info


@pytest.fixture
def datainfo_dataframe_list(datainfo_listfmt):
    df = pl.read_csv(datainfo_listfmt.datareader.files_path / 'temperature_data.csv',
                     dtypes=datainfo_listfmt.datatypes)
    return df


@pytest.fixture
def datainfo_dataframe_dict(datainfo_dictfmt):
    df = pl.read_csv(datainfo_dictfmt.datareader.files_path / 'temperature_data.csv',
                     dtypes=datainfo_dictfmt.datatypes)
    return df


@pytest.mark.filterwarnings('ignore::UserWarning')
@pytest.mark.parametrize("index, expected_type",
                         [(1, pl.Datetime), (2, pl.Float64), (3, pl.Utf8), (4, pl.Int64),
                          (5, pl.Boolean)]
                         )
def test_datainfo_datatypes_verifcation_list(index, expected_type, datainfo_dataframe_list):
    """
    Test that the datatypes of the datainfo object are correctly identified and converted from the
    datatypes parameter passed via list format with python datatypes.
    The correct datatypes are:
    data_dict = {
    'Timestamp': pl.Int64,
    'Date': pl.Datetime,
    'DailyMinTemp': pl.Float64,
    'WeatherType': pl.Utf8,
    'ReadingNum': pl.Int64,
    'IsHot': pl.Boolean}
    """
    assert datainfo_dataframe_list.dtypes[
               index] == expected_type, f"Failed for index {index}: expected {expected_type}," \
                                        f"got {datainfo_dataframe_list.datatypes[index]}"


@pytest.mark.filterwarnings('ignore::UserWarning')
@pytest.mark.parametrize("index, expected_type",
                         [(1, pl.Datetime), (2, pl.Float64), (3, pl.Utf8), (4, pl.Int64),
                          (5, pl.Boolean)]
                         )
def test_datainfo_datatypes_internal_class_values_list(index, expected_type, datainfo_listfmt):
    assert datainfo_listfmt.datatypes[
               index] == expected_type, f"Failed for index {index}: expected {expected_type}," \
                                        f"got {datainfo_listfmt.datatypes[index]}"


@pytest.mark.filterwarnings('ignore::UserWarning')
@pytest.mark.parametrize("index, expected_type",
                         [(1, pl.Datetime), (2, pl.Float64), (3, pl.Utf8), (4, pl.Int64),
                          (5, pl.Boolean)]
                         )
def test_datainfo_datatypes_verifcation_dict(index, expected_type, datainfo_dataframe_dict):
    """
    Test that the datatypes of the datainfo object are correctly identified and converted from the
    datatypes parameter passed via dictionary format with python datatypes.
    The correct datatypes are:
    data_dict = {
    'Timestamp': pl.Int64,
    'Date': pl.Datetime,
    'DailyMinTemp': pl.Float64,
    'WeatherType': pl.Utf8,
    'ReadingNum': pl.Int64,
    'IsHot': pl.Boolean}
    """
    assert datainfo_dataframe_dict.dtypes[
               index] == expected_type, f"Failed for index {index}: expected {expected_type}," \
                                        f"got {datainfo_dataframe_dict.datatypes[index]}"


@pytest.mark.filterwarnings('ignore::UserWarning')
@pytest.mark.parametrize("keys, values",
                         {'Timestamp': pl.Int64,
                          'Date': pl.Datetime,
                          'DailyMinTemp': pl.Float64,
                          'WeatherType': pl.Utf8,
                          'ReadingNum': pl.Int64,
                          'IsHot': pl.Boolean}.items()
                         )
def test_datainfo_datatypes_internal_class_values_dict(keys, values, datainfo_dictfmt):
    assert datainfo_dictfmt.datatypes[keys] == values, \
        f"Failed for keys {keys}: expected {values}, got {datainfo_dictfmt.datatypes[keys]}"
