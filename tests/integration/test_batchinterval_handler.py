"""
Copyright (C) 2023-2024 Anthony Sweeney - email: safsweeney@gmail.com

Please view the LICENSE file for the terms and conditions
associated with this software.
"""
import datetime
import pytest
import polars as pl
from pathlib import Path

from timeseriesfuser.classes import BatchEveryIntervalHandler


@pytest.fixture
def setup_parquet_data_1second():
    fpath = Path('./data/interval_handler/1second_letters.parquet').resolve()
    df = pl.read_parquet(source=fpath)
    columns = df.columns
    timestamp_col = 'Timestamp'
    return df, columns, timestamp_col


@pytest.fixture
def data_1s(setup_parquet_data_1second):
    df, columns, timestamp_col = setup_parquet_data_1second
    mdf = df[0:200, :]
    return mdf, columns, timestamp_col


@pytest.fixture
def handler_1s(mocker):
    bihandler = BatchEveryIntervalHandler(batch_interval='1s',
                                          output_path='./data/interval_handler/output',
                                          save_every_n_batch=10000000000,  #  never save
                                          disable_pl_inference=True,
                                          store_full=True)
    mocker.patch.object(bihandler, '_file_write', return_value=None, autospec=True)
    return bihandler


@pytest.fixture
def setup_parquet_data_1minute():
    fpath = Path('./data/interval_handler/1minute_letters.parquet').resolve()
    df = pl.read_parquet(source=fpath)
    columns = df.columns
    timestamp_col = 'Timestamp'
    return df, columns, timestamp_col


@pytest.fixture
def data_1m(setup_parquet_data_1minute):
    df, columns, timestamp_col = setup_parquet_data_1minute
    mdf = df[0:200, :]
    return mdf, columns, timestamp_col


@pytest.fixture
def handler_1m(mocker):
    bihandler = BatchEveryIntervalHandler(batch_interval='1m',
                                          output_path='./data/interval_handler/output',
                                          save_every_n_batch=10000000000,    # never save for test
                                          disable_pl_inference=True,
                                          store_full=True)
    mocker.patch.object(bihandler, '_file_write', return_value=None, autospec=True)
    return bihandler


@pytest.fixture
def setup_parquet_data_1day():
    fpath = Path('./data/interval_handler/1day_letters.parquet').resolve()
    df = pl.read_parquet(source=fpath)
    columns = df.columns
    timestamp_col = 'Timestamp'
    return df, columns, timestamp_col


@pytest.fixture
def data_1d(setup_parquet_data_1day):
    df, columns, timestamp_col = setup_parquet_data_1day
    mdf = df[0:200, :]
    return mdf, columns, timestamp_col


@pytest.fixture
def handler_1d(mocker):
    bihandler = BatchEveryIntervalHandler(batch_interval='1d',
                                          output_path='./data/interval_handler/output',
                                          save_every_n_batch=10000000000,   # never save for test
                                          disable_pl_inference=True,
                                          store_full=True)
    mocker.patch.object(bihandler, '_file_write', return_value=None, autospec=True)
    return bihandler


def test_1m_batcher(data_1m, handler_1m):
    df, columns, timestamp_col = data_1m
    columns_idxs = []
    for cn in range(0, len(columns)):
        columns_idxs.append(df[:, cn].to_list())
    for row in zip(*columns_idxs):
        msg = {k: v for k, v in zip(columns, row)}
        ts = msg[timestamp_col]
        handler_1m.process(ts, msg)
    handler_1m.finalize()
    df = pl.DataFrame(handler_1m.full_data).with_columns(
        pl.from_epoch("__timestamp", time_unit="ms")
    )
    verify_ts = df[0:5].select('__timestamp').get_columns()[0].to_list()
    correct_ts = [datetime.datetime(2020, 1, 1, 0, 1),
                  datetime.datetime(2020, 1, 1, 0, 2),
                  datetime.datetime(2020, 1, 1, 0, 3),
                  datetime.datetime(2020, 1, 1, 0, 4),
                  datetime.datetime(2020, 1, 1, 0, 5)]
    verify_column = df[0:5].select('Letter').get_columns()[0].to_list()
    correct_column = ['A', 'B', 'C', 'D', 'E']
    assert verify_ts == correct_ts and verify_column == correct_column


def test_1d_batcher(data_1d, handler_1d):
    df, columns, timestamp_col = data_1d
    columns_idxs = []
    for cn in range(0, len(columns)):
        columns_idxs.append(df[:, cn].to_list())
    for row in zip(*columns_idxs):
        msg = {k: v for k, v in zip(columns, row)}
        ts = msg[timestamp_col]
        handler_1d.process(ts, msg)
    handler_1d.finalize()
    df = pl.DataFrame(handler_1d.full_data).with_columns(
        pl.from_epoch("__timestamp", time_unit="ms")
    )
    verify_ts = df[0:5].select('__timestamp').get_columns()[0].to_list()
    correct_ts = [datetime.datetime(2020, 1, 2, 0, 0),
                  datetime.datetime(2020, 1, 3, 0, 0),
                  datetime.datetime(2020, 1, 4, 0, 0),
                  datetime.datetime(2020, 1, 5, 0, 0),
                  datetime.datetime(2020, 1, 6, 0, 0)]
    verify_column = df[0:5].select('Letter').get_columns()[0].to_list()
    correct_column = ['A', 'B', 'C', 'D', 'E']
    assert verify_ts == correct_ts and verify_column == correct_column

