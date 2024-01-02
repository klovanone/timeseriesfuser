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
def setup_parquet_data_1second_fill():
    fpath = Path('./data/interval_handler/1second_letters_gaps.parquet').resolve()
    df = pl.read_parquet(source=fpath)
    columns = df.columns
    timestamp_col = 'Timestamp'
    return df, columns, timestamp_col


@pytest.fixture
def data_1s_fill(setup_parquet_data_1second_fill):
    df, columns, timestamp_col = setup_parquet_data_1second_fill
    mdf = df[0:200, :]
    return mdf, columns, timestamp_col


@pytest.fixture
def handler_1s_fill(mocker):
    bihandler = BatchEveryIntervalHandler(batch_interval='1s',
                                          output_path='./data/interval_handler/output',
                                          save_every_n_batch=10000000000,  # never save for test
                                          disable_pl_inference=True,
                                          store_full=True)
    mocker.patch.object(bihandler, '_file_write', return_value=None, autospec=True)
    return bihandler


@pytest.fixture
def handler_1s_fill_keys(mocker):
    bihandler = BatchEveryIntervalHandler(batch_interval='1s',
                                          output_path='./data/interval_handler/output',
                                          save_every_n_batch=10000000000,    # never save for test
                                          disable_pl_inference=True,
                                          store_full=True,
                                          ffill_keys=['Letter'])
    mocker.patch.object(bihandler, '_file_write', return_value=None, autospec=True)
    return bihandler


def test_1s_batcher_no_fill_keys(data_1s_fill, handler_1s_fill):
    df, columns, timestamp_col = data_1s_fill
    columns_idxs = []
    for cn in range(0, len(columns)):
        columns_idxs.append(df[:, cn].to_list())
    for row in zip(*columns_idxs):
        msg = {k: v for k, v in zip(columns, row)}
        ts = msg[timestamp_col]
        handler_1s_fill.process(ts, msg)
    handler_1s_fill.finalize()
    df = pl.DataFrame(handler_1s_fill.full_data).with_columns(
        pl.from_epoch("__timestamp", time_unit="ms")
    )
    verify_ts = df.select('__timestamp').get_columns()[0].to_list()
    correct_ts = [
        datetime.datetime(2020, 1, 1, 0, 0, 1),
        datetime.datetime(2020, 1, 1, 0, 0, 2),
        datetime.datetime(2020, 1, 1, 0, 0, 3),
        datetime.datetime(2020, 1, 1, 0, 0, 4),
        datetime.datetime(2020, 1, 1, 0, 0, 5),
        datetime.datetime(2020, 1, 1, 0, 0, 6),
        datetime.datetime(2020, 1, 1, 0, 0, 7),
        datetime.datetime(2020, 1, 1, 0, 0, 8),
        datetime.datetime(2020, 1, 1, 0, 0, 9),
        datetime.datetime(2020, 1, 1, 0, 0, 10),
        datetime.datetime(2020, 1, 1, 0, 0, 11),
        datetime.datetime(2020, 1, 1, 0, 0, 12),
        datetime.datetime(2020, 1, 1, 0, 0, 13),
        datetime.datetime(2020, 1, 1, 0, 0, 14),
        datetime.datetime(2020, 1, 1, 0, 0, 15),
        datetime.datetime(2020, 1, 1, 0, 0, 16),
        datetime.datetime(2020, 1, 1, 0, 0, 17),
        datetime.datetime(2020, 1, 1, 0, 0, 18),
        datetime.datetime(2020, 1, 1, 0, 0, 19),
        datetime.datetime(2020, 1, 1, 0, 0, 20),
        datetime.datetime(2020, 1, 1, 0, 0, 21)
    ]
    verify_column = df.select('Letter').get_columns()[0].to_list()
    correct_letter_column = ['A',
                             None,
                             None,
                             None,
                             None,
                             'B',
                             None,
                             None,
                             None,
                             None,
                             'C',
                             None,
                             None,
                             None,
                             None,
                             'D',
                             None,
                             None,
                             None,
                             None,
                             'E']

    assert verify_ts == correct_ts and verify_column == correct_letter_column


def test_1s_batcher_no_fill(data_1s_fill, handler_1s_fill_keys):
    df, columns, timestamp_col = data_1s_fill
    columns_idxs = []
    for cn in range(0, len(columns)):
        columns_idxs.append(df[:, cn].to_list())
    for row in zip(*columns_idxs):
        msg = {k: v for k, v in zip(columns, row)}
        ts = msg[timestamp_col]
        handler_1s_fill_keys.process(ts, msg)
    handler_1s_fill_keys.finalize()
    df = pl.DataFrame(handler_1s_fill_keys.full_data).with_columns(
        pl.from_epoch("__timestamp", time_unit="ms")
    )
    verify_ts = df.select('__timestamp').get_columns()[0].to_list()
    correct_ts = [
        datetime.datetime(2020, 1, 1, 0, 0, 1),
        datetime.datetime(2020, 1, 1, 0, 0, 2),
        datetime.datetime(2020, 1, 1, 0, 0, 3),
        datetime.datetime(2020, 1, 1, 0, 0, 4),
        datetime.datetime(2020, 1, 1, 0, 0, 5),
        datetime.datetime(2020, 1, 1, 0, 0, 6),
        datetime.datetime(2020, 1, 1, 0, 0, 7),
        datetime.datetime(2020, 1, 1, 0, 0, 8),
        datetime.datetime(2020, 1, 1, 0, 0, 9),
        datetime.datetime(2020, 1, 1, 0, 0, 10),
        datetime.datetime(2020, 1, 1, 0, 0, 11),
        datetime.datetime(2020, 1, 1, 0, 0, 12),
        datetime.datetime(2020, 1, 1, 0, 0, 13),
        datetime.datetime(2020, 1, 1, 0, 0, 14),
        datetime.datetime(2020, 1, 1, 0, 0, 15),
        datetime.datetime(2020, 1, 1, 0, 0, 16),
        datetime.datetime(2020, 1, 1, 0, 0, 17),
        datetime.datetime(2020, 1, 1, 0, 0, 18),
        datetime.datetime(2020, 1, 1, 0, 0, 19),
        datetime.datetime(2020, 1, 1, 0, 0, 20),
        datetime.datetime(2020, 1, 1, 0, 0, 21)
    ]
    verify_letter_column = df.select('Letter').get_columns()[0].to_list()
    correct_letter_column = ['A',
                             'A',
                             'A',
                             'A',
                             'A',
                             'B',
                             'B',
                             'B',
                             'B',
                             'B',
                             'C',
                             'C',
                             'C',
                             'C',
                             'C',
                             'D',
                             'D',
                             'D',
                             'D',
                             'D',
                             'E']
    verify_nofill_letter_column = df.select('Nonfill_letter').get_columns()[0].to_list()
    correct_nofill_letter_column = ['A',
                                    None,
                                    None,
                                    None,
                                    None,
                                    'B',
                                    None,
                                    None,
                                    None,
                                    None,
                                    'C',
                                    None,
                                    None,
                                    None,
                                    None,
                                    'D',
                                    None,
                                    None,
                                    None,
                                    None,
                                    'E']

    assert verify_ts == correct_ts and \
        verify_letter_column == correct_letter_column and \
        verify_nofill_letter_column == correct_nofill_letter_column

