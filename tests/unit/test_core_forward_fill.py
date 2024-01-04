"""
Copyright (C) 2023-2024 Anthony Sweeney - email: safsweeney@gmail.com

Please view the LICENSE file for the terms and conditions
associated with this software.
"""
from pathlib import Path

import polars as pl
import pytest

from timeseriesfuser.classes import DataInfo, ExampleHandler
from timeseriesfuser.core import TimeSeriesFuser
from timeseriesfuser.datasources import ParquetSrc


@pytest.fixture
def ff_tsf():
    hdlr = ExampleHandler()
    fp = Path(__file__).parent / 'data/forward_fill'
    datar = ParquetSrc(files_path=fp)
    data_info = DataInfo(
        descriptor='testdata_ff',
        datareader=datar,
        datatypes=[pl.Int64, pl.Utf8],
        timestamp_col_name='Timestamp')
    tsf = TimeSeriesFuser(datainfos=[data_info],
                          handler=hdlr
                          )
    return tsf


def test_core_forward_fill_singlefile(ff_tsf):
    """
    Files in forward fill directory have only the ask and bid data in the FIRST ROW only. If
    these rows are correctly filled up to the end of first file, then the forward fill funciton
    is working in the first file/ dataframe at least. While it is usually the case to deal with
    the public interface only when testing, due to the nature of the TimeSeriesFuser class,
    it is preferable to test the private method _forward_fill_dataframe directly.
    """
    #  set filepath to source directory
    sourcepath = Path(__file__).parent / 'data/forward_fill'
    files = sorted(file for file in sourcepath.glob('*.parquet'))
    df = pl.read_parquet(files[0])
    filled_df = ff_tsf._forward_fill_dataframe(df)
    filled_last_row = filled_df.tail(1).rows(named=True)[0]
    # note: rowdata changes slightly between single and multi
    assert filled_last_row == {'Timestamp': 1695145567099,
                               'bid': 6.829,
                               'ask': 6.804,
                               'bid_size': 146.0,
                               'ask_size': 829.5,
                               'Syn_id': 3499999,
                               'Batch_uid': 47855}


def test_core_forward_fill_multifile(ff_tsf):
    """
    Files in forward fill directory have only the ask and bid data in the FIRST ROW only. If
    these rows are correctly filled up to the end of the data (last file), then the test passed.
    While it is usually the case to deal with the public interface only when testing, due to the
    nature of the TimeSeriesFuser class, it is preferable to test the private method
    _forward_fill_dataframe directly.
    """
    #  set filepath to source directory
    sourcepath = Path(__file__).parent / 'data/forward_fill'
    files = sorted(file for file in sourcepath.glob('*.parquet'))
    filled_df = None
    for f in files:
        df = pl.read_parquet(f)
        filled_df = ff_tsf._forward_fill_dataframe(df)
    filled_last_row = filled_df.tail(1).rows(named=True)[0]
    # note: rowdata changes slightly between single and multi
    assert filled_last_row == {'Timestamp': 1695151437952,
                               'bid': 6.829,
                               'ask': 6.804,
                               'bid_size': 146.0,
                               'ask_size': 829.5,
                               'Syn_id': 3599999,
                               'Batch_uid': 47855}
