"""
Copyright (C) 2023-2024 Anthony Sweeney - email: safsweeney@gmail.com

Please view the LICENSE file for the terms and conditions
associated with this software.
"""
import pytest
import polars as pl
from pathlib import Path

from timeseriesfuser.core import TimeSeriesFuser
from timeseriesfuser.classes import DataInfo, BatchEveryIntervalHandler
from timeseriesfuser.datasources import CSVSrc
from timeseriesfuser.helpers.helpers import toutcisotime


@pytest.mark.filterwarnings('ignore::UserWarning')
@pytest.fixture
def multi_datainfos():
    sym = 'BTC-USD-OVERLAP'
    test_data_trades_btc = Path(__file__).parent / f'data/full_tests/sourcedata/trades/dydx/{sym}'
    data_btc = CSVSrc(files_path=test_data_trades_btc)
    di_trades_btc = DataInfo(descriptor='BTC',
                             datareader=data_btc,
                             timestamp_col_name='Timestamp',
                             file_sort_idx=0,
                             datatypes=[int, float, float, int, int, int, int, int],
                             remove_cols=['TradeID', 'RecTimestamp', 'Syn_id', 'Batch_uid', 'CRC'])
    sym = 'ETH-USD-OVERLAP'
    test_data_trades_eth = Path(__file__).parent / f'data/full_tests/sourcedata/trades/dydx/{sym}'
    data_eth = CSVSrc(files_path=test_data_trades_eth)
    di_trades_eth = DataInfo(descriptor='ETH',
                             datareader=data_eth,
                             timestamp_col_name='Timestamp',
                             file_sort_idx=0,
                             datatypes=[int, float, float, int, int, int, int, int],
                             remove_cols=['TradeID', 'RecTimestamp', 'Syn_id', 'Batch_uid', 'CRC'])
    data_infos = [di_trades_btc, di_trades_eth]
    return data_infos


@pytest.fixture
def tsf_handler_overlapping(multi_datainfos, mocker):
    start_timestamp = None
    end_timestamp = None
    opath = Path(__file__).parent / f'data/merge_drop/output/'
    hdlr = BatchEveryIntervalHandler(batch_interval='1m',
                                     save_every_n_batch=10000,
                                     output_fmt='csv',
                                     output_path=opath,
                                     store_full=True,
                                     ffill_keys=['Price'],
                                     disable_pl_inference=True)
    # disable writing to disk for tests
    mocker.patch.object(hdlr, '_file_write', return_value=None, autospec=True)
    tsfp = TimeSeriesFuser(datainfos=multi_datainfos,
                           procstart=start_timestamp,
                           procend=end_timestamp,
                           handler=hdlr,
                           forward_fill_data=True,
                           force_ignore_pl_read_errors=True
                           )
    return tsfp, hdlr


@pytest.mark.filterwarnings('ignore::UserWarning')
def test_tsf_multi_overlap(tsf_handler_overlapping):
    tsfp, hdlr = tsf_handler_overlapping
    tsfp.start_tsf()
    vpath = Path(__file__).parent / f'data/verification/multi_overlap_drop_merge.parquet'
    verifydf = pl.read_parquet(vpath)
    outputdf = pl.DataFrame(hdlr.full_data, schema=hdlr.output_schema)
    assert outputdf.equals(verifydf)
    #  there is also a plotly chart to verify that it looks ok -
    #  run plot_check_core_multi_overlapping_drop_merge.py
