import polars as pl
import pytest
from pathlib import Path

from timeseriesfuser.classes import DataInfo, BatchEveryIntervalHandler
from timeseriesfuser.core import TimeSeriesFuser
from timeseriesfuser.datasources import CSVSrc


@pytest.mark.filterwarnings('ignore::UserWarning')
@pytest.fixture
def single_datainfos():
    sym_a = 'ETH-USD-PERP'
    test_path_eth = Path(__file__).parent / f'./data/full_tests/sourcedata/trades/dydx/{sym_a}'
    data_eth = CSVSrc(files_path=test_path_eth)
    di_trades_a = DataInfo(descriptor='trades_eth',
                           datareader=data_eth,
                           timestamp_col_name='Timestamp',
                           file_sort_idx=0,
                           datatypes=[int, float, float, str, int, int, int, int])
    data_infos = [di_trades_a]
    return data_infos


@pytest.fixture
def tsf_handler_non_overlapping(single_datainfos, mocker):
    start_timestamp = None
    end_timestamp = None
    hdlr = BatchEveryIntervalHandler(batch_interval='10s', save_every_n_batch=100000,
                                     output_fmt='parquet',
                                     output_path='./data/full_tests/'
                                                 'single_nonoverlapping/output',
                                     store_full=True,
                                     store_full_filename='core_single_nonoverlapping.parquet',
                                     ffill_keys=['Price'],
                                     disable_pl_inference=False)
    mocker.patch.object(hdlr, '_file_write', return_value=None, autospec=True)
    tsfp = TimeSeriesFuser(datainfos=single_datainfos,
                           procstart=start_timestamp,
                           procend=end_timestamp,
                           handler=hdlr,
                           forward_fill_data=True,
                           force_ignore_pl_read_errors=True
                           )
    return tsfp, hdlr


@pytest.mark.filterwarnings('ignore::UserWarning')
@pytest.fixture
def single_datainfos_with_gap():
    sym_a = 'ETH-USD-PERP'
    sym_b = 'BTC-USD-PERP'
    test_path_eth = Path(__file__).parent / f'./data/full_tests/sourcedata/trades/dydx/{sym_a}'
    data_eth = CSVSrc(files_path=test_path_eth)
    di_trades_a = DataInfo(descriptor='trades_eth',
                           datareader=data_eth,
                           timestamp_col_name='Timestamp',
                           file_sort_idx=0,
                           datatypes=[int, float, float, str, int, int, int, int])
    test_path_btc = Path(__file__).parent / f'./data/full_tests/sourcedata/trades/dydx/{sym_b}'
    data_btc = CSVSrc(files_path=test_path_btc)
    di_trades_b = DataInfo(descriptor='trades_btc',
                           datareader=data_btc,
                           timestamp_col_name='Timestamp',
                           file_sort_idx=0,
                           datatypes=[pl.Int64, pl.Float64, pl.Float64, pl.Utf8, pl.Int64,
                                      pl.Int64, pl.Int64, pl.Int64])
    data_infos = [di_trades_a, di_trades_b]
    return data_infos


@pytest.mark.filterwarnings('ignore::UserWarning')
@pytest.fixture
def single_datainfo_large():
    sym_a = 'BTC-USDT'
    data_trades = CSVSrc(files_path=f'./data/full_tests/sourcedata/trades/binance/{sym_a}')
    di_trades_a = DataInfo(descriptor='trades_btc2018_1st_half',
                           datareader=data_trades,
                           timestamp_col_name='Timestamp(T)',
                           file_sort_idx=-1,  # last extracted regex value
                           datatypes=[int, bool, int, int, int, bool, float, float])
    data_infos = [di_trades_a]
    return data_infos


@pytest.fixture
def tsf_handler_non_overlapping_large(single_datainfo_large, mocker):
    start_timestamp = None
    end_timestamp = None
    hdlr = BatchEveryIntervalHandler(batch_interval='1h',
                                     save_every_n_batch=100000,
                                     output_fmt='parquet',
                                     output_path='./data/full_tests/single_nonoverlapping/output',
                                     store_full=True,
                                     store_full_filename='core_single_nonoverlapping_lrg.parquet',
                                     ffill_keys=['price(p)'],
                                     disable_pl_inference=False)
    mocker.patch.object(hdlr, '_file_write', return_value=None, autospec=True)
    tsfp = TimeSeriesFuser(datainfos=single_datainfo_large,
                           procstart=start_timestamp,
                           procend=end_timestamp,
                           handler=hdlr,
                           forward_fill_data=True
                           )
    return tsfp, hdlr


@pytest.mark.filterwarnings('ignore::UserWarning')
def test_tsf_single_no_overlap(tsf_handler_non_overlapping):
    tsfp, hdlr = tsf_handler_non_overlapping
    tsfp.start_tsf()
    verifydf = pl.read_parquet('./data/verification/single_nooverlap_dydx_ethusdperp.parquet')
    outputdf = pl.DataFrame(hdlr.full_data)
    assert outputdf.frame_equal(verifydf)


@pytest.mark.slow
@pytest.mark.filterwarnings('ignore::UserWarning')
def test_tsf_single_no_overlap_large(tsf_handler_non_overlapping_large):
    tsfp, hdlr = tsf_handler_non_overlapping_large
    tsfp.start_tsf()
    verifydf = pl.read_csv('./data/verification/single_nooverlap_binance_btcusdt_large_1h.csv')
    outputdf = pl.DataFrame(hdlr.full_data)
    assert outputdf.frame_equal(verifydf)
