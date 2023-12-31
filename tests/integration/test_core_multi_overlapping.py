import pytest
import polars as pl
from datetime import timezone, datetime
from pathlib import Path

from timeseriesfuser.core import TimeSeriesFuser
from timeseriesfuser.classes import DataInfo, BatchEveryIntervalHandler
from timeseriesfuser.datasources import CSVSrc
from timeseriesfuser.helpers.helpers import toutcisotime


@pytest.mark.filterwarnings('ignore::UserWarning')
@pytest.fixture
def multi_datainfos():
    sym = 'MEME-USDT'
    test_data_trades = Path(__file__).parent / f'data/full_tests/sourcedata/trades/binance/{sym}'
    data_trades = CSVSrc(files_path=test_data_trades)
    di_trades = DataInfo(descriptor='trades_meme',
                         datareader=data_trades,
                         timestamp_col_name='Timestamp',
                         file_sort_idx=0,
                         datatypes=[int, float, float, int, int, int, int, int])
    test_data_spread = Path(__file__).parent / f'data/full_tests/sourcedata/spread/binance/{sym}'
    data_spread = CSVSrc(files_path=test_data_spread)
    di_spread = DataInfo(descriptor='bidask_meme',
                         datareader=data_spread,
                         timestamp_col_name='Timestamp',
                         file_sort_idx=0,
                         datatypes=[int, float, float, float, float, int, int])
    data_infos = [di_trades, di_spread]
    return data_infos


@pytest.fixture
def tsf_handler_overlapping(multi_datainfos, mocker):
    start_timestamp = int(
        datetime(2023, 11, 4, 13, 48,
                 tzinfo=timezone.utc).timestamp() * 1000)
    end_timestamp = int(
        datetime(2023, 11, 6,
                 tzinfo=timezone.utc).timestamp() * 1000)
    opath = Path(__file__).parent / f'data/full_tests/output'
    hdlr = BatchEveryIntervalHandler(batch_interval='100l',
                                     save_every_n_batch=10000,
                                     output_fmt='csv',
                                     output_path=opath,
                                     store_full=True,
                                     ffill_keys=['Price'],
                                     disable_pl_inference=True)
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
    vpath = Path(__file__).parent / f'data/verification/multi_overlap_millis.parquet'
    verifydf = pl.read_parquet(vpath)
    outputdf = pl.DataFrame(hdlr.full_data, schema=hdlr.output_schema)
    assert outputdf.frame_equal(verifydf)
