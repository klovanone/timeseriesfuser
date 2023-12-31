import pytest
import polars as pl

from datetime import timezone, datetime
from pathlib import Path

from timeseriesfuser.core import TimeSeriesFuser
from timeseriesfuser.classes import DataInfo, BatchEveryIntervalHandler
from timeseriesfuser.datasources import ParquetSrc
from timeseriesfuser.helpers.helpers import toutcisotime


def convert_time_format(time_column: str) -> pl.Expr:
    #  this uses the rust chrono library to convert the timestamp column to a datetime column.
    #  In this instance, the chrono library is smart enough to automatically infer the format of
    #  the datetime string.
    return (
        pl.col(time_column).str.strptime(pl.Datetime).dt.epoch(time_unit="ms")
    )


@pytest.mark.filterwarnings('ignore::UserWarning')
@pytest.fixture
def multi_datainfos():
    sym = 'MEME-USDT-PQ'
    test_data_trades = Path(__file__).parent / f'data/full_tests/sourcedata/trades/binance/{sym}'
    data_trades = ParquetSrc(files_path=test_data_trades)
    di_trades = DataInfo(descriptor='trades_meme',
                         datareader=data_trades,
                         convert_timestamp_function=convert_time_format,
                         timestamp_col_name='str_iso_timestamp',
                         file_sort_idx=0,
                         datatypes=[float, float, int, int, int, int, int, str])
    test_data_spread = Path(__file__).parent / f'data/full_tests/sourcedata/spread/binance/{sym}'
    data_spread = ParquetSrc(files_path=test_data_spread)
    di_spread = DataInfo(descriptor='bidask_meme',
                         datareader=data_spread,
                         convert_timestamp_function=convert_time_format,
                         timestamp_col_name='str_iso_timestamp',
                         file_sort_idx=0,
                         datatypes=[int, float, float, float, float, int, str])
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
                                     store_full_filename='spread_trades_multi_overlap_millis',
                                     ffill_keys=['Price'],
                                     disable_pl_inference=True)
    mocker.patch.object(hdlr, '_file_write', return_value=None, autospec=True)
    tsfp = TimeSeriesFuser(datainfos=multi_datainfos,
                           procstart=start_timestamp,
                           procend=end_timestamp,
                           handler=hdlr,
                           forward_fill_data=True
                           )
    return tsfp, hdlr


@pytest.mark.filterwarnings('ignore::UserWarning')
def test_tsf_multi_overlap(tsf_handler_overlapping):
    tsfp, hdlr = tsf_handler_overlapping
    tsfp.start_tsf()
    vpath = Path(__file__).parent / f'data/verification/multi_overlap_millis.parquet'
    verifydf = pl.read_parquet(vpath)
    outputdf = pl.DataFrame(hdlr.full_data, schema=hdlr.output_schema)
    outputdf = outputdf.select(verifydf.columns)
    assert outputdf.frame_equal(verifydf)
