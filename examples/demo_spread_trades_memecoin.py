"""
Copyright (C) 2023-2024 Anthony Sweeney - email: safsweeney@gmail.com

Please view the LICENSE file for the terms and conditions
associated with this software.
"""
from datetime import datetime, timezone
from pathlib import Path

import plotly.graph_objects as go
import polars as pl
from plotly.subplots import make_subplots

from timeseriesfuser.classes import BatchEveryIntervalHandler, DataInfo
from timeseriesfuser.core import TimeSeriesFuser
from timeseriesfuser.datasources import ParquetSrc


#  the time format in the files is in string/iso standard, use a Polars expression to convert into
#  TimeSeriesFuser format.
def convert_time_format(time_column: str) -> pl.Expr:
    #  this uses the rust chrono library to convert the timestamp column to a datetime column.
    #  In this instance, the chrono library is smart enough to automatically infer the format of
    #  the datetime string.
    return (
        pl.col(time_column).str.strptime(pl.Datetime).dt.epoch(time_unit="ms")
    )


def process_tsf():
    sym = 'MEME-USDT-PQ'
    #  create the DataInfo associated with trades data
    tradespath = Path(__file__).parent / f'data/full_tests/sourcedata/trades/binance/{sym}'
    data_trades = ParquetSrc(files_path=tradespath)
    di_trades = DataInfo(descriptor='trades_meme',
                         datareader=data_trades,
                         convert_timestamp_function=convert_time_format,
                         timestamp_col_name='str_iso_timestamp',
                         file_sort_idx=0,
                         datatypes=[float, float, int, int, int, int, int, str])

    #  create the DataInfo associated with the bid-ask spread data
    spreadpath = Path(__file__).parent / f'data/full_tests/sourcedata/spread/binance/{sym}'
    data_spread = ParquetSrc(files_path=spreadpath)
    di_spread = DataInfo(descriptor='bidask_meme',
                         datareader=data_spread,
                         convert_timestamp_function=convert_time_format,
                         timestamp_col_name='str_iso_timestamp',
                         file_sort_idx=0,
                         datatypes=[int, float, float, float, float, int, str])
    data_infos = [di_trades, di_spread]

    #  select time windows for the output data
    start_timestamp = int(
        datetime(2023, 11, 4, 15, 22,
                 tzinfo=timezone.utc).timestamp() * 1000)
    end_timestamp = int(
        datetime(2023, 11, 4, 15, 34,
                 tzinfo=timezone.utc).timestamp() * 1000)

    #  Create a batch handler to sample every 10th of a second and save to file.
    opath = Path(__file__).parent / f'data/full_tests/output'
    hdlr = BatchEveryIntervalHandler(batch_interval='100l',
                                     save_every_n_batch=10000,
                                     output_fmt='csv',
                                     output_path=opath,
                                     store_full=True,
                                     store_full_filename='spread_trades_multi_overlap_millis',
                                     ffill_keys=['Price', 'bid', 'ask'],
                                     disable_pl_inference=True)

    #  Pass in details to TimeSeriesFuser to start merge -> Batcher -> event execution entrypoint
    tsfp = TimeSeriesFuser(datainfos=data_infos,
                           procstart=start_timestamp,
                           procend=end_timestamp,
                           handler=hdlr,
                           forward_fill_data=True
                           )

    tsfp.start_tsf()


def plot_spread_and_ohlc():
    price_col = 'Price'
    vol_col = 'Quantity'
    # Load the CSV file into a Polars DataFrame
    df = pl.read_csv('data/full_tests/output/spread_trades_multi_overlap_millis.csv',
                     infer_schema_length=None
                     )
    timestamp = pl.from_epoch(df['__timestamp'], time_unit="ms"). \
        dt.replace_time_zone("UTC").to_list()

    price = df[price_col].to_list()
    volume = df[vol_col].to_list()
    bid = df['bid'].to_list()
    ask = df['ask'].to_list()

    fig = make_subplots(rows=2, cols=1, horizontal_spacing=0.05, vertical_spacing=0.05,
                        row_heights=[0.9, 0.1], shared_xaxes=True)
    fig.add_trace(go.Scatter(name="Price over Time",
                             x=timestamp,
                             y=price,
                             mode='lines',
                             line=dict(color='#2b8cbe', width=2),
                             ),
                  row=1, col=1)
    fig.add_trace(go.Scatter(name="Bid over Time",
                             x=timestamp,
                             y=bid,
                             mode='lines',
                             line=dict(color='#99d8c9', width=1)
                             ),
                  row=1, col=1)
    fig.add_trace(go.Scatter(name="Ask over Time",
                             x=timestamp,
                             y=ask,
                             mode='lines',
                             line=dict(color='#e34a33', width=1)
                             ),
                  row=1, col=1)

    fig.add_trace(go.Scatter(x=timestamp,
                             y=volume,
                             name='Volume',
                             line=dict(color='#ff7f0e')),
                  row=2, col=1)

    fig.update_layout(
        showlegend=True,
        title_text='Price Volume data against Bid/Ask',
        font=dict(size=11),
        title_pad={'t': 0.01}
    )
    # Show the chart
    fig.show()


if __name__ == '__main__':
    process_tsf()
    plot_spread_and_ohlc()
