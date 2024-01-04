"""
Copyright (C) 2023-2024 Anthony Sweeney - email: safsweeney@gmail.com

Please view the LICENSE file for the terms and conditions
associated with this software.
"""
import plotly.graph_objects as go
import polars as pl
from plotly.subplots import make_subplots


def plot_ohlc(exchangename):
    fn = 'core_single_nonoverlapping'
    price_col = None
    vol_col = None
    if exchangename == 'binance':
        price_col = 'price(p)'
        vol_col = 'quantity(q)'
    elif exchangename == 'dydx':
        price_col = 'Price'
        vol_col = 'Quantity'
    df = pl.read_csv(f'./../data/full_tests/output/{fn}.parquet',
                     infer_schema_length=None
                     )
    timestamp = pl.from_epoch(df['__timestamp'], time_unit="ms").\
        dt.replace_time_zone("UTC").to_list()
    price = df[price_col].to_list()
    volume = df[vol_col].to_list()
    fig = make_subplots(rows=2, cols=1, horizontal_spacing=0.05, vertical_spacing=0.05,
                        row_heights=[0.9, 0.1], shared_xaxes=True)
    fig.add_trace(go.Scatter(name="Price over Time",
                             x=timestamp,
                             y=price,
                             mode='lines',
                             line=dict(color='#2b8cbe', width=2),
                             ),
                  row=1, col=1)
    fig.add_trace(go.Scatter(x=timestamp,
                             y=volume,
                             name='Volume over Time',
                             line=dict(color='#ff7f0e')),
                  row=2, col=1)
    fig.update_layout(
        showlegend=True,
        title_text=f'{exchangename.upper()}: ETH-USD-PERP | Price and Volume data',
        font=dict(size=11),
        title_pad={'t': 0.01}
    )
    fig.show()

