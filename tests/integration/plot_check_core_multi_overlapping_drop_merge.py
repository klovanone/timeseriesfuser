"""
Copyright (C) 2023-2024 Anthony Sweeney - email: safsweeney@gmail.com

Please view the LICENSE file for the terms and conditions
associated with this software.
"""
import plotly.graph_objects as go
import polars as pl
from plotly.subplots import make_subplots


def plot_ohlc():
    price_col1 = 'Price||BTC'
    vol_col1 = 'Quantity||BTC'
    price_col2 = 'Price||ETH'
    vol_col2 = 'Quantity||ETH'

    df = pl.read_csv('/home/crypwiz/personal_projects/logcombiner-gitlab/tests/integration/data/'
                     'merge_drop/output/FULLDATA.csv',
                     infer_schema_length=None
                     )

    timestamp = pl.from_epoch(df['__timestamp'], time_unit="ms"). \
        dt.replace_time_zone("UTC").to_list()
    price_btc = df[price_col1].to_list()
    volume_btc = df[vol_col1].to_list()

    price_eth = df[price_col2].to_list()
    volume_eth = df[vol_col2].to_list()

    fig = make_subplots(rows=2, cols=1, horizontal_spacing=0.05, vertical_spacing=0.05,
                        row_heights=[0.9, 0.1], shared_xaxes=True,
                        specs=[[{"secondary_y": True}], [{"secondary_y": True}]])
    fig.add_trace(go.Scatter(name="BTC Price over Time",
                             x=timestamp,
                             y=price_btc,
                             mode='lines',
                             line=dict(color='#2b8cbe', width=2),
                             ),
                  row=1, col=1)

    fig.add_trace(go.Scatter(x=timestamp,
                             y=volume_btc,
                             name='BTC Volume',
                             line=dict(color='#ff7f0e', width=1),
                             ),
                  row=2, col=1)

    fig.add_trace(go.Scatter(name="ETH Price over Time",
                             x=timestamp,
                             y=price_eth,
                             mode='lines',
                             line=dict(color='#be702b', width=2)
                             ),
                  row=1, col=1, secondary_y=True)

    fig.add_trace(go.Scatter(x=timestamp,
                             y=volume_eth,
                             name='ETH Volume',
                             line=dict(color='#0e9cff', width=1),
                             ),
                  row=2, col=1, secondary_y=True)

    fig.update_layout(
        showlegend=True,
        title_text='DYDX: ETH and BTC Price and Volume data',
        # title_pad=dict(t=1, b=1, l=1, r=1),
        font=dict(size=11),
        title_pad={'t': 0.01}
    )
    # Show the chart
    fig.show()


plot_ohlc()
