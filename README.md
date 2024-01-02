# Time series data merging with event-driven distribution. 


A small lightweight library designed to easily merge together multiple data sources in chronological order with minimal manual data-munging. I created this tool for a financial services project that I had been working on, as it was becoming a chore to have to merge time series data from many alternative data formats. Primarily designed to be used as a tool to simulate live data replay, but it can also be used to save merged and processed data for later re-use. 


##  Benefits:

- **Fast** - the [Polars](https://github.com/pola-rs/polars) dataframe library is used under the hood to provide lightning-fast read, sort, multi-sort, and processing capabilities.

- **Flexible** - convert different time formats on the fly into a common standard. 

- **Lightweight** - processing from flat files allows minimal infrastructure requirements - no database needed. 

- **Extendible** - the modular architecture allows new formats and output handlers to be easily added. 

- **Memory efficient** - designed to batch process large (TB+) amounts of data. Batches are processed on a piece by piece basis.  <insert bit about using Polars lazyframe support to select the processing columns later> 

***


![image](https://github.com/klovanone/timeseriesfuser/blob/main/docs/tsf_chunking_animation_v2.svg)

*The batch loading process is designed to handle overlapping data across multiple sources.* 

## Quick Start: 

TimeSeriesFuser operates through the use of DataInfo objects, with each DataInfo representing a datasource. For this example we have previously captured the websockets data from Binance for the bid-ask spread and also the individual trades data for a literal meme-coin 📈📉. For demonstration purposes, the dataset size and window is reduced to a few files. 

**Data sources:**

*Trades data*

![image](https://github.com/klovanone/timeseriesfuser/assets/39015947/60ec3bb8-5c47-4f99-b96c-b7cba5d4381a)



*Bid-Ask data*

![image](https://github.com/klovanone/timeseriesfuser/assets/39015947/bef15b93-b61e-40a3-9133-ed10073c57f1)



Both the trades data and the spread data have a lot of events, so let's sample the data every 1/10th of a second through the use of a Handler. 

A Handler is responsible for the final data formatting to coerce the data into whatever format the event execution software at the end of the processing pipeline uses (i.e. it should replicate how live data would be passed on). In this case a **BatchEveryIntervalHandler** is used, and this re-samples to a time interval, and also saves the converted data to file for later re-use.

```python
import polars as pl

from datetime import timezone, datetime
from pathlib import Path

from timeseriesfuser.core import TimeSeriesFuser
from timeseriesfuser.classes import DataInfo, BatchEveryIntervalHandler
from timeseriesfuser.datasources import CSVSrc, ParquetSrc

#  the time format in the files is in string/iso standard, use a Polars expression to convert into
#  TimeSeriesFuser format (millisecond epoch).
def convert_time_format(time_column: str) -> pl.Expr:
    #  convert iso string format to epoch in milliseconds
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

    
    #  Create a batch handler to sample every 10th of a second (100 millis ❤️)  and save to file.
    opath = Path(__file__).parent / f'data/full_tests/output'
    hdlr = BatchEveryIntervalHandler(batch_interval='100l',
                                     save_every_n_batch=10000,
                                     output_fmt='csv',
                                     output_path=opath,
                                     store_full=True,
                                     store_full_filename='spread_trades_multi_overlap_millis',
                                     ffill_keys=['Price', 'bid', 'ask'],
                                     disable_pl_inference=True)

    
    #  Pass in details to TimeSeriesFuser to start merge -> Batcher -> event execution
    tsfp = TimeSeriesFuser(datainfos=data_infos,
                           procstart=start_timestamp,
                           procend=end_timestamp,
                           handler=hdlr
                           )

    tsfp.start_tsf()
```



After processing, since *store_full* was used, it's possible to plot the full processed data. 

![image](https://github.com/klovanone/timeseriesfuser/assets/39015947/adc0dd11-177e-4615-9cc8-90555560be97)

Full code with plotting code (provided by [Plotly](https://plotly.com/python/)) is [HERE](https://github.com/klovanone/timeseriesfuser/blob/main/examples/demo_spread_trades_memecoin.py)

For more detailed examples see the [Documentation](https://github.com/klovanone/timeseriesfuser/tree/main/docs) , and also the [Testing code](https://github.com/klovanone/timeseriesfuser/tree/main/tests) examples. 


## Installation:

**Prerequisites:**

It's recommended to create a virtual environment for testing if you already use Polars in other projects. The Polars project is frequently updated with changes that break or completely change existing functionality so having a seperate VENV with the exact Polars versions will ease your pain. 

- Python 3.9+ 

- Polars 0.19.8


**<clone with github by runnning: >** 

How to test

**<install via pip>** 

pip install etc. 
    





## Supported Formats:  

TimeSeriesFuser currently supports these backends for reading data at present:

- CSV / compressed

- Parquet

It should be relatively easy to add new file and database formats by subclassing the Src class in [datasources.py](https://github.com/klovanone/timeseriesfuser/blob/main/timeseriesfuser/datasources.py) (Documentation to follow)


## Limitations:

- sometimes events from sources will be missed if they are exactly on a boundary and data is unsorted. This can happen when capturing from an API that sends out of order events. A more in-depth sliding window implementation is on the things todo. A workaround is to sort and save the data source so that data is pre-sorted before processing.
    

## Author:
	
Anthony Sweeney - email: [safsweeney@gmail.com](safsweeney@gmail.com)
	

## LICENSE:
	
	Copyright (C) 2023-2004  Anthony Sweeney

	This program is free software: you can redistribute it and/or modify
	it under the terms of the GNU Affero General Public License as published by
	the Free Software Foundation, either version 3 of the License, or
	(at your option) any later version.

	This program is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
	GNU Affero General Public License for more details.

	You should have received a copy of the GNU Affero General Public License
	along with this program.  If not, see <http://www.gnu.org/licenses/>.
			





