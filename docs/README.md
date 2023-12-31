
# TimeSeriesFuser Documentation 

## Preparing data for the DataInfo class: 


Place data related to each datasource in seperate directory. The assumption for data is that it has been scraped or recorded into an already clean format. Where 'clean' in this case means that each file:

- has a column that has the datetime/date/timestamp for each entry. This can be converted into a common format by passing a callable or Polars expression via the **convert_timestamp_function** argument. 

- these datetime/date/timestamps do not overlap with other files in the same directory. I.e. files are in sequential order. If there is some minor overlap between files related to a DataInfo (the scraped API passes the occasional message out of order for example), then the later overlapping entries will not be processed. 

- contains a value in the filename that allows the processing order of the file to be determined. 

Example valid file formats:

- simple: *file1, file2, file3, etc.* 

- more complex: *2018-02scraped_on_2023-01.parquet, 2018-12scraped_on_2023.parquet, 2019-01scraped_on_2023.parquet, etc.* 







## Describing the data:

Use the descriptor argument to give a name to the data, if not specified then a random one will be generated from a uuid. The descriptor is useful for debugging and is also appended to the column name in the case of multiple datainfos with identical column names. 

```python
data = ParquetSrc(files_path=DATADIR)
Datainfo = (
    datareader=data,
    timestamp_col_name='timestamp',
    descriptor='financedata')
```


## Indicating file order:

Use a combination of regex and index to indicate the order the files should be processed in. 

Where index related to the match number in python index format (0 for first, -1 for last)

The default setting is to find the last numeric sequence in the filename. This would work with the simple example above. 

For the more complex example, the DataInfo supports the file_sort_index and file_sort_regex keyword args.


```python
data = ParquetSrc(files_path=DATADIR)
Datainfo = (
    datareader=data,
    timestamp_col_name='timestamp',
    file_sort_idx = 0
    file_sort_regex = r'(\d{4}-\d{2})'
)
```


Indicating the column that contains the timestamp information.

Each file associated with a DataInfo must contain a date or time column.


The default format is unix epoch in milliseconds, negative values are allowed.  e.g. 1314 (Gregorian propleptic): -20701353600000 , 2000:  946684800000

If the date and time is in another format, then a timestamp conversion function must be passed into the DataInfo to convert it to the millisecond unix epoch format. 

Specify a column name or column number (rows from the left) for files that have no header information:

```python
data = ParquetSrc(files_path=DATADIR)
Datainfo = (
    datareader=data,
    filespath=DATADIR, 
    timestamp_col_name='Processed TS'
)
```


OR:	


```python
data = ParquetSrc(files_path=DATADIR)
Datainfo = (
    datareader=data, 
    timestamp_col_name='Processed TS'
    has_headers=False,
    timestamp_col_num=1
)
```



## Timestamp conversion function.

To convert the main time column to the correct format, use a Polars expression < https://pola-rs.github.io/polars/user-guide/basics/expressions/index.html within a function that will override the below.

```python
def my_time_conversion(time_column: str) -> pl.Expr:
    pass
```

It is recommended to use an expression that is compatibile with the Polars lazy API if possible.


Generic example to convert a datetime in string format with automatic conversion (if supported):  

```python
def convert_time_format(time_column: str) -> pl.Expr:
    #  this uses the rust chrono library to convert the timestamp column to a datetime column. In this instance,
    #  the chrono library is smart enough to automatically infer the format of the datetime string.
    return (
        pl.col(time_column).str.strptime(pl.Datetime).dt.epoch(time_unit="ms")
    )

data = ParquetSrc(files_path=DATADIR)
Datainfo = (
    datareader=data,
    timestamp_col_name='datetime'
    convert_timestamp_function=convert_time_format
)
```


More specific example:


```python
def convert_time_format(time_column: str) -> pl.Expr:
    #  this uses the rust chrono library to convert the timestamp column to a datetime column. In this instance,
    #  the chrono library is smart enough to automatically infer the format of the datetime string.
    return (
        pl.col('datetime').str.strptime(pl.Datetime, format='%Y-%m-%d %H:%M:%S').dt.epoch(time_unit="ms")
    )

data = ParquetSrc(files_path=DATADIR)
Datainfo = (
    datareader=data,
    timestamp_col_name='datetime'
    convert_timestamp_function=convert_time_format
)
```




## Datatypes:

Polars tries to infer datatypes automatically from data passed in, but this can be error prone particularly when merging high frequency data with low frequency data.

It is highly recommended to always pass in the correct datatypes if possible. This will improve performance by preventing files from being reloaded when Polars infers the incorrect type. 

datatypes can be specified in list format: 

```
[pl.Int64, pl.Utf8, pl.Datetime]
```


or as a dictionary with column names:

```
{'product_number': pl.Int64,
 'product': pl.Utf8,
 'time': pl.Datetime}
```


datatypes can also be specified in Python format for the most common datatypes and DataInfo will convert to the Polars equivalent:


*[int, str, datetime]*


**Selecting columns:**

Specifying column names to remove in list format if only a select number of columns are desired. 





## TimeSeriesFuser class


Specifying start and end times

Pass in a unix epoch timestamp in milliseconds to limit the processing window via the *procstart* and *procend* arguments


## Sorting data

Data is automatically sorted inside the TimeSeriesFuser chronologically. It's also possible to perform a secondary sort on data where any rows with identical timestamps are sorted again based on a second column. E.g. sequence number.  Specify the name using the secondary_sort_col kwarg - it must be a numeric datatype.


## Forward filling data

The TimeSeriesFuser can forward fill null values for all data. This is only a very basic forward fill that affects all columns for now, (replace null with last known value) since ideally the batcher should handle fills instead, as replaying data through the Fuser should match how data would be received in a live situation. <see handler <ffkeys> >


## Polars datatypes errors.

Polars has very strict datatype rules. When converting data to Polars dataframes internally, sometimes errors are raised due to mismatching types. E.g. a column is specified as an integer datatype, but one row contains a float or Nan. To minimize this, always try to specify the datatypes explicitly in the passed in Datainfos, or pass force_ignore_pl_read_errors=True into the TimeSeriesFuser. Note while this will convert floats to ints successfully if they are 125.0, 10.0, 1.0, etc. in the case of a string/utf8 columns it can lead to the entire column being replaced with null values.


```python
rename_identi_cols: Optional[bool] = True

merge_col_names: Optional[list[str]] = None
```



## Identically named columns

If multiple datainfos have the same column names, then by default these column names will have  '||**<datainfo.descriptor>**' suffix appended to them on merge.

It's possible to add exceptions by passing a list of column names that you do want to merge via merge_col_names

The separator can be changed from '||' by passing in the desired separator as a string.


E.g. rename all identical columns, append **'_<datainfo.descriptor>'** but merge the 'ReceivedTimestamp' column entries into one.


```python
tsf = TimeSeriesFuser(datainfos=DATAINFOS,
handler=HANDLER,
merge_col_names=['ReceivedTimestamp'],
col_name_sep='_')
```






## Handlers

A handler in the TimeSeriesFuser library is what is responsible for massaging the output data and passing it onto the entrypoints of an event based software. The TimeSeriesFuser always passes on a Python dictionary to a Handler for each row of merged data, and it's the Handlers responsibility to modify and convert this.

For example, if your software depends on websockets data passed via JSON when running live, you could write a Handler that converts the  JSON, and then passes on the JSON message to your asyncio Websockets server to broadcast. Or whatever you need to do to replicate how data is normally received live.

One principle to note is that any heavy data transformations is preferred within the Handler vs vectorized manipulation via Polars. You want the replayed data from the Fuser to match live as much as possible. This ensures that the same event driver logic is used throughout. While it's tempting to forward fill data in Polars to make processing faster, in a live situation this is unlikely to be part of your pipeline so coding forward fill within a handler is a better choice as it reduces the chance of peeking ahead into the future from the stored data.


A Handler has a main process() method that modifies the chronologically sorted message. This performs modifcations before being passed to distribute_to_event_handlers() where the message can be passed on.


TimeSeriesFuser provides a couple of example Handlers.

## ExampleHandler:

This simple Handler simply outputs if the current message was overlapping (multiple files with overlapping times are being processes) or non-overlapping every n messages.

Specify the n messages to output every via **outputevery** argument.

Example to output every 10000 messages received:

```python
hdlr = ExampleHandler(10000)
data = ParquetSrc(files_path=DATADIR)
data_info = DataInfo(
    descriptor='testdata',
    datatreader=data, 
    datatypes=[pl.Int64, pl.Utf8],
    timestamp_col_name='timestamp')
    tsf = TimeSeriesFuser(datainfos=[data_info],
    handler=hdlr
)
```




## BatchHandler:

This handler passes every message through as standard but saves to file every n messages. This can be used to saved the sorted and merged messages for later analysis, or to re-use later without having to go through the computationally expensive merging process again. If you are processing TB of data this can be useful to store the transformed data in smaller batches.

More information on the arguements in the code: < link to code>


**Example:**


```python
hdlr = BatchHandler(output_path=PATH, save_every_n_batch=10000)
data = ParquetSrc(files_path=DATADIR)
data_info = DataInfo(
    descriptor='testdata',
    datareader=data,
    datatypes=[pl.Int64, pl.Utf8],
    timestamp_col_name='timestamp')
    tsf = TimeSeriesFuser(datainfos=[data_info],
    handler=hdlr
)
```


## BatchEveryIntervalHandler:

This is a slightly more complex Handler that can be used to resample high frequency data into longer intervals. It can also save this modified format to file for later reuse.

**Example:**

Convert to every 2 hour intervals and save after 20,000 hours of messages have been passed in. Forward fill the Price column - this will only take effect whenever there are gaps in the source data greater than 2 hours. Also, store_full is set to save ALL the transformed data. This can be handy to debug or plot data after processing - but beware of running out of memory.

```python
hdlr = BatchEveryIntervalHandler(batch_interval='2h', save_every_n_batch=10_000, output_fmt='parquet',
output_path=opath,
store_full=True,
ffill_keys=['Price'],
)
```

More information on the arguments in the [BatchEveryIntervalHandler Docstrings](https://github.com/klovanone/timeseriesfuser/blob/main/timeseriesfuser/classes.py)
