"""
Copyright (C) 2023-2024 Anthony Sweeney - email: safsweeney@gmail.com

Please view the LICENSE file for the terms and conditions
associated with this software.
"""
import collections
import gzip
import itertools
import logging
import uuid
import warnings
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional, Union

import polars as pl

from timeseriesfuser.datasources import Src
from timeseriesfuser.helpers.helpers import toutcisotime
from timeseriesfuser.statics import FEATUREFLAG


@dataclass
class DataInfo:
    """
    Stores the data for the files in each folder being processed. One set of files with the same
    format should go into a seperate folder. Each file in a set should be the same format,
    where: 1) all files either have headers or do not have headers, and if they do then all
    files have the same column names.

     2) all files have the same number of columns. Malformed files support is on the list of
     things to add.

    Attributes:
        timestamp_col_num (Optional[int]): The column number for timestamps.
        timestamp_col_name (Optional[str]): The column name for timestamps (if not using column
            number, and the file has headers). files_process_type (Optional[str]): The process type
            for files. CSV (compressed using gzip or uncompressed) can be used, or parquet files.
            Defaults to 'csv'.
        file_sort_regex (Optional[str]): The regex pattern for file sorting.
        datatypes (Optional[list]): The data types for each column. If not provided, the
               data types will be inferred.
        has_headers (Optional[bool]): True if the files have headers, False if not. For
            formats that support no headers, such as CSV. Defaults to True.
        convert_timestamp_function (Optional[Callable[[str], pl.Expr]]): A function to convert the
            column to unix epoch time in milliseconds. This is only needed if the timestamp column
            is not in unix epoch time and is in utf/string format.
    """
    datareader: [Src]
    descriptor: Optional[str] = None
    timestamp_col_num: Optional[int] = None
    timestamp_col_name: Optional[str] = None
    file_sort_regex: Optional[str] = r'\d+'
    file_sort_idx: Optional[int] = -1
    datatypes: Optional[Union[list, dict]] = None
    has_headers: Optional[bool] = True
    remove_cols: Optional[list] = None
    convert_timestamp_function: Optional[Callable[[str], pl.Expr]] = None

    def __post_init__(self):
        """
        Create the additional attributes for the dataclass to be modified during processing.
        """
        if self.timestamp_col_num is None and self.timestamp_col_name is None:
            raise RuntimeError('Either timestamp_col_num or timestamp_col_name must be set.')
        if self.timestamp_col_num is not None and self.timestamp_col_name is not None:
            raise RuntimeError('Only one of timestamp_col_num or timestamp_col_name can be set.')
        if self.descriptor is None:
            self.descriptor = str(uuid.uuid4())[-12:]  # a random descriptor if not provided
        if self.timestamp_col_name is not None:
            self.ts_col = self.timestamp_col_name
        self.proc_inputsrcs = collections.deque()
        self.cdf = None  # current dataframe that is being processes
        self.files_process_type = None
        self.cfile_start_ts = None
        self.cfile_end_ts = None
        self.last_secondary_sort_id = None
        self.cfile_process = None
        self.load_next_file = False
        self.initial_numcols = None  # check if each file has the correct number of columns
        self.header_fmt = []
        self.header_fmt_modified = []
        self.header_fmt_file_load = []
        self.rename_cols = {}
        self.exsym_lu = None
        self.procstart_ts = None
        self.procend_ts = None
        self.data_procstart_ts = None
        self.data_procend_ts = None
        self.nodata_from_start_to_end = False
        #  conversion table for the most common python datatypes as an aid to the user
        self.datatype_conversion = {int: pl.Int64,
                                    float: pl.Float64,
                                    str: pl.Utf8,
                                    bool: pl.Boolean,
                                    datetime: pl.Datetime
                                    }
        self.datatypes_schema = None
        self.data_sorted = False
        self._check_datatypes()  # changes datatypes and datatypes_schema if needed
        self.convert_timestamps = False
        if self.convert_timestamp_function:
            self.convert_timestamps = True
            setattr(self, 'convert_time_format', self.convert_timestamp_function)
        self._columns_init = False
        self._schema_init = False  # used to set polars schema in dict format upon first data load

    def convert_time_format(self, time_column: str) -> pl.Expr:
        """
        Override this method to convert the datetime column to unix epoch time in milliseconds.
        A function to convert the column to unix epoch time in milliseconds. This is only needed
        if the timestamp column is not in unix epoch time and is in utf/string format. Example
        function: def convert_timestamp_format(time_col: str) -> pl.Expr: return ( pl.col(
        time_col).str.strptime(pl.Datetime).dt.epoch(time_unit="ms") )

        """
        raise NotImplementedError('Override the convert_time_format method to convert the '
                                  'datetime column to unix epoch time! See the '
                                  'convert_time_format() documentation in DataInfo for an '
                                  'example.')

    def set_datatypes_schema(self, column_names: list) -> None:
        """
        Set the datatypes for the schema.
        Args:
            column_names: list: The list of column names.
        """
        self.datatypes_schema = {k: v for k, v in zip(column_names, self.datatypes)}

    def _check_datatypes(self) -> None:
        """
        Check if the datatypes are in valid polars format, and convert them if not.
        """
        if self.datatypes is None:
            msg = f'Warning - [{self.descriptor}] - no datatypes specified. TimeSeriesFuser ' \
                  f'will attempt to automatically infer datatypes, but there is the possibility ' \
                  f'of conversion errors. For best results, specify the datatypes in polars' \
                  f' format: ' \
                  f'e.g.: pl.Int64, pl.Float64, pl.Utf8, etc. '
            warnings.warn(msg)
        else:
            dtypes = self.datatypes
            polars_dtypes = True
            if isinstance(self.datatypes, list):
                if not all([isinstance(x, pl.datatypes.classes.DataTypeClass) for x in
                            self.datatypes]):
                    dtypes = [self.datatype_conversion[x] for x in self.datatypes]
                    polars_dtypes = False
            # the below is commented out since it is not currently used and conflicts with casting
            # to the correct datatype
            elif isinstance(self.datatypes, dict):
                if not all([isinstance(v, pl.datatypes.classes.DataTypeClass) for v in
                            self.datatypes.values()]):
                    dtypes = {k: self.datatype_conversion[v] for k, v in self.datatypes.items()}
                    polars_dtypes = False
                self.datatypes_schema = self.datatypes
                self.datareader.datatypes_schema = self.datatypes_schema
            else:
                raise RuntimeError('Invalid datatype format. Must be a list of datatypes or '
                                   'dict with column name.')
            if not polars_dtypes:
                msg = f'Warning - [{self.descriptor}] datatypes are not in polars dtypes format. ' \
                      f'Converting to polars format, but this may affect performance or cause ' \
                      f'errors. For best results, specify the datatypes in polars format: ' \
                      f'e.g.: pl.Int64, pl.Float64, pl.Utf8, etc. '
                warnings.warn(msg)
            self.datatypes = dtypes
            self.datareader.datatypes = dtypes

    def _init_columns_source(self, _df, get_ts_col_flag, get_ts_col_num_flag):
        if not self._columns_init:
            if get_ts_col_flag:
                self.ts_col = _df.columns[self.timestamp_col_num]
            if get_ts_col_num_flag:
                self.timestamp_col_num = _df.columns.index(
                    self.timestamp_col_name)
            self._columns_init = True

    def get_datasrc_global_start_timestamp(self, get_ts_col_flag=False, get_ts_col_num_flag=False,
                                           ignore_errors=False):
        _df = self.datareader.get_global_start_data(ignore_errors=ignore_errors)
        self._init_columns_source(_df, get_ts_col_flag, get_ts_col_num_flag)
        if self.convert_timestamps:
            _df = _df.with_columns(self.convert_time_format(self.ts_col))
        return int(_df[self.ts_col][0])

    def get_datasrc_global_end_timestamp(self, get_ts_col_flag=False, get_ts_col_num_flag=False,
                                         ignore_errors=False):
        _df = self.datareader.get_global_end_data(ignore_errors=ignore_errors)
        self._init_columns_source(_df, get_ts_col_flag, get_ts_col_num_flag)
        if self.convert_timestamps:
            _df = _df.with_columns(self.convert_time_format(self.ts_col))
        return int(_df[self.ts_col][-1])

    def load_datasrc_overlay(self, read_src, *, ignore_errors, num_rows=None):
        #  set the schema if not set before (needs column names)
        #  load data
        df = self.datareader.load_data(read_src, num_rows=num_rows, ignore_errors=ignore_errors)
        if not self._schema_init:
            if isinstance(self.datatypes, list):
                self.set_datatypes_schema(df.columns)
            self.datareader.datatypes_schema = self.datatypes_schema
            self.datareader.schema_init = True
            self._schema_init = True
        return df

    def load_datasrc_get_start_ts_overlay(self, read_src, *, ignore_errors):
        _df = self.load_datasrc_overlay(read_src, num_rows=5, ignore_errors=ignore_errors)
        if self.convert_timestamps:
            _df = _df.with_columns(self.convert_time_format(self.ts_col))
        return int(_df[self.ts_col][0])

    def load_datasrc_get_end_ts_overlay(self, read_src, *, ignore_errors):
        _df = self.load_datasrc_overlay(read_src, ignore_errors=ignore_errors)
        if self.convert_timestamps:
            _df = _df.with_columns(self.convert_time_format(self.ts_col))
        return int(_df[self.ts_col][-1])

    def load_datasrc_get_start_end_ts_overlay(self, read_src, *, ignore_errors):
        #  get both the start and end timestamps in one read
        _df = self.load_datasrc_overlay(read_src, ignore_errors=ignore_errors)
        if self.convert_timestamps:
            _df = _df.with_columns(self.convert_time_format(self.ts_col))
        return int(_df[self.ts_col][0]), int(_df[self.ts_col][-1])

    def set_header_formats(self, dataframe):
        if self.has_headers:
            self.header_fmt_file_load = list(dataframe.columns)

            if self.remove_cols:
                self.header_fmt = []
                for col in self.header_fmt_file_load:
                    if col not in self.remove_cols:
                        self.header_fmt.append(col)
            else:
                self.header_fmt = list(dataframe.columns)
        else:
            raise NotImplementedError('No non-headered support yet')
        return self.header_fmt


class BaseHandler(ABC):
    """
    A handler for processing each line of the combined files for event-driven processing.
    This can be subclassed to add more functionality.
    """

    def __init__(self, *args, **kwargs):
        """
        Args:
            eventcount: number of events processed
        """
        self.logger = logging.getLogger(__name__)
        self.output_schema = None

    @abstractmethod
    def process(self, timestamp: int, msg: dict) -> None:
        """
        Generic example process method for processing each event.
        Args:
            timestamp: int: The timestamp of the event in milliseconds format
            msg: dict: The message of the event with your custom keys.
        Returns:
            None
        """
        pass

    @abstractmethod
    def finalize(self) -> None:
        """
        Generic example finalize method to finish processing after all events have been
        processed. E.g. TimeSeriesFuser class has reached end of data.
        """
        pass

    @abstractmethod
    def distribute_to_event_handlers(self, msg: dict) -> None:
        """
        Generic example method to distribute the data to the event handlers in whatever format
        they require. Override this method to send the data to your event handlers. Args: msg:
        dict: The message of the event with your custom keys.
        """
        pass

    def modify_transformations(self, *, rename_cols: dict, separator: str) -> None:
        """
        Modify the internal variables that are affected by actions that transform dataframe
        loaded, such as drops, renames, etc. Args: rename_cols: dict: The dict of the columns
        names that will be renamed with the original column name as the key and the new column
        name as the value. separator: str: The separator used to seperate the original column
        name from the new column name.
        """
        pass

    def set_datatypes(self, output_schema: dict, *, remove_internal_keys: bool) -> None:
        """
        Method to set the datatypes for the handler. This is used for output of data only. This
        method is only called by the TimeSeriesFuser class when the output schema has been
        calculated. Example uses for this is saving to file via polars so that the output
        datatypes match the schema from the source data Args: output_schema: dict: The schema of
        the output data in dict format. example: {'__timestamp': pl.Int64, 'price': pl.Float64}
        remove_internal_keys: bool: True if the internal keys used by the Fuser (e.g.
        exsym_lookup) will be removed from the output schema, False if not.
        """
        if remove_internal_keys:
            output_schema.pop('exsym_lookup')
        self.output_schema = output_schema

    def get_results(self) -> None:
        """
        Generic example method to return data from the handler back to the TimeSeriesFuser class.
        """
        return None


class ExampleHandler(BaseHandler):
    """
    An example handler for processing each line of the combined files for event-driven
    processing. Pass in a boolean to process() to demonstrate if the files have overlapping
    timestamps or not. Attributes: outputevery (int): The number of events to process before
    outputting a status message.
    """

    def __init__(self, outputevery=10000):
        """
        Args:
            outputevery: log every n number of events. Defaults to 10000.
        """
        super().__init__()
        self.outputevery = outputevery
        self.eventcount = 0

    def process(self, timestamp: int, msg: dict, *, datainfo: DataInfo = None,
                overlap: bool = False) -> None:
        """
        Generic example process method for processing each event. Args: timestamp: int: The
        timestamp of the event in milliseconds format msg: dict: The message of the event with
        your custom keys. datainfo: the DataInfo object for the folder being processed overlap:
        bool: True if the event passed in is from files that have overlapping timestamps,
        False if not. This isn't important but is used in this case to show how the processing
        works. Returns: None
        """
        self.eventcount += 1
        printmsg = False
        #  modify the msg here for the format needed in the (imaginary) event entrypoint
        msg['ProcessedTime'] = datetime.utcfromtimestamp(timestamp / 1000). \
            replace(tzinfo=timezone.utc)
        del msg['__timestamp']
        self.distribute_to_event_handlers(msg)
        if self.eventcount % self.outputevery == 0:
            d = datainfo.descriptor
            if overlap:
                self.logger.info(f'OVERLAP Line: {self.eventcount}, {toutcisotime(timestamp)} '
                                 f'UTC, {d} ')
            else:
                self.logger.info(f'NO OVERLAP Line: {self.eventcount}, {toutcisotime(timestamp)} '
                                 f'UTC, {d} ')
            if printmsg:
                self.logger.info(msg)

    def finalize(self):
        """
        Called after all events have been processed. Override this method to do any final
        processing and empty data held in any data structures used by a handler.
        """
        pass

    def distribute_to_event_handlers(self, msg: dict) -> None:
        """
        Generic example method to distribute the data to the event handlers in whatever format
        they require. Override this method to send the data to your event handlers. Args: msg:
        dict: The message of the event with your custom keys.
        """
        print(f'ExampleHandler: {msg} to be processed. ')


class BatchHandler(BaseHandler):
    """
    A basic handler that saves data every n messages.
    Args:
        output_path: str: The path to save the output data.
        save_every_n_batch: int: The number of batches to process before saving the data.
        output_fmt: str: The output format. Can be 'csv' or 'parquet'. Defaults to 'parquet'.
        compression: bool: True if output should be compressed, False if not. Defaults to False.
        store_full: bool: True if the full data should be stored in memory, and saved at the end
        of processing. False if not. Defaults to False.
    """

    def __init__(self, *,
                 output_path: Union[str, Path],
                 save_every_n_batch: int = 10000,
                 output_fmt: str = 'parquet',
                 compression: bool = False,
                 store_full: bool = False,
                 store_full_filename: str = 'FULLDATA',
                 disable_pl_inference=False,
                 base_timezone: timezone = timezone.utc
                 ):
        super().__init__()
        self.save_every_n_batch = save_every_n_batch
        self.compression = compression
        self.output_path = Path(output_path)
        self.output_fmt = output_fmt
        self.disable_pl_inference = disable_pl_inference
        self.timezone = base_timezone
        self.store_full_data = store_full
        self.store_full_fn = store_full_filename
        self.data_batch = []
        self.data_batch_cnt = 0
        self.full_data = []
        self.results_data = {}
        self._initialize_paths()

    def process(self, timestamp: int, msg: dict) -> None:
        """
        Generic example process method for processing each event.
        Args:
            timestamp: int: The timestamp of the event in milliseconds format
            msg: dict: The message of the event with your custom keys.
        Returns:
            None
        """
        self._batch_append(msg)

    def finalize(self) -> None:
        """
        Save any data stored in the deque data structures at end of processing.
        """
        self._save_batch(final=True)

    def get_results(self) -> dict:
        """
        Return the data processed in dict format.
        """
        results = {'data': self.full_data,
                   'output_path': self.output_path,
                   'fulldata_path': self.output_full_fname}
        return results

    def distribute_to_event_handlers(self, msg: dict) -> None:
        """
        Generic example method to distribute the data to the event handlers in whatever format
        they require. Override this method to send the data to your event based method
        Args:
            msg: dict: The message of the event with your custom keys.
        """
        # usually this method would do something useful and pass on to the code that needs the
        # data.
        pass

    def _initialize_paths(self) -> None:
        """
        Initialize the paths for saving the output data.
        """
        self.output_path.mkdir(parents=True, exist_ok=True)
        if self.output_fmt == 'csv':
            if self.compression:
                self.output_fext = '.csv.gz'
            else:
                self.output_fext = '.csv'
        elif self.output_fmt == 'parquet':
            self.output_fext = '.parquet'
        if self.store_full_fn != 'FULLDATA':
            if any(ext in self.store_full_fn for ext in ['csv', 'csv.gz', 'parquet']):
                file_extension = Path(self.store_full_fn).suffix
                if file_extension != self.output_fext:
                    raise ValueError(
                        f"Error: File Extension '{file_extension}' does not match specified "
                        f"extension {self.output_fext}'")
            else:
                self.store_full_fn = self.store_full_fn
        self.output_fnum = 0
        self.output_cur_fname = self.output_path / f'output-{self.output_fnum}' / \
            f'{self.output_fext}'
        self.output_full_fname = None

    def _batch_append(self, msg: dict):
        """
        Append the message to the batch of data.
        """
        self.data_batch.append(msg)
        self.distribute_to_event_handlers(msg)
        self.data_batch_cnt += 1
        if self.data_batch_cnt >= self.save_every_n_batch:
            self.logger.info(f'Processed {self.data_batch_cnt} events. Saving data...')
            self._save_batch()
            self.data_batch_cnt = 0

    def _save_batch(self, final=False):
        """
        Save the batch of data to file.
        """
        if final:
            self.logger.info('Saving final batch...')
        else:
            self.logger.info('Saving batch...')
        if self.data_batch:
            df = pl.DataFrame(self.data_batch, schema=self.output_schema)
            self._file_write(df, self.output_cur_fname)
            if self.store_full_data:
                self.full_data += self.data_batch
            self.output_fnum += 1
            self.output_cur_fname = self.output_path / f'output-{self.output_fnum}' / \
                f'{self.output_fext}'
            self.data_batch = []
            if final and self.store_full_data:
                #  save full data to file
                self.output_full_fname = self.output_path / f'{self.store_full_fn}' / \
                    f'{self.output_fext}'
                fdf = pl.DataFrame(self.full_data, schema=self.output_schema)
                self._file_write(fdf, self.output_full_fname)

    def _file_write(self, dataframe: pl.DataFrame, path: Path):
        """
        Write the dataframe to the current output file.
        Args:
            dataframe: pl.DataFrame: The dataframe to write.
            path: Path: The path to write the dataframe to.
        """
        if self.output_fmt == 'csv':
            if self.compression:
                with gzip.open(path, 'wt') as f:
                    dataframe.write_csv(f)
            else:
                dataframe.write_csv(path)
        elif self.output_fmt == 'parquet':
            if self.compression:
                _cpress = 'gzip'
            else:
                _cpress = 'snappy'
            dataframe.write_parquet(path, compression=_cpress)


class BatchEveryIntervalHandler(BatchHandler):
    """
    A handler that batches data based on a specified interval. This is useful for event
    processing data that has large gaps in-between values, or where the data is not recorded at
    a regular interval. The batch handler will save the data to file in batches so that the data
    can be processed in a streaming fashion later.
    Args:
        batch_interval: int: The interval inseconds to process each batch of events.
        output_path: str: The path to save the output data.
        save_every_n_batch: int: The number of batches to process before saving the data.
        output_fmt: str: The output format. Can be 'csv' or 'parquet'. Defaults to 'parquet'.
        compression: bool: True if the output should be compressed, False if not. Defaults to
             False.
        store_full: bool: True if the full data should be stored in memory, and saved at the end of
            processing. False if not. Defaults to False.
        ffill_keys: list: A list of keys to fill forward any missing data. Defaults to None.
        disable_pl_inference: bool: True if polars should not infer the schema, False if it should.
            Defaults to False.
        process_batch_end: bool: If the final data msg passed into the batch is half-way through
            an interval, then setting this to True will process that data as if it were the last
            datawhen the next interval occurs. In some cases, this may be desired, but in others
            it may not. For example, if the data source was being recorded every minute, with the
            batchinterval set to 1 hour, and the last data in the login was at 1.01, then
            potentially there could be 59 minutes of missing data. So by setting this to False,
            the 1.01 data is not processed, and 1.00 will be the last time in the output
            data/passed on messages.
            Defaults to True.

    """

    def __init__(self, *args,
                 batch_interval: str,
                 ffill_keys: list = None,
                 process_batch_end=True,
                 **kwargs
                 ):
        """
        Args:
            batch_interval: int: The interval in seconds to process each batch of events.
            save_every_n_batch: int: The number of batches to process before saving the data.
        """
        super().__init__(*args, **kwargs)
        self.batch_interval = batch_interval
        self.batch_interval_ms = interval_string_to_milliseconds(batch_interval)
        self.process_batch_end = process_batch_end
        self.init = False
        self.next_batch_ts = None
        self.prev_msg = None
        self.blank_msg = None
        self.current_ts = None
        self.ffill_keys = ffill_keys

    def process(self, timestamp: int, msg: dict) -> None:
        """
        Generic example process method for processing each event.
        Args:
            timestamp: int: The timestamp of the event in milliseconds format
            msg: dict: The message of the event with your custom keys.
        Returns:
            None
        """
        if not self.init:
            self._initialize_timing(timestamp)
            self._initialize_fill_data(msg)
            self.init = True
        if timestamp >= self.next_batch_ts:
            #  deal with any missed timestamps
            prev_filled = False
            catchup_ts = get_next_interval(timestamp, self.batch_interval, initialize=True) - \
                self.batch_interval_ms
            self.prev_msg['__timestamp'] = self.next_batch_ts
            self._batch_append(self.prev_msg.copy())
            self.next_batch_ts = next(self.next_batch_gen)
            while self.next_batch_ts <= catchup_ts:
                #  if there is a previous message, then fill forward the keys
                if self.ffill_keys:
                    fill_msg = self.blank_msg
                    if not prev_filled:
                        for k in self.ffill_keys:
                            fill_msg[k] = self.prev_msg[k]
                        prev_filled = True
                else:
                    fill_msg = self.blank_msg
                #  TODO - have different fill types, such as fill with 0 or fill with previous
                fill_msg['__timestamp'] = self.next_batch_ts
                self._batch_append(fill_msg.copy())
                self.next_batch_ts = next(self.next_batch_gen)
        self.prev_msg = msg
        self.current_ts = timestamp

    def finalize(self) -> None:
        """
        Save any data stored in the deque data structures at end of processing.
        """
        if self.process_batch_end:
            if (self.next_batch_ts - self.current_ts) > 0:  # if not on an interval boundary
                #  add the last message to the batch. This is optional since there could be missing
                #  data that was not captured after the last entry passed in.
                self.prev_msg['__timestamp'] = self.next_batch_ts
                self._batch_append(self.prev_msg)
        self._save_batch(final=True)

    def get_results(self) -> dict:
        """
        Return the data processed in dict format.
        """
        results = {'data': self.full_data,
                   'output_path': self.output_path,
                   'fulldata_path': self.output_full_fname}
        return results

    def modify_transformations(self, *, rename_cols: dict, separator: str) -> None:
        """
        Modify the internal variables that are affected by actions that transform dataframe loaded,
        such as drops, renames, etc.
        Args:
            rename_cols: dict: The dict of the columns names that will be renamed with the original
                column name as the key and the new column name as the value.
            separator: str: The separator used to seperate the original column name from the new
                column name.
        """
        if self.ffill_keys is not None:
            #  modify ffills_keys to match the new column names
            for fkey in self.ffill_keys.copy():
                for k, v in rename_cols.items():
                    if k.split(separator)[0] == fkey:
                        self.ffill_keys.remove(fkey) if fkey in self.ffill_keys else None
                        self.ffill_keys.append(k)

    def _initialize_timing(self, timestamp: int):
        """
        Initialize the timing for the batch processing.
        Args:
            timestamp: int: The timestamp of the first message in the data to create the internal
            tracking timestamps.
        """
        #  initalize the first timestamp to line up with the next batch interval
        self.next_batch_ts = get_next_interval(timestamp, self.batch_interval, initialize=True,
                                               to_timezone=self.timezone)
        #  create a generator which will return the next batch interval timestamp forever
        following_ts = self.next_batch_ts + self.batch_interval_ms
        self.next_batch_gen = itertools.count(start=following_ts, step=self.batch_interval_ms)
        self.prev_ts = self.next_batch_ts - self.batch_interval_ms

    def _initialize_fill_data(self, msg: dict):
        """
        Initialize the fill data keys that will be used to fill forward any missing data.
        Args:
            msg: dict: The first message passed in.
        """
        self.blank_msg = {k: None for k in list(msg.keys())}  # create a dict the msg values


@dataclass
class ReplayStatusObj:
    """
    Stores replay status information.
    TODO - remove this at some point, since it is a leftover from conversion from the source
        code which synced live data with stored data in databases/ flat files/ etc. so that live
        could catch up in a few seconds with the stored data. This is why it's so simple :O

    Attributes:
        status (str): The status of the files. Can be 'OK', 'FORCESTOP', or 'NOVALIDFILESTOPROCESS'
        start_ts (int): The start timestamp of the files in unix epoch time in milliseconds.
        end_ts (int): The end timestamp of the files in unix epoch time in milliseconds.
    """
    status: str
    start_ts: int
    end_ts: int


def interval_string_to_milliseconds(interval: str) -> int:
    """
    Convert an interval string to millisecond format
    Args:
        interval: str: The interval string, e.g., '10s', '1m', '1h', '1d
    Returns:
        int: The interval in milliseconds.
    """
    timeunit = interval[-1]
    timevalue = int(interval[:-1])
    #  check if timevalues is numeric:
    if timeunit == 'l':  # milliseconds
        return timevalue
    elif timeunit == 's':
        return timevalue * 1000
    elif timeunit == 'm':
        return timevalue * 60 * 1000
    elif timeunit == 'h':
        return timevalue * 60 * 60 * 1000
    elif timeunit == 'd':
        return timevalue * 24 * 60 * 60 * 1000
    else:
        raise RuntimeError(f'Invalid interval string: {interval}')


def get_next_interval(timestamp: int, interval: str, initialize: bool = False,
                      to_timezone: timezone = timezone.utc) -> int:
    """
    Get the next interval in unix epoch format - millisecond format + align the timestamp to the
    interval boundary if requested.
    E.g. if the interval is 1h, and the timestamp is 10:52:00, the next interval will be 11:00:00.
    Args:
        timestamp: int: The timestamp in unix epoch format in milliseconds.
        interval: str: The interval string, e.g., '10s', '1m', '1h', '1d
        initialize: bool: If True, align the timestamp to the interval boundary.
            If False, return the next interval.
            E.g. :
            True: if the interval is 1h, and the timestamp is 10:52:00, the next interval will be
            11:00:00.
            False: if the interval is 1h, and the timestamp is 10:52:00, the next interval
            will be 11:52:00.
        to_timezone: timezone: Used when <XXd> 'days' is specified. The interval boundary will be
            midnight in thattimezone. The default is UTC.
    """
    timeunit = interval[-1]
    timevalue = int(interval[:-1])
    _ts = timestamp / 1000
    if timeunit == 'l':  # milliseconds
        d = timevalue / 1000
        if initialize:
            next_interval = ((_ts // d * d) + (
                interval_string_to_milliseconds(interval) / 1000)) * 1000
            tolerance = 1e-7
            if abs(next_interval - timestamp) < tolerance:
                # workaround for floating point rounding error with millis
                next_interval = timestamp + d * 1000
        else:
            next_interval = timestamp + d * 1000
    elif timeunit == 's':
        d = timevalue
        if initialize:
            next_interval = ((_ts // d * d) + (
                interval_string_to_milliseconds(interval) / 1000)) * 1000
        else:
            next_interval = timestamp + d * 1000
    elif timeunit == 'm':
        d = timevalue * 60
        if initialize:
            next_interval = ((_ts // d * d) + (
                interval_string_to_milliseconds(interval) / 1000)) * 1000
        else:
            next_interval = timestamp + d * 1000
    elif timeunit == 'h':
        d = timevalue * 60 * 60
        if initialize:
            next_interval = ((_ts // d * d) + (
                interval_string_to_milliseconds(interval) / 1000)) * 1000
        else:
            next_interval = timestamp + d * 1000
    elif timeunit == 'd':
        d = timevalue * 60 * 60 * 24
        if initialize:
            day_dt = datetime.fromtimestamp(timestamp / 1000).date()
            midnight_tz = int((datetime(day_dt.year, day_dt.month, day_dt.day,
                                        tzinfo=to_timezone).timestamp() * 1000))
            next_interval = midnight_tz + d * 1000
        else:
            next_interval = timestamp + d * 1000
    else:
        raise ValueError(f'Invalid interval string: {interval}')
    if initialize and next_interval == timestamp:
        # deal with cases where rounded timestamp is on interval boundary
        next_interval = timestamp + d * 1000
    return int(next_interval)
