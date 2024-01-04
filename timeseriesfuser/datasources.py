"""
Copyright (C) 2023-2024 Anthony Sweeney - email: safsweeney@gmail.com

Please view the LICENSE file for the terms and conditions
associated with this software.
"""
import glob
import logging
import os
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union

import polars as pl


@dataclass
class Src(ABC):
    """
    Abstract base class for the data sources. This is used to allow different formats such as
    files, databases, etc. to be used as a data source
    Attributes:
        logger: logging.Logger: The logger for the class.
        sorted_srcs: list: The list of sorted sources to process. Think of this as a list of
            chunks of data to process.
        datatypes: list: Datatypes for the data in Python list format
        datatypes_schema: dict: This is the schema used by Polars when creating Dataframes. It has
            the format of {'column_name': pl.datatypes.classes.DataTypeClass}. It is typically
            set by the DataInfo class once data has been initially loaded.
        datasrc: str: How to access the data source. This is typically a path to a folder or file
            but can be anything depending on the data source such as a URL, database connection,
            etc.
    """

    def __post__init__(self):
        self.logger = logging.getLogger(__name__)
        self.sorted_srcs = None
        self.datatypes = None  # set from DataInfo parent
        self.datatypes_schema = None  # set from DataInfo parent
        self.schema_init = False  # set from DataInfo parent
        self.datasrc = None

    @abstractmethod
    def sort_data(self) -> None:
        """
        Sort the data sources in chronological order.
        """
        pass

    @abstractmethod
    def load_data(self, read_source: Union[str, Path], *,
                  ignore_errors: bool,
                  num_rows: int = None) -> pl.DataFrame:
        """
        Load the data from the datasource into polars.
        Args:
            read_source: how to read the data source.
            ignore_errors: bool: True if errors should be ignored. This relates to Polars
                misinterpreting the datatypes of the columns. If True, then Polars will attempt to
                cast to the datatypes passed down from the DataInfo class. **warning**: if the
                datatypes are specified wrong this can result in null columns if a non-converting
                type is used. E.g. turning a Polars UTF8 column into a Polars INT.
            num_rows: number of rows in dataframe to read and return. Defaults to None which reads
                all rows.
        Returns:
            pl.DataFrame: The dataframe of the data.
        """
        pass

    @abstractmethod
    def get_global_start_data(self, *, ignore_errors: bool) -> pl.DataFrame:
        """
        Get the global start timestamp of the data source
        Args:
            ignore_errors:
                See load_data() for description.
        Returns:
            pl.DataFrame: The dataframe of the data.
        """
        pass

    @abstractmethod
    def get_global_end_data(self, *, ignore_errors: bool) -> pl.DataFrame:
        """
        Get the global end timestamp of the data source
        Args:
            ignore_errors:
                See load_data() for description.
        Returns:
            pl.DataFrame: The dataframe of the data.
        """
        pass


@dataclass
class FilesSrc(Src):
    """
    A class for handling files as a data source.
    Attributes:
        files_path: Union[str, Path]: The path to the folder containing the files.
        file_sort_regex: Optional[str]: The regex to use to sort the files. Defaults to find any
            number in the filename and sorts ascending.
        file_sort_idx: Optional[int]: The index of the sorted files to use. Defaults to -1.
        has_headers: Optional[bool]: True if the files have headers, False if not. Defaults to
            True.
        allow_retry: Optional[bool]: True if the files should be reloaded if there is a Polars
            error loading them. This can happen when not enough rows of data are passed when
            creating a Polars dataframe False if not. Defaults to True.
        datasrc: str: How to access the data source. This is typically a path to a folder or file
    """
    files_path: Union[str, Path]
    file_sort_regex: Optional[str] = r'\d+'
    file_sort_idx: Optional[int] = -1
    has_headers: Optional[bool] = True
    allow_retry: Optional[bool] = True

    def __post__init__(self):
        super().__post__init__()
        self.files_path = Path(self.files_path)
        self.compression = self._check_compression()  # autodetect if files are compressed
        self.datasrc = self._generate_data_paths(self.files_path)

    @abstractmethod
    def _generate_data_paths(self, base_path: Union[str, Path]) -> Path:
        """
        Generate the paths to filter out everything but the correct extention
        Args:
            base_path: str: The path to the folder containing the files.
        Returns:
            str: The filtered paths with the correct base extenssions.
        """
        pass

    @abstractmethod
    def _check_compression(self) -> bool:
        """
        Check if the files in the folder are compressed or not via file extension.
        Returns:
            False if the files are not compressed, True if they are compressed.
        """
        pass

    @abstractmethod
    def load_data(self, read_source: Union[str, Path], *,
                  ignore_errors: bool,
                  num_rows: int = None) -> pl.DataFrame:
        """
        Load the data from the datasource into polars.
        Args:
            read_source: how to read the data source.
            ignore_errors: bool: True if errors should be ignored. This relates to Polars
                misinterpreting the datatypes of the columns. If True, then Polars will attempt to
                cast to the datatypes passed down from the DataInfo class. **warning**: if the
                datatypes are specified wrong this can result in null columns if a non-converting
                type is used. E.g. turning a Polars UTF8 column into a Polars INT.
            num_rows: number of rows in dataframe to read and return. Defaults to None which reads
                all rows.
        Returns:
            pl.DataFrame: The dataframe of the data.
        """
        pass

    @abstractmethod
    def get_global_start_data(self, *, ignore_errors: bool) -> pl.DataFrame:
        """
        Get the global start timestamp of the data source
        Args:
            ignore_errors:
                See load_data() for description.
        Returns:
            pl.DataFrame: The dataframe of the data.
        """
        pass

    @abstractmethod
    def get_global_end_data(self, *, ignore_errors: bool) -> pl.DataFrame:
        """
        Get the global end timestamp of the data source
        Args:
            ignore_errors:
                See load_data() for description.
        Returns:
            pl.DataFrame: The dataframe of the data.
        """
        pass

    def sort_data(self) -> None:
        """
        Sort the data sources in chronological order.
        """
        filelist = glob.glob(str(self.datasrc))
        if len(filelist) == 0:
            self.logger.warning(f'No files found in {self.datasrc}')
        elif len(filelist) == 1:
            ipf = []
            _l = list(tuple([filelist[0], 0]))
            ipf.append(_l)
        else:
            sortl = {}
            for rfile in filelist:
                try:
                    filename_sorter = re.findall(
                        self.file_sort_regex, rfile)[self.file_sort_idx]
                    sortl[rfile] = filename_sorter
                except IndexError:
                    pass
            if len(sortl) == 0:
                self.logger.warning(f'procmode includes files but no data found '
                                    f'in {self.datasrc}')
            self.sorted_srcs = sorted(sortl.items(), key=lambda x: int(x[1]))


@dataclass
class CSVSrc(FilesSrc):
    """
    A class for handling CSV files as a data source.
    Attributes:
        proc_csv_endfile_lowmem_mode: ptional[bool]: True if the last file should be read in low
            memory mode. This uses an alternative way of loading data that uses the os library to
            seek inside a CSV file using Python builtins instead of Polars methods. Defaults to
             False.
    """
    proc_csv_endfile_lowmem_mode: Optional[bool] = False

    def __post_init__(self):
        super().__post__init__()

    def load_data(self, read_source: Union[str, Path], *,
                  ignore_errors: bool,
                  num_rows: int = None) -> pl.DataFrame:
        """
        Load the data from CSV into polars.
        Args:
            read_source: the path to the file to read
            ignore_errors: bool: True if errors should be ignored. This relates to Polars
                misinterpreting the datatypes of the columns. If True, then Polars will attempt to
                cast to the datatypes passed down from the DataInfo class. **warning**: if the
                datatypes are specified wrong this can result in null columns if a non-converting
                type is used. E.g. turning a Polars UTF8 column into a Polars INT.
            num_rows: number of rows in dataframe to read and return. Defaults to None which reads
                all rows.
        Returns:
            pl.DataFrame: The dataframe of the data.
        """
        try:
            df = pl.read_csv(read_source,
                             n_rows=num_rows,
                             has_header=self.has_headers,
                             dtypes=self.datatypes,
                             ignore_errors=ignore_errors)
        except pl.ComputeError as e:
            if self.allow_retry:
                #  use full data to infer dtypes
                df = pl.read_csv(read_source,
                                 n_rows=num_rows,
                                 has_header=self.has_headers,
                                 infer_schema_length=None,
                                 dtypes=self.datatypes,
                                 ignore_errors=ignore_errors)
            else:
                raise e
        return df

    def load_data_end_lowmem_mode(self) -> pl.DataFrame:
        """
        Load the data from the end of the last file using the os library seek commands so that
        minimal data is loaded into memory.
        Returns:
            pl.DataFrame: The dataframe of the data.
        """
        if self.sorted_srcs[-1][0].endswith('.gz') or self.sorted_srcs[-1][0].endswith('.zip'):
            raise ValueError('proc_csv_endfile_lowmem_mode only works with '
                             'uncompressed CSV files')
        with open(self.sorted_srcs[-1][0], 'rb') as rfile:
            try:
                rfile.seek(-2, os.SEEK_END)
                while rfile.read(1) != b'\n':
                    rfile.seek(-2, os.SEEK_CUR)
            except OSError:
                rfile.seek(0)
            last_line = rfile.readline().decode().strip()
            if last_line:
                #  columns we already read above
                rows = last_line.split(',')
                lrows = [rows]
                df = pl.DataFrame(lrows, self.datatypes_schema)
            else:
                raise ValueError(
                    'No valid data at end of file with proc_csv_endfile_lowmem_mode set.')
        return df

    def get_global_start_data(self, *, ignore_errors: bool) -> pl.DataFrame:
        """
        Get the global start timestamp of the data source
        Args:
            ignore_errors:
                See load_data() for description.
        Returns:
            pl.DataFrame: The dataframe of the data.
        """
        return self.load_data(self.sorted_srcs[0][0], num_rows=50, ignore_errors=ignore_errors)

    def get_global_end_data(self, *, ignore_errors: bool) -> pl.DataFrame:
        """
        Get the global end timestamp of the data source
        Args:
            ignore_errors:
                See load_data() for description.
        Returns:
            pl.DataFrame: The dataframe of the data.
        """
        if self.proc_csv_endfile_lowmem_mode:
            df = self.load_data_end_lowmem_mode()
        else:  # normal reading behaviour
            df = self.load_data(self.sorted_srcs[-1][0], ignore_errors=ignore_errors)
        return df

    def _check_compression(self) -> bool:
        """
        Check if the files in the folder are compressed or not via file extension.
        Returns:
            False if the files are not compressed, True if they are compressed.
        """
        csvfound = any(self.files_path.glob('*.csv'))
        csvgzfound = any(self.files_path.glob('*.csv.gz'))
        zipfound = any(self.files_path.glob('*.zip'))
        #  check if more than one type has been found:
        if sum([csvfound, csvgzfound, zipfound]) > 1:
            raise RuntimeError(f'More than one compression file type found in {self.files_path}.')
        elif sum([csvfound, csvgzfound, zipfound]) == 0:
            raise RuntimeError(f'No csv, csv.gz, or zip files found in {self.files_path}.')
        if sum([csvgzfound, zipfound]) == 1:
            return True
        else:
            return False

    def _generate_data_paths(self, base_path: Union[str, Path]) -> Path:
        """
        Generate the paths to filter out everything but the correct extention
        Args:
            base_path: str: The path to the folder containing the files.
        Returns:
            str: The filtered paths with the correct base extenssions.
        """
        base_path = Path(base_path)
        if self.compression:
            datadir = base_path / '*.csv.gz'
        else:
            datadir = base_path / '*.csv'
        return datadir


@dataclass
class ParquetSrc(FilesSrc):
    """
    A class for handling Parquet files as a data source.
    Attributes:
        force_schema: Optional[bool]: True if the schema should be forced to the datatypes passed
            by casting to the datatypes passed down from the DataInfo class. By default the schema
            is encoded in the Parquet file itself.
    """
    force_schema: Optional[bool] = False

    def __post_init__(self):
        super().__post__init__()

    def load_data(self, read_source: Union[str, Path], *,
                  ignore_errors: bool,
                  num_rows: int = None) -> pl.DataFrame:
        """
        Load the data from Parquet format  into polars.
        Args:
            read_source: the path to the file to read
            ignore_errors: bool: True if errors should be ignored. This relates to Polars
                misinterpreting the datatypes of the columns. If True, then Polars will attempt to
                cast to the datatypes passed down from the DataInfo class. **warning**: if the
                datatypes are specified incorrectly this can result in null columns if a
                non-converting type is used. E.g. turning a Polars UTF8 column into a Polars INT.
            num_rows: number of rows in dataframe to read and return. Defaults to None which reads
                all rows.
        Returns:
            pl.DataFrame: The dataframe of the data.
        """
        try:
            df = pl.read_parquet(read_source, n_rows=num_rows)
        except pl.ComputeError as e:
            if self.allow_retry:
                #  need to test this eventuality....
                raise e
            else:
                raise e
        if self.force_schema and self.schema_init:
            df = df.cast(self.datatypes_schema)
        return df

    def get_global_start_data(self, *, ignore_errors: bool) -> pl.DataFrame:
        """
        Get the global start timestamp of the data source
        Args:
            ignore_errors:
                See load_data() for description.
        Returns:
            pl.DataFrame: The dataframe of the data.
        """
        return self.load_data(self.sorted_srcs[0][0], num_rows=50, ignore_errors=ignore_errors)

    def get_global_end_data(self, *, ignore_errors: bool) -> pl.DataFrame:
        """
        Get the global end timestamp of the data source
        Args:
            ignore_errors:
                See load_data() for description.
        Returns:
            pl.DataFrame: The dataframe of the data.
        """
        df = self.load_data(self.sorted_srcs[-1][0], ignore_errors=ignore_errors)
        return df

    def _check_compression(self) -> bool:
        """
        Check if the files in the folder are compressed or not via file extension.
        Returns:
            False if the files are not compressed, True if they are compressed.
        """
        #  parquet files have compression built in if enabled, but extension does
        #  not change
        return False

    def _generate_data_paths(self, base_path: Union[str, Path]) -> Path:
        """
        Generate the paths to filter out everything but the correct extention
        Args:
            base_path: str: The path to the folder containing the files.
        Returns:
            str: The filtered paths with the correct base extenssions.
        """
        base_path = Path(base_path)
        return base_path / '*.parquet'
