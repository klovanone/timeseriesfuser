"""
Copyright (C) 2023-2024 Anthony Sweeney - email: safsweeney@gmail.com

Please view the LICENSE file for the terms and conditions
associated with this software.
"""
import logging
import math
import re
import warnings
from datetime import datetime, timezone
from threading import Thread
from typing import Union

import numpy as np


def get_intervals_seconds(timeframe: str) -> int:
    """
    Convert a timeframe string to the corresponding number of milliseconds.
    Args:
        timeframe (str): A string representing the timeframe, e.g., '1s' for 1 second.
    Returns:
        int: The number of milliseconds corresponding to the provided timeframe.
    """
    if timeframe[-1] == 's':
        secs = int(timeframe[:-1])
        return secs * 1000
    else:
        raise NotImplementedError(
            'Only seconds are supported. Please use a timeframe in seconds, e.g., 1s, 5s, 10s, '
            'etc.')


def count_digits(n: Union[int, float]) -> int:
    """
    Count the number of digits in an integer or float
    Args:
        n (int or float): The integer to count digits in.
    Returns:
        int: The count of digits in the integer.
    """
    # 15 digits, python limit otherwise there are errors (in some cases depending on OS)
    if n <= 999999999999997:
        return int(math.log10(n)) + 1
    else:
        if isinstance(n, float):
            n = int(n)
        return len('%i' % n)


def convert_time_obj_to_epoch_format(dateobj: Union[int, float, str, datetime, np.int64]) -> int:
    """
    Convert various time representations to Unix epoch format in milliseconds.
    Args:
        dateobj (Union[int, float, str, datetime, np.int64]): The time object to convert.I t can be
            in various formats:
            - int or float: Unix epoch timestamp (in seconds or milliseconds).
            - str: ISO 8601 formatted string.
            - datetime: Python datetime object.
            - np.int64: NumPy int64 type.
    Returns:
        int: The Unix epoch timestamp in milliseconds.
    """
    if isinstance(dateobj, (int, float, np.int64)):
        if isinstance(dateobj, np.int64):
            dateobj = int(dateobj)
        objlen = count_digits(dateobj)
        if objlen == 10:  # Timestamp column is in ms, change to seconds
            timestamp = int(dateobj * 1000)
        elif objlen == 13:
            timestamp = dateobj
        else:
            raise ValueError(f'Invalid timestamp format - {dateobj}')
    elif isinstance(dateobj, str):
        # Convert ISO 8601 formatted string to Unix epoch
        dto = datetime.fromisoformat(dateobj.replace('Z', '+00:00')).replace(tzinfo=timezone.utc)
        timestamp = int(dto.timestamp() * 1000)
    elif isinstance(dateobj, datetime):
        try:
            offset = dateobj.utcoffset().total_seconds()
        except AttributeError:
            dateobj = dateobj.astimezone(timezone.utc)
            warnings.warn('Warning - naive timezone passed in - attempting to guess timezone and '
                          'converting to UTC.')
            offset = 0
        if offset == 0:  # already a datetime, in UTC format!
            timestamp = int(dateobj.timestamp() * 1000)
        else:
            dateobj = dateobj.astimezone(timezone.utc)
            timestamp = int(dateobj.timestamp() * 1000)
    else:
        raise RuntimeError('Terminating - the format of the datetime is not a recognized format '
                           '(string, epoch timestamp format, Python Datetime, or np.int64).')
    return timestamp


def isotime(timestamp: int) -> str:
    """
    Convert a timestamp to an ISO 8601 formatted string in LOCAL timezone format. Used for logging
    purposes.
    Args:
        timestamp int: The timestamp to convert in millisecond format.
    Returns:
        str: The ISO 8601 formatted string representation of the timestamp in UTC timezone, if the
            format is valid.
    """
    if timestamp is None:
        # with complex projects, occasionally the calling timestamp var has not been set yet
        return 'NO TIMESTAMP'
    timestamp = timestamp / 1000  # convert to seconds for datetime which handles 1AD to 9999AD
    return datetime.fromtimestamp(timestamp).isoformat()


def toutcisotime(timestamp: int) -> str:
    """
    Convert a timestamp to an ISO 8601 formatted string in UTC timezone. Used for logging purposes.
    Args:
        timestamp int: The timestamp to convert in milliseconds epoch format.
    Returns:
        str: The ISO 8601 formatted string representation of the timestamp in UTC timezone, if the
            format is valid.
    """
    if timestamp is None:
        # occasionally the calling timestamp var has not been set yet
        return 'NO TIMESTAMP'
    timestamp = timestamp / 1000  # convert to seconds for datetime which handles 1AD to 9999AD
    return datetime.fromtimestamp(timestamp).astimezone(timezone.utc).isoformat()


def convert_to_bytes(size_str: str) -> int:
    """
        Convert a size string to the corresponding number of bytes.
        Args:
            size_str (str): The size string to convert, e.g., '1GB', '1.5MB', '2.8TB'.
        Returns:
            int: The size in bytes.
        """
    units = {"B": 1, "KB": 1024, "MB": 1024 ** 2, "GB": 1024 ** 3, "TB": 1024 ** 4}
    match = re.match(r'(\d+(\.\d+)?)\s*([A-Za-z]+)', size_str)
    if match:
        size, _, unit = match.groups()
        size = float(size)
        if unit in units:
            bytes_num = size * units[unit]
            return bytes_num
        else:
            raise ValueError("Invalid unit specified- use B, KB, MB, GB, or TB")
    else:
        raise ValueError("Invalid size string passed in: ({size_str})- example valid strings are "
                         "1GB, 1.5MB, 2.8TB")


class ReturnThread(Thread):
    """
    This class modifes Thread to return the return value of the target function, and also to raise
    any exceptions. Notes: It's usually not a good idea to use this class, as it is better to use
    the concurrent.futures module as it handles cleanup properly when lots of threads are ending
    and being recreated. However, I like this one for long-running jobs or when to run core logic
    in a seperate thread while the main thread is used for GUI processing.
    """

    def __init__(self, group=None, target=None, name=None, args=(), kwargs=None, *, daemon=None):
        Thread.__init__(self, group, target, name, args, kwargs, daemon=daemon)
        self._return = None
        self._exc = None

    def run(self):
        try:
            if self._target:
                self._return = self._target(*self._args, **self._kwargs)
        except BaseException as e:
            self._exc = e
        finally:  # added from Thread code Nov 22
            # Avoid a refcycle if the thread is running a function with
            # an argument that has a member that points to the thread.
            del self._target, self._args, self._kwargs

    def join(self, timeout=None):
        Thread.join(self, timeout=timeout)
        if self._exc:
            raise self._exc
        else:
            return self._return


class CustomAdapter(logging.LoggerAdapter):
    """
    This class is used to add extra info to the log output. Use this to more easily segment the
    source of logging messages. For example, if you have multiple threads and many complex modules,
     you can add a unique identifier to each one.
    """

    def process(self, msg, kwargs):
        return '[%s] %s' % (self.extra['info'], msg), kwargs


class ServiceExit(Exception):
    """
    Custom exception which is used to trigger the clean exit
    of all running threads and the main program. Intentionally blank.
    """
    pass


def service_shutdown(signum, frame):
    """
    Function to catch signals and trigger a clean exit of all running threads and the main program.
    Binds SIGTERM and SIGINT to this function.
    """
    print('-------------------------------------------------------------------------------')
    print(f'Caught signal {signum}, {frame}')
    print('Attempting to gracefully shutdown program by raising a ServiceExit exception...')
    raise ServiceExit
