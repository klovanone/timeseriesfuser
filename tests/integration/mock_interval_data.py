"""
Copyright (C) 2023-2024 Anthony Sweeney - email: safsweeney@gmail.com

Please view the LICENSE file for the terms and conditions
associated with this software.
"""
import os.path
from datetime import datetime, timedelta, timezone
import itertools
import random
import pandas as pd
import string


def create_1second():
    # Set the start time to midnight UTC on Jan 1st, 2020
    start_time = datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    entries = [start_time + timedelta(seconds=i) for i in range(59)]
    entries_with_random_offset = [entry + timedelta(milliseconds=random.randint(0, 999))
                                  for entry in entries]
    entry_list = [int(entry.timestamp() * 1000) for entry in entries_with_random_offset]
    letters = list(itertools.islice(itertools.cycle(string.ascii_uppercase), len(entry_list)))

    df = pd.DataFrame({'Timestamp': entry_list, 'Human_Timestamp': entries_with_random_offset,
                       'Letter': letters})
    df['Human_Timestamp'] = df['Human_Timestamp'].astype(str)
    fpath = './data/interval_handler/'
    if not os.path.exists(fpath):
        os.makedirs(fpath)
    # df.to_csv('./data/interval_handler/1second_letters.csv', index=False)
    df.to_parquet('./data/interval_handler/1second_letters.parquet', index=False)


def create_1minute():
    start_time = datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    entries = [start_time + timedelta(minutes=i) for i in range(26)]
    entries_with_random_offset = [entry + timedelta(seconds=random.randint(0, 59))
                                  for entry in entries]
    entry_list = [int(entry.timestamp() * 1000) for entry in entries_with_random_offset]
    letters = list(itertools.islice(itertools.cycle(string.ascii_uppercase), len(entry_list)))

    df = pd.DataFrame({'Timestamp': entry_list, 'Human_Timestamp': entries_with_random_offset,
                       'Letter': letters})
    df['Human_Timestamp'] = df['Human_Timestamp'].astype(str)
    fpath = './data/interval_handler/'
    if not os.path.exists(fpath):
        os.makedirs(fpath)
    # df.to_csv('./data/interval_handler/1minute_letters.csv', index=False)
    df.to_parquet('./data/interval_handler/1minute_letters.parquet', index=False)


def create_1hour():
    start_time = datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    entries = [start_time + timedelta(hours=i) for i in range(26)]
    entries_with_random_offset = [entry + timedelta(minutes=random.randint(0, 59))
                                  for entry in entries]
    entry_list = [int(entry.timestamp() * 1000) for entry in entries_with_random_offset]
    letters = list(itertools.islice(itertools.cycle(string.ascii_uppercase), len(entry_list)))
    df = pd.DataFrame({'Timestamp': entry_list, 'Human_Timestamp': entries_with_random_offset,
                       'Letter': letters})
    df['Human_Timestamp'] = df['Human_Timestamp'].astype(str)
    fpath = './data/interval_handler/'
    if not os.path.exists(fpath):
        os.makedirs(fpath)
    # df.to_csv('./data/interval_handler/1hours_letters.csv', index=False)
    df.to_parquet('./data/interval_handler/1hours_letters.parquet', index=False)


def create_1day():
    start_time = datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    entries = [start_time + timedelta(days=i) for i in range(26)]
    entries_with_random_offset = [entry + timedelta(hours=random.randint(0, 23))
                                  for entry in entries]
    entry_list = [int(entry.timestamp() * 1000) for entry in entries_with_random_offset]
    letters = list(itertools.islice(itertools.cycle(string.ascii_uppercase), len(entry_list)))
    df = pd.DataFrame({'Timestamp': entry_list, 'Human_Timestamp': entries_with_random_offset,
                       'Letter': letters})
    df['Human_Timestamp'] = df['Human_Timestamp'].astype(str)
    fpath = './data/interval_handler/'
    if not os.path.exists(fpath):
        os.makedirs(fpath)
    # df.to_csv('./data/interval_handler/1day_letters.csv', index=False)
    df.to_parquet('./data/interval_handler/1day_letters.parquet', index=False)


def create_1second_gaps():
    # Set the start time to midnight UTC on Jan 1st, 2020
    start_time = datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    entries = [start_time + timedelta(seconds=(i * 5)) for i in range(5)]
    entries_with_random_offset = [entry + timedelta(milliseconds=random.randint(0, 999))
                                  for entry in entries]
    entry_list = [int(entry.timestamp() * 1000) for entry in entries_with_random_offset]
    letters = list(itertools.islice(itertools.cycle(string.ascii_uppercase), len(entry_list)))
    df = pd.DataFrame({'Timestamp': entry_list,
                       'Human_Timestamp': entries_with_random_offset,
                       'Letter': letters,
                       'Nonfill_letter': letters})
    df['Human_Timestamp'] = df['Human_Timestamp'].astype(str)
    fpath = './data/interval_handler/'
    if not os.path.exists(fpath):
        os.makedirs(fpath)
    # df.to_csv('./data/interval_handler/1second_letters.csv', index=False)
    df.to_parquet('./data/interval_handler/1second_letters_gaps.parquet', index=False)


create_1second()
create_1minute()
create_1hour()
create_1day()
create_1second_gaps()
