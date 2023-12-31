import collections
import gzip
import logging
import math
import time
import warnings
import polars as pl
from datetime import datetime, timezone
from typing import Optional, Union, Any
from pathlib import Path

from timeseriesfuser import LOGSPATH
from timeseriesfuser.statics import FEATUREFLAG
from timeseriesfuser.classes import DataInfo, BaseHandler, ReplayStatusObj
from timeseriesfuser.helpers import helpers
from timeseriesfuser.helpers.helpers import toutcisotime


class TimeSeriesFuser:
    """
    Read multiple datasources in chunks and combine them in chronological order. Outputs to a
    Handler to prepare the timeseries data for event-driven processing.
    """

    def __init__(self, *,
                 datainfos: list[DataInfo],
                 handler: BaseHandler,
                 logger: Optional[logging.Logger] = None,
                 procstart: Optional[int] = None,
                 procend: Optional[int] = None,
                 root_logspath: Optional[Union[str, Path]] = LOGSPATH,
                 secondary_sort_col: Optional[str] = None,
                 remove_internal_cols: Optional[bool] = True,
                 forward_fill_data: Optional[bool] = False,
                 force_ignore_pl_read_errors: Optional[bool] = False,
                 rename_identi_cols: Optional[bool] = True,
                 merge_col_names: Optional[list[str]] = None,
                 col_name_sep: Optional[str] = '||'
                 ):
        """
        Args:
            datainfos (list[DataInfo]): A list of DataInfo objects.
            handler (BaseHandler): A handler object that has a process method.
            logger (logging.Logger): A logger object.
            procstart (int): The start timestamp to process from (in epoch ms).
            procend (int): The end timestamp to process to (in epoch ms).
            root_logspath (Union[str, Path]): The root path to store logs.
            remove_internal_cols: bool: If True, remove internal columns from being passed to the
                handler. E.g. exsym_lookup (used internally for merging).
            forward_fill_data: bool: Forward fill data using previous value if nulls are
            encountered using a Polars expression. A very basic forward fill. This should be
            specified in a Handler instead for more control. Defaults to False.
            rename_identi_cols: bool: If True, rename identical columns in the DataInfo objects to
                be unique by appending the DataInfo descriptor to the column name.
            merge_col_names: list[str]: A list of column names to merge on.
            col_name_sep: str: The separator to use when renaming identical columns.
                Default is '||'. The descriptor of each DataInfo will be added to the end of the
                column name.

        """
        self.loggermain = logger or logging.getLogger(__name__)
        self.logger = helpers.CustomAdapter(self.loggermain, {'info': 'TSF'})
        self.handler = handler
        self.datainfos = datainfos
        self.di_lu = {}
        self.di_same_header_fmt = True
        self.di_timestamp_col = '__timestamp'  # column that common time will be converted to
        self.di_secondary_sort_col = secondary_sort_col
        self.di_global_col_fmt = None
        self.di_global_col_dtypes = None
        self.di_global_col_schema = None
        self.di_global_col_rename = None
        self.root_logpath = Path(root_logspath)
        self.remove_internal_cols = remove_internal_cols
        self.forward_fill = forward_fill_data
        self.force_ignore_pl_read_errors = force_ignore_pl_read_errors
        self.continueprocessing = True
        self.process_results = None
        self.last_ts = 0
        self.rename_identi_cols = rename_identi_cols
        if merge_col_names is None:
            self.merge_col_names = []
        else:
            self.merge_col_names = merge_col_names
        self.col_name_sep = col_name_sep
        self.last_row_vals = None
        self._create_lookups(datainfos)
        self._get_global_start_end_timestamps(datainfos, start_ts=procstart, end_ts=procend)
        self._remove_out_of_bounds_data_infos()  # reduce data size if possible

    def start_tsf(self) -> list[dict[str, Any]]:
        """
        Start processing the files and attempt to merge. Processing starts here.
        Returns
            dict: self.proc_results: The results of the processing
        """
        if len(self.datainfos) == 0:
            raise RuntimeError(f'No Valid DataInfos to process, exiting.')
        start_timer = time.time()
        start_ts, end_ts = self._pre_setup()
        if self.di_global_col_rename:
            self.handler.modify_transformations(rename_cols=self.di_global_col_rename,
                                                separator=self.col_name_sep)
        self.handler.set_datatypes(self.di_global_col_schema,
                                   remove_internal_keys=self.remove_internal_cols)
        rp_runstate = self._replay_merge_via_file_multi_polars(startproc_ts=start_ts,
                                                               endproc_ts=end_ts)
        if not rp_runstate.status == 'OK':
            raise RuntimeError(f'Replay runstate not ok! Status is {rp_runstate.status}')
        self._end_proc(rp_runstate=rp_runstate, start_proc_time=start_timer)
        # Now replay is finished, continue as normal
        self.logger.info('Merge complete.')
        self.logger.info(f'TimeSeriesFuser FINISHED.')
        return self.process_results

    def stop_tsf(self, forceclose: bool = False) -> None:
        """
        Stop Processing, set a flag for the external thread to stop processing and exit cleanly.
        """
        if forceclose:
            self.logger.info('TimeSeriesFuser Stop signal received- FORCE closed...')
        else:
            self.logger.info('TimeSeriesFuser Stop signal received')
        self.handler.finalize()
        self.continueprocessing = False

    def _create_lookups(self, datainfos: list[DataInfo]) -> None:
        """
        Create a lookup dictionary for the DataInfo objects to reference quickly later on.
        """
        lu = 0
        for di in datainfos:
            lu_key = lu
            di.exsym_lu = lu_key
            self.di_lu[lu_key] = di
            lu += 1

    def _get_global_start_end_timestamps(self, datainfos: list[DataInfo], *,
                                         start_ts=None, end_ts=None) -> None:
        """
        Helper function to get the global start and end timestamps for all files.
        Args:
            start_ts: int: The start timestamp to process from (in epoch ms).
            end_ts: int: The end timestamp to process to (in epoch ms).
        """
        remove_dis = []
        valid_dis = []
        global_procstart_ts = math.inf
        global_procend_ts = -math.inf
        for di in datainfos:
            dataprocstart = self._get_global_proc_start_end_from_files('start', datainfo=di)
            dataprocend = self._get_global_proc_start_end_from_files('end', datainfo=di)
            if dataprocstart is None or dataprocend is None:
                raise RuntimeError(f'No proc data found for {di.descriptor}')
            if dataprocend < dataprocstart:
                raise ValueError(f'Proc mode start time is after end time!!! - check that the file'
                                 f' sort regex and file sort idx are correct for {di.descriptor}. '
                                 f'Currenty it is looking at the {di.file_sort_idx} value (-1 is '
                                 f'the last entry in the file) and using regex '
                                 f'{di.file_sort_regex} to extract the order from the source files'
                                 )
            di.procstart_ts = dataprocstart
            if start_ts:
                exst = helpers.convert_time_obj_to_epoch_format(start_ts)
                if exst > dataprocstart:
                    di.procstart_ts = exst
            di.procend_ts = dataprocend
            if end_ts:
                exend = helpers.convert_time_obj_to_epoch_format(end_ts)
                if exend < dataprocend:
                    di.procend_ts = exend
            if di.procstart_ts >= di.procend_ts:
                self.logger.warning(
                    f'{di.descriptor}: proc start time {toutcisotime(di.procstart_ts)} UTC is '
                    f'after the proc end time {toutcisotime(di.procend_ts)} UTC for the '
                    f'{di.descriptor} files at {toutcisotime(di.procend_ts)} UTC')
                remove_dis.append(di)
            else:
                valid_dis.append(di)
                di.data_procstart_ts = di.procstart_ts
                di.data_procend_ts = dataprocend
                if di.procstart_ts < global_procstart_ts:
                    global_procstart_ts = di.procstart_ts
                if di.procend_ts > global_procend_ts:
                    global_procend_ts = di.procend_ts
        self.global_procstart = global_procstart_ts
        self.global_procend = global_procend_ts
        #  The below should not be triggered now unless the wrong proc times are passed in through
        #  human error.
        if valid_dis:
            if self.global_procstart > self.global_procend:
                raise ValueError(f'proc mode start time is after end time!!!')
            if self.global_procstart == self.global_procend:
                raise ValueError(f'proc mode start time is the same as the end time!!!')
        self.datainfos_to_remove = remove_dis

    def _remove_out_of_bounds_data_infos(self) -> None:
        """
        Remove any DataInfo objects that have no data between the start and end timestamps to be
        more efficient.
        """
        for di in self.datainfos_to_remove:
            self.logger.warning(f'Removing {di.descriptor} from processing - as data within falls '
                                f'outside of the start and end times specifed.')
            self.datainfos.remove(di)
        self.datainfos_to_remove = None

    def _pre_setup(self) -> tuple[int, int]:
        """
        Setup the DataInfo objects and get the start and end timestamps for processing.
        """
        startproc_ts = self._replay_get_proc_times_multiasset_start()
        endproc_ts = self._replay_get_proc_times_multiasset_end()
        global_col_fmt = None
        global_column_names = []
        for di in self.datainfos:
            # --  start of section to multi arrange files for processing with first file with ts
            if len(di.datareader.sorted_srcs) == 0:
                self.logger.warning(f'No files found for {di.descriptor}, removing '
                                    f'{di.descriptor} from processing')
                self.datainfos.remove(di)
                continue

            input_srcs = self._filter_data_sources(datainfo=di, start_ts=startproc_ts)
            df = di.load_datasrc_overlay(input_srcs[0], num_rows=5,
                                         ignore_errors=self.force_ignore_pl_read_errors)
            header_fmt = di.set_header_formats(df)
            global_column_names.append(header_fmt)
            if global_col_fmt:
                if header_fmt != global_col_fmt:
                    self.di_same_header_fmt = False
            else:
                global_col_fmt = header_fmt

            #  check the shape of the file so that we can detect malformed files later
            di.initial_numcols = df.shape[1]
            if len(input_srcs) == 0:
                warnmsg = f'proc/Replay startdate not found in {self.di_lu[di.exsym_lu]} ' \
                          f'files- this may be ok if end of data has been hit '
                warnings.warn(warnmsg)
                di.eofs = True
            else:
                di.eofs = False
            di.proc_inputsrcs = collections.deque(input_srcs)
            # --  end of section to multi arrange files for processing with first file with ts

            if di.has_headers:
                di.header_fmt_file_load = list(df.columns)

                if di.remove_cols:
                    di.header_fmt = []
                    for col in di.header_fmt_file_load:
                        if col not in di.remove_cols:
                            di.header_fmt.append(col)
                else:
                    di.header_fmt = list(df.columns)
                global_column_names.append(di.header_fmt)
                if global_col_fmt:
                    if di.header_fmt != global_col_fmt:
                        self.di_same_header_fmt = False
                else:
                    global_col_fmt = di.header_fmt
            else:
                raise NotImplementedError('No non-headered files support for CSV files yet')

        if len(self.datainfos) == 0:
            self.logger.info(f'No Valid DataInfos to process, exiting.')
            raise RuntimeError(
                f'No Valid entries in files from times {toutcisotime(startproc_ts)} '
                f'to {toutcisotime(endproc_ts)} - exiting.')

        self._set_global_column_format(global_column_names)
        tdfs = self._prepare_initial_data(start_ts=startproc_ts)

        #  since polars diagonal concat will be used (append different dataframe shapes to one
        #  dataframe) the column names will change so take a reference to the column order here...
        if self.di_same_header_fmt:
            tdf = pl.concat(tdfs)
        else:
            tdf = pl.concat(tdfs, how='diagonal')  # align and put null values in missing columns
        #  here is where to transform columns based on future transformations. E.g. drop
        #  columns, add columns in case of utf8 to timestamp int but keep original etc. columns
        #  have to be dropped
        self.di_global_col_fmt = list(tdf.columns)
        self.di_global_col_dtypes = tdf.dtypes
        self.di_global_col_schema = {k: v for k, v in zip(self.di_global_col_fmt,
                                                          self.di_global_col_dtypes)}
        return startproc_ts, endproc_ts

    def _set_global_column_format(self, global_column_names):
        if len(self.datainfos) > 1:
            identical_cols = set(global_column_names[0]).intersection(*global_column_names[1:])
            if len(identical_cols) > 0 and self.rename_identi_cols:
                self.di_same_header_fmt = False
                global_rename_cols = {}
                for di in self.datainfos:
                    merge_cols = [di.ts_col]
                    if self.merge_col_names:
                        merge_cols = merge_cols + self.merge_col_names
                    identical_cols.difference_update(merge_cols)
                    for col in di.header_fmt:
                        if col in identical_cols:
                            rencol = f'{col}{self.col_name_sep}{di.descriptor}'
                            # this is reversed to have unique keys to pass to batcher
                            global_rename_cols[rencol] = col
                            di.rename_cols[col] = rencol
                            di.header_fmt_modified.append(
                                f'{col}{self.col_name_sep}{di.descriptor}')
                        else:
                            di.header_fmt_modified.append(col)
                self.di_global_col_rename = global_rename_cols

    def _prepare_initial_data(self, *, start_ts):
        tdfs = []
        for di in self.datainfos:
            di.nodata_from_start_to_end = False
            try:
                currentsrc = di.proc_inputsrcs.popleft()
                cfprocess = True
            except IndexError:
                di.nodata_from_start_to_end = True
                cfprocess = False
                currentsrc = None
            if cfprocess:
                df = self._load_dataframe_from_source_polars(source=currentsrc, datainfo=di,
                                                             force_full_schema_read=True,
                                                             discovery=False)
                df = df.rename({di.ts_col: self.di_timestamp_col})
                _cdft = df.filter(df[self.di_timestamp_col] >= start_ts)
                if _cdft.shape[0] == 0:  # empty
                    di.nodata_from_start_to_end = True
                else:
                    _cdft = _cdft.with_columns(pl.lit(di.exsym_lu).alias('exsym_lookup'))
                    di.cdf = _cdft
                    di.cfile_start_ts = di.cdf.head(1)[self.di_timestamp_col][0]
                    di.cfile_end_ts = di.cdf.tail(1)[self.di_timestamp_col][0]
                    if self.di_secondary_sort_col:
                        if not di.last_secondary_sort_id:
                            # only set if we have not processed already
                            # subtract 1 as current row of secondary sort not processed yet.
                            di.last_secondary_sort_id = \
                                di.cdf.head(1)[self.di_secondary_sort_col][0] - 1
                    tdfs.append(_cdft)
        return tdfs

    def _replay_merge_via_file_multi_polars(self, startproc_ts=None, endproc_ts=None) \
            -> ReplayStatusObj:
        """
        Replay the files in the DataInfo objects in timestamp order, and merge them into a single
        file.
        Args:
            startproc_ts: int: The start timestamp to process from (in epoch ms).
            endproc_ts: int: The end timestamp to process to (in epoch ms).
        Returns:
            ReplayStatusObj: The state of the replay for later processing to utilize.
        """
        chunksize = 1000000  # this only affects the combined dataframe size, NOT loading in
        # chucks of each file. Has no effect on speed after tesing, but leave in as it might reduce
        # memory usage when converting polars dataframe -> to_list() object. at chunksize 1000000
        # it will read full file anyway. Reduce below 500000 to have an effect.
        global_replaystatusobj = None
        st_prof_ts = time.time()
        anchordi = None
        endhit = False
        ts = None
        # if it remains none at the end of the proc then we know there were no entries between
        # the start and end timestamps, and we can return a ReplayStatusObject played up to
        # endproc_ts
        while not endhit:
            minst = []
            minend = []
            dis_r = []  # di's remaining to process
            for di in self.datainfos:
                if not di.eofs and not di.nodata_from_start_to_end:
                    dis_r.append(di)

            if len(dis_r) > 1:
                for di in dis_r:
                    minst.append(di.cfile_start_ts)
                    minend.append(di.cfile_end_ts)
                earliest_start = min(minst)
                earliest_end = min(minend)
                if earliest_start == earliest_end:
                    print(
                        f'{dis_r[0].descriptor}: start {toutcisotime(dis_r[0].cfile_start_ts)}, '
                        f'end {toutcisotime(dis_r[0].cfile_end_ts)} ')
                    print(
                        f'{dis_r[1].descriptor}: start {toutcisotime(dis_r[1].cfile_start_ts)}, '
                        f'end {toutcisotime(dis_r[1].cfile_end_ts)} ')

                for di in dis_r:
                    if di.cfile_start_ts == earliest_start:
                        anchordi = di
                    if di.cfile_end_ts == earliest_end:
                        di.load_next_file = True
                overlaps = False
                for di in dis_r:
                    if di == anchordi:
                        di.cfile_process = True
                    else:
                        if not di.eofs:
                            overlaps = self._timestamps_overlapping(
                                first_src_start_ts=anchordi.cfile_start_ts,
                                first_src_end_ts=anchordi.cfile_end_ts,
                                second_src_start_ts=di.cfile_start_ts,
                                second_src_end_ts=di.cfile_end_ts,
                                debug_anchor=anchordi,
                                debug_datainfo=di)
                        if overlaps:
                            di.cfile_process = True
                        else:
                            di.cfile_process = False
            elif len(dis_r) == 1:  # only one di remaining, so do not worry about overlaps
                anchordi = dis_r[0]
                overlaps = False
            else:  # no datainfos remaining have any inputfiles - exit loop
                if ts:
                    global_replaystatusobj = self._replay_init_complete_multi(
                        start_ts=startproc_ts, end_ts=ts)
                else:
                    global_replaystatusobj = self._replay_init_complete_multi(
                        start_ts=startproc_ts, end_ts=endproc_ts)
                break
            if overlaps:
                # most of the time will be spent processing and sorting multiple files in this
                # section. Processing speed is important.
                tdfsl = []
                endchunk_tss = []
                for di in dis_r:
                    print(f'{di.descriptor}: start {toutcisotime(di.cfile_start_ts)}, '
                          f'end {toutcisotime(di.cfile_end_ts)}')
                    if di.cfile_process:
                        endchunk_tss.append(di.cfile_end_ts)
                cend_ts = min(endchunk_tss)
                for di in dis_r:
                    if di.cfile_process:
                        #  TODO - instead of using >= self.last_ts use the index to know which row
                        #   was last processed in each datainfo.
                        if self.di_secondary_sort_col:
                            tdfsl.append(
                                di.cdf.filter(
                                    (di.cdf[
                                         self.di_secondary_sort_col] > di.last_secondary_sort_id) &
                                    (di.cdf[self.di_timestamp_col] <= cend_ts) &
                                    (di.cdf[self.di_timestamp_col] <= endproc_ts)
                                ))  # only takes 5ms, so don't worry about performance here
                        else:
                            tdfsl.append(di.cdf.filter(
                                (di.cdf[self.di_timestamp_col] <= cend_ts) &
                                (di.cdf[self.di_timestamp_col] <= endproc_ts) &
                                (di.cdf[self.di_timestamp_col] >= self.last_ts)
                            ))
                        if di.cfile_end_ts == cend_ts:
                            di.load_next_file = True
                        else:
                            di.load_next_file = False
                # the below only works if all datainfos have the same format, an intermediary
                # step would be required on this line here to align each dataframe inside tdfsl
                # to have matching columns into a different format.
                if self.di_same_header_fmt:
                    tdf = pl.concat(tdfsl)
                else:
                    tdf = pl.concat(tdfsl, how='diagonal')  # align and put null values in

                self._check_column_order(tdf)  # verify that concat did not do anything weird

                if self.di_secondary_sort_col:
                    tdf = tdf.sort([self.di_timestamp_col,
                                    self.di_secondary_sort_col])
                else:
                    tdf = tdf.sort([self.di_timestamp_col])
                #  if requested, forward fill and store the last line so that future files can be
                #  forward filled too
                if self.forward_fill:
                    tdf = self._forward_fill_dataframe(tdf)
                endofchunk = False
                sl_st = 0
                sl_end = chunksize
                tdflen = tdf.shape[0]
                while not endofchunk:
                    cdf = tdf[sl_st:sl_end, :]
                    columns_idxs = []
                    for cn in range(0, len(self.di_global_col_fmt)):
                        columns_idxs.append(cdf[:, cn].to_list())
                    for row in zip(*columns_idxs):
                        msg = {k: v for k, v in zip(self.di_global_col_fmt, row)}
                        ts = msg[self.di_timestamp_col]
                        di = self.di_lu[msg['exsym_lookup']]
                        if self.remove_internal_cols:
                            del msg['exsym_lookup']
                        if self.di_secondary_sort_col:
                            di.last_secondary_sort_id = msg[self.di_secondary_sort_col]
                        self._process_func(ts, msg)
                        if ts >= endproc_ts:
                            endhit = True
                        #  end if master signal to stop has been triggered
                        if not self.continueprocessing:
                            self._stop_processing(ts, startproc_ts)
                            retstatusobj = ReplayStatusObj('FORCESTOP', start_ts=startproc_ts,
                                                           end_ts=ts)
                            return retstatusobj

                    sl_st = sl_st + chunksize
                    sl_end = sl_end + chunksize
                    if sl_st > tdflen:
                        endofchunk = True
                if endhit:
                    if ts:
                        global_replaystatusobj = self._replay_init_complete_multi(
                            start_ts=startproc_ts, end_ts=ts)
                    else:
                        # there were no entries between start and end timestamps,
                        # and we can return a ReplayStatusObject played up to endproc_ts
                        global_replaystatusobj = self._replay_init_complete_multi(
                            start_ts=startproc_ts, end_ts=endproc_ts)
                for di in dis_r:
                    if di.cfile_process:
                        if ts >= di.cfile_start_ts:  # we have processed up to ts
                            di.cfile_start_ts = ts
                        process = False
                        if di.load_next_file:
                            if di.proc_inputsrcs:
                                process = True
                            else:
                                di.eofs = True
                        if process:
                            currentsrc = di.proc_inputsrcs.popleft()
                            self.logger.info(f'NEW SOURCE: {currentsrc}, {toutcisotime(ts)} UTC')
                            df = self._load_dataframe_from_source_polars(source=currentsrc,
                                                                         datainfo=di)
                            df = df.with_columns(pl.lit(di.exsym_lu).alias('exsym_lookup'))
                            df = df.rename({di.ts_col: self.di_timestamp_col})
                            if self.di_secondary_sort_col:
                                df = df.filter(
                                    df[self.di_secondary_sort_col] > di.last_secondary_sort_id)

                            di.cdf = df
                            di.cfile_start_ts = di.cdf.head(1)[self.di_timestamp_col][0]
                            di.cfile_end_ts = di.cdf.tail(1)[self.di_timestamp_col][0]

            else:
                # non-overlapping di code (only one datafile to process until next file is
                # loaded). Some code duplication here, but it is worth it for the performance
                # gains on datasets where there are: long gaps between overlapping datafiles,
                # or when processing a single DataInfo object.
                di = anchordi
                if self.di_secondary_sort_col:
                    tdf = di.cdf.filter(
                        (di.cdf[self.di_secondary_sort_col] > di.last_secondary_sort_id) &
                        (di.cdf[self.di_timestamp_col] <= endproc_ts))
                    tdf = tdf.sort([self.di_timestamp_col,
                                    self.di_secondary_sort_col])
                else:
                    tdf = di.cdf.filter((di.cdf[self.di_timestamp_col] <= endproc_ts) &
                                        (di.cdf[self.di_timestamp_col] >= self.last_ts))
                    tdf = tdf.sort([self.di_timestamp_col])

                if not self.di_same_header_fmt:
                    # need to add the missing columns in here to match the global column format
                    empty_df = pl.DataFrame(schema=self.di_global_col_schema)
                    tdf = pl.concat([tdf, empty_df], how='diagonal')

                self._check_column_order(tdf)  # verify dataframe is still in the correct fmt

                if self.forward_fill:
                    tdf = self._forward_fill_dataframe(tdf)

                endofchunk = False
                sl_st = 0
                sl_end = chunksize
                tdflen = tdf.shape[0]
                while not endofchunk:
                    cdf = tdf[sl_st:sl_end, :]
                    columns_idxs = []
                    for cn in range(0, len(self.di_global_col_fmt)):
                        columns_idxs.append(cdf[:, cn].to_list())
                    for row in zip(*columns_idxs):
                        msg = {k: v for k, v in zip(self.di_global_col_fmt, row)}
                        ts = msg[self.di_timestamp_col]
                        di = self.di_lu[msg['exsym_lookup']]
                        if self.remove_internal_cols:
                            del msg['exsym_lookup']
                        if self.di_secondary_sort_col:
                            di.last_secondary_sort_id = msg[self.di_secondary_sort_col]
                        # self.combined_handler.process(ts, msg, datainfo=di)
                        self._process_func(ts, msg)
                        if ts >= endproc_ts:
                            endhit = True
                            endofchunk = True

                        #  check to see if master signal to stop has been triggered; end if so
                        if not self.continueprocessing:
                            self._stop_processing(ts, startproc_ts)
                            retstatusobj = ReplayStatusObj('FORCESTOP', start_ts=startproc_ts,
                                                           end_ts=ts)
                            return retstatusobj

                    sl_st = sl_st + chunksize
                    sl_end = sl_end + chunksize
                    if sl_st > tdflen:
                        endofchunk = True
                if endhit:
                    if ts:
                        global_replaystatusobj = self._replay_init_complete_multi(
                            start_ts=startproc_ts, end_ts=ts)
                    else:
                        # there were no entries between start and end timestamps,
                        # and we can return a ReplayStatusObject played up to endproc_ts
                        global_replaystatusobj = self._replay_init_complete_multi(
                            start_ts=startproc_ts, end_ts=endproc_ts)
                #  we have played all entries in the non-overlapping file. Now move onto the next
                #  one.
                if di.proc_inputsrcs:
                    currentsrc = di.proc_inputsrcs.popleft()
                    df = self._load_dataframe_from_source_polars(source=currentsrc, datainfo=di)
                    df = df.with_columns(pl.lit(di.exsym_lu).alias('exsym_lookup'))
                    di.cdf = df.rename({di.ts_col: self.di_timestamp_col})
                    if self.di_secondary_sort_col:
                        di.cdf = df.filter(
                            df[self.di_secondary_sort_col] > di.last_secondary_sort_id)
                    if di.cdf.shape[0] == 0:
                        di.eofs = True
                    else:
                        di.cfile_start_ts = di.cdf.head(1)[self.di_timestamp_col][0]
                        di.cfile_end_ts = di.cdf.tail(1)[self.di_timestamp_col][0]
                else:  # no more files for non-overlapping di
                    di.eofs = True
        end_prof_ts = time.time()
        print(f'Replay of data completed in {end_prof_ts - st_prof_ts:.2f} seconds!')
        return global_replaystatusobj

    def _replay_init_complete_multi(self, *, start_ts: int, end_ts: int) -> ReplayStatusObj:
        """
        Helper function to return a ReplayStatusObj with the correct start and end timestamps.
        Args:
            start_ts: int timestamp in ms format
            end_ts: int timestamp in ms format
        Returns:
            ReplayStatusObj
        """
        global_replaystatusobj = self._get_replay_status_multi(state='OK', start_ts=start_ts,
                                                               end_ts=end_ts)
        return global_replaystatusobj

    def _replay_get_proc_times_multiasset_end(self) -> int:
        """
        loop though all the DataInfo objects and get the latest time to process.
        Returns:
            int: the last timestamp to process up to in the DataInfo objects
        """
        latest = -math.inf
        for di in self.datainfos:
            if di.procend_ts > latest:  # as procend_ts is set in __init__
                latest = di.procend_ts
        return latest

    def _replay_get_proc_times_multiasset_start(self) -> int:
        """
        loop though all the DataInfo objects and get the earliest time to process.
        Returns:
            int: the earliest timestamp to process from in the DataInfo objects
        """
        earliest = math.inf
        for di in self.datainfos:
            if di.data_procstart_ts < earliest:
                #    before the desired start time
                earliest = di.data_procstart_ts
        return earliest

    def _get_replay_status_multi(self, *, state: str, start_ts, end_ts) -> ReplayStatusObj:
        """
        Helper function to set the Replay status of all DataInfo objects + the global replay state.
        Args:
            state: str: The state to set the replay status to.
            start_ts: int: The start timestamp to process from (in epoch ms).
            end_ts: int: The end timestamp to process to (in epoch ms).
        Returns:
            ReplayStatusObj
        """
        for di in self.datainfos:
            di.replaystatusobj = ReplayStatusObj(state, start_ts=start_ts, end_ts=end_ts)
        global_replaystatusobj = ReplayStatusObj(state, start_ts=start_ts, end_ts=end_ts)
        return global_replaystatusobj

    def _get_global_proc_start_end_from_files(self, start_end: str, *, datainfo: DataInfo) \
            -> Union[int, None]:
        """
        Helper function to get the start or end timestamp from all files associated with a
        directory of a DatInfo object.
        Args:
            start_end: str: 'start' or 'end' to get the start or end timestamp from the files.
            datainfo: the DataInfo object with all information about the files to process.
        Returns:
            int or None: The start or end timestamp (in epoch ms) if files are found or None if no
            files are found.
        """
        get_ts_col_flag = True
        if datainfo.ts_col is not None:
            get_ts_col_flag = False
        get_ts_col_num_flag = True
        if datainfo.timestamp_col_num is not None:
            get_ts_col_num_flag = False

        self._sort_data_sources(datainfo=datainfo)

        if start_end == 'start':
            return self._get_data_start_timestamp(datainfo=datainfo,
                                                  get_ts_col_flag=get_ts_col_flag,
                                                  get_ts_col_num_flag=get_ts_col_num_flag)

        if start_end == 'end':
            return self._get_data_end_timestamp(datainfo=datainfo,
                                                get_ts_col_flag=get_ts_col_flag,
                                                get_ts_col_num_flag=get_ts_col_num_flag)

    def _get_start_timestamp_from_file(self, *, datainfo: DataInfo, file: str) -> Union[int, None]:
        """
        Branching function to get the start timestamp in epoch format for various formats.
        Args:
            datainfo: DataInfo: the object with all information about the directory to process.
            file: str: the path to the file in str formatto get the start timestamp from
        Returns:
            int or None: The start timestamp (in epoch ms) if files are found or None if no files
            are found.
        """
        if datainfo.files_process_type == 'csv':
            return self._get_start_timestamp_from_file_csv(datainfo=datainfo, file=file)
        elif datainfo.files_process_type == 'parquet':
            return self._get_start_timestamp_from_file_parquet(datainfo=datainfo, file=file)

    def _get_start_timestamp_from_file_csv(self, *, datainfo: DataInfo, file: str) \
            -> Union[int, None]:
        """
        Args:
            datainfo: DataInfo: the object with all information about the directory to process.
            file: str: the path to the file in str formatto get the start timestamp from
        Returns:
            int or None: The start timestamp (in epoch ms) if files are found or None if no files
            are found.
        """
        self.logger.debug(f'Getting start timestamp from file {file}')
        first_lines = []
        num_lines = 2
        with open(file, 'rb') as readf:
            try:
                compressed = readf.read(2) == b'\x1f\x8b'
            except OSError as e:
                raise e

        if compressed:
            with gzip.open(file, 'rt') as readf:
                try:
                    for _ in range(num_lines):
                        line = readf.readline().strip()
                        if line:
                            first_lines.append(line)
                except OSError as e:
                    raise e
        else:
            with open(file, 'rb') as readf:
                try:
                    readf.seek(0)
                    for _ in range(num_lines):
                        line = readf.readline().decode().strip()
                        if line:
                            first_lines.append(line)
                except OSError as e:
                    raise e

        if first_lines:
            columns = first_lines[0].split(',')
            rows = [fl.split(',') for fl in first_lines[1:]]
            df = pl.DataFrame(rows, schema=columns)
            return int(df[datainfo.ts_col][0])
        else:
            return None

    def _sort_data_sources(self, *, datainfo: DataInfo):
        if not datainfo.data_sorted:
            datainfo.datareader.sort_data()
            datainfo.data_sorted = True

    def _filter_data_sources(self, *, datainfo: DataInfo, start_ts: int):
        input_srcs = datainfo.datareader.sorted_srcs
        #  now using starttime , remove all input_srcs that have a timestamp before the
        #  start time
        ret_srcs = []
        startidx = 0
        for src_pair in input_srcs:
            src = src_pair[0]
            chunk_last_ts = self._get_data_chunk_end_timestamp(datainfo=datainfo, source=src)
            if chunk_last_ts < start_ts:
                #  if the last timestamp in the chunk is before the start time, then skip the
                #  chunk
                startidx += 1
            else:
                break  # we have found the first chunk to process, no need to read any more

        ipf = list(input_srcs)[startidx:]
        for s in ipf:
            ret_srcs.append(s[0])

        return ret_srcs

    def _get_data_start_timestamp(self, *, datainfo: DataInfo,
                                  get_ts_col_flag,
                                  get_ts_col_num_flag):
        return datainfo.get_datasrc_global_start_timestamp(
            get_ts_col_flag=get_ts_col_flag,
            get_ts_col_num_flag=get_ts_col_num_flag,
            ignore_errors=self.force_ignore_pl_read_errors)

    def _get_data_end_timestamp(self, *, datainfo: DataInfo,
                                get_ts_col_flag,
                                get_ts_col_num_flag):
        return datainfo.get_datasrc_global_end_timestamp(
            get_ts_col_flag=get_ts_col_flag,
            get_ts_col_num_flag=get_ts_col_num_flag,
            ignore_errors=self.force_ignore_pl_read_errors)

    def _get_data_chunk_start_timestamp(self, *, datainfo: DataInfo, source: str):
        first_ts = datainfo.load_datasrc_get_start_ts_overlay(source,
                                                    ignore_errors=self.force_ignore_pl_read_errors)
        return first_ts

    def _get_data_chunk_end_timestamp(self, *, datainfo: DataInfo, source: str):
        last_ts = datainfo.load_datasrc_get_end_ts_overlay(source,
                                             ignore_errors=self.force_ignore_pl_read_errors)
        return last_ts

    def _get_data_chunk_start_end_timestamps(self, *, datainfo: DataInfo, source: str):
        first_ts, last_ts = datainfo.load_datasrc_get_start_end_ts_overlay(source,
                                                    ignore_errors=self.force_ignore_pl_read_errors)
        return first_ts, last_ts

    def _get_data_sources(self, *, datainfo: DataInfo):
        ret_src = []
        if datainfo.datareader.sorted_srcs:
            for srcpair in datainfo.datareader.sorted_srcs:
                ret_src.append(srcpair[0])
        return ret_src

    def _get_start_timestamp_from_file_parquet(self, *, datainfo: DataInfo, file: str) \
            -> Union[int, None]:
        """
        Args:
            datainfo: DataInfo: the object with all information about the directory to process.
            file: str: the path to the file in str formatto get the start timestamp from
        Returns:
            int or None: The start timestamp (in epoch ms) if files are found or None if no files
            are found.
        """
        self.logger.debug(f'Getting start timestamp from file {file}')
        empty = False
        df = pl.scan_parquet(file)  # lazyframe
        try:
            df = df.select(datainfo.ts_col).head(1).collect()
        except pl.ComputeError:  # empty file or no data in column
            empty = True
        if not empty:
            if datainfo.convert_timestamps:
                df = df.with_columns(datainfo.convert_time_format(datainfo.ts_col))
            return int(df.item())
        else:
            return None

    def _end_proc(self, *, rp_runstate: ReplayStatusObj, start_proc_time: float) -> None:
        """
        Helper function to end the processing gracefully, calculate processing time, and return the
        run results for analysis and processing if needed later on.
        Args:
            rp_runstate: the ReplayStatusObj that contains the information on the timing of the
                replay.
            start_proc_time: unlike all other timestamps, this is a float generated by time.time()
                and not an epoch timestamp in ms format. (Should probably fix this to be consistent
                 with everything else!)
        Returns:

        """
        bt_endtime = datetime.utcfromtimestamp(rp_runstate.end_ts / 1000).replace(
            tzinfo=timezone.utc)
        bt_starttime = datetime.utcfromtimestamp(rp_runstate.start_ts / 1000).replace(
            tzinfo=timezone.utc)
        days = (bt_endtime - bt_starttime).days
        self.logger.info(f'---------------------------------------------------------------------'
                         f'------------------')
        self.logger.info(f'Total runtime:  {(time.time() - start_proc_time) / 60:.3f} minutes')
        self.logger.info(f'proc from {bt_starttime.isoformat()} to {bt_endtime.isoformat()}: '
                         f'proc over {days} days')
        di_results = []
        for di in self.datainfos:
            res = {'run_result': {}, 'description': di.descriptor}
            runstats = {'procend_ts': di.procend_ts,
                        'procstart_ts': di.procstart_ts,
                        'proc_filedata_start_ts': di.data_procstart_ts,
                        'proc_filedata_end_ts': di.data_procend_ts}
            res['run_result']['runstats'] = runstats
            di_results.append(res)
        results = {}

        handler_results = self.handler.get_results()
        results['datainfos'] = di_results
        results['handler'] = handler_results
        #  add in handler results here
        self.logger.info('Finished thread, returning statistics...')
        self.process_results = handler_results  # returned after continueprocessing set to False
        self.stop_tsf()

    def _load_dataframe_from_source_polars(self, *, source: str,
                                           datainfo: DataInfo,
                                           force_full_schema_read: bool = False,
                                           discovery: bool = False) -> pl.DataFrame:
        """
        Args:
            source: source path in string format.
            datainfo: DataInfo object with all information about the source to process, and state
                of processing so far.
            discovery: bool: True if the source is being loaded for discovery purposes, i.e. the
                first few lines are being read to determine the datatypes.
        Returns:
            pl.DataFrame: Polars dataframe representing the data from the data source.
        """
        if discovery:
            n_rows = pl.datatypes.N_INFER_DEFAULT
        else:
            n_rows = None
        if force_full_schema_read:
            n_rows = None
        df = datainfo.load_datasrc_overlay(source, num_rows=n_rows,
                                           ignore_errors=self.force_ignore_pl_read_errors)

        df = self._check_transform_headers_on_load(dataframe=df, source=source,
                                                   datainfo=datainfo)

        if datainfo.convert_timestamps:
            # take a copy of the datetime/time column and convert the original to ms epoch format
            df = df.with_columns(pl.col(datainfo.ts_col).alias(f'__{datainfo.ts_col}')) \
                .with_columns(datainfo.convert_time_format(datainfo.ts_col))

        return df

    def _check_transform_headers_on_load(self, *, datainfo: DataInfo, dataframe: pl.DataFrame,
                                         source: str):
        if datainfo.header_fmt:
            inputfcols = list(dataframe.columns)
            fmtcorrect = all(elem in inputfcols for elem in datainfo.header_fmt)
            if not fmtcorrect:
                raise RuntimeError(
                    f'source [{source}] seems to be in the in the wrong format. Headers '
                    f'do not match\n'
                    f'...should contain:{datainfo.header_fmt}\n'
                    f'...instead contains: {inputfcols}')
        if dataframe.shape[1] != datainfo.initial_numcols:
            raise RuntimeError(
                f'source [{source}] seems to be in the in the wrong format. Number of columns does'
                f'not match\n')

        # TODO - add in the ability drop columns here if required. Combine with lazyframes if
        #  poss to increase speed
        if datainfo.remove_cols:
            dataframe = dataframe.drop(datainfo.remove_cols)
        if datainfo.rename_cols:
            dataframe = dataframe.rename(datainfo.rename_cols)

        if datainfo.header_fmt_modified:
            inputfcols = list(dataframe.columns)
            fmtcorrect = all(elem in inputfcols for elem in datainfo.header_fmt_modified)
            if not fmtcorrect:
                raise RuntimeError(f'Something went wrong with [{source}] . Tried to modify '
                                   f'columns/headers'
                                   f'...should contain:{datainfo.header_fmt_modified}\n'
                                   f'...instead contains: {inputfcols}')
        return dataframe

    def _check_column_order(self, dataframe: pl.DataFrame):
        """
        Helper function to check the column order of the dataframe to ensure that the columns are
        aligned as was set in _pre_setup()
        """
        if dataframe.columns != self.di_global_col_fmt:
            raise ValueError(f'Column order is incorrect. Expected: {self.di_global_col_fmt}, '
                             f'got: {dataframe.columns}')

    def _timestamps_overlapping(self, *,
                                first_src_start_ts,
                                first_src_end_ts,
                                second_src_start_ts,
                                second_src_end_ts,
                                debug_anchor: DataInfo = None,
                                debug_datainfo: DataInfo = None
                                ):
        """
        Helper function to check if two sets of timestamp ranges overlap.
        Args:
            first_src_start_ts:
            first_src_end_ts:
            second_src_start_ts:
            second_src_end_ts:
            debug_anchor: DataInfo object of the anchor (first_file). The anchor is the one with
                the EARLIEST
                or equal(first) timestamp of the timestamp sets being compared. Only used for
                debugging if passed in.
            debug_datainfo: DataInfo object of the second_file. Only used for debugging if passed
            in.
        Returns:
            bool: True if the timestamp ranges overlap, False if they do not.
        """
        if debug_anchor:
            self.logger.debug(f'Comparing {debug_anchor} first_file_start_ts: '
                              f'{first_src_start_ts} with: {debug_datainfo} second_file_start_ts:'
                              f'{second_src_start_ts}')
        if first_src_start_ts == first_src_end_ts or second_src_start_ts == second_src_end_ts:
            if first_src_start_ts == first_src_end_ts:
                if second_src_start_ts <= first_src_start_ts <= second_src_end_ts:
                    return True
                else:
                    return False
            elif second_src_start_ts == second_src_end_ts:
                if first_src_start_ts <= second_src_start_ts <= first_src_end_ts:
                    return True
                else:
                    return False
        else:
            return first_src_start_ts <= second_src_end_ts and \
                second_src_start_ts <= first_src_end_ts

    def _forward_fill_dataframe(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Helper function to forward fill the dataframe.
        Args:
            df: pl.DataFrame: the dataframe to forward fill, using the previous files last row
            values if required.
        """
        if len(df) == 0:
            return df
        if not self.last_row_vals:
            #  forward fill the normal way
            df = df.select(pl.all().forward_fill())
        else:
            # if the first row of the dataframe has nulls, then compare with the last row of the
            # previous file, fill in the nulls with the previous row, and then forward fill
            first_row_vals = df.row(0, named=True)
            filled = {key: self.last_row_vals.get(key, value)
                      for key, value in first_row_vals.items()}
            if first_row_vals != filled:
                # HOLY JEEBUS - this is still fast, even though it is verbose. Polars is
                # designed to update columns quickly vs dealing with rows, so this is the one of
                # the fastest ways for now (that I) can find. It can likely be improved upon by
                # avoiding the convertion to and from python objects, but good enough for now.
                try:
                    df = \
                        df.select(
                            (pl.when((pl.first().cumcount() == 0) &
                                     (pl.col(df.columns[i]).is_null()))
                             .then(pl.lit(x))
                             .otherwise(pl.col(df.columns[i])))
                            .alias(df.columns[i]) for i, x in enumerate(
                                list(self.last_row_vals.values()))
                        )
                    df = df.select(pl.all().forward_fill())
                except Exception as e:
                    self.logger.error(f'Error forward filling dataframe')
                    raise e
        self.last_row_vals = df.row(-1, named=True)
        return df

    def _stop_processing(self, ts: int, startproc_ts: int):
        """
        Helper function to stop the processing of the data.
        """
        self.continueprocessing = False
        self.logger.info(f'Process stopped by master signal @ {toutcisotime(ts)} UTC')
        retstatusobj = ReplayStatusObj('FORCESTOP', start_ts=startproc_ts, end_ts=ts)
        return retstatusobj

    def _process_func(self, ts, msg):
        """
        Helper function to process the message and timestamp.
        Override this function to do custom processing of the messages if passing in extra
        arguments from the TimeSeriesFuser class to the Handler class.
        Args:
            ts: int: timestamp in ms format
            msg: dict: message to process
        """
        # if ts < self.last_ts:
        #     print(
        #         f'error here #{self.error_count} @ {ts} / {toutcisotime(ts)}  |||  last ts was '
        #         f'{self.last_ts} / {toutcisotime(self.last_ts)}')
        #     self.error_count += 1
        self.last_ts = ts
        self.handler.process(ts, msg)
