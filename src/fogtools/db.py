"""Functionality to write a fog-database

This module contains functionality to write a fog-database.  It is a new
implementation of functionality initially written by Thomas Leppelt in
create_fog_vect_rast.py.  This rewrite is intended to update to newer versions
of fogpy and satpy, to make the code more maintainable by improved modularity
and adding unitests, and to support ground data over the USA.
"""

import os
import time
import logging
import subprocess
import tempfile
import pathlib
import functools
import collections
import abc
import pkg_resources
import fogpy.utils

import numpy
import pandas
import satpy
import satpy.readers
import satpy.readers.yaml_reader
import yaml
import yaml.loader
import appdirs

from . import abi, sky, isd, core, log

logger = logging.getLogger(__name__)


class FogDBError(Exception):
    pass


class FogDB:
    """Database of fog cases.

    Database of fog cases with ground measurements, satellite measurement,
    fogpy retrievals, and auxiliary measurements.  This database may serve for
    validation and training purposes.

    This was initially started by Thomas Leppelt for SEVIRI data and the old
    mipp/mpop-based fogpy.  This class aims to replicate the functionality as
    implemented in the script ``create_fog_vect_rast.py`` and partially
    documented at
    https://ninjoservices.dwd.de/wiki/display/SATMET/Nebeldatenbank .

    It currently gathers:

      - synop
      - satellite data (ABI)
      - model data (ICON)
      - cloud microphysics (NWCSAF)
      - fogpy-calculated outputs such as fog
      - products calculated from DEM

    It is planned to add:

      - metar
      - swis
      - more satellite data (SEVIRI, FCI)
      - more model data (IFS, COSMO, ...)
      - more cloud microphysics data (NWCSAF, ABI-L2)

    Data are written out as a parquet file.
    """

    # A lot of this could be done in parallel, perhaps they should all return
    # awaitables?  Much of the data gathering is I/O bound *or* coming from a
    # subprocess.  Only the fogpy fog calculation is CPU bound.  Consider using
    # concurrent.futures:
    #
    # - ICON stuff is slow (wait for sky tape)
    # - ABI stuff is slow (download from AWS)
    # - NWCSAF is slow (external process) but depends on the former two
    # - loading synop and DEM is probably fast enough
    # - calculating fog is CPU bound and depends on other stuff being there
    #
    # so the most sense to do synchronously is downloading ABI and ICON.  The
    # NWCSAF software already monitors for files to appear and runs in the
    # background "naturally", so the procedure should be to asynchronously
    # download ICON and ABI data to the right place, then wait for NWCSAF
    # files to appear.  Where does concurrent.futures come in exactly?

    # TODO:
    #   - use concurrent.futures (but try linear/serial first)
    #   - add other datasets

    sat = nwp = cmic = ground = dem = fog = data = None

    def __init__(self):
        self.sat = _ABI()
        self.nwp = _ICON()
        self.cmic = _NWCSAF(dependencies={"sat": self.sat, "nwp": self.nwp})
        self.ground = _SYNOP()
        self.dem = _DEM("new-england")
        self.fog = _Fog(dependencies={"sat": self.sat, "cmic": self.cmic,
                                      "dem": self.dem})

    def __setattr__(self, k, v):
        getattr(self, k)  # will trigger AttributeError if not found
        super().__setattr__(k, v)

    def extend(self, timestamp, onerror="raise"):
        """Add data from <timestamp> to database.

        This module extends the database, creatig it if it doesn't exist yet,
        with measurements for the time indicated by <timestamp>.  It calculates
        ground station measurements, then uses the lats and lons.

        Args:
            timestamp (pandas.Timestamp):
                Time for which to add data to database
            onerror (str):
                What to do on error: "raise" or "log"
        """

        with log.LogToTimeFile(timestamp):
            try:
                # first get the ground stations: these determine which points I
                # want to # extract
                logger.info(f"Loading data for {timestamp:%Y-%m-%d %H:%M:%S}")
                synop = self.ground.load(timestamp)

                # for each non-unique lat/lon, choose the time closest to
                # /timestamp/
                synop = self._select_closest_latlon(synop, timestamp)

                lats = synop.index.get_level_values("LATITUDE")
                lons = synop.index.get_level_values("LONGITUDE")

                # FIXME: use concurrent.futures here
                # extract will also call .load thus taking care of dependencies
                satdata = self.sat.extract(timestamp, lats, lons)
                nwpdata = self.nwp.extract(timestamp, lats, lons)
                # FIXME: with concurrent.futures, wait for sat and nwp to be
                # finished
                cmicdata = self.cmic.extract(timestamp, lats, lons)
                demdata = self.dem.extract(timestamp, lats, lons)
                # FIXME: with concurrent.futures, wait for cmic and dem to be
                # finished
                fogdata = self.fog.extract(timestamp, lats, lons)
                logger.info("Collected all fogdb components, "
                            "putting it all together")
                df = _concat_mi_df_with_date(
                        satdata,
                        synop=synop,
                        nwp=nwpdata,
                        cmic=cmicdata,
                        dem=demdata,
                        fog=fogdata)
                if self.data is None:
                    self.data = df
                else:
                    self.data = pandas.concat([self.data, df], axis=0)
            except (FogDBError, OSError, EOFError):
                if onerror == "raise":
                    raise
                elif onerror == "log":
                    logger.exception("Failed to extend database with data "
                                     f"from {timestamp:%Y-%m-%d %H:%M:%S}:")
                else:
                    raise ValueError("Unknown error handling option: "
                                     f"{onerror!s}")

    def store(self, f):
        """Store database to file.

        Store database to a parquet file.

        Args:
            f (pathlib.Path or str): output file
        """
        if self.data is None:
            raise ValueError("No entries in database!")
        logger.info(f"Storing fog database to {f!s}")
        self.data.to_parquet(f)

    @staticmethod
    def _select_closest_latlon(synop, timestamp):
        """For repeated lat/lon, select row closest in time to timestamp.

        When the dataframe in synop contains rows with identical lat/lons but
        different timestamps, such as may occur when there are multiple synop
        measurements within a certain time tolerance returned by Synop.load,
        return a new dataframe where for each such row the row is selected
        where the time is closest to timestamp.
        """
        tdiff = abs(synop.index.get_level_values("DATE") - timestamp)
        ids = synop.assign(delta=tdiff).delta.groupby(
                level=["LATITUDE", "LONGITUDE"]).idxmin()
        logger.debug(f"Reducing from {synop.shape[0]:d} to "
                     f"{ids.shape[0]:d} to avoid repeated lat/lons")
        synop = synop.loc[ids]
        return synop


def _concat_mi_df_with_date(df1, **dfs):
    """Concatenate multiple multi-index dataframes setting date.

    Having multiple dataframes with a [DATE, LATITUDE, LONGITUDE] MultiIndex,
    concatenate them column-wise, but taking the dates from the very first
    dataframe (lat/lon are still required to match).  The main dataframe
    dictates the dates for the others and is passed first, the rest are passed
    as keyword arguments (because we need their names to rename the dates).

    If lat and lon don't match, this will fail.
    """
    dfc = pandas.concat(
            [df1.reset_index("DATE")] +
            [df.reset_index("DATE").rename(columns={"DATE": f"date_{k:s}"})
                for (k, df) in dfs.items()],
            axis=1)
    return dfc.reset_index(["LATITUDE", "LONGITUDE"]).set_index(
            ["DATE", "LATITUDE", "LONGITUDE"])


class _DB(abc.ABC):
    """Get/cache/store/... DB content for quantity.

    Abstract class to do various operations such as get, cache, extract, store,
    etc. on a particular category of data within the database.  See the source
    code for examples of various implementations.
    """

    dependencies = None
    base = None
    _data = None

    @property
    @abc.abstractmethod
    def reader(self):
        raise NotImplementedError()  # pragma: no cover

    @property
    @abc.abstractmethod
    def name(self):
        raise NotImplementedError()  # pragma: no cover

    def __init__(self, dependencies=None):
        """Initialise DB object.

        Args:
            dependencies: Collection[DB instances]
                DB objects for which data must be collected first.
        """
        logger.debug(f"Initialising {self!s}")
        self.dependencies = dependencies if dependencies else {}
        self.base = pathlib.Path(appdirs.user_cache_dir("fogtools")) / "fogdb"
        self._data = {}

    @abc.abstractmethod
    def find(self, timestamp, complete=False):
        """Return (all) files needed at ``timestamp``.

        Check what files cover ``timestamp``.  Return a collection of files
        found.  If complete is True, return this collection only if all
        expected files are found, and an empty collection otherwise.

        Args:
            timestamp (Pandas.Timestamp): Time at which to collect.
            complete (Optional[bool]): Require completeness.
        """
        raise NotImplementedError()  # pragma: no cover

    def ensure(self, timestamp):
        logger.debug(f"Ensuring {self!s} is available")
        if not self.find(timestamp, complete=True):
            logger.debug("Input data unavailable or incomplete for "
                         f"{self!s} for {timestamp:%Y-%m-%d %H:%M}, "
                         "downloading / generating")
            self.store(timestamp)
        if not self.find(timestamp, complete=True):
            raise FogDBError("I tried to download or generate data for "
                             f"{self!s} covering {timestamp:%Y-%m-%d %H:%M}, "
                             "but it's still not there.  Something may have "
                             "gone wrong trying to download or generate the "
                             "data.")

    def ensure_deps(self, timestamp):
        for (k, dep) in self.dependencies.items():
            logger.debug(f"Ensuring dependency {k:s}")
            dep.ensure(timestamp)
            self.link(dep, timestamp)

    def load(self, timestamp):
        self.ensure(timestamp)
        logger.debug(f"Loading {self!s}")
        sc = satpy.Scene(
                filenames=self.find(timestamp, complete=True),
                reader=self.reader)
        sc.load(sc.available_dataset_names())
        return sc

    @abc.abstractmethod
    def store(self, timestamp):
        """Store in cache.
        """
        raise NotImplementedError()  # pragma: no cover

    def link(self, dep, timestamp):
        """If needed, add symlink for dependency

        If dependency ``dep`` has been generated but generating the product for
        self requires this to be in a certain location (such as for generating
        NWCSAF), this method can create such a symlink or symlinks.  Subclasses
        can override this, by default it does nothing.

        Args:
            dep (_DB): dependency in question
            timestamp (pandas.Timestamp): Time for which to generate
        """
        pass

    def extract(self, timestamp, lats, lons):
        """Extract data points

        Given a time and a series of lats and lons, extract data points from
        images/datasets.  Typically those lat/lons would correspond to
        ground stations.  This method assumes that self.load has been
        implemented to return a scene object.  If a subclass implements
        self.load differently then it must also override extract.

        Note: this will compute any dask arrays from which points are to be
        extracted (such as in the scene from ABI_.load).

        Args:
            timestamp (pandas.Timestamp): time for which to extract
            lats (array_like): latitudes for which to extract
            lons (array_like): longitudes for which to extract

        Returns:
            pandas.DataFrame with the desired data
        """
        sc = self.load(timestamp)
        logger.debug(f"Extracting data for {self!s} "
                     f"{timestamp:%Y-%m-%d %H:%M}")
        vals = {}
        # pyproj 2.6 does not support other arrays than ndarrays, this may
        # change with https://github.com/pyproj4/pyproj/issues/573 such that I
        # can pass any array_like; but with pyproj 2.6, passing a
        # pandas.Float64Index results in a SystemError, see for more info
        # https://pytroll.slack.com/archives/C17CEU728/p1589903078309800 and
        # onward conversation
        st_time = None
        for da in sc:
            nm = f"{da.attrs.get('name', getattr(da, 'name'))!s}"
            if "area" not in da.attrs:
                logger.debug(
                        f"Not extracting from {nm:s}, "
                        "as it doesn't have an area attribute, probably "
                        "a non-geographical dataset.")
                continue
            if any(d not in da.dims for d in "xy"):
                logger.debug(
                        f"Not extracting from {nm:s}, "
                        "as it doesn't have both x and y dimensions. "
                        "It has: " + ",".join(da.dims))
                continue
            for d in set(da.dims) - {"x", "y"}:
                da = da.squeeze(d)
            (x, y) = da.attrs["area"].get_xy_from_lonlat(
                    numpy.array(lons),
                    numpy.array(lats))
            # x, y may contain masked values --- index where unmasked, and get
            # nan where masked
            try:
                src = da.data.compute()
            except AttributeError:
                src = da.data
            extr = src[
                numpy.where(x.mask, 0, y),
                numpy.where(y.mask, 0, x)]
            ths_sttime = da.attrs["start_time"] or pandas.Timestamp("NaT")
            if st_time is None:
                st_time = ths_sttime
            elif (ths_sttime != st_time
                  and pandas.notnull(ths_sttime)
                  and pandas.notnull(st_time)):
                logger.warning("Different datasets in scene have different "
                               "start times: expected "
                               f"{st_time:%Y-%m-%d %H:%M:%S.%f}, "
                               f"got {ths_sttime:%Y-%m-%d %H:%M:%S.%f}")
                ths_sttime = st_time
            vals[da.attrs["name"]] = pandas.Series(
                    numpy.where(x.mask | y.mask, numpy.nan, extr),
                    index=pandas.MultiIndex.from_arrays(
                        [pandas.Series(ths_sttime).repeat(
                            lats.size),
                         lats, lons],
                        names=["DATE", "LATITUDE", "LONGITUDE"]))
        df = pandas.DataFrame(vals)
        return df

    def __str__(self):
        return f"[fogdb component {self.name:s}]"


class _Sat(_DB):
    """Placeholder class in case further satellites are added.
    """
    pass


class _ABI(_Sat):
    reader = "abi_l1b"
    name = "ABI"

    def _search_file_two_dirs(self, ts1, ts2, chan):
        """Look through two directories for file covering now

        Even when looking for file covering NOW, still need to search in both
        directory covering now AND in directory covering T-15 minutes
        """
        dirs = {
            abi.get_dl_dir(
                self.base,
                ts - pandas.Timedelta(i, "minutes"),
                chan)
            for ts in (ts1, ts2)
            for i in (0, 15)}
        return collections.ChainMap(*[
                satpy.readers.find_files_and_readers(
                    ts1.to_pydatetime().replace(tzinfo=None),
                    ts2.to_pydatetime().replace(tzinfo=None),
                    dir,
                    "abi_l1b",
                    missing_ok=True)
                for dir in dirs])

    def _chan_ts_exists(self, ts, chan):
        """Check if one file covering timestamp for channel exists.

        Returns a collection of what has been found.
        """

        # ABI full disk files appear either ever 15 minutes (mode M3) or every
        # 10 minutes (mode M6), but the end time is around 10 minutes after the
        # start time even in mode M3, with gaps between files.  This means we
        # don't always find a file covering exactly start_time, but if we don't
        # we are still interested in the most recent file as long as it covers
        # T-10 minutes.  If there is a file covering now, we accept it,
        # otherwise we accept a file covering between T-10 minutes and now.
        # This will still give a false positive for "exists" if there is a file
        # at T-10 minutes but none at now, even though there should be one now.
        #
        # How to tell the difference between "there is no file, but there
        # should be" and "there is no file, and we shouldn't expect one"?
        # Based on mode?  What if mode changes?  For now we accept the false
        # positives.

        # even when looking for file covering NOW, need to search in directory
        # including for T-15 minutes, delegate this to _search_file_two_dirs
        files = self._search_file_two_dirs(ts, ts, chan)
        cnt1 = files.get("abi_l1b", [])
        if len(cnt1) == 1:
            return {pathlib.Path(cnt1[0])}
        elif len(cnt1) > 1:
            raise FogDBError(f"Channel {chan:d} found multiple times?! "
                             + ", ".join(str(c) for c in cnt1))
        # try again with T - 10 minutes
        start_search = ts - pandas.Timedelta(10, "minutes")
        files = self._search_file_two_dirs(start_search, ts, chan)
        if not files:
            return set()
        cnt2 = files["abi_l1b"]
        if len(cnt2) == 1:
            return {pathlib.Path(cnt2[0])}
        elif len(cnt2) < 1:  # not sure if this is possible
            raise RuntimeError(
                    "Got empty file list from satpys "
                    "find_files_and_readers.  "
                    "This cannot happen.")  # pragma: no cover
        elif len(cnt2) > 1:
            # if T-0 is not found, T-10 should not be found twice
            raise FogDBError(f"Channel {chan:d} found multiple times?! "
                             + ", ".join(str(c) for c in cnt2))

    def find(self, timestamp, complete=True, past=False):
        """Check if files covering timestamp exist

        Check if ABI files covering 'timestamp' exist for all channels.
        If ``past`` is True, will also ensure ABI files covering T-60 minutes
        up to T-0 minutes exist, as SAFNWC software can use this.
        """

        found = set()
        # So this function either searches for:
        #   - ``[T-x, T]``
        #   - ``[T-y, T]``
        #
        # Is x always at most 15 minutes?
        #
        # Problem with scan modes, such that the end of file x doesn't
        # correspond to the start of file x+1, there are gaps as it's doing
        # C, M1, or M2...  need to cover case where the time is not covered.
        # I actually want to do the opposite.  satpy.find_files_from_readers
        # will be strict here, but to me it's still a match even if the image
        # was finished taking 10 seconds ago.
        #
        # In M6 NWCSAF searches T-60, T-20, in M3 it searches T-60, T-30.
        # I'll check that T-60 and T-0 must exist, as well as either T-30 or
        # T-20.

        if not complete:
            raise NotImplementedError("ABI.find only implemented for complete")

        ot = functools.partial(pandas.Timedelta, unit="minutes")
        logger.debug("Checking all required ABI channels at "
                     f"{timestamp:%Y-%m-%d %H:%M}")
        found = set()
        for chan in abi.nwcsaf_abi_channels | abi.fogpy_abi_channels:
            logger.debug(f"Checking channel {chan:d}")
            chan_ts = self._chan_ts_exists(timestamp, chan)
            if not chan_ts:
                logger.debug(f"Channel {chan:d} missing at "
                             f"{timestamp:%Y-%m-%d %H:%M}")
                return set()
            found.update(chan_ts)
            if past:
                chan_ts_min = {}
                for i in (20, 30, 60):
                    chan_ts_min[i] = self._chan_ts_exists(
                            timestamp - ot(i), chan)
                if not (chan_ts_min[60] and chan_ts_min[20]
                        or chan_ts_min[30]):
                    logger.debug(f"Channel {chan:d} available at "
                                 f"{timestamp:%Y-%m-%d %H:%M}, but missing "
                                 "one or more previous data files")
                    return set()
                found.update(*(x for i in (20, 30, 60)
                               if (x := chan_ts_min[i])))  # noqa: E203, E231
        else:
            return found
        raise RuntimeError("This code is unreachable")  # pragma: no cover

    def store(self, timestamp):
        """Store ABI for timestamp

        Store ABI to disk to ensure coverage for timestamp t
        """
        # This also needs to consider the "impossible" option if the relevant
        # file doesn't exist at the server, because a certain time is not
        # covered, either because it's out of range for ABI, or because the
        # mode asked (F, C, M1, M2) is not available at that time.
        #
        # Does the latter actually matter?  Every pixel is anyway measured at a
        # particular time within [T, T+delta_T], whether the rest of delta_T is
        # spent on F, C, M1, or M2 doesn't matter, does it?  That means the end
        # time is not relevant?
        abi.download_abi_period(
                timestamp-pandas.Timedelta(65, "minutes"),
                timestamp+pandas.Timedelta(5, "minutes"),
                tps="F",
                basedir=self.base)

    def load(self, timestamp):
        """Get scene containing relevant ABI channels
        """
        self.ensure(timestamp)
        selection = self.find(timestamp, complete=True)
        # I want to select those files where the time matches.  More files may
        # have been downloaded, in particular for the benefit of NWCSAF.  How
        # to do this matching?  Could use pathlib.Path.match or perhaps
        # satpy.readers.group_files.
        #
        # cannot match against {timestamp:%Y%j%H%M%S%f} (which I copied from
        # the Satpy abi.yaml definition file): strftime will always
        # generate a 6-digit microsecond component, but the ABI filenames only
        # contain a single digit for deciseconds (see PUG L1B, Volume 3, page
        # 291, # PDF page 326).  This doesn't affect strptime which is
        # apparently what Satpy uses.
        logger.debug("Loading ABI from local disk")
        sc = satpy.Scene(
                filenames={str(x) for x in selection},
                reader="abi_l1b")
        sc.load([f"C{ch:>02d}" for ch in
                 abi.nwcsaf_abi_channels | abi.fogpy_abi_channels])
        return sc


class _NWP(_DB):
    pass


class _ICON(_NWP):
    reader = "grib"
    name = "ICON"

    def find(self, timestamp, complete=False):
        """Get best ICON path for timestamp.

        Given a timestamp, get the most suitable ICON path to read.  That's
        either the analysis file (if nearest hour correspond to one) or a
        forecast for up to five hours.
        """

        rb = sky.RequestBuilder(self.base)
        timestamp = timestamp.round("H")  # get forecast for nearest whole hour
        # get six-hour forecast period corresponding to timestamp, up to next
        # analysis
        period = sky.timestamp2period(timestamp)
        rb.get_request_ba(sky.period2daterange(period))
        # all expected output files according to the sky query builder
        exp = rb.expected_output_files
        fn = sky.make_icon_nwcsaf_filename(
                self.base,
                period.start_time,
                timestamp.hour - period.start_time.hour)
        if fn not in exp:
            raise RuntimeError("Impossible")  # pragma: no cover
            # "I would expect filename {fn!s}, but I'm told "
            # "to expect only {','.join([str(f) for f in exp])!s}")
        if complete and not fn.exists():
            return set()
        else:
            # I don't care about the logfiles "ihits" and "info"
            return {fn}

    def store(self, timestamp):
        """Get model analysis and forecast for input to NWCSAF

        Get model analysis and forecast that we need as input to NWCSAF.
        This will come from ICON.
        """

        logger.info(f"Retrieving ICON from SKY for {timestamp:%Y-%m-%d %H:%M}")
        period = sky.timestamp2period(timestamp)
        sky.get_and_send(self.base, period)


class _CMIC(_DB):
    """Parent class for any cloud microphysics-related functionality.

    Currently empty, but if there are mulitple sources perhaps some
    functionality will be shared.
    """
    pass


class _NWCSAF(_CMIC):
    """Class for handling NWCSAF output.

    Expects that the SAFNWC environment variable is set for correct
    functioning.  See NWCSAF software documentation.
    """
    reader = "nwcsaf-geo"
    name = "NWCSAF-GEO"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        base = os.getenv("SAFNWC")
        if not base:
            raise FogDBError("Environment variable SAFNWC not set")
        self.base = pathlib.Path(base)

    def find(self, timestamp, complete=False):
        before = timestamp - pandas.Timedelta(15, "minutes")
        return satpy.readers.find_files_and_readers(
            before.to_pydatetime().replace(tzinfo=None),
            timestamp.to_pydatetime().replace(tzinfo=None),
            self.base / "export" / "CMIC",
            "nwcsaf-geo",
            missing_ok=True).get("nwcsaf-geo", set())

    def find_log(self, timestamp):
        """Find NWCSAF logfile covering timestamp.

        Search the logfile directory, as we're expecting NWCSAF to write, for a
        file that has a start time in the past 15 minutes.
        """

        # This is a bit tricky because the CMIC logfile file name
        # format is not covered by anything any satpy reader reads,
        # but there may be no NWCSAF CMIC output at this exact time,
        # however I should be able to determine the exact time
        # from the satellite file... perhaps easiest to just search
        # "by hand"?  Or, can get starting time for satellite with
        # satpy.readers.yaml_reader.FileYAMLReader.filename_items_for_filetype,
        # or do that for all log files searching for one starting in
        # the last 15 minutes?

        before = timestamp - pandas.Timedelta(15, "minutes")
        logdir = self.base / "export" / "LOG"
        files = logdir.glob("S_NWC_LOG_*Z.log")
        pat = ("S_NWC_LOG_{platform_name:s}_{region:s}_"
               "{start_time:%Y%m%dT%H%M%S}Z.log")
        f_with_info = satpy.readers.yaml_reader.FileYAMLReader.\
            filename_items_for_filetype(
                files,
                {"file_patterns": [pat]})
        for (nm, info) in f_with_info:
            if before < info["start_time"] < timestamp:
                return nm
        raise FileNotFoundError("Found no logfile that might cover "
                                f"{timestamp:%Y-%m-%d %H:%M}")

    def store(self, timestamp):
        """Store NWCSAF output.

        The NWCSAF output is generated using the SAFNWC software.  Whenever the
        software is running and the dependencies are present in the right
        place, it should be generated automatically.  This function ensures
        that the dependencies are in the right place and starts running the
        SAFNWC software if needed, but does not wait for the files to be
        present.
        """
        # This is generated with the SAFNWC software, see
        # <https://ninjoservices.dwd.de/wiki/display/SATMET/NWC+SAF+Software
        #  #NWCSAFSoftware-NWCSAFv2018.1f%C3%BCrOFF-LINEmodemitGOES-16ABIDaten>
        # and http://www.nwcsaf.org/
        #
        # normally, if the Task Monitor (TM) is running, it be monitoring for
        # changes and # start processing automatically when satellite files
        # are added.

        if timestamp > pandas.Timestamp("2019-04-23"):
            raise FogDBError("ABI-NWCSAF newer than 2019-04-23 not "
                             "supported, see fogtools#19")
        self.ensure_deps(timestamp)
        if not self.is_running():
            self.start_running()

    @staticmethod
    def is_running():
        """Check whether the NWCSAF software is running.

        Using the task manager, check whether the NWCSAF software is running.
        Return True if it is or False otherwise.  Raises CalledProcessError if
        the task manager doesn't exist or exits with a different errorcode than
        1 (such as with a signal).  Raises a FogDBError if the task manager
        exited successfully but with an unexpected output.
        """

        # need to use a temporary file because ``tm`` refuses to write to a
        # pipe (it will fail with Segmentation Fault), it can write to a file
        # with the -o flag
        logger.debug("Checking NWCSAF software status")
        with tempfile.NamedTemporaryFile(mode="rb") as ntf:
            try:
                subprocess.run(["tm", f"-o{ntf.name:s}", "status"], check=True)
            except subprocess.CalledProcessError as cpe:
                if cpe.returncode == 1:
                    return False
                else:
                    raise
            out = ntf.read()
            if b"Active Mode" not in out:
                raise FogDBError("Unexpected output from task manager:\n" +
                                 out.decode("ascii"))
        return True

    def start_running(self):
        """Run NWCSAF software.

        Start running the NWCSAF software.  As this takes a moment to get
        started, users might want to run this module using
        `ThreadPoolExecutor.submit`.

        Returns:
            CalledProcess
        """
        logger.info("Starting NWCSAF software")
        return subprocess.run(["SAFNWCTM"], check=True)

    def _get_dep_loc(self, dep):
        """Find location from where dependency should be symlinked.

        The SAFNWC software expects input data to be in a particular directory,
        but the data may be elsewhere.  This method determines where
        these data are expected by the SAFNWC software.
        """
        p = pathlib.Path(self.base) / "import"
        if isinstance(dep, _Sat):
            return p / "Sat_data"
        elif isinstance(dep, _NWP):
            return p / "NWP_data"
        else:
            raise TypeError("Passed unrecognised dependency type: "
                            f"{type(dep)!s}")

    def link(self, dep, timestamp):
        """Link NWCSAF dependency.

        Data generated for a dependency is probably not where NWCSAF wants it.
        Add symlinks so that NWCSAF can find it.
        """
        logger.debug("Linking NWCSAF dependenices")
        link_dsts = dep.find(timestamp, complete=True)
        link_src_dir = self._get_dep_loc(dep)
        link_src_dir.mkdir(exist_ok=True, parents=True)
        for p in link_dsts:
            src = (link_src_dir / p.name)
            try:
                src.symlink_to(p)
            except FileExistsError:
                logger.warning(f"Src already exists: {src!s}")

    def wait_for_output(self, timestamp, timeout=600):
        """Wait for SAFNWC outputs.

        With the SAFNWC code running, wait until the results are there.
        """
        if not self.is_running():
            raise FogDBError("SAFNWC is not running")
        t = 0
        logger.info("Waiting for SAFNWC results in "
                    f"{self.base / 'export' / 'CMIC'!s}")
        while t < timeout:
            if self.find(timestamp, complete=True):
                return
            try:
                logfile = self.find_log(timestamp)
            except FileNotFoundError:
                pass  # no logfile yet
            else:
                with open(logfile, "r") as fp:
                    text = fp.read()
                    if "Error opening" in text:
                        raise FogDBError("NWCSAF apparently had an error "
                                         "opening or reading satellite input "
                                         f"files.  Please check {logfile!s} "
                                         "for details.")
            time.sleep(10)
            t += 10
        else:
            raise FogDBError(f"No SAFNWC result after {timeout:d} s")

    def ensure(self, timestamp):
        """Ensure that NWCSAF output for timestamp exists.

        Generate NWCSAF output (using self.store) and wait for results.
        To generate without waiting, call self.store.
        """
        self.store(timestamp)
        self.wait_for_output(timestamp)


class _Ground(_DB):
    """Base class for any ground-based datasets.

    Currently empty.
    """
    pass


class _SYNOP(_Ground):
    """Base class for handling data from the Integrated Surface Dataset (ISD).
    """
    reader = None  # cannot be read with Satpy
    name = "SYNOP/ISD"

    def find(self, timestamp, complete=False):
        loc = isd.get_db_location()
        if complete and not loc.exists():
            return set()
        else:
            return {loc}

    _db = None

    def load(self, timestamp, tol=pandas.Timedelta("30m")):
        """Get ground based measurements from Integrated Surface Dataset.

        Return ground based measurements from the Integrated Surface Dataset
        (ISD) for timestamp within tolerance.

        Args:
            timestamp (pandas.Timestamp): Time for which to locate
                measurements.
            tol (Optional[pandas.Timedelta]): Tolerance, measurements how long
                before or after the requested time to consider a match.
                Defaults to 30m.

        Returns:
            pandas.Dataframe with measurements
        """
        self.ensure(timestamp)
        if self._db is None:
            logger.debug("Reading ground measurements database from locally "
                         "stored selection of ISD")
            db = isd.read_db()
            if db.index.names != ["DATE", "LATITUDE", "LONGITUDE"]:
                db = db.set_index(["DATE", "LATITUDE", "LONGITUDE"])
            self._db = db.sort_index()
        return self._db.loc[timestamp-tol:timestamp+tol]

    def store(self, _):
        """Create ISD database locally.  See isd module.
        """
        isd.create_db()


class _DEM(_DB):
    """Class for handling digital elevation model data.
    """
    reader = "generic_image"
    name = "DEM"

    _regions = {"new-england": "abi", "europe": "seviri"}
    location = None

    def __init__(self, region):
        """Initialise DEM class.

        Initialise class to provide DEM information to database.

        Args:
            region (str): Either "new-england" or "europe"
        """

        D = yaml.load(
                open(
                    pkg_resources.resource_filename(
                        "fogpy",
                        f"etc/composites/{self._regions[region]:s}.yaml"),
                    "r"),
                Loader=yaml.loader.UnsafeLoader)
        self.region = region
        self.location = pathlib.Path(pkg_resources.resource_filename(
                "fogpy",
                D["composites"]["_intermediate_fls_day"]["path_dem"]))

    def find(self, timestamp, complete=False):
        if complete and not self.location.exists():
            return set()
        else:
            return {self.location}

    def store(self, _):
        """Download DEM for New England.

        Uses `fogpy.utils` module.
        """
        if self.region == "new-england":
            logger.info("Downloading DEMs")
            fogpy.utils.dl_dem(self.location)
        else:
            raise NotImplementedError("Can only download New England DEM")

    def load(self, timestamp):
        """Load Digital Elevation Model.

        Loads it as a scene object with the dataset ``"dem"``.
        """
        sc = super().load(timestamp)
        sc["dem"] = sc["image"]
        del sc["image"]
        return sc


class _Fog(_DB):
    """Gather fog outputs.

    Run fogpy to collect fog products.
    So far implemented for ABI / NWCSAF-GEO.
    """

    reader = "generic_image"  # stored as geotiff
    name = "Fogpy"

    def find(self, timestamp, complete=False, sensorreader="nwcsaf-geo"):
        b = self.base / f"fog-{timestamp:%Y%m%d-%H%M}.tif"
        if complete and not b.exists():
            return set()
        else:
            return {b}

    def store(self, timestamp):
        logger.info("Calculating fog")
        sc = core.get_fog(
                "abi_l1b",
                self.dependencies["sat"].find(timestamp),
                "nwcsaf-geo",
                self.dependencies["cmic"].find(timestamp),
                "new-england-500",
                "overview")
        sc.save_dataset("fls_day", str(self.find(timestamp).pop()))

    def load(self, timestamp):
        sc = super().load(timestamp)
        sc["fog"] = sc["image"]
        del sc["image"]
        return sc


# TODO: _IFS, _COSMO, _METAR, _SWIS, _SEVIRI
