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

import pandas
import satpy
import satpy.readers
import abc

import sattools.io

from . import abi, sky, isd, core, dem

logger = logging.getLogger(__name__)


class FogDBError(Exception):
    pass


class FogDB:
    """Database of fog cases

    Database of fog cases with ground measurements, satellite measurement,
    fogpy retrievals, and auxiliary measurements.  This database may serve for
    validation and training purposes.

    This was initially started by Thomas Leppelt for SEVIRI data and the old
    mipp/mpop-based fogpy.  This class aims to replicate the functionality as
    implemented in the script ``create_fog_vect_rast.py`` and partially
    documented at
    https://ninjoservices.dwd.de/wiki/display/SATMET/Nebeldatenbank .

    It intends to contain:

      - synop
      - metar
      - swis
      - satellite data (SEVIRI / ABI)
      - model data (COSMO / ...)
      - cloud microphysics (CMSAF / NWCSAF / ABI-L2 / ...)
      - fogpy-calculated outputs such as fog
      - products calculated from DEM

    Originally this was written out as csv, shapefiles, geotiff, and
    npy files.  This class will likely write it out as a `pandas.DataFrame`.
    I imagine that there will be subclasses implementing methods depending on
    whether the data are for SEVIRI/Germany or ABI/USA.


    """

    # There are different methods called get_x here, but there are two
    # different aims for getting data.  One is adding to the fog database,
    # the other # is gathering the inputs needed to calculate the fog.
    # Should not confuse the two, however they overlap.  Maybe one should
    # be called prepare?  Or maybe the whole functionality should be split
    # in different classes?
    #
    # What is to be done:
    #
    # - prepare input data for running fogpy
    #   - if already there (where?) take from cache
    # - run fogpy
    #   - if already run for date, take from cache
    #       - use trollsift + yaml for config?
    # - gather the results + other data
    # - process this and build the database
    # - needs a framework for getting location of cached data etc
    #   - for each of sat, nwp, dem, nwcsaf, synop, ...:
    #       - get path where cached
    #       - check if already stored there
    #       - if not, generate:
    #           - for each input, perform step above
    #           - possibly add symlink
    #           - store in cache
    #       - select reference pixels
    #       - add to database
    #
    # Should each sub-db have its own Scene object or should they all share
    # one, coordinate by the overall FogDB?  Probably easier to merge the Scene
    # objects later, as the Scene object wants to know upon instantiation
    # already the readers and filenames involved.
    #
    # A lot of this could be done in parallel, perhaps they should all return
    # awaitables?  Much of the data gathering is I/O bound *or* coming from a
    # subprocess.  Only the fogpy fog calculation is CPU bound.  Consider using
    # asyncio and coroutines:
    #
    # - ICON stuff is slow (wait for sky tape)
    # - ABI stuff is slow (download from AWS)
    # - NWCSAF is slow (external process) but depends on the former two
    # - loading synop and DEM is probably fast enough
    # - calculating fog is CPU bound and depends on other stuff being there
    #
    # so the most sense to do synchranously is downloading ABI and ICON.  The
    # NWCSAF software already monitors for files to appear and runs in the
    # background "naturally", so the procedure should be to asynchronously
    # download ICON and ABI data to the right place, then wait for NWCSAF
    # files to appear.  Where does asyncio come in exactly?

    # TODO:
    #   - use concurrent.futures (but try linear/serial first)
    #   - collect results
    #   - add results to database

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

    def extend(self, timestamp):
        """Add data from <timestamp> to database

        This module extends the database, creatig it if it doesn't exist yet,
        with measurements for the time indicated by <timestamp>.  It calculates
        ground station measurements, then uses the lats and lons

        Args:
            timestamp (pandas.Timestamp): Time for which to add data to
            database
        """

        # first get the ground stations: these determine which points I want to
        # extract
        logger.info(f"Loading data for {timestamp:%Y-%m-%d %H:%M:%S}")
        synop = self.ground.load(timestamp)
        lats = synop.index.get_level_values("LATITUDE")
        lons = synop.index.get_level_values("LONGITUDE")
        # FIXME: use concurrent.futures here
        # extract will also call .load thus taking care of dependencies
        satdata = self.sat.extract(timestamp, lats, lons)
        nwpdata = self.nwp.extract(timestamp, lats, lons)
        # FIXME: with concurrent.futures, wait for sat and nwp to be finished
        cmicdata = self.cmic.extract(timestamp, lats, lons)
        demdata = self.dem.extract(timestamp, lats, lons)
        # FIXME: with concurrent.futures, wait for cmic and dem to be finished
        fogdata = self.fog.extract(timestamp, lats, lons)
        # FIXME: rename some fields?
        # FIXME: this needs a tolerance on the time, perhaps lat/lon too
        df = pandas.concat([synop, satdata, nwpdata, cmicdata, demdata,
                            fogdata], axis=1)
        if self.data is None:
            self.data = df
        else:
            self.data = pandas.concat([self.data, df], axis=0)

    def store(self, f):
        """Store database to file
        """
        if self.data is None:
            raise ValueError("No entries in database!")
        logger.info(f"Storing database to {f!s}")
        self.data.to_parquet(f)


class _DB(abc.ABC):
    """Get/cache/store/... DB content for quantity
    """

    # - needs a framework for getting location of cached data etc
    #   - for each of sat, nwp, dem, nwcsaf, synop, ...:
    #       - get path where cached
    #       - check if already stored there
    #       - if not, generate:
    #           - for each input, perform step above
    #           - possibly add symlink
    #           - store in cache
    #       - select reference pixels
    #
    # Do I really need a different subclass for each type of content?  Or will
    # instances suffice?  Reading the content is going to be different for
    # each.  Which parts can be fully in common?  Who creates the instances?

    dependencies = None
    base = None
    _data = None
    _generated = None  # dictionary keeping track of data files per timestamp

    @property
    @abc.abstractmethod
    def reader(self):
        raise NotImplementedError()  # pragma: no cover

    def __init__(self, dependencies=None):
        logger.debug(f"Initialising {self.__class__!s}")
        self.dependencies = dependencies if dependencies else {}
        self.base = sattools.io.get_cache_dir(subdir="fogtools") / "fogdb"
        self._data = {}
        self._generated = {}

    @abc.abstractmethod
    def get_path(self, timestamp):
        # sometimes it's one file, sometimes multiple files (such as input
        # NWCSAF with multiple channels or times), how to handle this?
        # Always return a collection?
        # It should probably always return a collection, which probably means I
        # need the flexibility of reimplementing it in subclasses and I can't
        # have it all defined from yaml files?
        raise NotImplementedError()  # pragma: no cover

    def exists(self, timestamp):
        for p in self.get_path(timestamp):
            if not p.exists():
                return False
        return True

    def ensure(self, timestamp):
        if not self.exists(timestamp):
            self.store(timestamp)

    def ensure_deps(self, timestamp):
        for (k, dep) in self.dependencies.items():
            logger.debug(f"Ensuring dependency {k:s}")
            dep.ensure(timestamp)
            self.link(dep, timestamp)

    def load(self, timestamp):
        logger.debug(f"Loading with {self.__class__!s}")
        sc = satpy.Scene(
                filenames=self.get_path(timestamp),
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

        Args:
            timestamp (pandas.Timestamp): time for which to extract
            lats (array_like): latitudes for which to extract
            lons (array_like): longitudes for which to extract

        Returns:
            pandas.DataFrame with the desired data
        """
        sc = self.load(timestamp)
        logger.debug(f"Extracting lat/lons for {self.__class__!s}")
        vals = {}
        for da in sc:
            (x, y) = da.attrs["area"].get_xy_from_lonlat(lons, lats)
            vals[da.attrs["name"]] = da.data[x, y]
        return pandas.DataFrame(
                vals,
                index=pandas.MultiIndex.from_arrays(
                    [pandas.Series(timestamp).repeat(lats.size), lats, lons],
                    names=["DATE", "LATITUDE", "LONGITUDE"]))


class _Sat(_DB):
    """Placeholder class in case further satellites are added.
    """
    pass


class _ABI(_Sat):
    reader = "abi_l1b"

    def get_path(self, timestamp):
        if timestamp in self._generated:
            return self._generated[timestamp]
        else:
            raise NotImplementedError("Cannot calculate path for ABI before "
                                      "data are available")

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
                    notfound_ok=True)
                for dir in dirs])

    def _chan_ts_exists(self, ts, chan):
        """Check if one file covering timestamp for channel exists
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
            return True
        elif len(cnt1) > 1:
            raise FogDBError(f"Channel {chan:d} found multiple times?! "
                             + ", ".join(str(c) for c in cnt1))
        # try again with T - 10 minutes
        start_search = ts - pandas.Timedelta(10, "minutes")
        files = self._search_file_two_dirs(start_search, ts, chan)
        if not files:
            return False
        cnt2 = files["abi_l1b"]
        if len(cnt2) == 1:
            return True
        elif len(cnt2) < 1:  # not sure if this is possible
            raise RuntimeError("Got empty file list from satpys "
                    "find_files_and_readers.  "
                    "This cannot happen.")  # pragma: no cover
        elif len(cnt2) > 1:
            # if T-0 is not found, T-10 should not be found twice
            raise FogDBError(f"Channel {chan:d} found multiple times?! "
                             + ", ".join(str(c) for c in cnt2))

    def exists(self, timestamp, past=False):
        """Check if files covering  timestamp exist

        Check if ABI files covering 'timestamp' exist for all channels.
        If ``past`` is True, will also ensure ABI files covering T-60 minutes
        up to T-0 minutes exist, as SAFNWC software can use this.
        """

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

        ot = functools.partial(pandas.Timedelta, unit="minutes")
        for chan in abi.nwcsaf_abi_channels | abi.fogpy_abi_channels:
            if not self._chan_ts_exists(timestamp, chan):
                return False
            if past and not (
                    (self._chan_ts_exists(timestamp - ot(60), chan)
                     and self._chan_ts_exists(timestamp - ot(20), chan)
                     or self._chan_ts_exists(timestamp - ot(30), chan))):
                return False
        else:
            return True
        raise RuntimeError("This code is unreachable")  # pragma: no cover

    def store(self, timestamp):
        """Store ABI for timestamp

        Store ABI to disk to ensure coverage for timestamp t
        """
        # FIXME: SAFNWC can use previous and pre-previous files and actually
        # looks an hour back, so this should probably start at
        # T-75 minutes to ensure covering at least T-60 minutes!
        #
        # This also needs to consider the "impossible" option if the relevant
        # file doesn't exist at the server, because a certain time is not
        # covered, either because it's out of range for ABI, or because the
        # mode asked (F, C, M1, M2) is not available at that time.
        #
        # Does the latter actually matter?  Every pixel is anyway measured at a
        # particular time within [T, T+delta_T], whether the rest of delta_T is
        # spent on F, C, M1, or M2 doesn't matter, does it?  That means the end
        # time is not relevant?
        self._generated[timestamp] = abi.download_abi_day(timestamp)

    def load(self, timestamp):
        """Get scene containing relevant ABI channels
        """
        self.ensure(timestamp)
        files = self._generated[timestamp]
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
        logger.debug("Loading ABI")
        selection = [p for p in files if p.match(
            f"*_s{timestamp:%Y%j%H%M%S}*.nc")]
        sc = satpy.Scene(
                filenames=selection,
                reader="abi_l1b")
        sc.load([f"C{ch:>02d}" for ch in
                 abi.nwcsaf_abi_channels | abi.fogpy_abi_channels])
        return sc


class _NWP(_DB):
    pass


class _ICON(_NWP):
    reader = "grib"

    def get_path(self, timestamp):
        rb = sky.RequestBuilder(self.base)
        period = sky.timestamp2period(timestamp)
        rb.get_request_ba(sky.period2daterange(period))
        exp = rb.expected_output_files
        # I don't care about the logfiles "ihits" and "info"
        return {e for e in exp if e.suffix == ".grib"}

    def store(self, timestamp):
        """Get model analysis and forecast for input to NWCSAF

        Get model analysis and forecast that we need as input to NWCSAF.
        This will come from ICON.
        """

        period = sky.timestamp2period(timestamp)
        self._generated[timestamp] = sky.get_and_send(self.base, period)


class _CMIC(_DB):
    pass


class _NWCSAF(_CMIC):
    reader = "nwcsaf-geo"

    def get_path(self, timestamp):
        return [(self.base /
                 f"{timestamp:%Y}" / f"{timestamp:%m}" / f"{timestamp:%d}" /
                 f"S_NWC_CMIC_GOES16_NEW-ENGLAND-NR_"
                f"{timestamp:%Y%m%dT%H%M%S}Z.nc")]

    def store(self, timestamp):
        """Store NWCSAF output

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

        self.ensure_deps(timestamp)
        if not self.is_running():
            self.start_running()

    @staticmethod
    def is_running():
        """Check whether the NWCSAF software is running

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
        """Run NWCSAF software

        Start running the NWCSAF software.  As this takes a moment to get
        started, users might want to run this module using
        `ThreadPoolExecutor.submit`.

        Returns:
            CalledProcess
        """
        logger.debug("Starting NWCSAF software")
        return subprocess.run(["SAFNWCTM"], check=True)

    @staticmethod
    def _get_dep_loc(dep):
        """Find location from where dependency should be symlinked

        The SAFNWC software expects input data to be in a particular directory,
        but the data may be elsewhere.  This static method determines where
        these data are expected by the SAFNWC software.
        """
        safnwc = os.getenv("SAFNWC")
        if not safnwc:
            raise FogDBError("Environment variable SAFNWC not set")
        p = pathlib.Path(safnwc) / "import"
        if isinstance(dep, _Sat):
            return p / "Sat_data"
        elif isinstance(dep, _NWP):
            return p / "NWP_data"
        else:
            raise TypeError("Passed unrecognised dependency type: "
                            f"{type(dep)!s}")

    def link(self, dep, timestamp):
        logger.debug("Linking NWCSAF dependenices")
        link_dsts = dep.get_path(timestamp)
        link_src_dir = self._get_dep_loc(dep)
        link_src_dir.mkdir(exist_ok=True, parents=True)
        for p in link_dsts:
            (link_src_dir / p.name).symlink_to(p)

    def wait_for_output(self, timestamp, timeout=600):
        """Wait for SAFNWC outputs

        With the SAFNWC code running, wait until the results are there.
        """
        if not self.is_running():
            raise FogDBError("SAFNWC is not running")
        logger.info("Waiting for SAFNWC results")
        t = 0
        of = self.get_path(timestamp)[0]
        while t < timeout:
            if of.exists():
                return
            time.sleep(10)
            t += 10
        else:
            raise FogDBError(f"No SAFNWC result after {timeout:d} s")

    def ensure(self, timestamp):
        self.wait_for_output(timestamp)


class _Ground(_DB):
    pass


class _SYNOP(_Ground):
    reader = None  # cannot be read with Satpy

    def get_path(self, timestamp):
        return [isd.get_db_location()]

    _db = None

    def load(self, timestamp, tol=pandas.Timedelta("30m")):
        """Get ground based measurements from Integrated Surface Dataset

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
            logger.debug("Reading ground measurements database from ISD")
            db = isd.read_db()
            if db.index.names != ["DATE", "LATITUDE", "LONGITUDE"]:
                db = db.set_index(["DATE", "LATITUDE", "LONGITUDE"])
            self._db = db.sort_index()
        return self._db.loc[timestamp-tol:timestamp+tol]

    def store(self, _):
        isd.create_db()


class _DEM(_DB):
    reader = "generic_image"

    dem_new_england = pathlib.Path("/media/nas/x21308/DEM/USGS/merged-500.tif")
    dem_europe = pathlib.Path("/media/nas/x21308/DEM/dem_eu_1km.tif")
    location = None

    def __init__(self, region):
        """Initialise DEM class

        Initialise class to provide DEM information to database.

        Args:
            region (str): Either "new-england" or "europe"
        """
        self.location = getattr(self, "dem_" + region.replace("-", "_"))

    def get_path(self, _):
        return self.location

    def store(self, _):
        if self.location == self.dem_new_england:
            logger.info("Downloading DEMs")
            out_all = dem.dl_usgs_dem_in_range(38, 49, -82, -66,
                                               self.location.parent)
        else:
            raise NotImplementedError("Can only download New England DEM")
        with tempfile.NamedTemporaryFile() as ntf:
            logger.info("Merging DEMs")
            subprocess.run(["gdal_merge.py", "-o", ntf.name] +
                           [str(p) for p in out_all],
                           check=True)
            subprocess.run(
                    ["gdalwarp", "-r", "bilinear", "-t_srs", "+proj=eqc "
                     "+lat_ts=0 +lat_0=0 +lon_0=0 +x_0=0 +y_0=0 +ellps=WGS84 "
                     "+units=m +no_defs +type=crs", "-tr", "500", "500",
                     ntf.name, str(self.location)],
                    check=True)


class _Fog(_DB):
    reader = "generic_image"  # stored as geotiff

    def get_path(self, timestamp, sensorreader="nwcsaf-geo"):
        return [self.base / f"fog-{timestamp:%Y%m%d-%H%M}.tif"]

    def store(self, timestamp):
        logger.info("Calculating fog")
        sc = core.get_fog(
                "abi_l1b",
                self.dependencies["sat"].get_path(timestamp),
                self.dependencies["cmic"].get_path(timestamp),
                "new-england-500")
        sc.save_dataset("fls_day", self.get_path(timestamp)[0])


# TODO: _IFS, _COSMO, _METAR, _SWIS
