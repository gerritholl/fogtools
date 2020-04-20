"""Functionality to write a fog-database

This module contains functionality to write a fog-database.  It is a new
implementation of functionality initially written by Thomas Leppelt in
create_fog_vect_rast.py.  This rewrite is intended to update to newer versions
of fogpy and satpy, to make the code more maintainable by improved modularity
and adding unitests, and to support ground data over the USA.
"""

import logging
import subprocess
import tempfile
import pathlib

import pandas
import satpy
import abc

import sattools.io

from . import abi, sky, isd, core, dem

logger = logging.getLogger(__name__)


class FogDBError:
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
    #   - use concurrent.futures
    #   - symlinks for NWCSAF dependencies
    #   - collect results
    #   - add results to database
    #   - fix get_path for ABI

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
        """

        dfs = []
        for src in (self.sat, self.nwp, self.cmic, self.ground, self.dem,
                    self.fog):
            dfs.append(src.extract())
        df = pandas.concat(dfs, axis=1)
        if self.data is None:
            self.data = df
        else:
            self.data = pandas.concat(self.data, df, axis=0)

    def store(self, f):
        """Store database to file
        """
        if self.data is None:
            raise ValueError("No entries in database!")
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
        ...

    def __init__(self, dependencies=None):
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
        raise NotImplementedError()

    def exists(self, timestamp):
        for p in self.get_path(timestamp):
            if not p.exists():
                return False
        return True

    def ensure_deps(self, timestamp):
        for dep in self.dependencies.values():
            dep.ensure(timestamp)
        self.link(dep, timestamp)

    def ensure(self, timestamp):
        if not self.exists():
            self.store()

    def load(self, timestamp):
        sc = satpy.Scene(
                filenames=self.get_path(timestamp),
                reader=self.reader)
        sc.load(sc.available_dataset_names())
        return sc

    @abc.abstractmethod
    def store(self, timestamp):
        """Store in cache.
        """
        raise NotImplementedError()

    def link(self, dep, timestamp):
        """If needed, add symlink for dependency
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
        vals = {}
        for da in sc:
            (x, y) = da.attrs["area"].get_xy_from_lonlat(lons, lats)
            vals[da.attrs["name"]] = da.data[x, y]
        return pandas.DataFrame(vals)


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

    def exists(self, timestamp):
        for chan in abi.nwcsaf_abi_channels | abi.fogpy_abi_channels:
            dl_dir = abi.get_dl_dir(
                    self.basedir,
                    timestamp,
                    chan)
            cnt = list(dl_dir.glob(f"*C{chan:>02d}*"))
            if len(cnt) < 1:
                return False
            elif len(cnt) > 1:
                raise FogDBError(f"Channel {chan:d} found multiple times in "
                                 f"{dl_dir!s}?! " + ", ".join(
                                     str(c) for c in cnt))
        else:
            return True
        raise RuntimeError("This code is unreachable")

    def store(self, timestamp):
        """Store ABI for timestamp
        """
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
        selection = [p for p in files if p.match(
            f"*_s{timestamp:%Y%j%H%M%S%f}_*.nc")]
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
        return rb.expected_output_files

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
        # await existance of file?  Or up to caller?

    @staticmethod
    def is_running():
        """Check whether the NWCSAF software is running

        Using the task manager, check whether the NWCSAF software is running.
        Return True if it is or False otherwise.  Raises CalledProcessError if
        the task manager doesn't exist or exits with a different errorcode than
        1 (such as with a signal).  Raises a FogDBError if the task manager
        exited successfully but with an unexpected output.
        """
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
        logging.debug("Starting NWCSAF software")
        return subprocess.run(["SAFNWCTM"], check=True)

    def link(self, dep, timestamp):
        raise NotImplementedError()


class _Ground(_DB):
    pass


class _SYNOP(_Ground):
    reader = None  # cannot be read with Satpy

    def get_path(self, timestamp):
        return isd.get_db_location()

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
        if self._db is None:
            self._db = isd.read_db()
        selection = ((self._db.DATE > timestamp-tol) &
                     (self._db.DATE < timestamp+tol))
        return self._db.loc[selection]

    def store(self, timestamp):
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
            logging.info("Downloading DEMs")
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
        return [self.basedir / f"fog-{timestamp:%Y%m%d-%H%M}.tif"]

    def store(self, timestamp):
        self.dependecies["sat"]
        sc = core.get_fog_blend_for_sat(
                self.dependencies["sat"].get_path(),
                self.dependencies["cmic"].get_path(),
                "new-england-500")
        sc.save_dataset("fls_day", self.get_path()[0])


class _IFS(_NWP):
    def load(self, timestamp):
        """Get model analysis and forecast for comparison

        Get model analysis and forecast for comparing the fog result with.
        This may be from a regional model; Thomas Leppelt was using COSMO but
        that won't work in North-America.  Could also compare with IFS from
        ECMWF.  Need to think of a flexible way of implementing this, perhaps
        by passing a bunch of classes that each implement the same interface.
        """

        raise NotImplementedError()


class _COSMO(_NWP):
    def load(self, timestamp):
        raise NotImplementedError()


class _METAR(_Ground):
    def load(self, timestamp):
        # metar is another source of ground truth
        raise NotImplementedError()


class _SWIS(_Ground):
    def load(self, timestamp):
        # swis is another source of ground truth
        raise NotImplementedError()
