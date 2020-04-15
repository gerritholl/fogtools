"""Functionality to write a fog-database

This module contains functionality to write a fog-database.  It is a new
implementation of functionality initially written by Thomas Leppelt in
create_fog_vect_rast.py.  This rewrite is intended to update to newer versions
of fogpy and satpy, to make the code more maintainable by improved modularity
and adding unitests, and to support ground data over the USA.
"""

import pandas
import satpy
import abc

import sattools.io

from . import abi


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

    sat = nwp = cmic = ground = dem = fog = data = None

    def __init__(self):
        self.sat = _ABI()
        self.nwp = _ICON()
        self.cmic = _NWCSAF(dependencies={self.sat, self.nwp})
        self.ground = _SYNOP()
        self.dem = _DEM()
        self.fog = _Fog(dependencies=(self.sat, self.cmic, self.dem))

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
    _data = None

    def __init__(self, dependencies=None):
        self.dependencies = dependencies if dependencies else []
        self._data = {}

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
        for dep in self.dependencies:
            dep.ensure()
        self.link(dep, timestamp)

    def ensure(self, timestamp):
        if not self.exists():
            self.store()

    @abc.abstractmethod
    def load(self, timestamp):
        raise NotImplementedError()

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
    pass


class _ABI(_Sat):
    def get_path(self, timestamp):
        raise NotImplementedError("Cannot calculate path for ABI")

    def exists(self, timestamp):
        for chan in abi.nwcsaf_abi_channels | abi.fogpy_abi_channels:
            dl_dir = abi.get_dl_dir(
                    sattools.io.get_cache_dir(subdir="fogtools"),
                    timestamp,
                    chan)
            cnt = list(dl_dir.glob(f"*C{chan:>02d}*"))
            if len(cnt) < 1:
                return False
            elif len(cnt) > 1:
                raise FogDBError(f"Channel {chan:d} found multiple times in "
                                 f"{dl_dir!s}?! " + ", ".join(
                                     str(c) for c in cnt))
        return True

    def store(self, timestamp):
        self._generated[timestamp] = abi.download_abi_day(timestamp)

    def load(self, timestamp):
        """Get scene containing relevant ABI channels
        """
        if not self.exists(timestamp):
            self.store(timestamp)
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

    def load(self, timestamp):
        """Get model analysis and forecast for input to NWCSAF

        Get model analysis and forecast that we need as input to NWCSAF.
        This will come from ICON.
        """

        # this should use the sky module
        #
        # - check if data are present (where?)
        # - if not, get from SKY using sky module

        raise NotImplementedError()
    pass


class _CMIC(_DB):
    pass


class _NWCSAF(_CMIC):

    def load(self, timestamp):
        self.ensure_nwcsaf_inputs(timestamp)
        self.run_nwcsaf(timestamp)
        raise NotImplementedError()

    def ensure_nwcsaf_inputs(self, timestamp):
        """Ensure NWCSAF has inputs it needs
        """
        raise RuntimeError("Delete, this is in superclass")
        self.ensure_nwcsaf_nwp(timestamp)
        self.ensure_nwcsaf_sat(timestamp)

    def ensure_nwcsaf_nwp(self, timestamp):
        """Ensure NWCSAF NWP input data present and findable

        Relative to the NWCSAF directory, those have names such as
        import/NWP_data/S_NWC_NWP_2017-03-14T18:00:00Z_000.grib

        """
        # FIXME DELETE
        if not self.has_nwcsaf_nwp(timestamp):
            self.get_nwcsaf_nwp(timestamp)
        self.link_nwcsaf_nwp(timestamp)

    def ensure_nwcsaf_sat(self, timestamp):
        """Ensure sat input data present for NWCSAF

        Relatuve to the NWCSAF directory, those have names such as
        import/Sat_data/OR_ABI-L1b-RadF-M3C16_G16_s20170731751103_e20170731801481_c20170731801539.nc
        """
        # FIXME DELETE

        if not self.has_nwcsaf_sat(timestamp):
            self.get_nwcsaf_sat(timestamp)
        self.link_nwcsaf_sat(timestamp)

    def link(self, dep, timestamp):
        raise NotImplementedError()


class _Ground(_DB):
    pass


class _SYNOP(_Ground):
    def load(self, timestamp):
        # this should use the isd module
        #
        # - check if data are present (where?)
        # - if not, get from ISD
        raise NotImplementedError()


class _DEM(_DB):
    def load(self):
        raise NotImplementedError()


class _Fog(_DB):
    def load(timestamp):
        raise NotImplementedError()


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
