"""Functionality to write a fog-database

This module contains functionality to write a fog-database.  It is a new
implementation of functionality initially written by Thomas Leppelt in
create_fog_vect_rast.py.  This rewrite is intended to update to newer versions
of fogpy and satpy, to make the code more maintainable by improved modularity
and adding unitests, and to support ground data over the USA.
"""

import abc

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

    sat = nwp = cmic = ground = dem = fog = None

    def __init__(self):
        self.sat = _ABI()
        self.nwp = _ICON()
        self.cmic = _NWCSAF(dependencies={self.sat, self.nwp})
        self.ground = _SYNOP()
        self.dem = _DEM()
        self.fog = _Fog(dependencies=(self.sat, self.cmic, self.dem))


    def __setattr__(self, k, v):
        getattr(self, k)  # will trigger AttributeError if not found
        super().__setattr__(self, k, v)

    def extend(self, timestamp):
        """Add data from <timestamp> to database
        """

        # there's a dependency tree to worry about here, can I use Satpy
        # functionality for that?  Or is that more trouble than it's worth?
        # Probably more trouble than it's worth.
        sat = self.get_sat(timestamp)
        ground = self.get_ground_truth(timestamp)
        dem = self.get_dem(timestamp)
        analysis_input = self.get_input_analysis(timestamp)
        analysis_comp = self.get_comparison_analysis(timestamp)
        cmip = self.get_cloud_microphysics(timestamp)
        fog = self.get_fogpy_products(timestamp)
        df = self.build(self, sat, ground, dem, analysis, cmip, fog)
        # do what with df?

    def get_ground_truth(self, timestamp):
        """Get ground weather reports for <timestamp>

        These are used to compare against.
        """

        synop = self.get_synop(timestamp)
        metar = self.get_metar(timestamp)
        swis = self.get_swis(timestamp)
        df = self.build(synop, metar, swis)
        return df

    def get_comparison_input(self, timestamp):
        """Get model analysis and forecast for input to NWCSAF

        Get model analysis and forecast that we need as input to NWCSAF.
        This will come from ICON.
        """

        # this should use the sky module
        #
        # - check if data are present (where?)
        # - if not, get from SKY using sky module

        raise NotImplementedError()

    def get_comparison_analysis(self, timestamp):
        """Get model analysis and forecast for comparison

        Get model analysis and forecast for comparing the fog result with.
        This may be from a regional model; Thomas Leppelt was using COSMO but
        that won't work in North-America.  Could also compare with IFS from
        ECMWF.  Need to think of a flexible way of implementing this, perhaps
        by passing a bunch of classes that each implement the same interface.
        """

        raise NotImplementedError()

    def get_sat(self, timestamp):
        # this should use the abi module
        #
        # - check if data are present (where?)
        # - if not, download (for ABI, from S3)
        raise NotImplementedError()

    def get_synop(self, timestamp):
        # this should use the isd module
        #
        # - check if data are present (where?)
        # - if not, get from ISD
        raise NotImplementedError()

    def get_metar(self, timestamp):
        # metar is another source of ground truth
        raise NotImplementedError()

    def get_swis(self, timestamp):
        # swis is another source of ground truth
        raise NotImplementedError()

    def get_dem(self):
        raise NotImplementedError()

    def get_cosmo(self, timestamp):
        raise NotImplementedError()

    def get_cloud_microphysics(self, timestamp):
        return self.get_nwcsaf(timestamp)

    def get_nwcsaf(self, timestamp):
        self.ensure_nwcsaf_inputs(timestamp)
        self.run_nwcsaf(timestamp)
        raise NotImplementedError()

    def ensure_nwcsaf_inputs(self, timestamp):
        """Ensure NWCSAF has inputs it needs
        """
        self.ensure_nwcsaf_nwp(timestamp)
        self.ensure_nwcsaf_sat(timestamp)

    def ensure_nwcsaf_nwp(self, timestamp):
        """Ensure NWCSAF NWP input data present and findable

        Relative to the NWCSAF directory, those have names such as
        import/NWP_data/S_NWC_NWP_2017-03-14T18:00:00Z_000.grib

        """

        if not self.has_nwcsaf_nwp(timestamp):
            self.get_nwcsaf_nwp(timestamp)
        self.link_nwcsaf_nwp(timestamp)

    def ensure_nwcsaf_sat(self, timestamp):
        """Ensure sat input data present for NWCSAF

        Relatuve to the NWCSAF directory, those have names such as
        import/Sat_data/OR_ABI-L1b-RadF-M3C16_G16_s20170731751103_e20170731801481_c20170731801539.nc
        """

        if not self.has_nwcsaf_sat(timestamp):
            self.get_nwcsaf_sat(timestamp)
        self.link_nwcsaf_sat(timestamp)

    def get_fogpyproducts(timestamp):
        raise NotImplementedError()


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

    def __init__(self, dependencies=None):
        self.dependencies = dependencies if dependencies else []

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
            self.generate()

    @abc.abstractmethod
    def generate(self, timestamp):
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

    @abc.abstractmethod
    def extract(self, timestamp, lats, lons):
        # there should be a way to share this, at least between all contents
        # that are part of a Scene object: ABI, NWCSAF, DEM.  Should in
        # principle also work for ICON, GFS, IFS if in GRIB2, using Satpy grib2
        # reader.
        raise NotImplementedError()


class _Sat(_DB):
    pass


class _ABI(_Sat):
    def get_path(self, timestamp):
        raise NotImplementedError("Cannot calculate path for ABI")

    def exists(self, timestamp):
        for chan in abi.nwcsaf_abi_channels|abi.fogpy_abi_channels:
            dl_dir = abi.get_dl_dir(
                    sattools.io.get_cache_dir(subdir="fogtools"),
                    timestamp,
                    chan)
            cnt = list(dl_dir.glob(f"*C{chan:>02d}*"))
            if len(cnt) < 1:
                return False
            elif len(cnt) > 1:
                raise FogDBError(f"Channel {chan:d} found multiple times in "
                        f"{dl_dir!s}?! " + ", ".join(str(c) for c in cnt))
        return True

    _generated = {}
    def generate(self, timestamp):
        abi.download_abi_day(timestamp)
        self._generated[timestamp] = True

    def store(self, timestamp):
        # already stored by .generate
        if not self._generated.get(timestamp, False):
            raise FogDBError("Called .store(...) before .generate(...)")


class _NWP(_DB):
    pass


class _ICON(_NWP):
    pass


class _NWCSAF(_DB):
    def link(self, dep, timestamp):
        raise NotImplementedError()


class _SYNOP(_DB):
    pass


class _DEM(_DB):
    pass


class _Fog(_DB):
    pass
