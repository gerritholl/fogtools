"""Functionality to write a fog-database

This module contains functionality to write a fog-database.  It is a new
implementation of functionality initially written by Thomas Leppelt in
create_fog_vect_rast.py.  This rewrite is intended to update to newer versions
of fogpy and satpy, to make the code more maintainable by improved modularity
and adding unitests, and to support ground data over the USA.
"""


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

    # There are different methods called get_x here, but there are two different
    # aims for getting data.  One is adding to the fog database, the other
    # is gathering the inputs needed to calculate the fog.  Should not confuse
    # the two, however they overlap.  Maybe one should be called prepare?  Or
    # maybe the whole functionality should be split in different classes?
    #
    # - prepare input data for running fogpy
    # - run fogpy
    # - gather the results + other data
    # - process this and build the database

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
        # swis is another source fo ground truth
        raise NotImplementedError()

    def get_dem(self, timestamp):
        raise NotImplementedError()

    def get_cosmo(self, timestamp):
        raise NotImplementedError()

    def get_cloud_microphysics(self, timestamp):
        return self.get_nwcsaf(timestamp)

    def get_nwcsaf(timestamp):
        raise NotImplementedError()

    def get_fogpyproducts(timestamp):
        raise NotImplementedError()
