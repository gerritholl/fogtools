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

    Parameters passed to constructor, also attributes:

        timestamp (pandas.Timestamp)
            Time for which to add to database
        sat_in (pathlib.Path)
            Directory for satellite data
        cmp_in (pathlib.Path)
            Directory for cloud microphysics data (CMSAF, NWCSAF, ABI-L2)

    """

    timestamp = None
    sat_in = None
    cmp_in = None

    def __init__(self, *, timestamp, sat_in, cmp_in, **kwargs):
        for (k, v) in kwargs.items():
            self.timestamp = timestamp
            self.sat_in = sat_in
            self.cmp_in = cmp_in
            setattr(self, k, v)

    def __setattr__(self, k, v):
        getattr(self, k)  # will trigger AttributeError if not found
        super().__setattr__(self, k, v)
