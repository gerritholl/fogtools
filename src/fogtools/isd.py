"""Routines related to Integrated Surface Database (ISD)

"""

import logging
import os
import pathlib
import pandas
import pkg_resources
import itertools
import dask.dataframe

LOG = logging.getLogger(__name__)


def get_stations():
    """Return a list of ISD stations as a pandas DataFrame

    Returns:

        pandas.DataFrame with the stations
    """
    station_list = pkg_resources.resource_filename(
                    "fogtools", "data/isd-history.txt")
    df = pandas.read_fwf(station_list, skiprows=20,
                         parse_dates=["BEGIN", "END"],
                         dtype={"WBAN": "string",
                                "USAF": "string"})
    # The first line of the dataframe is blank.  Normally, one should be able
    # to pass skip_blank_lines=True to read_fwf, but this has no effect:
    #
    # https://github.com/pandas-dev/pandas/issues/22693
    #
    # therefore, explicitly drop the first entry

    return df.drop(0)


def select_stations(df,
                    states=["RI", "MA", "VT", "NH", "ME", "CT", "NY"]):
    """From the stations list, select those we want

    From the stations list returned by :func:`get_stations`, return only those
    that are in New England or New York and active in 2020.

    Args:
        df (pandas.DataFrame)
            DataFrame with a list of stations, such as returned by
            :func:`get_stations`.
        states (List, optional)
            List of US states to consider.  Defaults to New England states +
            NY.

    Returns:
        pandas.DataFrame with only those stations we want to consider
    """
    return df[df.isin({"ST": states}).any(1) & (df.END > "20200101")]


def get_station_ids(df):
    """Get station IDs from station list

    From the station list (DataFrame), obtain a pandas.Series of IDs for
    direct download through AWS noaa-global-hourly-pds, which consists of
    the columns USAF and WBAN.

    Args:
        df (pandas.DataFrame):
            DataFrame with station list, such as from :func:`get_stations`

    Returns:
        pandas.Series with IDs.
    """
    return df["USAF"] + df["WBAN"]


def dl_station(year, id_):
    """Download station from AWS.

    Download the measurements for a station and year from AWS S3 and return it
    as a pandas DataFrame.

    Args:
        year (int):
            Year to download
        id_ (str):
            Station ID as returned by :func:`get_station_ids`

    Returns:
        pandas.DataFrame with station contents.
    """

    s3_uri = f"s3://noaa-global-hourly-pds/{year:04d}/{id_:s}.csv"
    LOG.debug(f"Reading from S3: {s3_uri:s}")
    df = pandas.read_csv(s3_uri,
                         usecols=["STATION", "NAME", "DATE", "LATITUDE",
                                  "LONGITUDE", "ELEVATION", "VIS", "TMP",
                                  "DEW"],
                         parse_dates=["DATE"],
                         dtype=dict.fromkeys(["STATION", "NAME", "VIS",
                                              "TMP", "DEW"],
                                             pandas.StringDtype()))
#            index_col="DATE")
#    _obj_to_str(df)
    return df


def _get_cache_dir(base=None):
    """Get directory to use for caching

    Get (and create, if necessary) directory to use for caching.

    Args:
        base (str or pathlib.Path):
            Directory in which to create cache dir.  If not given, use
            XDG_CACHE_HOME or otherwise ~/.cache.

    Returns:
        pathlib.Path object pointing to cache dir
    """
    cacheroot = (base or
                 os.environ.get("XDG_CACHE_HOME") or
                 pathlib.Path.home() / ".cache")
    cacheroot = pathlib.Path(cacheroot)
    cacheroot /= "fogtools"
    cacheroot.mkdir(parents=True, exist_ok=True)
    return cacheroot


def get_station(year, id_):
    """Get station as DataFrame from cache or AWS.

    Try to get a station from local disk cache ($XDG_CACHE_HOME/fogtools if
    this is set, otherwise ~/.cache/fogtools).  If it's not there, get it from
    AWS, then store it to cache.

    Args:
        year (int):
            Year to download
        id_ (str):
            Station ID as returned by :func:`get_station_ids`

    Returns:
        pandas.DataFrame with station contents.
    """

    cachedir = _get_cache_dir()
    cachefile = (cachedir / str(year) / id_).with_suffix(".feather")
    try:
        LOG.debug(f"Reading from cache: {cachefile!s}")
        return pandas.read_feather(cachefile)
    except OSError:  # includes pyarrow.lib.ArrowIOError
        df = dl_station(year, id_)
        cachefile.parent.mkdir(parents=True, exist_ok=True)
        LOG.debug(f"Storing to cache: {cachefile!s}")
        df.to_feather(cachefile)
        return df


def extract_vis(df):
    """From a measurement dataframe, extract visibilities

    Visibilities are reported in the AWS ISD CSV files with a sort of nested
    CSV.  This function unpacks the string and reports visibility, visibility
    quality code, visibility variability code, and visibility quality
    variability code.  See the ISD format document, page 10.

    Args:
        df (pandas.DataFrame):
            DataFrame with station list, such as from :func:`get_stations`

    Returns:
        pandas.DataFrame with four visibilities named vis, vis_qc, vis_vc, and
        vis_qvc.
    """
    # dask fails with extracting if dtype is StringDtype:
    # see https://github.com/dask/dask/issues/5833
    tmp = df.VIS.str.extract(r"(\d{6}),(\d),([NV9]),(\d)")
    tmp.columns = ["vis", "vis_qc", "vis_vc", "vis_qvc"]
    # dask-friendly alternative to pandas.to_numeric while handling bad data
    vis = tmp.vis.where(~tmp.vis.isna(), "999999").astype("u4")
    tmp = tmp.drop("vis", axis=1)
    tmp["vis"] = vis
    return tmp


def _count_station_years(stations, start, end):
    """Count station years
    """
    return sum(min(fi, end).year-max(st, start).year+1
               for (st, fi) in zip(stations["BEGIN"], stations["END"]))


def create_db(f=None, start=pandas.Timestamp(2017, 1, 1),
              end=pandas.Timestamp.now()):
    """Create a parquet database with all New England measurements

    Create a Parquet database with all New England-based measurements between
    2017 and 2020 (inclusive), for the fields that we are interested in.

    Args:
        f (str or pathlib.Path)
            Where to write the database.  Defaults to a file "store.parquet" in
            the cache directory.  File will be overwritten.
    """
    # TODO, this should merge the vis extraction
    stations = select_stations(get_stations())
    ids = get_station_ids(stations)
    cachedir = _get_cache_dir()
    f = f or (cachedir / "store.parquet")
    if not isinstance(start, pandas.Timestamp):
        start = pandas.Timestamp(start)
    if not isinstance(end, pandas.Timestamp):
        end = pandas.Timestamp(end)
    n = _count_station_years(stations, start, end)
    LOG.info(f"Expecting {n:d} station·years")
    L = []
    c = itertools.count()
    next(c)
    for (id_, st, fi) in zip(ids, stations["BEGIN"], stations["END"]):
        for year in pandas.date_range(
                pandas.Timestamp(max(start, st).year, 1, 1),
                pandas.Timestamp(min(end, fi).year+1, 1, 1), freq="Y").year:
            LOG.debug(f"Adding to store, {year:d} for station {id_:s}, "
                      f"no {next(c):d}/{n:d}")
            try:
                df = get_station(year, id_)
            except FileNotFoundError:
                LOG.warning(f"Not available: {id_:s}/{year:d}")
            else:
                L.append(df)
    df_total = pandas.concat(L)
    LOG.debug(f"Storing to {f!s}")
    df_total.to_parquet(f)


def read_db(f=None, usedask=True):
    """Read parquet DB
    """

    cachedir = _get_cache_dir()
    f = f or (cachedir / "store.parquet")
    if usedask:
        df = dask.dataframe.read_parquet(f)
        df = df.repartition(npartitions=16)
        return df
    else:
        return pandas.read_parquet(f)


def count_fogs_per_day(df, max_vis=150):
    """Count how many stations register fog per day

    Based on a dataframe containing aggregated measurements for
    """
    df["vis"] = extract_vis(df)["vis"]
    lowvis = (df["vis"] < max_vis) & (df["vis"] > 0)
    sel = df[lowvis]
    grouped = sel.groupby([sel["STATION"], sel["DATE"].dt.date])
    cnt_st_dt = grouped.size()
    cnt_dt = cnt_st_dt.groupby("DATE").size()
    return cnt_dt
