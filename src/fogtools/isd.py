"""Routines related to Integrated Surface Database (ISD)

"""

import logging
import itertools
import functools
import operator
import pandas
import pkg_resources
import pathlib

from sattools import io as stio

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
    return df


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

    # using pickle for cache files because feather or parquet do not
    # preserve dtypes: https://github.com/pandas-dev/pandas/issues/31497
    # and https://github.com/pandas-dev/pandas/issues/29752

    cachedir = stio.get_cache_dir(subdir="fogtools")
    cachefile = (cachedir / str(year) / id_).with_suffix(".pkl")
    try:
        LOG.debug(f"Reading from cache: {cachefile!s}")
        return pandas.read_pickle(cachefile)
    except OSError:  # includes pyarrow.lib.ArrowIOError
        df = dl_station(year, id_)
        LOG.debug(f"Storing to cache: {cachefile!s}")
        cachefile.parent.mkdir(parents=True, exist_ok=True)
        df.to_pickle(cachefile)
        return df


def extract_vis(df):
    """From a measurement dataframe, extract visibilities

    Visibilities are reported in the AWS ISD CSV files with a sort of nested
    CSV.  This function unpacks the string and reports visibility, visibility
    quality code, visibility variability code, and visibility quality
    variability code.  See the ISD format document, page 10.

    Args:
        df (pandas.DataFrame):
            DataFrame with measurementsn from station, such as returned by
            :func:`get_station`

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


def extract_temp(df, tp="TMP"):
    """From a measurement dataframe, extract temperatures or dew points

    Temperatures and dew points are reported in the AWS ISD CSV files
    with a sort of nested CSV.  This function unpacks the string and
    reports temperature and quality code.  See the ISD format focument,
    page 10 and 11.

    Args:
        df (pandas.DataFrame):
            DataFrame with measurementsn from station, such as returned by
            :func:`get_station`

        tp (str):
            Can be "TMP" for temperature or "DEW" for dew point.

    Returns:
        pandas.DataFrame with temperature and corresponding quality code
    """

    tmp = df[tp].str.extract(r"([+-]\d{4}),([012345679ACIMPRU])")
    tmp.columns = [tp.lower(), f"{tp.lower():s}_qc"]
    temp = tmp[tp.lower()].where(
            ~tmp[tp.lower()].isna(), "+9999").astype("f4")/10
    tmp = tmp.drop(tp.lower(), axis=1)
    tmp[tp.lower()] = temp
    return tmp


def extract_and_add_all(df):
    """Extract visibility and temperatures and add to dataframe

    Extract visibility and temperatures, select rows where those are
    valid, and add to the dataframe.  Here, "valid" means the quality codes for
    each of visibility, temperature, and dew point must be 1, 4, 5, C, I, or M.
    See the ISD format documentation for details.

    Args:
        df (pandas.DataFrame):
            DataFrame with measurementsn from station, such as returned by
            :func:`get_station`

    Returns:
        pandas.DataFrame with numeric fields "vis", "temp", and "dew" added
        and the string fields "VIS", "TMP", and "DEW" removed.
    """

    vis = extract_vis(df)
    tmp = extract_temp(df, "TMP")
    dew = extract_temp(df, "DEW")
    qual_ok = ["1", "4", "5", "C", "I", "M"]

    ok = functools.reduce(
            operator.and_,
            (f.isin(qual_ok) for f in (vis.vis_qc, tmp.tmp_qc, dew.dew_qc)))
    ok = ok.fillna(False)
    df = df[ok]
    df = df.drop(["VIS", "TMP", "DEW"], axis=1)
    df["vis"] = vis.vis[ok]
    df["temp"] = tmp.tmp[ok]
    df["dew"] = dew.dew[ok]
    return df


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
    cachedir = stio.get_cache_dir(subdir="fogtools")
    f = pathlib.Path(f) if f is not None else get_db_location()
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
                df = extract_and_add_all(df)
                L.append(df)
    df_total = pandas.concat(L)
    LOG.debug(f"Storing to {f!s}")
    f.parent.mkdir(exist_ok=True, parents=True)
    df_total.to_parquet(f)


def get_db_location():
    """Get location for parquet DB
    """
    cachedir = stio.get_cache_dir(subdir="fogtools")
    return cachedir / "store.parquet"


def read_db(f=None):
    """Read parquet DB
    """

    f = f or get_db_location()
    return pandas.read_parquet(f)


def count_fogs_per_day(df, max_vis=150):
    """Count how many stations register fog per day

    Based on a dataframe containing aggregated measurements such as returned
    by :func:`read_db`.
    """
    if "vis" not in df.columns:
        df["vis"] = extract_vis(df)["vis"]
    lowvis = (df["vis"] < max_vis) & (df["vis"] > 0)
    sel = df[lowvis]
    grouped = sel.groupby([sel["STATION"], sel["DATE"].dt.date])
    cnt_st_dt = grouped.size()
    cnt_dt = cnt_st_dt.groupby("DATE").size()
    return cnt_dt
