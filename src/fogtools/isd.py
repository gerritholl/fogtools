"""Routines related to Integrated Surface Database (ISD)

"""

import os
import pathlib
import numpy
import pandas
import pkg_resources

def get_stations():
    """Return a list of ISD stations as a pandas DataFrame

    Returns:

        pandas.DataFrame with the stations
    """
    station_list = pkg_resources.resource_filename(
                    "fogtools", "data/isd-history.txt")
    df = pandas.read_fwf(station_list, skiprows=20,
                         parse_dates=["BEGIN", "END"],
                         dtype={"WBAN": "O", "USAF": "O"})
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
    return df[df.isin({"ST": states}).any(1) & (df.END>"20200101")]

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

def _obj_to_str(df):
    """Ensure all object-dtypes in df contain only str

    When a pandas DataFrame has a column with strings, it will make an object
    dtype.  However, if it contains a mixture of strings and ints, this object
    dtype may have poor performance as an HDF.  See
    https://stackoverflow.com/q/22998859/974555 and answers there.

    Args:
        df (pandas.DataFrame):
            Dataframe on which to operate.

    Returns nothing, operates in-place.
    """

    oc = [k for (k, v) in df.dtypes.items() if v==numpy.dtype("O")]
    df.loc[:, oc] = df[oc].applymap(str)

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

    df = pandas.read_csv(
         f"s3://noaa-global-hourly-pds/{year:04d}/{id_:s}.csv",
         dtype={"STATION": str})
    _obj_to_str(df)
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
    cachefile = (cachedir / str(year) / id_).with_suffix(".h5")
    try:
        return pandas.read_hdf(cachefile, "root")
    except FileNotFoundError:
        df = dl_station(year, id_)
        df.to_hdf(cachefile, "root")
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
    tmp = df.VIS.str.extract(r"(\d{6}),(\d),([NV9]),(\d)")
    tmp.columns = ["vis", "vis_qc", "vis_vc", "vis_qvc"]
    vis = pandas.to_numeric(
            tmp["vis"],
            errors="coerce").fillna(999999).astype(numpy.int32)
    tmp.drop("vis", inplace=True, axis=1)
    tmp["vis"] = vis
    return tmp

def create_db(f=None):
    """Create a HDF5 database with all New England measurements

    Args:
        f (str or pathlib.Path)
            Where to write the database.  Defaults to a file "store.h5" in
            the cache directory.  File will be overwritten.
    """
    stations = select_stations(get_stations())
    ids = get_station_ids(stations)
    cachedir = _get_cache_dir()
    f = f or (cachedir / "store.h5")
    with pandas.HDFStore(f, mode="w", complevel=3) as store:
        for (id_, st, fi) in zip(ids, stations["BEGIN"], stations["END"]):
            for year in pandas.date_range(
                    pandas.Timestamp(st.year, 1, 1),
                    pandas.Timestamp(fi.year+1, 1, 1), freq="Y").year:
                df = get_station(year, id_)
                store.append("root", df)
