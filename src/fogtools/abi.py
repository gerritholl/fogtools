"""Routines related to interacting with ABI

Routines related to interacting with ABI, such as downloading from AWS.
"""

import logging
import re

from sattools import io as stio
import s3fs
import pandas
import satpy.readers

logger = logging.getLogger(__name__)

nwcsaf_abi_channels = {2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14, 15, 16}
fogpy_abi_channels = {2, 3, 5, 7, 11, 14, 15}


def get_s3_uri(dt, tp="C"):
    """Get S3 URI for GOES ABI for day

    Construct the S3 URI that should contain GOES 16 ABI L1B data for the day
    and L1 type.  This construction is purely offline, i.e. no attempt is made
    to verify this path actually exists.

    Args:
        dt (pandas.Timestamp): Time for which to construct S3 URI.  Will be
            interpreted up to hourly resolution.
        tp (Optional[str]): What type of ABI to get, can be "C" (CONUS),
            "F" (full disk), "M1" or "M2" (mesoscale).

    Returns:
        str: URI with S3 scheme pointing to directory containing GOES ABI for
            that day for all channels.
    """
    return (f"s3://noaa-goes16/ABI-L1b-Rad{tp:s}/{dt.year:>04d}"
            f"/{dt.dayofyear:>03d}/{dt.hour:>02d}")


def s3_select_period(dt1, dt2, chans, tp="C"):
    """Generator to yield S3 URIs for date, channel, type

    Get S3 URIs pointing to individual files containing GOES 16 ABI L1B data
    for date, channel, and type.  Those are based on actual listings --- this
    function accesses the network and the URIs returned are reported to exist
    by the S3 server.

    Args:
        dt1 (pandas.Timestamp): First datetime for which to get files.
        dt2 (pandas.Timestamp): Final datetime for which to get files.
        chan (List[int]): ABI channel number(s)
        tp (Optional[Str]): What type of ABI to get: "C", "F", "M1", "M2".

    Yields:
        str, URIs pointing to ABI L1B data files on S3
    """
    fs = s3fs.S3FileSystem(anon=True)
    # loop through hours, because data files sorted per hour in AWS
    for dt in pandas.date_range(dt1.floor("H"), dt2.floor("H"), freq="H"):
        s3_uri = get_s3_uri(dt, tp=tp)
        res = satpy.readers.find_files_and_readers(
                base_dir=s3_uri,
                fs=fs,
                reader="abi_l1b",
                start_time=max(dt, dt1),
                end_time=min(dt+pandas.Timedelta(1, "hour"), dt2),
                missing_ok=True).get("abi_l1b", [])
        yield from (f for f in res if
                    any(f"C{chan:>02d}" in f for chan in chans))


def get_dl_dir(cd, t, chan):
    """Get the cached download destination directory for file

    Get the path for the destination directory for channel for GOES 16 ABI L1B
    is downloaded.  It does not check if the destination directory exists.

    Args:
        cd (pathlib.Path): Root path for cache directory
        t (pandas.Timestamp): Timestamp (down to the minute) to which the file
            corresponds
        chan (int): ABI channel number

    Returns:
        pathlib.Path object pointing to directory that should contain files
    """
    dd = cd / "abi" / t.strftime("%Y/%m/%d/%H") / f"C{chan:>01d}"
    return dd


def get_dl_dest(cd, t, chan, f):
    """Get the cached download destination directory for file

    Get the path for the destination directory for channel for GOES 16 ABI L1B
    is downloaded.

    Args:
        cd (pathlib.Path): Root path for cache directory
        t (pandas.Timestamp): Timestamp (down to the minute) to which the file
            corresponds
        chan (int): ABI channel number

    Returns:
        pathlib.Path object pointing to where the file will end up.
    """
    return get_dl_dir(cd, t, chan) / f.split("/")[-1]


def get_time_from_fn(fn):
    """Get starting time from ABI filename
    """
    m = re.search(r"_s[0-9]{14}_", fn)
    return pandas.to_datetime(m[0], format="_s%Y%j%H%M%S%f_")


def _get_chan_from_name(nm):
    return int(nm.split("/")[-1][19:21])


def download_abi_day(dt, chans=fogpy_abi_channels | nwcsaf_abi_channels,
                     tps="C"):
    """Download ABI for day

    Args:
        dt (Timestamp)
            Date for which to download

    Returns:
        List[pathlib.Path]
            Files downloaded or already present
    """

    return download_abi_period(
            dt.floor("D"), dt.ceil("D"), chans, tps=tps)


def download_abi_period(
        start, end, chans=fogpy_abi_channels | nwcsaf_abi_channels, tps="C"):
    """Download ABI for period if not already present

    Consider the period between start and end, and download any ABI data not
    already present in local cache.  Data will be downloaded to
    ``sattools.get_cache_dir() / fogtools``.

    Args:
        start (Timestamp)
            Starting time
        end (Timestamp)
            Ending time
        chans (array_like, optional)
            List of channels, defaults to those needed for NWCSAf and Fogpy
        tps (array_like, optional)
            String of types, defaults to "C" for "CONUS", can be "F" or "FC"

    Returns:
        List[pathlib.Path]
            Files downloaded or already present
    """

    fs = s3fs.S3FileSystem(anon=True)
    cd = stio.get_cache_dir(subdir="fogtools")
    L = []
    logger.info(f"Downloading ABI for {start:%Y-%m-%d %H:%M} -- "
                f"{end:%Y-%m-%d %H:%M}")
    # loop through hours, because data files sorted per hour in AWS
    for tp in tps:
        for f in s3_select_period(start, end, chans, tp=tp):
            chan = _get_chan_from_name(f)
            df = get_dl_dest(cd, get_time_from_fn(f), chan, f)
            if df.exists():
                logger.debug(f"Already exists: {df!s}")
            else:
                logger.info(f"Downloading {f!s} to {df!s}")
                df.parent.mkdir(exist_ok=True, parents=True)
                fs.get(f"s3://{f:s}", df)
            L.append(df)
    return L
