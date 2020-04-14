"""Routines related to interacting with ABI

Routines related to interacting with ABI, such as downloading from AWS.
"""

import itertools
import logging
import re

from sattools import io as stio
import s3fs
import pandas

logger = logging.getLogger(__name__)

nwcsaf_abi_channels = {2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14, 15, 16}
fogpy_abi_channels = {2, 3, 5, 7, 11, 14, 15}


def get_s3_uri(dt, tp="C"):
    """Get S3 URI for GOES ABI for day

    Construct the S3 URI that should contain GOES 16 ABI L1B data for the day and
    L1 type.  This construction is purely offline, i.e. no attempt is made to
    verify this path actually exists.

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


def s3_select(dt, chan, tp="C"):
    """Generator to yield S3 URIs for date, channel, type

    Get S3 URIs pointing to individual files containing GOES 16 ABI L1B data
    for date, channel, and type.  Those are based on actual listings --- this
    function accesses the network and the URIs returned are reported to exist
    by the S3 server.

    Args:
        dt (pandas.Timestamp): Datetime for which to get files, down to the
            hour.
        chan (int): ABI channel number
        tp (Optional[Str]): What type of ABI to get: "C", "F", "M1", "M2".

    Yields:
        str, URIs pointing to ABI L1B data files on S3
    """
    s3_uri = get_s3_uri(dt, tp=tp)
    fs = s3fs.S3FileSystem(anon=True)
    yield from fs.glob(s3_uri + f"/*C{chan:>02d}*")


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
    dd = cd / "abi" / t.strftime("%Y/%m/%d/%H/%M") / f"{chan:>01d}"
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


def download_abi_day(dt, chans=fogpy_abi_channels | nwcsaf_abi_channels,
                     tps="C"):
    """Download ABI for day

    Args:
        dt (Timestamp)
            Date for which to download
    """

    fs = s3fs.S3FileSystem(anon=True)
    cd = stio.get_cache_dir(subdir="fogtools")
    # loop through hours, because data files sorted per hour in AWS
    for (t, chan, tp) in itertools.product(
            pandas.date_range(dt.floor("D"), periods=24, freq="1H"),
            chans,
            tps):
        for f in s3_select(t, chan, tp=tp):
            df = get_dl_dest(cd, get_time_from_fn(f), chan, f)
            if df.exists():
                logger.debug(f"Already exists: {df!s}")
            else:
                logger.info(f"Downloading {f!s}")
                df.parent.mkdir(exist_ok=True, parents=True)
                fs.get(f"s3://{f:s}", df)
