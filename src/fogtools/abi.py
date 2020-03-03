"""Routines related to interacting with ABI

Routines related to interacting with ABI, such as downloading from AWS.
"""

import itertools
import logging
import re

from . import io as ftio
import s3fs
import pandas

logger = logging.getLogger(__name__)

# NWCSAF gebruikt 2, 6, 7, 11, 14
nwcsaf_abi_channels = {2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14, 15, 16}
# fogpy gebruikt 2, 3, 5, 7, 11, 14, 15
fogpy_abi_channels = {2, 3, 5, 7, 11, 14, 15}


def get_s3_uri(dt, tp="C"):
    """Get S3 URI for GOES ABI for day
    """
    return (f"s3://noaa-goes16/ABI-L1b-Rad{tp:s}/{dt.year:>04d}"
            f"/{dt.dayofyear:>03d}/{dt.hour:>02d}")


def s3_select(dt, chan, tp="C"):
    s3_uri = get_s3_uri(dt, tp=tp)
    fs = s3fs.S3FileSystem(anon=True)
    yield from fs.glob(s3_uri + f"/*C{chan:>02d}*")


def get_dl_dest(cd, t, chan, f):
    dd = cd / "abi" / t.strftime("%Y/%m/%d/%H/%M") / f"{chan:>01d}"
    df = dd / f.split("/")[-1]
    return df


def get_time_from_fn(fn):
    """Get starting time from ABI filename
    """
    m = re.search(r"_s[0-9]{14}_", fn)
    return pandas.to_datetime(m[0], format="_s%Y%j%H%M%S%f_")


def download_abi_day(dt, chans=fogpy_abi_channels|nwcsaf_abi_channels,
                     tps="C"):
    """Download ABI for day

    Args:
        dt (Timestamp)
            Date for which to download
    """

    fs = s3fs.S3FileSystem(anon=True)
    cd = ftio.get_cache_dir()
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
