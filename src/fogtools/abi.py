"""Routines related to interacting with ABI

Routines related to interacting with ABI, such as downloading from AWS.
"""

import logging

from . import io as ftio
import s3fs
import pandas

logger = logging.getLogger(__name__)


def get_s3_uri(dt, tp="C"):
    """Get S3 URI for GOES ABI for day
    """
    return (f"s3://noaa-goes16/ABI-L1b-Rad{tp:s}/{dt.year:>04d}"
            f"/{dt.dayofyear:>03d}/{dt.hour:>02d}")


def s3_select(dt, chan, tp="C"):
    s3_uri = get_s3_uri(dt, tp=tp)
    fs = s3fs.S3FileSystem(anon=True)
    yield from fs.glob(s3_uri + f"/*C{chan:>02d}*")


def download_abi_day(dt, chans, tp="C"):
    """Download ABI for day

    Args:
        dt (Timestamp)
            Date for which to download
    """

    fs = s3fs.S3FileSystem(anon=True)
    cd = ftio._get_cache_dir()
    for t in pandas.date_range(dt.floor("D"), periods=24, freq="1H"):
        for chan in chans:
            for f in s3_select(t, chan):
                dd = cd / "abi" / t.strftime("%Y/%m/%d/%H/%M") / f"{chan:>02d}"
                df = dd / f.split("/")[-1]
                if df.exists():
                    logger.debug(f"Already exists: {df!s}")
                else:
                    dd.mkdir(exist_ok=True, parents=True)
                    logger.debug(f"Getting {f!s}")
                    fs.get(f"s3://{f:s}", df)
