"""Utilities related to logging."""

import logging
import datetime
import pathlib
import pandas

import appdirs
import sattools.log

logger = logging.getLogger(__name__)


class LogToTimeFile(sattools.log.LogToTimeFile):
    """Logging context to log to a file with a certain time.

    This is intended to be used when files are processed, and a corresponding
    logfile shall be written.
    """

    def __init__(self, time):
        logdir = pathlib.Path(appdirs.user_log_dir("fogtools"))
        now = datetime.datetime.now()
        logfile = logdir / f"{now:%Y-%m-%d}" / f"fogdb-{time:%Y%m%d-%H%M}.log"
        logfile.parent.mkdir(parents=True, exist_ok=True)
        super().__init__(logfile)


def collect_filterstats_from_log(fp):
    """Collect statistics on fogpy filters from logfiles.

    From the content of logfiles, collect statistics on how many pixels have
    been removed from the various logfiles.

    Args:
        fp (file): Stream with log.  NB: Will be consumed!
    """
    D = {}
    while (line := fp.readline()):  # noqa: E203, E231
        if "Filter results for" in line:
            filt = line.split()[-1]
            fp.readline()  # empty
            fp.readline()  # "Filtering"
            fp.readline()  # Array size
            fp.readline()  # cumulative masked
            fp.readline()  # previously masked
            filtered_line = fp.readline()  # new filtered
            if "New filtered" not in filtered_line:
                raise ValueError("Expected 'filtered' six lines after "
                                 f"'filter results', got {filtered_line:s}")
            count = int(filtered_line.split()[-1])
            if filt in D:
                raise ValueError(f"Found results for {filt:s} multiple times!")
            D[filt] = count
    return D


def collect_filterstats_from_logfiles(*args):
    """Collect statistics from a series of logfiles.

    From the content of many logfiles, collect statistics per logfile on how
    many pixels have been removed.

    Args:
        *args (List[pathlib.Path]): Filenames
    """
    all_stats = {}
    for path in args:
        with path.open("r") as fp:
            all_stats[path.name] = collect_filterstats_from_log(fp)
    return pandas.DataFrame(all_stats.values(), index=all_stats.keys())
