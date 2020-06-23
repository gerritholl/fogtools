"""Utilities related to logging."""

import sys
import logging
import datetime
import pathlib
import pandas

import appdirs

logger = logging.getLogger(__name__)


def setup_main_handler(
        mods=("fogtools", "typhon", "fogpy", "sattools", "fcitools"),
        level=logging.DEBUG):
    """Setup the main stderr StreamHandler.

    Args:
        mods (Collection[str]): Modules to log for.
        level (logging level): At what level to log to stderr.
    """
    handler = logging.StreamHandler(sys.stderr)
    formatter = logging.Formatter(
        "{levelname:<8s} {name:s} {asctime:s} "
        "{module:s}.{funcName:s}:{lineno:d}: {message:s}",
        style="{")
    handler.setFormatter(formatter)
    for m in mods:
        log = logging.getLogger(m)
        log.setLevel(level)
        log.addHandler(handler)


# this class is based on
# https://docs.python.org/3.10/howto/logging-cookbook.html#using-a-context-manager-for-selective-logging  # noqa: E501
class LoggingContext:
    def __init__(self, logger, level=None, handler=None, close=True):
        self.logger = logger
        self.level = level
        self.handler = handler
        self.close = close

    def __enter__(self):
        if self.level is not None:
            self.old_level = self.logger.level
            self.logger.setLevel(self.level)
        if self.handler:
            self.logger.addHandler(self.handler)

    def __exit__(self, et, ev, tb):
        if self.level is not None:
            self.logger.setLevel(self.old_level)
        if self.handler:
            self.logger.removeHandler(self.handler)
        if self.handler and self.close:
            self.handler.close()
        # implicit return of None => don't swallow exceptions


class LogToTimeFile(LoggingContext):
    """Logging context to log to a file with a certain time.

    This is intended to be used when files are processed, and a corresponding
    logfile shall be written.
    """

    def __init__(self, time):
        logger = logging.getLogger()  # root logger
        logdir = pathlib.Path(appdirs.user_log_dir("fogtools"))
        now = datetime.datetime.now()
        logfile = logdir / f"{now:%Y-%m-%d}" / f"fogdb-{time:%Y%m%d-%H%M}.log"
        logfile.parent.mkdir(parents=True, exist_ok=True)
        self.logfile = logfile
        handler = logging.FileHandler(logfile, encoding="utf-8")
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
                "{asctime:s} {name:s} {levelname:s} "
                "{processName:s}-{process:d} {threadName:s}-{thread:d} "
                "{pathname:s}:{lineno:d} {funcName:s}: {message:s}",
                style="{")
        handler.setFormatter(formatter)
        super().__init__(logger, level=logging.DEBUG, handler=handler,
                         close=True)

    def __enter__(self):
        super().__enter__()
        logger.info(f"Opening logfile at {self.logfile!s}")
        return self

    def __exit__(self, et, ev, tb):
        logger.info(f"Closing logfile at {self.logfile!s}")
        super().__exit__(et, ev, tb)


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
