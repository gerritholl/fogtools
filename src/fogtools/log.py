"""Utilities related to logging."""

import sys
import logging
import datetime
import pathlib

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


# this class is based on https://docs.python.org/3.10/howto/logging-cookbook.html#using-a-context-manager-for-selective-logging
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
        formatter = logging.Formatter(
                "{asctime:s} {levelname:s} {processName:s}-{process:d} "
                "{threadName:s}-{thread:d} "
                "{pathname:s}:{lineno:d} {funcName:s}: {message:s}",
            style="{")
        handler.setFormatter(formatter)
        super().__init__(logger, level=logging.DEBUG, handler=handler,
                         close=True)

    def __enter__(self):
        super().__enter__()
        logger.info(f"Opening logfile at {self.logfile!s}")

    def __exit__(self, et, ev, tb):
        logger.info(f"Closing logfile at {self.logfile!s}")
        super().__exit__(et, ev, tb)
