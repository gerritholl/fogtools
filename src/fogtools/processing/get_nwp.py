"""Get NWP data for NWCSAF
"""

import os
import logging
import sys
import argparse
import pandas
from .. import sky


def get_parser():
    parser = argparse.ArgumentParser(
            description=__doc__,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
            "date", action="store", type=pandas.Period,
            help="Date to download, ISO 8601 format")

    return parser


def getnwp(dt):
    sky.verify_period(dt)
    safnwc = os.getenv("SAFNWC")
    if not safnwc:
        sys.exit("Environment variable SAFNWC not set, exiting")
    sky.get_and_send(safnwc, dt)


def main():
    p = get_parser().parse_args()
    h = logging.StreamHandler(sys.stderr)
    h.setFormatter(logging.Formatter(
        "%(levelname)-8s %(name)s %(asctime)s "
        "%(module)s.%(funcName)s:%(lineno)s: %(message)s"))
    for m in ("fogtools", "typhon"):
        log = logging.getLogger(m)
        log.setLevel(logging.DEBUG)
        log.addHandler(h)
    getnwp(p.date)
