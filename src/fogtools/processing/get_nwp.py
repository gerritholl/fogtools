"""Get NWP data for NWCSAF
"""

import os
import sys
import argparse
import pandas

from .. import sky
from .. import log


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
    log.setup_main_handler()
    getnwp(p.date)
