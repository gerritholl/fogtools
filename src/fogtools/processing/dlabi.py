"""For downloading ABI
"""

import logging
import sys
import argparse
import pandas
from .. import abi


def get_parser():
    parser = argparse.ArgumentParser(
            description=__doc__,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
            "date", action="store", type=pandas.Timestamp,
            help="Date to download")

    parser.add_argument(
            "--channels", action="store", type=int,
            nargs="+",
            default=abi.fogpy_abi_channels | abi.nwcsaf_abi_channels,
            help="Channels to download")

    parser.add_argument(
            "--types", action="store", type=str,
            nargs="+",
            choices=["C", "F", "M"],
            default=["C"],
            help="Download 'C'ONUS, 'F'ull disk, or 'M'esoscale")

    return parser


def dlabi(dt, chans, tp):
    abi.download_abi_day(dt, chans, tp)


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
    p = get_parser().parse_args()
    dlabi(p.date, p.channels, p.types)
