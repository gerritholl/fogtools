"""For downloading ABI
"""

import sys
import argparse
import pandas

from .. import abi
from .. import log


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
    abi.download_abi_period(
            dt.floor("D"),
            (dt + pandas.Timedelta(1, "day")).floor("D"),
            chans, tp)


def main():
    p = get_parser().parse_args()
    log.setup_main_handler()
    p = get_parser().parse_args()
    dlabi(p.date, p.channels, p.types)
