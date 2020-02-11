"""For downloading ABI
"""

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
            "channels", action="store", type=int,
            nargs="+",
            help="Channels to download")

    parser.add_argument(
            "--type", action="store", type=str,
            choices=["C", "F", "M"],
            default="C",
            help="Download 'C'ONUS, 'F'ull disk, or 'M'esoscale")

    return parser


def dlabi(dt, chans, tp):
    abi.download_abi_day(dt, chans, tp)


def main():
    p = get_parser().parse_args()
    dlabi(p.date, p.channels, p.type)
