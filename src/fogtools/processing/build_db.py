"""Build the fog database

Build the fog dabatase.  Currently functional for ABI only.
"""

import pathlib
import argparse

import pandas
from .. import db
from .. import log
from .. import isd


def get_parser():
    parser = argparse.ArgumentParser(
            description=__doc__,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
            "out",
            action="store", type=pathlib.Path,
            help="Where to store fog database.")

    parser.add_argument(
            "--date", action="store", type=pandas.Timestamp,
            help="Add these datetimes to fog database.  Any format "
                 "understood by pandas.Timestamp is understood.")

    parser.add_argument(
            "--top-n", action="store", type=int,
            help="Process the top n cases automatically.  If "
                 "given, date will be ignored.")

    parser.add_argument(
            "--max-vis", action="store", type=float,
            help="Max. vis to consider fog (when searching with top-n)",
            default=1000)

    return parser


def parse_cmdline():
    return get_parser().parse_args()


def main():
    p = parse_cmdline()
    log.setup_main_handler()
    fogdb = db.FogDB()
    if p.top_n is not None:
        top = isd.top_n("H", 1000, p.top_n)
        for dt in top.index:
            fogdb.extend(dt, onerror="log")
    else:
        fogdb.extend(p.date)
    fogdb.store(p.out)
