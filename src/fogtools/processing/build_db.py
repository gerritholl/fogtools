"""Build the fog database

Build the fog dabatase.  Currently functional for ABI only.
"""

import pathlib
import argparse
import sys

import pandas
from .. import db
from .. import log


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

    return parser


def parse_cmdline():
    return get_parser().parse_args()


def main():
    p = parse_cmdline()
    log.setup_main_handler()
    fogdb = db.FogDB()
    fogdb.extend(p.date)
    fogdb.store(p.out)
