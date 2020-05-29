"""Build the fog database

Build the fog dabatase.  Currently functional for ABI only.
"""

import logging
import pathlib
import argparse
import sys

import pandas
from .. import db


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
    h = logging.StreamHandler(sys.stderr)
    h.setFormatter(logging.Formatter(
        "%(levelname)-8s %(name)s %(asctime)s "
        "%(module)s.%(funcName)s:%(lineno)s: %(message)s"))
    for m in ("fogtools", "typhon", "fogpy", "sattools", "fcitools"):
        log = logging.getLogger(m)
        log.setLevel(logging.DEBUG)
        log.addHandler(h)
    fogdb = db.FogDB()
    fogdb.extend(p.date)
    fogdb.store(p.out)
