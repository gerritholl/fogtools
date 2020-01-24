"""Download data from Integrated Surface Dataset (ISD)

Download hourly synoptic value measurements from the Integrated Surface Dataset
(ISD) as available at Amazon Web Services (AWS) at
https://registry.opendata.aws/noaa-isd/ .  Collect all the relevant
measurements, which by default means New England between 2017 and 2020, and put
them in a local database file in the Apache Parquet format.
"""

import argparse
from .. import isd


def get_parser():
    parser = argparse.ArgumentParser(
            description=__doc__,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
            "--out", action="store", type=str,
            help=("Where to store parquet file.  If not given, use "
                  "fogtools/store.parquet in user cache directory."))

    parser.add_argument(
            "--start", action="store", type=str, default="2017-01-01",
            help="Starting date for measurements."),

    parser.add_argument(
            "--end", action="store", type=str, default="2020-12-31",
            help="Ending date for measurements.")

    return parser


def mkisd():
    isd.create_db()


def main():
    p = get_parser().parse_args()
    mkisd(p.out, p.start, p.end)
