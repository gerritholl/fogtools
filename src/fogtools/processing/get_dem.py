"""Get DEM from USGS
"""


import pathlib
import sys
import argparse

from .. import dem
from .. import log


def get_parser():
    parser = argparse.ArgumentParser(
            description=__doc__,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
            "--latrange", action="store", type=int,
            nargs=2,
            default=[38, 49],
            help="Latitude range to download")

    parser.add_argument(
            "--lonrange", action="store", type=int,
            nargs=2,
            default=[-82, -66],
            help="Longitude range to download")

    parser.add_argument(
            "--outdir", action="store", type=pathlib.Path,
            default=pathlib.Path("."),
            help="Longitude range to download")

    return parser


def main():
    p = get_parser().parse_args()
    log.setup_main_handler()
    dem.dl_usgs_dem_in_range(*p.latrange, *p.lonrange, p.outdir)
