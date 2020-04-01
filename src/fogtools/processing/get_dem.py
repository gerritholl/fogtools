"""Get DEM from USGS
"""


import pathlib
import logging
import sys
import argparse

from .. import dem


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
    h = logging.StreamHandler(sys.stderr)
    h.setFormatter(logging.Formatter(
        "%(levelname)-8s %(name)s %(asctime)s "
        "%(module)s.%(funcName)s:%(lineno)s: %(message)s"))
    for m in ("fogtools", "typhon"):
        log = logging.getLogger(m)
        log.setLevel(logging.DEBUG)
        log.addHandler(h)
    dem.dl_usgs_dem_in_range(*p.latrange, *p.lonrange, p.outdir)
