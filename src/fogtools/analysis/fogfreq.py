"""Plot frequency at which fog occurs

Using the database created with collect-isd, visualise how common fog is.
"""

import argparse
import dask.distributed
from .. import plot


def get_parser():
    parser = argparse.ArgumentParser(
            description=__doc__,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
            "--out", action="store", type=str,
            help="Where to write plot file")

    return parser


def main():
    p = get_parser().parse_args()
    c = dask.distributed.Client(n_workers=8)
    with dask.distributed.performance_report(filename="/tmp/dask-report.html"):
        plot.plot_fog_frequency()
