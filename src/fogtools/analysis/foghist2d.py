"""Plot joint distribution fog and delta-T

Plot a joint distribution between the frequency of fog occurence
and the difference between dew point and temperature.
"""

import argparse
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
    get_parser().parse_args()
    plot.Visualiser().plot_fog_dt_hist()
