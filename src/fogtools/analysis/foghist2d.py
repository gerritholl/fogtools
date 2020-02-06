"""Plot joint distribution fog and delta-T

Plot a joint distribution between the frequency of fog occurence
and the difference between dew point and temperature.
"""

import logging
import sys
import argparse
from .. import plot

LOG = logging.getLogger(__name__)

def get_parser():
    parser = argparse.ArgumentParser(
            description=__doc__,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
            "--out", action="store", type=str,
            help="Where to write plot file")

    return parser


def main():
    # from satpy.utils import debug_on
    p = get_parser().parse_args()
    from .. import isd
    h = logging.StreamHandler(sys.stderr)
    h.setFormatter(logging.Formatter(
        "%(levelname)-8s %(name)s %(asctime)s "
        "%(module)s.%(funcName)s:%(lineno)s: %(message)s"))
    for m in ("fogtools", "typhon"):
        l = logging.getLogger(m)
        l.setLevel(logging.DEBUG)
        l.addHandler(h)
    plot.Visualiser().plot_fog_dt_hist(p.out)
