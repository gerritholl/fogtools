"""Display fog image for scene

Given NWCSAF and SEVIRI files, write file with fog image
"""

import argparse
from .. import vis


def get_parser():
    parser = argparse.ArgumentParser(
            description=__doc__,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
            "--seviri", action="store", type=str,
            nargs="+", required=True,
            help="List of SEVIRI files")

    parser.add_argument(
            "--nwcsaf", action="store", type=str,
            required=True, nargs="+",
            help="List of NWCSAF files")

    parser.add_argument(
            "outfile",
            action="store", type=str,
            help="Output file")

    parser.add_argument(
            "-a", "--area", action="store", type=str,
            default="eurol",
            help="Area for which to generate image")

    return parser


def parse_cmdline():
    return get_parser().parse_args()


def main():
    from satpy.utils import debug_on
    debug_on()
    p = parse_cmdline()
    im = vis.get_fog_blend_from_seviri_nwcsaf(
            p.seviri,
            p.nwcsaf,
            p.area,
            "overview")
    im.save(p.outfile)
