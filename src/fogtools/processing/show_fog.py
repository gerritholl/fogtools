"""Display fog image for scene

Given NWCSAF and SEVIRI files, write file with fog image
"""

import argparse
from .. import vis
import fogpy.composites


def get_parser():
    parser = argparse.ArgumentParser(
            description=__doc__,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
            "--sat", action="store", type=str,
            nargs="+", required=True,
            help="List of satellite files")

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

    parser.add_argument(
            "-e", "--extra", action="store", type=str,
            help="File to which to store extra information")

    parser.add_argument(
            "-m", "--mode", type=str, action="store",
            default="seviri_l1b_hrit", choices=["abi_l1b", "seviri_l1b_hrit"],
            help="Which satellite to use")

    return parser


def parse_cmdline():
    return get_parser().parse_args()


def main():
    from satpy.utils import debug_on
    debug_on()
    p = parse_cmdline()
    rv = vis.get_fog_blend_for_sat(
            p.mode,
            p.sat,
            p.nwcsaf,
            p.area,
            "overview",
            return_extra=p.extra is not None)
    if p.extra is not None:
        (im, ex) = rv
        fogpy.composites.save_extras(ex, p.extra)
    else:
        im = rv
    im.save(p.outfile)
