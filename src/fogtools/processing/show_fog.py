"""Display fog image for scene

Given NWCSAF and SEVIRI files, write file with fog image
"""

import pathlib
import argparse
from .. import vis
import fogpy.composites


def get_parser():
    parser = argparse.ArgumentParser(
            description=__doc__,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
            "--sat", action="store", type=pathlib.Path,
            nargs="+", required=True,
            help="List of satellite files")

    parser.add_argument(
            "--nwcsaf", action="store", type=pathlib.Path,
            required=True, nargs="+",
            help="List of NWCSAF files")

    parser.add_argument(
            "out",
            action="store", type=pathlib.Path,
            help="Where to store output.  If storing a single file, this "
                 "is interpreted as a filename.  If storing multiple files, "
                 "it will be interpreted as a directory that will be created "
                 "and that will contain all output files.  Storing multiple "
                 "files happens when passing -i or -d.  In this case, each "
                 "dataset will be stored as `dataset.tif` within the output "
                 "directory.")

    parser.add_argument(
            "-a", "--area", action="store", type=str,
            default="eurol",
            help="Area for which to generate image")

    parser.add_argument(
            "-i", "--store-intermediates", action="store_true",
            help="Also store intermediates, i.e. any values that Fogpy "
                 "calculates from the inputs before calculating the fog "
                 "mask.")

    parser.add_argument(
            "-d", "--store-dependencies", action="store_true",
            help="Also write all dependencies, that means all products used "
                 "directly by fogpy to generate the fog products")

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
    (im, sc) = vis.get_fog_blend_for_sat(
            p.mode,
            [str(f) for f in p.sat],
            [str(f) for f in p.nwcsaf],
            p.area,
            "overview")
    if p.store_intermediates or p.store_dependencies:
        p.out.mkdir(exist_ok=True, parents=True)
        im.save(str(p.out / "fog_blend.tif"))
    else:
        im.save(str(p.out))

    if p.store_dependencies:
        sc.save_datasets(filename=p.out / "{name:s}.tif",
                         datasets={d.name for d in sc.keys()})

    if p.store_intermediates:
        fogpy.composites.save_extras(sc, p.out / "intermediates.nc")
