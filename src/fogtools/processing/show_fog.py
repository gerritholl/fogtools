"""Display fog image for scene.

Given satellite imagery files and cloud microphysics files, use fogpy to
generate a blended image of detected fog.  Satellite imagery can come from
either SEVIRI or ABI.  Cloud microphysics may come from NWCSAF or CMSAF.
For example:

    show-fog $(plotdir)/out.tif
            --seviri /path/to/seviri/files/*
            --nwcsaf /path/to/cmsaf/files/*
            -a germ
"""

import xarray
import pathlib
import argparse
from .. import vis
import fogpy.composites


def get_parser():
    parser = argparse.ArgumentParser(
            description=__doc__,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)

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

    sat = parser.add_mutually_exclusive_group(required=True)
    sat.add_argument(
            "--seviri", action="store", type=pathlib.Path,
            nargs="+",
            help="List of SEVIRI HRIT files")
    sat.add_argument(
            "--abi", action="store", type=pathlib.Path,
            nargs="+",
            help="List of ABI L1B NetCDF files")

    cloud = parser.add_mutually_exclusive_group(required=True)
    cloud.add_argument(
            "--nwcsaf", action="store", type=pathlib.Path,
            nargs="+",
            help="List of NWCSAF CMIC files")
    cloud.add_argument(
            "--cmsaf", action="store", type=pathlib.Path,
            nargs="+",
            help="List of CMSAF CLAAS-2 CPP files")

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

    return parser


def parse_cmdline():
    return get_parser().parse_args()


def main():
    from satpy.utils import debug_on
    debug_on()
    p = parse_cmdline()
    (im, sc) = vis.get_fog_blend_for_sat(
            "seviri_l1b_hrit" if p.seviri else "abi_l1b",
            [str(x) for x in (p.seviri or p.abi)],
            "nwcsaf-geo" if p.nwcsaf else "cmsaf-claas2_l2_nc",
            [str(x) for x in (p.nwcsaf or p.cmsaf)],
            p.area,
            "overview")
    if p.store_intermediates or p.store_dependencies:
        p.out.mkdir(exist_ok=True, parents=True)
        im.save(str(p.out / "fog_blend.tif"))
    else:
        im.save(str(p.out))

    if p.store_dependencies:
        sc.save_datasets(filename=str(p.out / "{name:s}.tif"),
                         datasets={d.name for d in sc.keys() if
                                   isinstance(sc[d], xarray.DataArray)})

    if p.store_intermediates:
        fogpy.composites.save_extras(sc, p.out / "intermediates.nc")
