"""Routines related to visualisation
"""

import satpy
import satpy.writers
import trollimage.xrimage
import trollimage.colormap

import xarray
import sattools.ptc


def blend_fog(sc, other="overview"):
    """Blend fog onto composite

    For a scene with readily loaded composite, blend a fog image onto it.

    Returns:

        XRImage with fog blended on top of op background.
    """
    fogcol = trollimage.colormap.Colormap(
            (0.0, (0.0, 0.0, 0.8)),
            (1.0, (250 / 255, 200 / 255, 40 / 255)))
    ov = satpy.writers.get_enhanced_image(sc[other]).convert("RGBA")
    A = sc["fls_day"].sel(bands="A")
    Ap = A.where(A == 0, 0.5)
    im = trollimage.xrimage.XRImage(Ap)
    im.stretch()
    im.colorize(fogcol)
    RGBA = xarray.concat([im.data, Ap], dim="bands")
    blend = ov.blend(trollimage.xrimage.XRImage(RGBA))
    return blend


def get_fog_blend_for_sat(sensor_reader, sensor_files,
                          cloud_reader, cloud_files,
                          area, blend_background):
    """Get daytime fog blend for sensor

    Get a daytime fog blend.

    Args:
        sensor_reader (str): For which sensor/reader to derive the fog product.
            Must be a sensor/reader supported by fogpy.  Currently those are
            "seviri_l1b_hrit" or "abi_l1b".
        sensor_files (List[str]): List of filenames corresponding to satellite
            data.
        cloud_reader (str): Reader providing the cloud products.  Can be
            "nwcsaf-geo" or "cmsaf-claas2_l2_nc".
        cloud_files (List[str]): List of filenames corresponding to cloud
            microphysics data.
        area (str): Area for which to calculate fog.  Must be an AreaDefinition
            defined in satpy (or PPP_CONFIG_DIR), fcitools, or fogtools.
        blend_background (str): Satpy composite to be used as the background
            onto which the fog mask will be blended using :func:`blend_fog`.

    Returns:
        XRImage: RGB image with the fog mask blended onto the background
            composite

        Scene: Scene object reprojected onto area, containing the composite and
            all its dependencies.

    """
    sc = satpy.Scene(
        filenames={sensor_reader: sensor_files,
                   cloud_reader: cloud_files})

    chans = {"seviri_l1b_hrit": ["IR_108", "IR_087", "IR_016", "VIS006",
                                 "IR_120", "VIS008", "IR_039"],
             "abi_l1b": ["C02", "C03", "C05", "C07", "C11", "C14", "C15"]}
    cmic = {"nwcsaf-geo": ["cmic_reff", "cmic_lwp", "cmic_cot"],
            "cmsaf-claas2_l2_nc": ["reff", "cwp", "cot"]}

    sensor = sensor_reader.split("_")[0]
    sattools.ptc.add_all_pkg_comps_mods(sc, ["satpy", "fogpy"],
                                        sensors=[sensor])
    areas = {}
    for pkg in ["satpy", "fcitools", "fogtools"]:
        try:
            areas.update(sattools.ptc.get_all_areas([pkg]))
        except ModuleNotFoundError:
            pass
    sc.load(chans[sensor_reader] + cmic[cloud_reader] + ["overview"],
            unload=False)
    ls = sc.resample(areas[area], unload=False)
    ls.load(["fls_day", "fls_day_extra"], unload=False)

    blend = blend_fog(ls, blend_background)
    return (blend, ls)
