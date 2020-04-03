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
    Ap = (1-A).where(1-A == 0, 0.5)
    im = trollimage.xrimage.XRImage(Ap)
    im.stretch()
    im.colorize(fogcol)
    RGBA = xarray.concat([im.data, Ap], dim="bands")
    blend = ov.blend(trollimage.xrimage.XRImage(RGBA))
    return blend


def get_fog_blend_for_sat(sensorreader, fl_sat, fl_nwcsaf, area, other,
                          return_extra):
    sc = satpy.Scene(
        filenames={sensorreader: fl_sat,
                   "nwcsaf-geo": fl_nwcsaf})

    D = {"seviri_l1b_hrit": ["IR_108", "IR_087", "IR_016", "VIS006",
                             "IR_120", "VIS008", "IR_039"],
         "abi_l1b": ["C02", "C03", "C05", "C07", "C11", "C14", "C15"]}

    sensor = sensorreader.split("_")[0]
    sattools.ptc.add_all_pkg_comps_mods(sc, ["satpy", "fogpy"],
                                        sensors=[sensor])
    areas = sattools.ptc.get_all_areas(["satpy", "fcitools", "fogtools"])
    sc.load(["cmic_reff", "cmic_lwp", "cmic_cot", "overview"] + D[sensorreader])
    ls = sc.resample(areas[area])
    ls.load(["fls_day", "fls_day_extra"], unload=False)

    blend = blend_fog(ls, other)
    if return_extra:
        return (blend, ls)
    else:
        return blend


def get_fog_blend_from_seviri_nwcsaf(
        fl_sev,
        fl_nwcsaf,
        area="eurol",
        other="overview",
        return_extra=False):
    """Create a blended fog image with fogpy from NWCSAF and SEVIRI

    Get an image where fog is calculated for the scene and where this is then
    blended on top of a background composite, by default the seviri "overview"
    composite.

    Args:

        fl_sev (List):
            List of corresponding SEVIRI files.  These will be read with
            the satpy ``seviri_l1b_hrit`` reader.
        fl_nwcsaf (List):
            List of NWCSAF files.  These will be road with the satpy
            ``nwcsaf-geo`` reader.
        area (Optional[str]):
            Area on which to respample.  Defaults to "eurol".
        other (Optional[str]):
            Background composite.  Defaults to "overview".
        return_extra (Optional[bool]):
            Also return scene with extra information from the
            fogpy `"fls_day_extra"` "composite".  Defaults to false.

    Returns:

        XRImage with fog blended on top of op background.
        If ``return_extras`` is True, also return dataset with extras.
    """

    return get_fog_blend_for_sat("seviri_l1b_hrit", fl_sev, fl_nwcsaf, area,
                                 other, return_extra=return_extra)


def get_fog_blend_from_abi_nwcsaf(
        fl_abi,
        fl_nwcsaf,
        area="new-england-1000",
        other="overview",
        return_extra=False):
    return get_fog_blend_for_sat("abi_l1b", fl_abi, fl_nwcsaf, area, other,
                                 return_extra=return_extra)
