"""Routines related to visualisation
"""

import satpy
import satpy.writers
import trollimage.xrimage
import trollimage.colormap

import xarray


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


def get_fog_blend_from_seviri_nwcsaf(
        fl_sev,
        fl_nwcsaf,
        area="eurol",
        other="overview"):
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

    Returns:

        XRImage with fog blended on top of op background.
    """

    sc = satpy.Scene(
            filenames={"seviri_l1b_hrit": fl_sev,
                       "nwcsaf-geo": fl_nwcsaf})
    sc.load(["cmic_reff", "IR_108", "IR_087", "cmic_cot", "IR_016", "VIS006",
             "IR_120", "VIS008", "cmic_lwp", "IR_039", "overview"])
    ls = sc.resample(area)
    ls.load(["fls_day", "fls_day_extra"], unload=False)

    return blend_fog(ls, other)
