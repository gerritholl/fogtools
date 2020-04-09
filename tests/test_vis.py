"""Test visualisation routines
"""

from unittest.mock import patch, sentinel


def test_fog_blend(xrda):
    from fogtools.vis import blend_fog
    mm = {}
    mm["overview"] = xrda[0]
    mm["fls_day"] = xrda[1]
    blend_fog(mm)
    # later, I should do some proper tests of the values here


@patch("fogpy.composites.Scene")
@patch("satpy.Scene")
@patch("fogtools.vis.blend_fog", autospec=True)
def test_get_fog_blend_for_sat(fvb, sS, fcS, xrda):
    from fogtools.vis import get_fog_blend_for_sat
    sS.return_value["overview"] = xrda[0]
    sS.return_value["fls_day"] = xrda[1]
    fvb.return_value = sentinel.tofu
    rv = get_fog_blend_for_sat(
            "seviri_l1b_hrit",
            ["a", "b", "c"],
            ["d", "e", "f"],
            "germ",
            "overview")
    fvb.assert_called_once_with(
            sS.return_value.resample.return_value, "overview")
    assert rv[0] is sentinel.tofu
    assert rv[1] is sS.return_value.resample.return_value
