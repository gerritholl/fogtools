"""Test visualisation routines
"""

import numpy
import xarray
import pytest
from unittest.mock import patch, sentinel


@pytest.fixture
def xrda():
    overview = xarray.DataArray(
            numpy.linspace(0, 100, 3*100*100).reshape(3, 100, 100),
            dims=("bands", "y", "x"),
            coords={"bands": ["R", "G", "B"]})
    fls_day = xarray.DataArray(
            numpy.linspace(0, 100, 4*100*100).reshape(4, 100, 100),
            dims=("bands", "y", "x"),
            coords={"bands": ["R", "G", "B", "A"]})
    return (overview, fls_day)


def test_fog_blend(xrda):
    from fogtools.vis import blend_fog
    mm = {}
    mm["overview"] = xrda[0]
    mm["fls_day"] = xrda[1]
    blend_fog(mm)
    # later, I should do some proper tests of the values here


@patch("satpy.Scene", autospec=True)
@patch("fogtools.vis.blend_fog", autospec=True)
def test_get_fog_blend_from_seviri(fvb, sS, xrda):
    from fogtools.vis import get_fog_blend_from_seviri_nwcsaf
    sS.return_value["overview"] = xrda[0]
    sS.return_value["fls_day"] = xrda[1]
    fvb.return_value = sentinel.tofu
    rv = get_fog_blend_from_seviri_nwcsaf(
            ["a", "b", "c"],
            ["d", "e", "f"])
    fvb.assert_called_once()
    assert rv is sentinel.tofu
    rv2 = get_fog_blend_from_seviri_nwcsaf(
            ["a", "b", "c"],
            ["d", "e", "f"],
            return_extra=True)
    assert len(rv2) == 2
    assert rv2[0] is sentinel.tofu
    assert rv2[1] is sS.return_value.resample.return_value
