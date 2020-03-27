import pathlib
import pytest
import unittest.mock


@pytest.fixture
def b():
    return ("https://prd-tnm.s3.amazonaws.com/StagedProducts"
            "/Elevation/1/TIFF/")


def test_get_loc_lab():
    from fogtools.dem import get_loc_lab
    assert get_loc_lab(1, 2) == "n01e002"
    assert get_loc_lab(-1, -2) == "s01w002"
    assert get_loc_lab(-50, 100) == "s50e100"


def test_get_src_uri_dir(b):
    from fogtools.dem import get_src_uri_dir
    assert get_src_uri_dir(29, -107) == b + "n29w107/"
    assert get_src_uri_dir(-1, 1) == b + "s01e001/"


def test_get_src_uri_filename():
    from fogtools.dem import get_src_uri_filename
    assert get_src_uri_filename(29, -107, "tif") == "USGS_1_n29w107.tif"
    assert get_src_uri_filename(12, 10, "xml") == "USGS_1_n12e010.xml"
    assert get_src_uri_filename(-12, 10, "jpg") == "USGS_1_s12e010.jpg"
    assert get_src_uri_filename(-1, 1, "gpkg") == "s01e001.gpkg"
    with pytest.raises(ValueError):
        get_src_uri_filename(0, 0, "invalid")


def test_get_src_uris(b):
    from fogtools.dem import get_src_uris
    assert get_src_uris(42, 42) == [
        b + "n42e042/USGS_1_n42e042.tif",
        b + "n42e042/USGS_1_n42e042.jpg",
        b + "n42e042/USGS_1_n42e042.xml",
        b + "n42e042/n42e042.gpkg"]


def test_get_out_dir():
    from fogtools.dem import get_out_dir
    assert get_out_dir(3, 4, pathlib.Path("/fake")) == pathlib.Path(
            "/fake/n03e004")


@unittest.mock.patch("urllib.request.urlretrieve", autospec=True)
def test_dl_usgs_dem(uru, b):
    from fogtools.dem import dl_usgs_dem
    dl_usgs_dem(10, 20, pathlib.Path("/fake"))
    assert uru.call_count == 4
    uru.assert_has_calls([
        unittest.mock.call(
            b + "n10e020/USGS_1_n10e020.tif",
            pathlib.Path("/fake/USGS_1_n10e020.tif")),
        unittest.mock.call(
            b + "n10e020/n10e020.gpkg",
            pathlib.Path("/fake/n10e020.gpkg"))],
        any_order=True)


@unittest.mock.patch("urllib.request.urlretrieve", autospec=True)
def test_dl_usgs_dem_in_range(uru, b):
    from fogtools.dem import dl_usgs_dem_in_range
    dl_usgs_dem_in_range(-10, 10, -10, 10, pathlib.Path("/fake"))
    assert uru.call_count == 20*20*4
    uru.assert_has_calls([
        unittest.mock.call(
            b + "n05e005/n05e005.gpkg",
            pathlib.Path("/fake/n05e005/n05e005.gpkg"))])
