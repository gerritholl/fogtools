import pathlib
import subprocess
import pytest
from unittest import mock
import pandas
import lxml.etree


@pytest.fixture
def rb(tmp_path):
    from fogtools.sky import RequestBuilder
    t = pandas.Timestamp("20200224120000")
    return RequestBuilder(tmp_path, t)


def test_icon_filename(tmp_path):
    from fogtools.sky import make_icon_nwcsaf_filename
    t = pandas.Timestamp("20200224120000")
    fs = 3
    fn = make_icon_nwcsaf_filename(tmp_path, t, fs)
    assert fn == (tmp_path / "import" / "NWP_data" /
                  "S_NWC_NWP_2020-02-24T12:00:00Z_003.grib")


def test_refdate(rb):
    t = rb.refdate()
    assert (lxml.etree.tostring(t) ==
            b'<sky:referenceDate xmlns:sky="http://dwd.de/sky">'
            b'<sky:value>20200224120000</sky:value></sky:referenceDate>')


def test_step(rb):
    t = rb.step(0)
    assert (lxml.etree.tostring(t) ==
            b'<sky:field xmlns:sky="http://dwd.de/sky" name="STEP">'
            b'<sky:value>00</sky:value></sky:field>')
    t = rb.step(5)
    assert (lxml.etree.tostring(t) ==
            b'<sky:field xmlns:sky="http://dwd.de/sky" name="STEP">'
            b'<sky:value>05</sky:value></sky:field>')


def test_sort_order(rb):
    t = rb.sort_order()
    assert (lxml.etree.tostring(t) ==
            b'<sky:sort xmlns:sky="http://dwd.de/sky"><sky:order '
            b'name="FIRST_LEVEL"/><sky:order name="PARAMETER_SHORTNAME"/>'
            b'</sky:sort>')


def test_get_result(rb):
    t = rb.result()
    assert (lxml.etree.tostring(t) ==
            b'<sky:result xmlns:sky="http://dwd.de/sky"><sky:binary/>'
            b'<sky:info level="countXML"/></sky:result>')


def test_transfer(rb):
    t = rb.transfer(5)
    assert t[0].get("hitFile").endswith("ihits")
    assert t[0].get("infoFile").endswith("info")
    assert t[0].get("name").endswith(".grib")
    assert len(t) == 1
    assert len(t[0]) == 0


def test_edition(rb):
    t = rb.edition()
    assert (lxml.etree.tostring(t) ==
            b'<sky:field xmlns:sky="http://dwd.de/sky" '
            b'name="edit"><sky:value>2</sky:value></sky:field>')


def test_surf_anal_props(rb):
    t = rb.select_surf_anal_props()
    assert (lxml.etree.tostring(t[2]) ==
            b'<sky:field xmlns:sky="http://dwd.de/sky" name="PARAMETER_'
            b'SHORTNAME"><sky:value>HSURF</sky:value><sky:value>FR_LAND'
            b'</sky:value></sky:field>')


def test_surf_forc_props(rb):
    t = rb.select_surf_forc_props(3)
    assert t[1][0].text == "03"
    assert t[2][0].text == "T_2M"
    assert t[2][6].text == "QV_2M"
    assert len(t[2]) == 7


def test_level_props(rb):
    t = rb.select_level_props(5)
    assert t[1][0].text == "05"
    assert t[2][0].text == "T"
    assert t[2][4].text == "V"
    assert len(t[2]) == 5
    assert len(t[3]) == 15
    assert t[3][0].text == "1000"
    assert t[3][14].text == "100000"


def test_get_request(rb, tmp_path):
    t = rb.get_request_et()
    assert all([x.tag.endswith("read") for x in t])
    assert t[0][0][0][0].text == "20200224120000"
    assert t[8][3][0].get("name") == str(
            tmp_path / "import" / "NWP_data" /
            "S_NWC_NWP_2020-02-24T12:00:00Z_003.grib")


def test_make_icon_request(tmp_path):
    from fogtools.sky import build_icon_request_for_nwcsaf
    t = pandas.Timestamp("20200224120000")
    s = build_icon_request_for_nwcsaf(tmp_path, t)
    assert b"'" not in s


@mock.patch("subprocess.run", autospec=True)
def test_send_to_sky(sr):
    from fogtools.sky import send_to_sky
    send_to_sky(b"lentils")
    sr.assert_called_once()
    sr.side_effect = subprocess.CalledProcessError(
            1, b"lettuce", b"stdout", b"stderr")
    with pytest.raises(subprocess.CalledProcessError):
        send_to_sky(b"tofu")


@mock.patch("subprocess.run", autospec=True)
def test_get_and_send(sr):
    from fogtools.sky import get_and_send
    t = pandas.Timestamp("20200224120000")
    with pytest.raises(FileNotFoundError):
        get_and_send(pathlib.Path("/tmp/lentils"), t)
    sr.assert_called_once()
