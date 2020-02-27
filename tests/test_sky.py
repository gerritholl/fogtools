import pathlib
import pytest
import pandas
import lxml.etree


@pytest.fixture
def rb():
    from fogtools.sky import RequestBuilder
    t = pandas.Timestamp("20200224120000")
    return RequestBuilder("/fake/path", t)


def test_icon_filename():
    from fogtools.sky import make_icon_nwcsaf_filename
    t = pandas.Timestamp("20200224120000")
    fs = 3
    fn = make_icon_nwcsaf_filename("/fake/path", t, fs)
    assert fn == pathlib.Path("/fake/path/import/NWP_data/"
                              "S_NWC_NWP_2020-02-24T12:00:00Z_003.grib")


def test_refdate(rb):
    t = rb.refdate()
    assert (lxml.etree.tostring(t) ==
            b'<sky:referenceDate xmlns:sky="http://dwd.de/sky">'
            b'<sky:value>20200224120000</sky:value></sky:referenceDate>')


def test_step(rb):
    t = rb.step(0)
    assert (lxml.etree.tostring(t) ==
            b'<sky:field xmlns:sky="http://dwd.de/sky"><sky:value>00'
            b'</sky:value></sky:field>')
    t = rb.step(5)
    assert (lxml.etree.tostring(t) ==
            b'<sky:field xmlns:sky="http://dwd.de/sky"><sky:value>05'
            b'</sky:value></sky:field>')


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
    assert (lxml.etree.tostring(t) ==
            b'<sky:transfer xmlns:sky="http://dwd.de/sky"><sky:file '
            b'hitFile="ihits" infoFile="info" name="/fake/path/import/'
            b'NWP_data/S_NWC_NWP_2020-02-24'
            b'T12:00:00Z_005.grib"/></sky:transfer>')


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


def test_get_request(rb):
    t = rb.get_request()
    assert all([x.tag.endswith("read") for x in t])
    assert t[0][0][0][0].text == "20200224120000"
    assert t[2][3][0].get("name") == (
            "/fake/path/import/NWP_data/"
            "S_NWC_NWP_2020-02-24T12:00:00Z_001.grib")


def test_make_icon_request():
    from fogtools.sky import make_icon_request_for_nwcsaf
    t = pandas.Timestamp("20200224120000")
    s = make_icon_request_for_nwcsaf("/fake/path", t)
    assert b"'" not in s
