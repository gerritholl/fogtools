import pathlib
import subprocess
import pytest
from unittest import mock
import pandas
import lxml.etree


@pytest.fixture
def rb(tmp_path):
    from fogtools.sky import RequestBuilder
    return RequestBuilder(tmp_path)


@pytest.fixture
def period():
    return pandas.Period("20200224120000")


@pytest.fixture
def timestamp():
    return pandas.Timestamp("20200224120000")


def test_icon_filename(tmp_path):
    from fogtools.sky import make_icon_nwcsaf_filename
    t = pandas.Timestamp("20200224120000")
    fs = 3
    fn = make_icon_nwcsaf_filename(tmp_path, t, fs)
    assert fn == (tmp_path / "import" / "NWP_data" /
                  "S_NWC_NWP_2020-02-24T12:00:00Z_003.grib")


def test_refdate(rb, timestamp):
    t = rb.refdate(timestamp)
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


def test_transfer(rb, timestamp):
    t = rb.transfer(timestamp, 5)
    assert t[0].get("hitFile").endswith("ihits")
    assert t[0].get("infoFile").endswith("info")
    assert t[0].get("name").endswith(".grib")
    assert len(t) == 1
    assert len(t[0]) == 0


def test_edition(rb):
    t = rb.edition()
    assert (lxml.etree.tostring(t) ==
            b'<sky:field xmlns:sky="http://dwd.de/sky" '
            b'name="GRIB_EDITION"><sky:value>2</sky:value></sky:field>')


def test_surf_anal_props(rb, timestamp):
    t = rb.select_surf_anal_props(timestamp)
    assert (lxml.etree.tostring(t[2]) ==
            b'<sky:field xmlns:sky="http://dwd.de/sky" name="PARAMETER_'
            b'SHORTNAME"><sky:value>HSURF</sky:value><sky:value>FR_LAND'
            b'</sky:value></sky:field>')


def test_surf_forc_props(rb, timestamp):
    t = rb.select_surf_forc_props(timestamp, 3)
    assert t[1][0].text == "03"
    assert t[2][0].text == "T_2M"
    assert t[2][6].text == "QV_2M"
    assert len(t[2]) == 7


def test_level_props(rb, timestamp):
    t = rb.select_level_props(timestamp, 5)
    assert t[1][0].text == "05"
    assert t[2][0].text == "T"
    assert t[2][4].text == "V"
    assert len(t[2]) == 5
    assert len(t[3]) == 15
    assert t[3][0].text == "1000"
    assert t[3][14].text == "100000"


def test_get_request(rb, tmp_path, timestamp):
    t = rb.get_request_et(
            [timestamp, timestamp.replace(year=2042),
             timestamp.replace(year=1800)])
    assert all([x.tag.endswith("read") for x in t])
    assert t[0][0][0][0].text == "20200224120000"
    assert t[0][3][0].get("name").endswith("2020-02-24T12:00:00Z_000.grib")
    assert t[8][3][0].get("name") == str(
            tmp_path / "import" / "NWP_data" /
            "S_NWC_NWP_2020-02-24T12:00:00Z_003.grib")
    assert len(t) == 39
    assert all([t[i][0][0][0].text == "20420224120000"
                for i in range(13, 26)])
    assert len(t[0][0][2]) == 2
    assert t[3][0][1][0].text == "00"
    assert len(t[3][0][2]) == 2
    assert t[3][1][1][0].text == "01"
    assert len(t[3][1][2]) == 7
    assert t[3][4][0].get("name").endswith("001.grib")
    assert t[12][0][1][0].text == "05"
    assert t[13][0][0][0].text == "20420224120000"
    assert t[13][3][0].get("name").endswith("2042-02-24T12:00:00Z_000.grib")
    assert t[16][4][0].get("name").endswith("2042-02-24T12:00:00Z_001.grib")


def test_make_icon_request(tmp_path, timestamp):
    from fogtools.sky import build_icon_request_for_nwcsaf
    s = build_icon_request_for_nwcsaf(tmp_path, timestamp)
    assert b"'" not in s


def test_make_icon_request_allday(tmp_path, timestamp):
    from fogtools.sky import sky_get_icon_for_day
    sky_get_icon_for_day(
            tmp_path,
            timestamp.to_period(freq="D"))
    with pytest.raises(ValueError):
        sky_get_icon_for_day(
                tmp_path,
                timestamp.to_period(freq="H"))


@mock.patch("subprocess.run", autospec=True)
def test_send_to_sky(sr):
    from fogtools.sky import send_to_sky
    send_to_sky(b"lentils")
    sr.assert_called_once()
    sr.side_effect = subprocess.CalledProcessError(
            1, b"lettuce", b"stdout", b"stderr")
    with pytest.raises(subprocess.CalledProcessError):
        send_to_sky(b"tofu")


def test_verify_period():
    from fogtools.sky import verify_period
    with pytest.raises(ValueError) as e:
        verify_period(pandas.Period("2010"))
    assert "exceeds 5 days" in e.value.args[0]
    with pytest.raises(ValueError) as e:
        verify_period(pandas.Period("2010-01-01T03:00:00"))
    assert "hour must be" in e.value.args[0]
    with pytest.raises(ValueError) as e:
        verify_period(pandas.Period("2010-01-01T06:07:08"))
    assert "must be whole hour" in e.value.args[0]


def test_period2daterange():
    from fogtools.sky import period2daterange
    p = pandas.Period("1985-08-13")
    dr = period2daterange(p)
    assert dr.size == 4
    assert [d.hour for d in dr] == [0, 6, 12, 18]
    p = pandas.Period("1985-08-13T12")
    dr = period2daterange(p)
    assert dr.size == 1
    assert dr[0] == pandas.Timestamp("1985-08-13T12")


@mock.patch("subprocess.run", autospec=True)
def test_get_and_send(sr, period):
    from fogtools.sky import get_and_send, SkyFailure
    with pytest.raises(SkyFailure):
        get_and_send(pathlib.Path("/tmp/lentils"), period)
    sr.assert_called_once()
