import tempfile
import pathlib
import fnmatch
import logging
import pandas
import pytest
from unittest.mock import patch, call


@pytest.fixture
def listing():
    return ["noaa-goes16/ABI-L1b-RadC/2020/060/12/"
            "OR_ABI-L1b-RadC-M6C10_G16_s202006012"
            + m
            for m in [
                '01142_e20200601203527_c20200601204022.nc',
                '06142_e20200601208527_c20200601209010.nc',
                '11142_e20200601213527_c20200601213582.nc',
                '16142_e20200601218527_c20200601218589.nc',
                '21142_e20200601223527_c20200601224006.nc',
                '26142_e20200601228527_c20200601229000.nc',
                '31142_e20200601233527_c20200601234003.nc',
                '36142_e20200601238527_c20200601238591.nc',
                '41142_e20200601243527_c20200601244005.nc',
                '46142_e20200601248527_c20200601249024.nc',
                '51142_e20200601253527_c20200601254020.nc',
                '56142_e20200601258527_c20200601259017.nc']]


def test_get3_uri():
    from fogtools.abi import get_s3_uri
    assert (get_s3_uri(
            pandas.Timestamp("2020-01-01T12")) ==
            "s3://noaa-goes16/ABI-L1b-RadC/2020/001/12")
    assert (get_s3_uri(
            pandas.Timestamp("2020-02-01T23")) ==
            "s3://noaa-goes16/ABI-L1b-RadC/2020/032/23")
    assert (get_s3_uri(
            pandas.Timestamp("2020-03-01T00"), "F") ==
            "s3://noaa-goes16/ABI-L1b-RadF/2020/061/00")


@patch("s3fs.S3FileSystem", autospec=True)
def test_s3_select_period(sS, listing):
    from fogtools.abi import s3_select_period
    t1 = pandas.Timestamp("2020-02-29T12")
    t2 = pandas.Timestamp("2020-02-29T14")
    sS.return_value.glob.return_value = listing
    g1 = s3_select_period(t1, t2, [10], "C")
    g2 = s3_select_period(t1, t2, [10], "F")
    assert list(g1) == listing
    assert list(g2) == listing
    sS.return_value.glob.assert_any_call(
        "s3://noaa-goes16/ABI-L1b-RadC/2020/060/12/"
        "??_???-L1b-???*-??C10_???_s?????????????*_"
        "e?????????????*_c?????????????*.nc*")
    sS.return_value.glob.assert_any_call(
        "s3://noaa-goes16/ABI-L1b-RadF/2020/060/12/"
        "??_???-L1b-???*-??C10_???_s?????????????*_"
        "e?????????????*_c?????????????*.nc*")
    # something missing in this unit test since it doesn't catch that it
    # doesn't actually work in practice


def test_get_dl_dest():
    from fogtools.abi import get_dl_dest

    b = pathlib.Path("/tmp")
    t = pandas.Timestamp("2020-03-01T12")
    assert get_dl_dest(b, t, 42, "lettuce") == \
        pathlib.Path("/tmp/abi/2020/03/01/12/C42/lettuce")
    t = pandas.Timestamp("2020-03-01T12:30")
    assert get_dl_dest(b, t, 42, "a/b/lettuce.nuts") == \
        pathlib.Path("/tmp/abi/2020/03/01/12/C42/lettuce.nuts")


@patch("s3fs.S3FileSystem", autospec=True)
def test_download_abi_day(sS, tmp_path):
    from fogtools.abi import download_abi_day
    from sattools.io import get_cache_dir
    t1 = pandas.Timestamp("2020-02-29T12")
    pat = ("s3://noaa-goes16/ABI-L1b-Rad{m:s}/2020/{d:>03d}/{h:>02d}/"
           "OR_ABI-L1b-RadC-M3C{ch:>02d}_G16_"
           "s2020{d:>03d}{h:>02d}00000_e2020{d:>03d}{h:>02d}10000_"
           "c2020{d:>03d}{h:>02d}15000.nc")
    ref = [pat.format(m=m, ch=ch, d=d, h=h)
           for m in "CF" for ch in (2, 3, 4)
           for d in (60, 61) for h in (10, 20)]

    def fake_glob(pattern):
        return [r for r in ref if fnmatch.fnmatch(r, pattern)]
    sS.return_value.glob.side_effect = fake_glob
    with patch("sattools.io.get_cache_dir", autospec=True) as fig:
        fig.return_value = tmp_path
        download_abi_day(t1, [1, 2, 3], "CF")
        assert sS.return_value.get.call_count == 2*2*2  # 2 h, 2 chan, 2 mode
        sS.return_value.get.assert_has_calls([
            call("s3://" + pat.format(m="C", ch=2, h=10, d=60),
                 tmp_path / "abi" / "2020" / "02" / "29" / "10" /
                 "C2" / pat.format(m="C", ch=2, h=10, d=60).split("/")[-1]),
            call("s3://" + pat.format(m="C", ch=2, h=20, d=60),
                 tmp_path / "abi" / "2020" / "02" / "29" / "20" /
                 "C2" / pat.format(m="C", ch=2, h=20, d=60).split("/")[-1]),
            call("s3://" + pat.format(m="F", ch=3, h=20, d=60),
                 tmp_path / "abi" / "2020" / "02" / "29" / "20" /
                 "C3" / pat.format(m="F", ch=3, h=20, d=60).split("/")[-1])],
            any_order=True)
    with tempfile.NamedTemporaryFile() as ntf, \
            patch("fogtools.abi.get_dl_dest", autospec=True) as fag:
        fag.return_value = pathlib.Path(ntf.name)
        download_abi_day(t1, [1, 2, 3])
        cd = get_cache_dir(subdir="fogtools")
        fag.assert_has_calls([
            call(cd, pandas.Timestamp("2020-02-29T10:00:00.0"), 2,
                 pat.format(m="C", ch=2, d=60, h=10)),
            call(cd, pandas.Timestamp("2020-02-29T20:00:00.0"), 3,
                 pat.format(m="C", ch=3, d=60, h=20))],
            any_order=True)


@patch("s3fs.S3FileSystem", autospec=True)
def test_download_abi_period(sS, tmp_path, monkeypatch, caplog, listing):
    import fogtools.abi
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))

    sS.return_value.glob.return_value = listing
    with caplog.at_level(logging.DEBUG):
        fogtools.abi.download_abi_period(
                pandas.Timestamp("2020-02-29"),
                pandas.Timestamp("2020-03-01"),
                chans=[10],
                tps="F")
        assert "Downloading ABI for 2020-02-29 00:00 -- 2020-03-01 00:00" in \
            caplog.text
        assert "Already exists" not in caplog.text
    assert sS.return_value.get.call_count == len(listing)
    sS.return_value.get.assert_has_calls([
        call("s3://" + listing[0],
             tmp_path / "fogtools" / "abi" / "2020" / "02" / "29" /
             "12" / "C10" / listing[0].split("/")[-1])])
