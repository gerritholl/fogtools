import tempfile
import pathlib
import pandas
from unittest.mock import patch, call


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
def test_s3_select(sS):
    from fogtools.abi import s3_select
    t1 = pandas.Timestamp("2020-03-01T12")
    sS.return_value.glob.return_value = iter("tempeh")
    g1 = s3_select(t1, 10, "C")
    g2 = s3_select(t1, 10, "F")
    next(g1)
    next(g2)
    sS.return_value.glob.assert_has_calls([
        call("s3://noaa-goes16/ABI-L1b-RadC/2020/061/12/*C10*"),
        call("s3://noaa-goes16/ABI-L1b-RadF/2020/061/12/*C10*")])


def test_get_dl_dest():
    from fogtools.abi import get_dl_dest

    b = pathlib.Path("/tmp")
    t = pandas.Timestamp("2020-03-01T12")
    assert get_dl_dest(b, t, 42, "lettuce") == \
        pathlib.Path("/tmp/abi/2020/03/01/12/00/42/lettuce")
    t = pandas.Timestamp("2020-03-01T12:30")
    assert get_dl_dest(b, t, 42, "a/b/lettuce.nuts") == \
        pathlib.Path("/tmp/abi/2020/03/01/12/30/42/lettuce.nuts")


@patch("s3fs.S3FileSystem", autospec=True)
def test_download_abi(sS):
    from fogtools.abi import download_abi_day
    from fogtools.io import get_cache_dir
    t1 = pandas.Timestamp("2020-03-01T12")
    ref = ["noaa-goes16/ABI-L1b-RadC/2017/059/00/OR_ABI-L1b-RadC-M3C12_G16_"
           "s20170590002505_e20170590005283_c20170590005323.nc",
           "noaa-goes16/ABI-L1b-RadC/2017/059/01/OR_ABI-L1b-RadC-M3C12_G16_"
           "s20170590122505_e20170590125283_c20170590125314.nc",
           "noaa-goes16/ABI-L1b-RadC/2017/059/06/OR_ABI-L1b-RadC-M3C03_G16_"
           "s20170590627505_e20170590630278_c20170590630319.nc",
           "noaa-goes16/ABI-L1b-RadF/2017/059/17/OR_ABI-L1b-RadF-M3C09_G16_"
           "s20170591751089_e20170591801462_c20170591801522.nc"]

    def fake_glob(pattern):
        if "RadC" in pattern:
            return iter(ref[:3])
        elif "RadF" in pattern:
            return iter(ref[3:4])
    sS.return_value.glob.side_effect = fake_glob
    with tempfile.TemporaryDirectory() as td, \
            patch("fogtools.io.get_cache_dir", autospec=True) as fig:
        ptd = pathlib.Path(td)
        fig.return_value = ptd
        download_abi_day(t1, [1, 2, 3], "CF")
        # a selection for each hour (24), each channel (3), each file (4)
        assert sS.return_value.get.call_count == 24 * 3 * 4
        sS.return_value.get.assert_has_calls([
            call("s3://" + ref[0],
                 ptd / "abi" / "2017" / "02" / "28" / "00" / "02" /
                 "1" / ref[0].split("/")[-1]),
            call("s3://" + ref[1],
                 ptd / "abi" / "2017" / "02" / "28" / "01" / "22" /
                 "2" / ref[1].split("/")[-1]),
            call("s3://" + ref[2],
                 ptd / "abi" / "2017" / "02" / "28" / "06" / "27" /
                 "3" / ref[2].split("/")[-1]),
            call("s3://" + ref[3],
                 ptd / "abi" / "2017" / "02" / "28" / "17" / "51" /
                 "1" / ref[3].split("/")[-1])],
            any_order=True)
    with tempfile.NamedTemporaryFile() as ntf, \
            patch("fogtools.abi.get_dl_dest", autospec=True) as fag:
        fag.return_value = pathlib.Path(ntf.name)
        download_abi_day(t1, [1, 2, 3])
        cd = get_cache_dir()
        fag.assert_has_calls([
            call(cd, pandas.Timestamp("2017-02-28T00:02:50.5"), 2, ref[0]),
            call(cd, pandas.Timestamp("2017-02-28T01:22:50.5"), 1, ref[1]),
            call(cd, pandas.Timestamp("2017-02-28T06:27:50.5"), 3, ref[2])],
            any_order=True)
