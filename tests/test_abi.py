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
    g = s3_select(t1, 10)
    next(g)
    sS.return_value.glob.assert_called_once_with(
            "s3://noaa-goes16/ABI-L1b-RadC/2020/061/12/*C10*")


def test_get_dl_dest():
    from fogtools.abi import get_dl_dest

    b = pathlib.Path("/tmp")
    t = pandas.Timestamp("2020-03-01T12")
    assert get_dl_dest(b, t, 42, "lettuce") == \
        pathlib.Path("/tmp/abi/2020/03/01/12/00/42/lettuce")
    assert get_dl_dest(b, t, 42, "a/b/lettuce.nuts") == \
        pathlib.Path("/tmp/abi/2020/03/01/12/00/42/lettuce.nuts")


@patch("s3fs.S3FileSystem", autospec=True)
def test_download_abi(sS):
    from fogtools.abi import download_abi_day
    from fogtools.io import get_cache_dir
    t1 = pandas.Timestamp("2020-03-01T12")
    sS.return_value.glob.side_effect = lambda *a: iter(
            ["seitan", "tofu", "tempeh"])
    with tempfile.TemporaryDirectory() as td, \
            patch("fogtools.io.get_cache_dir", autospec=True) as fig:
        ptd = pathlib.Path(td)
        fig.return_value = ptd
        download_abi_day(t1, [1, 2, 3])
        assert sS.return_value.get.call_count == 24 * 3 * 3
        ts = t1.strftime("%Y/%m/%d/%H/%M")
        sS.return_value.get.assert_has_calls([
            call("s3://tofu", ptd / "abi" / ts / "1/tofu"),
            call("s3://tempeh", ptd / "abi" / ts / "2/tempeh"),
            call("s3://seitan", ptd / "abi" / ts / "3/seitan")],
            any_order=True)
    with tempfile.NamedTemporaryFile() as ntf, \
            patch("fogtools.abi.get_dl_dest", autospec=True) as fag:
        fag.return_value = pathlib.Path(ntf.name)
        download_abi_day(t1, [1, 2, 3])
        cd = get_cache_dir()
        fag.assert_has_calls([
            call(cd, pandas.Timestamp("2020-03-01T06"), 2, "tofu"),
            call(cd, pandas.Timestamp("2020-03-01T16"), 1, "seitan"),
            call(cd, pandas.Timestamp("2020-03-01T00"), 3, "seitan")],
            any_order=True)
