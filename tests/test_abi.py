import pandas
from unittest.mock import patch


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


@patch("s3fs.S3FileSystem", autospec=True)
def test_download_abi(sS):
    from fogtools.abi import download_abi_day
    t1 = pandas.Timestamp("2020-03-01T12")
    sS.return_value.glob.side_effect = lambda *a: iter(
            ["seitan", "tofu", "tempeh"])
    download_abi_day(t1, [1, 2, 3])
    assert sS.return_value.get.call_count == 24 * 3 * 3
