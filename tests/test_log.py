import datetime
import logging
import io

import pytest
import numpy.testing


def test_log_context(tmp_path):
    import fogtools.log
    tm = datetime.datetime(1985, 8, 13, 15)
    tofu = logging.getLogger("tofu")
    veggie = logging.getLogger("veggie")
    # substitute for stderr on other handler
    oh = logging.FileHandler(tmp_path / "test")
    tofu.addHandler(oh)
    with fogtools.log.LogToTimeFile(tm) as c:
        # I want everything to be logged to the file
        f = c.logfile
        tofu.debug("tofu")
        veggie.debug("veggie")
    with f.open("r") as fp:
        text = fp.read()
        assert "tofu" in text
        assert "veggie" in text
    # I want none of this to appear on stderr
    with (tmp_path / "test").open("r") as fp:
        text = fp.read()
        assert "tofu" in text
        assert "veggie" not in text


def test_collect_filterstats_from_log():
    import fogtools.log
    s = """Filter results for WaterCloudFilter

                    Water cloud filtering for satellite images.
                    Array size:              8392332
                    Masking:                 7548551
                    Previous masked:         8386380
                    New filtered:            1778
                    Remaining:               4174
aoeu
uea
aoe
bla Filter results for LowCloudFilter

                    Filtering low clouds for satellite images.
                    Array size:              8392332
                    Masking:                 8392332
                    Previous masked:         8388158
                    New filtered:            4174
                    Remaining:               0"""

    D = fogtools.log.collect_filterstats_from_log(io.StringIO(s))
    assert D["WaterCloudFilter"] == 1778
    assert D["LowCloudFilter"] == 4174
    with pytest.raises(ValueError):
        D = fogtools.log.collect_filterstats_from_log(io.StringIO(s+s))
    with pytest.raises(ValueError):
        D = fogtools.log.collect_filterstats_from_log(io.StringIO(s[:100]))


def test_collect_filterstats_from_files(tmp_path):
    import fogtools.log
    p1 = tmp_path / "path1"
    s = """Filter results for WaterCloudFilter

                    Water cloud filtering for satellite images.
                    Array size:              8392332
                    Masking:                 7548551
                    Previous masked:         8386380
                    New filtered:            1778
                    Remaining:               4174
aoeu
uea
aoe
bla Filter results for LowCloudFilter

                    Filtering low clouds for satellite images.
                    Array size:              8392332
                    Masking:                 8392332
                    Previous masked:         8388158
                    New filtered:            4174
                    Remaining:               0"""
    with p1.open(mode="w") as fp:
        fp.write(s)

    p2 = tmp_path / "path2"
    s = """Filter results for WaterCloudFilter

                    Water cloud filtering for satellite images.
                    Array size:              8392332
                    Masking:                 7548551
                    Previous masked:         8386380
                    New filtered:            1777
                    Remaining:               4174
aoeu
uea
aoe
bla Filter results for LowCloudFilter

                    Filtering low clouds for satellite images.
                    Array size:              8392332
                    Masking:                 8392332
                    Previous masked:         8388158
                    New filtered:            4173
                    Remaining:               0"""

    with p2.open(mode="w") as fp:
        fp.write(s)

    df = fogtools.log.collect_filterstats_from_logfiles(p1, p2)
    numpy.testing.assert_array_equal(df.index, ["path1", "path2"])
    assert df.shape == (2, 2)
    numpy.testing.assert_array_equal(df.loc["path1"], [1778, 4174])
    numpy.testing.assert_array_equal(df.loc["path2"], [1777, 4173])
    numpy.testing.assert_array_equal(df["WaterCloudFilter"], [1778, 1777])
    numpy.testing.assert_array_equal(df["LowCloudFilter"], [4174, 4173])
