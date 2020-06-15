import logging
import math

import pytest
import numpy
import numpy.testing
import pandas
from unittest import mock


@pytest.fixture
def stations():
    from fogtools.isd import get_stations
    return get_stations()


@pytest.fixture
def subset(stations):
    from fogtools.isd import select_stations
    return select_stations(stations)


@pytest.fixture
def station_dask(station):
    import dask.dataframe as ddf
    return ddf.from_pandas(station, npartitions=2)


def test_get_stations(stations):
    # get_stations gets called in fixture
    assert len(stations) == 29742
    assert stations["END"].max() == pandas.Timestamp("2020-01-15")
    assert stations["BEGIN"].min() == pandas.Timestamp("1901-01-01")
    assert (stations["WBAN"].dtype ==
            stations["USAF"].dtype ==
            pandas.StringDtype())
    assert (stations["LAT"].dtype == stations["LON"].dtype ==
            stations["ELEV(M)"].dtype == numpy.dtype("f8"))
    assert (stations["BEGIN"].dtype ==
            stations["END"].dtype ==
            numpy.dtype("<M8[ns]"))


def test_select_stations(subset):
    # select_stations get called in fixture
    assert len(subset) == 167
    assert subset["END"].min() > pandas.Timestamp("2020-01-01")
    assert (subset["CTRY"] == "US").all()


def test_get_station_ids(subset):
    from fogtools.isd import get_station_ids
    ids = get_station_ids(subset)
    assert len(ids) == len(subset)
    assert isinstance(ids, pandas.Series)
    assert (ids.str.len() == 11).all()


def test_dl_station(station):
    # dl_station gets called in fixture
    assert len(station) == 5
    assert (station.STATION == "94733099999").all()
    assert all(dt == pandas.StringDtype()
               for dt in station.dtypes[
                   ["STATION", "NAME", "VIS", "TMP", "DEW"]])


def test_extract_vis(station, station_dask):
    from fogtools.isd import extract_vis
    df_vis = extract_vis(station)
    assert df_vis["vis"].min() == 100
    assert df_vis["vis"].max() == 25000
    assert (df_vis["vis_qc"] == "1").all()
    assert (df_vis["vis_vc"] == "9").all()
    assert (df_vis["vis_qvc"] == "9").all()
    assert all(dt == pandas.StringDtype()
               for dt in df_vis.dtypes[["vis_qc", "vis_vc", "vis_qvc"]])
    # test with dask
    import dask.dataframe as ddf
    df_vis_pandas = extract_vis(station_dask)
    assert isinstance(df_vis_pandas, ddf.DataFrame)
    assert df_vis_pandas.compute().equals(df_vis)


def test_extract_temp(station):
    from fogtools.isd import extract_temp
    df_temp = extract_temp(station, "TMP")
    assert math.isclose(df_temp["tmp"].min(), 20.9, rel_tol=1e-3)
    assert math.isclose(df_temp["tmp"].max(), 37.8, rel_tol=1e-3)
    assert (df_temp["tmp_qc"] == "1").all()
    assert df_temp.dtypes["tmp_qc"] == pandas.StringDtype()
    df_dew = extract_temp(station, "DEW")
    assert math.isclose(df_dew["dew"].min(), 17.2, rel_tol=1e-3)
    assert math.isclose(df_dew["dew"].max(), 21.3, rel_tol=1e-3)
    assert (df_dew["dew_qc"] == "1").all()
    assert df_dew.dtypes["dew_qc"] == pandas.StringDtype()


def test_extract_all(station):
    from fogtools.isd import extract_and_add_all
    df = extract_and_add_all(station)
    assert len(df) == 5
    assert df.dtypes["vis"] == numpy.dtype("u4")
    assert df.dtypes["temp"] == numpy.dtype("f4")
    assert df.dtypes["dew"] == numpy.dtype("f4")


def test_get_station():
    from fogtools.isd import get_station
    # need to mock/test case in which file supposedly exists
    # and case where it doesn't
    with mock.patch("pandas.read_pickle", autospec=True) as prf:
        get_station(2020, "1234567890")
        prf.assert_called_once()

    with mock.patch("pandas.read_pickle", side_effect=FileNotFoundError,
                    autospec=True) as prf, \
            mock.patch("fogtools.isd.dl_station", autospec=True) as ds:
        get_station(2020, "1234567890")
        ds.assert_called_once_with(2020, "1234567890")
        ds.return_value.to_pickle.assert_called_once()


@mock.patch("pandas.read_parquet", autospec=True)
def test_read_db(pr, gb_db):
    from fogtools.isd import read_db
    pr.return_value = gb_db
    read_db("/tmp/tofu")
    pr.assert_called_once_with("/tmp/tofu")


@mock.patch("fogtools.isd.select_stations", autospec=True)
@mock.patch("pandas.read_csv", autospec=True)
@mock.patch("pandas.concat", autospec=True)
def test_create_db(pc, pr, ss, stations, caplog):
    from fogtools.isd import create_db
    ss.return_value = stations.iloc[18000:18005]
    create_db()
    ss.assert_called_once()
    pc.assert_called_once()
    pc.return_value.to_parquet.assert_called_once()
    n = 12  # station-years in those 5 cases
    assert pr.call_count == n
    pr.side_effect = FileNotFoundError
    with caplog.at_level(logging.DEBUG):
        create_db()
    assert "Not available" in caplog.text
    # kw arguments return_value and side_effect not working?
    # https://stackoverflow.com/q/59882580/974555
    pr.reset_mock()
    pr.side_effect = None
    create_db("/dev/null", "20200101", "20200101")
    assert pr.call_count == 3


def test_count_fog(station, station_dask):
    from fogtools.isd import count_fogs_per_time
    cnt_dt = count_fogs_per_time(station, "D", max_vis=500)
    assert isinstance(cnt_dt, pandas.Series)
    assert len(cnt_dt) == 1
    assert cnt_dt[0] == 1
    numpy.testing.assert_array_equal(
            cnt_dt.index,
            pandas.DatetimeIndex(["2019-01-05"]))
    import dask.dataframe as ddf
    cnt_dk = count_fogs_per_time(station_dask, "D", max_vis=500)
    assert isinstance(cnt_dk, ddf.Series)
    assert cnt_dk.compute().equals(cnt_dt)
    cnt_dt_h = count_fogs_per_time(station, "H", max_vis=500)
    numpy.testing.assert_array_equal(
            cnt_dt_h.index,
            pandas.DatetimeIndex(["2019-01-05T22"]))


@mock.patch("fogtools.isd.read_db")
def test_top_n(fir, station):
    from fogtools.isd import top_n
    fir.return_value = station
    d = top_n("H", 1000, 1)
    assert len(d) == 1
    numpy.testing.assert_array_equal(
            d.index,
            pandas.DatetimeIndex([pandas.Timestamp("201901052200")]))
