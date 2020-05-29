import pathlib
import functools
import subprocess
import unittest.mock
import logging
import fnmatch
import re

import numpy.testing
import pandas
import pytest
import xarray
import satpy
import datetime


# TODO:
#   - test interface into .extract: it's going to fail for those databases
#     where results are not a Scene, but the unit tests are currently not
#     showing that

@pytest.fixture(scope="function")
def db():
    import fogtools.db
    return fogtools.db.FogDB()


@pytest.fixture
def ts():
    return pandas.Timestamp("1900-01-01T00:00:00")


def _dbprep(tmp_path, cls, *args, **kwargs):
    import fogtools.db
    db = getattr(fogtools.db, cls)(*args, **kwargs)
    return db


@pytest.fixture
def fakearea():
    """Make a 5x5 pixel area for full disc ABI."""
    from pyresample.geometry import AreaDefinition
    return AreaDefinition(
            "fribbulus xax", "fribbulus xax", "fribbulus xax",
            {'proj': 'geos', 'sweep': 'x', 'lon_0': -89.5, 'h': 35786023,
                'x_0': 0, 'y_0': 0, 'ellps': 'GRS80', 'units': 'm', 'no_defs':
                None, 'type': 'crs'},
            5, 5, (-5434894.8851, -4585692.5593, 4585692.5593, 5434894.8851))


@pytest.fixture
def fakescene():
    """Return a fake scene with mocked areas."""
    # let's make a Scene
    #
    # should I mock get_xy_from_lonlat here?  Probably as it's an external
    # dependency that I can assume to be correct, and it simplifies the
    # unit test here.
    st_tm = datetime.datetime(1899, 12, 31, 23, 55)
    sc = satpy.Scene()
    sc["raspberry"] = xarray.DataArray(
            numpy.arange(25).reshape(5, 5),
            dims=("x", "y"),
            attrs={"area": unittest.mock.MagicMock(),
                   "start_time": st_tm,
                   "name": "raspberry"})
    sc["cloudberry"] = xarray.DataArray(
            numpy.arange(25).reshape(5, 5),
            dims=("x", "y"),
            attrs={"area": unittest.mock.MagicMock(),
                   "start_time": st_tm,
                   "name": "cloudberry"})
    sc["cloudberry_pal"] = xarray.DataArray(
            numpy.arange(25).reshape(5, 5),
            dims=("color_a", "color_b"),
            attrs={"name": "cloudberry_pal",
                   "start_time": st_tm})
    sc["maroshki"] = xarray.DataArray(
            numpy.arange(25).reshape(5, 5),
            dims=("xx", "yy"),
            attrs={"name": "maroshki",
                   "area": unittest.mock.MagicMock(),
                   "start_time": st_tm})
    sc["banana"] = xarray.DataArray(
            numpy.arange(25).reshape(1, 5, 5),
            dims=("t", "x", "y"),
            attrs={"area": unittest.mock.MagicMock(),
                   "start_time": st_tm,
                   "name": "banana"})
    sc["raspberry"].attrs["area"].get_xy_from_lonlat.return_value = (
            numpy.ma.masked_array(
                numpy.array([1, 1]),
                [False, False]),
            numpy.ma.masked_array(
                numpy.array([1, 2]),
                [False, False]))
    for nm in ("banana", "cloudberry"):
        sc[nm].attrs["area"].get_xy_from_lonlat.return_value = (
                numpy.ma.masked_array(
                    numpy.array([2, 2]),
                    [False, False]),
                numpy.ma.masked_array(
                    numpy.array([2, 3]),
                    [False, False]))
    return sc


def _mk_fakescene_realarea(fakearea, st_tm, *names):
    """Return a fake scene with real fake areas."""
    import dask.array as da
    sc = satpy.Scene()
    for name in names:
        sc[name] = xarray.DataArray(
                da.arange(25).reshape(5, 5),
                dims=("x", "y"),
                attrs={"area": fakearea,
                       "start_time": st_tm,
                       "name": name})
        sc[name] = xarray.DataArray(
                da.arange(25).reshape(5, 5),
                dims=("x", "y"),
                attrs={"area": fakearea,
                       "start_time": st_tm,
                       "name": name})
    return sc


@pytest.fixture
def fake_df():
    """Produce fake dataframe.

    This fake dataframe is intended as a drop-in replacement for ground station
    data.
    """

    # the peculiar way of getting lat/lon is to make sure it doesn't correlate
    # too much with the time, so that if selecting several adjecent series the
    # points are not close to each other, however I'm still avoiding randomness
    # in unit tests
    df = pandas.DataFrame(
            {"DATE": (dr:=pandas.date_range(  # noqa: E231
                "18991231T12", "19000101T12",
                freq="15min")),
             "LATITUDE": numpy.linspace(0, 10000, dr.size) % 180 - 90,
             "LONGITUDE": numpy.linspace(0, 10000, dr.size) % 360 - 180,
             "values": numpy.empty(shape=dr.size)}).sample(
                     frac=1, random_state=42)
    return df.set_index(["DATE", "LATITUDE", "LONGITUDE"])


def _mkdf(idx, *fields):
    """Make a DataFrame with some fake data
    """
    nrows = idx.size
    return pandas.DataFrame(numpy.empty(shape=(nrows, len(fields))),
                            columns=fields,
                            index=idx)


@pytest.fixture
def abi(tmp_path):
    return _dbprep(tmp_path, "_ABI")


@pytest.fixture
def icon(tmp_path):
    return _dbprep(tmp_path, "_ICON")


@pytest.fixture
def nwcsaf(tmp_path, abi, icon):
    return _dbprep(tmp_path, "_NWCSAF", dependencies={"sat": abi, "nwp": icon})


@pytest.fixture
def synop(tmp_path):
    return _dbprep(tmp_path, "_SYNOP")


@pytest.fixture
def dem(tmp_path):
    return _dbprep(tmp_path, "_DEM", "new-england")


@pytest.fixture
def fog(tmp_path, dem, nwcsaf, abi):
    return _dbprep(tmp_path, "_Fog",
                   dependencies={"sat": abi, "dem": dem, "cmic": nwcsaf})


def _gen_abi_list(
        cs, st, ed, freq, pat,
        dt_end=pandas.Timedelta(10, "min"),
        dt_cre=pandas.Timedelta(20, "min")):
    """Generate fake src list for ABI."""
    L = []
    for c in cs:
        for d in pandas.date_range(st, ed, freq=freq):
            L.append(pat.format(
                c=c, start_time=d, end_time=d+dt_end, cre_time=d+dt_cre))
    return L


def _gen_abi_src(cs, st, ed):
    """Generate fake src list for ABI."""
    return _gen_abi_list(
            cs=cs, st=st, ed=ed, freq="15min",
            pat=("s3://noaa-goes16/ABI-L1b-RadF/{start_time:%Y}/"
                 "{start_time:%j}/{start_time:%H}/"
                 "OR_ABI-L1b-RadF-M3C{c:>02d}_G16_s{start_time:%Y%j%H%M%S0}"
                 "_e{end_time:%Y%j%H%M%S0}_c{cre_time:%Y%j%H%M%S0}.nc"))


def _gen_abi_dst(abi, cs, st, ed):
    """Generate fake dest list for ABI."""
    pat = str(abi.base / "abi" / "{start_time:%Y}" / "{start_time:%m}" /
              "{start_time:%d}" / "{start_time:%H}" / "C{c:>01d}" /
              "OR_ABI-L1b-RadF-M3C{c:>02d}_G16_s{start_time:%Y%j%H%M%S0}"
              "_e{end_time:%Y%j%H%M%S0}_c{cre_time:%Y%j%H%M%S0}.nc")
    return _gen_abi_list(cs=cs, st=st, ed=ed, freq="15min", pat=pat)


def test_init(db):
    assert db.sat is not None
    assert db.fog is not None


def test_extend(db, abi, icon, nwcsaf, fake_df, ts, caplog, fakearea):
    # TODO: rewrite test with less mocking
    #
    # function is probably mocking too much, the test passes but it fails in
    # the real world because the preconditions before calling .extract are not
    # met
    import fogtools.isd
    db.sat = abi
    db.sat.load = unittest.mock.MagicMock()
    db.sat.load.return_value = _mk_fakescene_realarea(
            fakearea,
            datetime.datetime(1899, 12, 31, 23, 55),
            "raspberry", "banana")
    db.nwp = icon
    db.nwp.load = unittest.mock.MagicMock()
    db.nwp.load.return_value = _mk_fakescene_realarea(
            fakearea,
            datetime.datetime(1899, 12, 31, 23, 0),
            "apricot", "pineapple")
    db.cmic.load = unittest.mock.MagicMock()
    db.cmic.load.return_value = _mk_fakescene_realarea(
            fakearea,
            datetime.datetime(1899, 12, 31, 23, 55),
            "peach", "redcurrant")
    db.dem.load = unittest.mock.MagicMock()
    db.dem.load.return_value = _mk_fakescene_realarea(
            fakearea,
            None,
            "damson", "prune")
    db.fog.load = unittest.mock.MagicMock()
    db.fog.load.return_value = _mk_fakescene_realarea(
            fakearea,
            datetime.datetime(1899, 12, 31, 23, 55),
            "aubergine", "shallot")
    loc = fogtools.isd.get_db_location()
    loc.parent.mkdir(parents=True)
    fake_df.to_parquet(fogtools.isd.get_db_location())
    db.ground.load(ts)
    with caplog.at_level(logging.INFO):
        db.extend(ts)
        assert "Loading data for 1900-01-01 00:00:00" in caplog.text
        # assert "Extracting data for [fogdb component ABI]
        # 1900-01-01 00:00:00" in caplog.text
    assert sorted(db.data.columns) == [
            "apricot", "aubergine", "banana", "damson", "date_cmic",
            "date_dem", "date_fog", "date_nwp", "date_synop", "peach",
            "pineapple", "prune", "raspberry", "redcurrant", "shallot",
            "values"]
    assert db.data.shape == (5, 16)
    db.extend(ts)
    assert db.data.shape == (10, 16)


def test_store(db, fake_df, tmp_path):
    with pytest.raises(ValueError):
        db.store(tmp_path / "nope.parquet")
    db.data = fake_df
    db.store(tmp_path / "yes.parquet")
    assert (tmp_path / "yes.parquet").exists()


class TestABI:
    @staticmethod
    def _get_fake_paths(abi, old=False, bad=False, tp="local"):
        import fogtools.abi
        d = abi.base / "abi" / "1899" / "12" / "31" / "23"
        for c in (fogtools.abi.nwcsaf_abi_channels
                  | fogtools.abi.fogpy_abi_channels):
            f = (d / f"C{c:>01d}" / f"OR_ABI-L1b-RadF-M3C{c:>02d}_G16_"
                 "s18993652355000_e19000010010000_c19000010020000.nc")
            yield f
            if bad:
                yield (d / f"C{c:>01d}" / f"OR_ABI-L1b-RadF-M6C{c:>02d}_G16_"
                       "s18993652355000_e19000010010000_"
                       "c19000010020000.nc")
            if old:
                # also make T-60, T-30, (last in in M6)
                yield (d / f"C{c:>01d}" / f"OR_ABI-L1b-RadF-M3C{c:>02d}_G16_"
                       "s18993652255000_e18993652310000_"
                       "c19000010020000.nc")
                yield (d / f"C{c:>01d}" / f"OR_ABI-L1b-RadF-M3C{c:>02d}_G16_"
                       "s18993652325000_e18993652340000_"
                       "c19000010020000.nc")

    @classmethod
    def _mk(cls, abi, old=False, bad=False):
        """Make some files in abi.base

        This should ensure that abi.exists(...) returns True.

        Trick!  Make a file for 5 minutes earlier.  It will cover this
        timestamp, but it won't find it as an exact match.  This will make a
        set of fake files:

        - starting 1899-12-31 23:55
        - ending 1900-01-01 00:10
        - created 1900-01-01 00:20
        """
        for f in cls._get_fake_paths(abi, old=old, bad=bad):
            f.parent.mkdir(parents=True, exist_ok=True)
            f.touch()

    def test_find(self, abi, ts, monkeypatch):
        import fogtools.db
        import fogtools.abi
        # this monkeypatching is just to make the tests run faster
        monkeypatch.setattr(fogtools.abi, "nwcsaf_abi_channels", {3})
        monkeypatch.setattr(fogtools.abi, "fogpy_abi_channels", {3})
        assert not abi.find(ts, complete=True)
        self._mk(abi, old=False)
        assert abi.find(ts, complete=True)
        assert abi.find(ts, complete=True) >= set(
                self._get_fake_paths(abi, old=False))
        assert abi.find(ts + pandas.Timedelta(12, "minutes"), complete=True)
        assert not abi.find(ts, past=True, complete=True)
        assert not abi.find(ts + pandas.Timedelta(2, "hours"), complete=True)
        self._mk(abi, old=True)
        assert abi.find(ts, past=True, complete=True)
        self._mk(abi, bad=True)
        with pytest.raises(fogtools.db.FogDBError):
            abi.find(ts + pandas.Timedelta(12, "minutes"), complete=True)
        with pytest.raises(fogtools.db.FogDBError):
            abi.find(ts, complete=True)
        with pytest.raises(NotImplementedError):
            abi.find(ts, complete=False)

    @unittest.mock.patch("s3fs.S3FileSystem", autospec=True)
    def test_store(self, sS, abi, monkeypatch):
        import fogtools.abi
        src_list = _gen_abi_src(
                cs={2, 3, 4},
                st=pandas.Timestamp("1899-12-31T12"),
                ed=pandas.Timestamp("1900-01-01T12"))
        t = pandas.Timestamp("1900-01-01T00")

        def fk_get(src, dest):
            dest.parent.mkdir(exist_ok=True, parents=True)
            dest.touch()

        def fk_glob(pat):
            return fnmatch.filter(src_list, pat)

        sS.return_value.glob.side_effect = fk_glob
        sS.return_value.get.side_effect = fk_get
        abi.store(t)
        monkeypatch.setattr(fogtools.abi, "nwcsaf_abi_channels", {2, 3, 4})
        monkeypatch.setattr(fogtools.abi, "fogpy_abi_channels", {2, 3, 4})
        assert abi.find(t, complete=True)

    @unittest.mock.patch("fogtools.abi.download_abi_period", autospec=True)
    @unittest.mock.patch("satpy.Scene", autospec=True)
    def test_load(self, sS, fad, abi, monkeypatch):
        import fogtools.abi
        monkeypatch.setattr(fogtools.abi, "nwcsaf_abi_channels", {10, 11})
        monkeypatch.setattr(fogtools.abi, "fogpy_abi_channels", {10, 11})
        t1 = pandas.Timestamp("1900-01-01T10")
        t2 = pandas.Timestamp("1900-01-01T11")
        exp = [pathlib.Path(x) for x in
               _gen_abi_dst(abi, cs={10, 11}, st=t1, ed=t2)]

        def mkexp(*args, **kwargs):
            for e in exp:
                e.parent.mkdir(exist_ok=True, parents=True)
                e.touch()
            return exp
        fad.side_effect = mkexp
        abi.load(t1)
        sS.assert_called_with(
                filenames={str(x) for x in
                           _gen_abi_dst(abi, cs={10, 11}, st=t1, ed=t1)},
                reader="abi_l1b")
        assert abi.find(t1, complete=True)
        # call again now that side effect has occured and files are already
        # present -> different logic, but reset fixture first
        fad.reset_mock()
        import fogtools.db
        abi2 = _dbprep(abi.base.parent, "_ABI")
        abi2.load(t1)
        fad.assert_not_called()

    # test concrete methods defined in base class here, as far as not
    # overwritten by _ABI or trivial (such as ensure_deps)
    @unittest.mock.patch("fogtools.abi.download_abi_period", autospec=True)
    def test_ensure(self, fad, abi, ts):
        abi.ensure(ts)
        fad.assert_called_once_with(
                ts - pandas.Timedelta(65, "minutes"),
                ts + pandas.Timedelta(5, "minutes"),
                tps="F",
                basedir=pathlib.Path(abi.base))
        self._mk(abi)
        abi.ensure(ts)
        # make sure it wasn't called again
        fad.assert_called_once()

    def test_link(self, abi, ts):
        abi.link(None, None)  # this doesn't do anything

    def test_extract(self, abi, ts, fakescene, caplog):
        abi.load = unittest.mock.MagicMock()
        abi.load.return_value = fakescene
        pt = pandas.Timestamp("1899-12-31T23:55")
        with caplog.at_level(logging.DEBUG):
            df = abi.extract(ts, numpy.array([10, 10]), numpy.array([10, 15]))
            assert "Not extracting from cloudberry_pal" in caplog.text
            assert "Not extracting from maroshki" in caplog.text
        numpy.testing.assert_array_equal(
                df.columns,
                ["raspberry", "cloudberry", "banana"])
        numpy.testing.assert_array_equal(df["raspberry"], [6, 11])
        numpy.testing.assert_array_equal(df["cloudberry"], [12, 17])
        numpy.testing.assert_array_equal(
                df.index.get_level_values("LATITUDE"), [10, 10])
        numpy.testing.assert_array_equal(
                df.index.get_level_values("LONGITUDE"), [10, 15])
        numpy.testing.assert_array_equal(
                df.index.get_level_values("DATE"),
                pandas.DatetimeIndex([pt, pt]))

    def test_str(self, abi):
        assert str(abi) == "[fogdb component ABI]"


class TestICON:
    @unittest.mock.patch("fogtools.sky.get_and_send", autospec=True)
    def test_store(self, fsg, icon, ts, tmp_path, caplog):
        fsg.return_value = [tmp_path / "pear"]
        with caplog.at_level(logging.INFO):
            icon.store(ts)
            assert ("Retrieving ICON from SKY for 1900-01-01 00:00"
                    in caplog.text)

    # concrete methods from parent class
    def test_find(self, icon, ts):
        t1 = pandas.Timestamp("1900-01-01T05:00:00")
        t2 = pandas.Timestamp("1900-01-01T05:25:00")
        p1 = {icon.base / "import" / "NWP_data" /
              f"S_NWC_NWP_1900-01-01T00:00:00Z_{i:>03d}.grib"
              for i in range(5)}
        for f in p1:
            f.parent.mkdir(parents=True, exist_ok=True)
            f.touch()
        assert not icon.find(t1, complete=True)
        assert icon.find(t1, complete=True) == set()
        p2 = (icon.base / "import" / "NWP_data" /
              "S_NWC_NWP_1900-01-01T00:00:00Z_005.grib")
        p2.touch()
        assert icon.find(t1, complete=True)
        assert icon.find(t1, complete=True) == {p2}
        assert icon.find(t2, complete=True) == {p2}

    @unittest.mock.patch("satpy.Scene", autospec=True)
    @unittest.mock.patch("fogtools.sky.send_to_sky", autospec=True)
    def test_load(self, fss, sS, icon, fake_process):
        t = pandas.Timestamp("1900-01-01T01:23:45")
        # the sky builder always creates all forecast files up to the next
        # analysis (why did I do that?)

        def fk_snd(ba):
            for f in set(re.findall(str(icon.base).encode("ascii")
                                    + rb"[^ ]*\.grib", ba)):
                with open(f, "wb") as fp:
                    fp.write(b"\00")
        fss.side_effect = fk_snd
        icon.load(t)
        sS.assert_called_once_with(
                filenames={
                    icon.base / "import" / "NWP_data" /
                    "S_NWC_NWP_1900-01-01T00:00:00Z_001.grib"},
                reader="grib")
        assert icon.find(t, complete=True)


class TestNWCSAF:
    def test_init(self, nwcsaf):
        assert isinstance(nwcsaf.base, pathlib.Path)

    def test_badinit(self, monkeypatch):
        import fogtools.db
        monkeypatch.delenv("SAFNWC")
        with pytest.raises(fogtools.db.FogDBError):
            fogtools.db._NWCSAF()

    def test_store(self, nwcsaf, ts):
        nwcsaf.ensure_deps = unittest.mock.MagicMock()
        nwcsaf.is_running = unittest.mock.MagicMock()
        nwcsaf.start_running = unittest.mock.MagicMock()
        nwcsaf.is_running.return_value = True
        nwcsaf.store(ts)
        nwcsaf.ensure_deps.assert_called_once_with(ts)
        nwcsaf.is_running.return_value = False
        nwcsaf.store(ts)
        nwcsaf.start_running.assert_called_once_with()

    @unittest.mock.patch("subprocess.run", autospec=True)
    def test_is_running(self, sr, nwcsaf, ts):
        import fogtools.db

        def mock_sr(args, check, txt):
            with open(args[1][2:], "wb") as fp:
                fp.write(txt)
        sr.side_effect = functools.partial(mock_sr, txt=b"Active Mode")
        assert nwcsaf.is_running()
        sr.side_effect = subprocess.CalledProcessError(
                returncode=1, cmd="tm-dummy")
        assert not nwcsaf.is_running()
        sr.side_effect = functools.partial(mock_sr, txt=b"fruit")
        with pytest.raises(fogtools.db.FogDBError):
            nwcsaf.is_running()
        sr.side_effect = subprocess.CalledProcessError(
                returncode=2, cmd="tm-dummy")
        with pytest.raises(subprocess.CalledProcessError):
            nwcsaf.is_running()

    @unittest.mock.patch("subprocess.run", autospec=True)
    def test_start_running(self, sr, nwcsaf):
        nwcsaf.start_running()
        sr.assert_called_once_with(["SAFNWCTM"], check=True)

    def test_get_dep_loc(self, nwcsaf, abi, icon, monkeypatch, tmp_path):
        monkeypatch.setenv("SAFNWC", str(tmp_path))
        p = nwcsaf._get_dep_loc(abi)
        assert p == nwcsaf.base / "import" / "Sat_data"
        p = nwcsaf._get_dep_loc(icon)
        assert p == nwcsaf.base / "import" / "NWP_data"
        with pytest.raises(TypeError):
            nwcsaf._get_dep_loc(object())

    def test_link(self, nwcsaf, abi, icon, ts, monkeypatch, tmp_path, caplog):
        import fogtools.abi
        monkeypatch.setenv("SAFNWC", str(tmp_path))
        monkeypatch.setattr(fogtools.abi, "nwcsaf_abi_channels", {3})
        monkeypatch.setattr(fogtools.abi, "fogpy_abi_channels", {3})
        # this will create a fake ABI file for 18991231T235500
        TestABI._mk(abi)
        # ...but then linking should accept 19000101T000000 as covered thereby
        nwcsaf.link(abi, ts)
        exp = (nwcsaf.base / "import" / "Sat_data" /
               "OR_ABI-L1b-RadF-M3C03_G16_"
               "s18993652355000_e19000010010000_c19000010020000.nc")
        assert exp.is_symlink()
        assert exp.resolve() == (abi.base / "abi" / "1899" / "12" / "31" / "23"
                                 / "C3"
                                 / "OR_ABI-L1b-RadF-M3C03_G16_"
                                   "s18993652355000_e19000010010000_"
                                   "c19000010020000.nc")
        exp.unlink()
        exp.touch()
        with caplog.at_level(logging.WARNING):
            nwcsaf.link(abi, ts)
            assert "Src already exists" in caplog.text
        t2 = pandas.Timestamp("1900-01-01T01:23:45")
        out = (icon.base / "import" / "NWP_data" /
               "S_NWC_NWP_1900-01-01T00:00:00Z_001.grib")
        out.parent.mkdir(exist_ok=True, parents=True)
        out.touch()
        nwcsaf.link(icon, t2)
        exp = (nwcsaf.base / "import" / "NWP_data" /
               "S_NWC_NWP_1900-01-01T00:00:00Z_001.grib")
        assert exp.is_symlink()
        assert (exp.resolve() == out)

    @unittest.mock.patch("time.sleep", autospec=True)
    def test_wait_for_output(self, tisl, nwcsaf, monkeypatch, tmp_path):
        import fogtools.db
        monkeypatch.setenv("SAFNWC", str(tmp_path))
        nwcsaf.is_running = unittest.mock.MagicMock()
        nwcsaf.is_running.return_value = False
        with pytest.raises(fogtools.db.FogDBError):
            nwcsaf.wait_for_output(ts, timeout=20)
        t = pandas.Timestamp("1900-01-01T00:00:00")
        p = (nwcsaf.base / "export" / "CMIC" / "S_NWC_CMIC_GOES16_NEW-ENGLAND-"
             "NR_18991231T235110Z.nc")
        p.parent.mkdir(exist_ok=True, parents=True)
        p.touch()
        nwcsaf.is_running.return_value = True
        nwcsaf.wait_for_output(t, timeout=20)
        assert tisl.call_count == 0
        p.unlink()
        with pytest.raises(fogtools.db.FogDBError):
            nwcsaf.wait_for_output(t, timeout=20)
        assert tisl.call_count == 2

    def test_ensure(self, nwcsaf, ts, fake_process):
        nwcsaf.wait_for_output = unittest.mock.MagicMock()
        nwcsaf.is_running = unittest.mock.MagicMock()
        nwcsaf.start_running = unittest.mock.MagicMock()
        nwcsaf.ensure_deps = unittest.mock.MagicMock()
        nwcsaf.is_running.return_value = False
        nwcsaf.ensure(ts)
        nwcsaf.start_running.assert_called_once_with()
        nwcsaf.ensure_deps.assert_called_once_with(ts)
        nwcsaf.is_running.return_value = True
        nwcsaf.ensure(ts)
        nwcsaf.start_running.assert_called_once_with()

    # concrete methods from parent class
    def test_ensure_deps(self, nwcsaf, abi, icon, ts):
        abi.ensure = unittest.mock.MagicMock()
        icon.ensure = unittest.mock.MagicMock()
        nwcsaf.link = unittest.mock.MagicMock()
        nwcsaf.ensure_deps(ts)
        abi.ensure.assert_called_once_with(ts)
        icon.ensure.assert_called_once_with(ts)
        assert nwcsaf.link.call_count == 2
        nwcsaf.link.assert_any_call(abi, ts)
        nwcsaf.link.assert_any_call(icon, ts)


class TestSYNOP:
    def test_find(self, synop, monkeypatch, tmp_path):
        monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
        p = synop.find(ts, complete=False)
        assert p == {tmp_path / "fogtools" / "store.parquet"}

    @unittest.mock.patch("fogtools.isd.read_db", autospec=True)
    @unittest.mock.patch("fogtools.isd.create_db", autospec=True)
    def test_load(self, fic, fir, synop, ts, fake_df, tmp_path, monkeypatch):
        monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
        fir.return_value = fake_df

        def fake_store(*args):
            (tmp_path / "fogtools").mkdir(parents=True, exist_ok=False)
            fake_df.to_parquet(tmp_path / "fogtools" / "store.parquet")
        fic.side_effect = fake_store
        sel = synop.load(ts, tol=pandas.Timedelta("31min"))
        fic.assert_called_once_with()
        assert sel.shape == (5, 1)
        assert sel.index.get_level_values("DATE")[0] == pandas.Timestamp(
                "18991231T2330")
        assert sel.index.get_level_values("DATE")[-1] == pandas.Timestamp(
                "19000101T0030")
        assert sel.index.names == ["DATE", "LATITUDE", "LONGITUDE"]
        sel2 = synop.load(ts, tol=pandas.Timedelta("31min"))
        assert sel.equals(sel2)
        synop._db = None
        fir.return_value = fake_df.reset_index()
        sel3 = synop.load(ts, tol=pandas.Timedelta("31min"))
        assert sel.equals(sel3)
        fic.assert_called_once_with()  # should not have been called twice

    @unittest.mock.patch("fogtools.isd.create_db", autospec=True)
    def test_store(self, fic, synop):
        synop.store(object())
        fic.assert_called_once_with()


class TestDEM:
    def test_find(self, dem, ts):
        import pkg_resources
        p = dem.find(ts, complete=False)
        assert p == {pathlib.Path(pkg_resources.resource_filename(
            "fogpy", "data/DEM/new-england-500m.tif"))}
        dem.location = pathlib.Path("/file/not/found")
        assert dem.find(pandas.Timestamp("1900"), complete=True) == set()

    @unittest.mock.patch("urllib.request.urlretrieve", autospec=True)
    @unittest.mock.patch("subprocess.run", autospec=True)
    @unittest.mock.patch("tempfile.NamedTemporaryFile", autospec=True)
    def test_store(self, tN, sr, uru, dem, tmp_path, ts):
        dem.location = tmp_path / "fake.tif"
        mtf = tmp_path / "raspberry"
        tN.return_value.__enter__.return_value.name = str(
                tmp_path / "raspberry")
        dem.store(ts)
        assert sr.call_count == 2
        c1 = unittest.mock.call(
                ["gdal_merge.py", "-o", str(mtf)] +
                [str(mtf.parent / f"n{lat:>02d}w{lon:>03d}" /
                     (("" if ext == "gpkg" else "USGS_1_") +
                         f"n{lat:>02d}w{lon:>03d}.{ext:s}"))
                    for lat in range(38, 49)
                    for lon in range(82, 66, -1)
                    for ext in ["tif", "jpg", "xml", "gpkg"]],
                check=True)
        c2 = unittest.mock.call(
                ["gdalwarp", "-r", "bilinear", "-t_srs",
                 "+proj=eqc +lat_ts=0 +lat_0=0 +lon_0=0 +x_0=0 +y_0=0 "
                 "+ellps=WGS84 +units=m +no_defs +type=crs",
                 "-tr", "500", "500", str(mtf),
                 str(tmp_path / "fake.tif")], check=True)
        sr.assert_has_calls([c1, c2])
        dem.region = "fribbulus xax"
        with pytest.raises(NotImplementedError):
            dem.store(object())

    @unittest.mock.patch("urllib.request.urlretrieve", autospec=True)
    @unittest.mock.patch("subprocess.run", autospec=True)
    @unittest.mock.patch("pkg_resources.resource_filename", autospec=True)
    def test_load(self, pr, sr, uru, dem, ts, tmp_path,
                  fake_process, fakearea, fakescene):
        pr.return_value = str(tmp_path / "fakedem.tif")
        fs = _mk_fakescene_realarea(
            fakearea,
            datetime.datetime(1899, 12, 31, 23, 55),
            "image")
        fs.save_dataset("image", str(tmp_path / "fakedem.tif"))
        sc2 = dem.load(ts)
        assert {did.name for did in sc2.keys()} == {"dem"}
        dem.location = pathlib.Path(tmp_path / "nodem.tif")
        with pytest.raises(ValueError):
            # will fail because files aren't there when mocking
            dem.load(ts)


class TestFog:
    def test_find(self, fog, ts):
        p = fog.find(ts, complete=False)
        assert p == {fog.base / "fog-19000101-0000.tif"}
        assert fog.find(pandas.Timestamp("2050-03-04"), complete=True) == set()

    @unittest.mock.patch("satpy.Scene")
    def test_store(self, sS, fog, abi, ts):
        fog.store(ts)
        sS.return_value.resample.return_value.save_dataset\
          .assert_called_once_with(
                   "fls_day", str(fog.base / "fog-19000101-0000.tif"))

    def test_load(self, fog, ts, fakearea):
        fs = _mk_fakescene_realarea(
            fakearea,
            datetime.datetime(1899, 12, 31, 23, 55),
            "foog")
        fp = fog.find(ts).pop()
        fs.save_dataset("foog", str(fp))
        sc = fog.load(ts)
        assert {did.name for did in sc.keys()} == {"fog"}


def test_contact_mi_dfs():
    from fogtools.db import _concat_mi_df_with_date
    mix1 = pandas.MultiIndex.from_arrays(
            [pandas.DatetimeIndex(
                ["1899-12-31T23:50:00"]*5 +
                ["1900-01-01T00:10:00"]*5),
             numpy.linspace(-90, 90, 10),
             numpy.linspace(-180, 180, 10)],
            names=["DATE", "LATITUDE", "LONGITUDE"])
    mix2 = pandas.MultiIndex.from_arrays(
            [pandas.DatetimeIndex(
                ["1899-12-31T23:55:00"]*5 +
                ["1900-01-01T00:05:00"]*5),
             numpy.linspace(-90, 90, 10),
             numpy.linspace(-180, 180, 10)],
            names=["DATE", "LATITUDE", "LONGITUDE"])
    mix3 = pandas.MultiIndex.from_arrays(
            [pandas.DatetimeIndex(
                ["1899-12-31T23:58:00"]*5 +
                ["1900-01-01T00:02:00"]*5),
             numpy.linspace(-90, 90, 10),
             numpy.linspace(-180, 180, 10)],
            names=["DATE", "LATITUDE", "LONGITUDE"])
    df1 = pandas.DataFrame(
            {"aubergine": numpy.arange(10),
             "banana": numpy.arange(10)/10},
            index=mix1)
    df2 = pandas.DataFrame(
            {"raspberry": numpy.arange(10)*2,
             "strawberry": numpy.arange(10)/5},
            index=mix2)
    df3 = pandas.DataFrame(
            {"coconut": numpy.arange(10)/2,
             "walnut": numpy.arange(10)*10},
            index=mix3)
    dfm = _concat_mi_df_with_date(df1, df2=df2, df3=df3)
    assert dfm.index.nlevels == 3
    assert dfm.index.names == ["DATE", "LATITUDE", "LONGITUDE"]
    numpy.testing.assert_array_equal(
            dfm.columns,
            ["aubergine", "banana", "date_df2", "raspberry", "strawberry",
             "date_df3", "coconut", "walnut"])
    assert dfm.shape == (10, 8)
