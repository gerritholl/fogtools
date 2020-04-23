import pathlib
import functools
import subprocess
import unittest.mock
import logging

import numpy.testing
import pandas
import pytest
import xarray
import satpy


# TODO:
#   - test interface into .extract: it's going to fail for those databases
#     where results are not a Scene, but the unit tests are currently not showing
#     that

@pytest.fixture(scope="function")
def db():
    import fogtools.db
    return fogtools.db.FogDB()


@pytest.fixture
def ts():
    return pandas.Timestamp("1900-01-01T00:00:00Z")


def _dbprep(tmp_path, cls, *args, **kwargs):
    import fogtools.db
    db = getattr(fogtools.db, cls)(*args, **kwargs)
    db.base = tmp_path / str(cls)
    return db


@pytest.fixture
def fakescene():
    # let's make a Scene
    #
    # should I mock get_xy_from_lonlat here?  Probably as it's an external
    # dependency that I can assume to be correct, and it simplifies the
    # unit test here.
    sc = satpy.Scene()
    sc["raspberry"] = xarray.DataArray(
            numpy.arange(25).reshape(5, 5),
            attrs={"area": unittest.mock.MagicMock(),
                   "name": "raspberry"})
    sc["cloudberry"] = xarray.DataArray(
            numpy.arange(25).reshape(5, 5),
            attrs={"area": unittest.mock.MagicMock(),
                   "name": "cloudberry"})
    sc["raspberry"].attrs["area"].get_xy_from_lonlat.return_value = (
            numpy.array([1, 1]), numpy.array([1, 2]))
    sc["cloudberry"].attrs["area"].get_xy_from_lonlat.return_value = (
            numpy.array([2, 2]), numpy.array([2, 3]))
    return sc


@pytest.fixture
def fake_df():
    df =  pandas.DataFrame(
            {"DATE": (dr:=pandas.date_range("18991231T12", "19000101T12",
                freq="15min", tz="UTC")),
             "LATITUDE": numpy.linspace(-89, 89, dr.size),
             "LONGITUDE": numpy.linspace(-179, 179, dr.size),
             "values": numpy.empty(shape=dr.size)})
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
    return _dbprep(tmp_path, "_Fog", dependencies=
            {"sat": abi, "dem": dem, "cmic": nwcsaf})


def test_init(db):
    assert db.sat is not None
    assert db.fog is not None


@unittest.mock.patch("fogtools.isd.read_db", autospec=True)
def test_extend(fir, db, fake_df, ts, caplog):
    db.sat = unittest.mock.MagicMock()
    db.nwp = unittest.mock.MagicMock()
    db.cmic = unittest.mock.MagicMock()
    db.dem = unittest.mock.MagicMock()
    db.fog = unittest.mock.MagicMock()
    fir.return_value = fake_df
    gd = db.ground.load(ts)
    db.sat.extract.return_value = _mkdf(gd.index, "raspberry", "banana")
    db.nwp.extract.return_value = _mkdf(gd.index, "apricot", "pineapple")
    db.cmic.extract.return_value = _mkdf(gd.index, "peach", "redcurrant")
    db.dem.extract.return_value = _mkdf(gd.index, "damson", "prune")
    db.fog.extract.return_value = _mkdf(gd.index, "aubergine", "shallot")
    with caplog.at_level(logging.INFO):
        db.extend(ts)
        assert "Loading data for 1900-01-01 00:00:00" in caplog.text
    assert sorted(db.data.columns) == [
            "apricot", "aubergine", "banana", "damson", "peach", "pineapple",
            "prune", "raspberry", "redcurrant", "shallot", "values"]
    assert db.data.shape == (5, 11)
    db.fog.extract.return_value = _mkdf(gd.index[:3], "aubergine", "shallot")
    db.extend(ts)
    assert db.data.shape == (10, 11)
    # TODO: this needs to test tolerances too, and now it has nans, will that
    # happen in the real world?  Perhaps.


def test_store(db, fake_df, tmp_path):
    with pytest.raises(ValueError):
        db.store(tmp_path / "nope.parquet")
    db.data = fake_df
    db.store(tmp_path / "yes.parquet")


class TestABI:
    def test_get_path(self, abi, ts):
        with pytest.raises(NotImplementedError):
            abi.get_path(ts)
        abi._generated[ts] = [pathlib.Path("/banana")]
        assert abi.get_path(ts) == [pathlib.Path("/banana")]

    @staticmethod
    def _mk(abi):
        """Make some files in abi.base

        This should ensure that abi.exists(...) returns True.
        """
        d = abi.base / "abi" / "1900" / "01" / "01" / "00" / "00"
        for c in range(1, 17):
            f = (d / f"{c:>01d}" / f"OR_ABI-L1b-RadF-M3C{c:>02d}_G16_"
                 "s19000010000000_e19000010000000_c19000010000000.nc")
            f.parent.mkdir(parents=True)
            f.touch()

    def test_exists(self, abi, ts):
        import fogtools.db
        assert not abi.exists(ts)
        self._mk(abi)
        assert abi.exists(ts)
        d = abi.base / "abi" / "1900" / "01" / "01" / "00" / "00"
        (d / "3" / f"OR_ABI-L1b-RadF-M3C03_G16_"
         "s19000010000000_e19000010000000_c19000010000000.nc").touch()
        (d / "3" / f"OR_ABI-L1b-RadF-M3C03_G16_"
         "s19000010000000_e19000010000000_c19000010000000b.nc").touch()
        with pytest.raises(fogtools.db.FogDBError):
            abi.exists(ts)

    @unittest.mock.patch("fogtools.abi.download_abi_day", autospec=True)
    def test_store(self, fad, abi, ts):
        fad.return_value = [pathlib.Path("/pineapple")]
        abi.store(ts)
        assert abi._generated[ts] == [pathlib.Path("/pineapple")]

    @unittest.mock.patch("fogtools.abi.download_abi_day", autospec=True)
    @unittest.mock.patch("satpy.Scene", autospec=True)
    def test_load(self, sS, fad, abi, ts, tmp_path):
        fad.return_value = [
                (tmp_path / f"OR_ABI-L1b-RadF-M3C{c:>02d}_G16_"
                    f"s1900{i:>03d}0000000_e1900{i:>03d}0000000_"
                    f"c1900{i:>03d}0000000.nc")
                for c in {10, 11}
                for i in {1, 2, 3}]
        abi.load(ts)
        sS.assert_called_with(
                filenames=[
                    (tmp_path / f"OR_ABI-L1b-RadF-M3C{c:>02d}_G16_"
                        f"s1900{i:>03d}0000000_e1900{i:>03d}0000000_"
                        f"c1900{i:>03d}0000000.nc")
                    for c in {10, 11}
                    for i in {1}],
                reader="abi_l1b")

    # test concrete methods defined in base class here, as far as not
    # overwritten by _ABI or trivial (such as ensure_deps)
    @unittest.mock.patch("fogtools.abi.download_abi_day", autospec=True)
    def test_ensure(self, fad, abi, ts):
        abi.ensure(ts)
        fad.assert_called_once_with(ts)
        self._mk(abi)
        abi.ensure(ts)
        # make sure it wasn't called again
        fad.assert_called_once_with(ts)

    def test_link(self, abi, ts):
        abi.link(None, None)  # this doesn't do anything

    def test_extract(self, abi, ts, fakescene):
        # TODO: this needs to test what happens if lat/lon are out of the area.
        # What does pyresample do here?  NAN or exception?
        abi.load = unittest.mock.MagicMock()
        abi.load.return_value = fakescene
        df = abi.extract(ts, numpy.array([10, 10]), numpy.array([10, 15]))
        numpy.testing.assert_array_equal(
                df.columns,
                ["raspberry", "cloudberry"])
        numpy.testing.assert_array_equal(df["raspberry"], [6, 7])
        numpy.testing.assert_array_equal(df["cloudberry"], [12, 13])
        numpy.testing.assert_array_equal(df.index.get_level_values("LATITUDE"), [10, 10])
        numpy.testing.assert_array_equal(df.index.get_level_values("LONGITUDE"), [10, 15])
        numpy.testing.assert_array_equal(df.index.get_level_values("DATE"), [ts, ts])


class TestICON:
    def test_get_path(self, icon, ts):
        ps = icon.get_path(ts)

        assert ps == {icon.base / "import" / "NWP_data" /
                      f"S_NWC_NWP_1900-01-01T00:00:00Z_{i:>03d}.grib"
                      for i in range(6)}

    @unittest.mock.patch("fogtools.sky.get_and_send", autospec=True)
    def test_store(self, fsg, icon, ts, tmp_path):
        fsg.return_value = [tmp_path / "pear"]
        icon.store(ts)
        assert icon._generated[ts] == [tmp_path / "pear"]

    # concrete methods from parent class
    def test_exists(self, icon, ts):
        for f in {icon.base / "import" / "NWP_data" /
                  f"S_NWC_NWP_1900-01-01T00:00:00Z_{i:>03d}.grib"
                  for i in range(5)}:
            f.parent.mkdir(parents=True, exist_ok=True)
            f.touch()
        assert not icon.exists(ts)
        (icon.base / "import" / "NWP_data" /
         "S_NWC_NWP_1900-01-01T00:00:00Z_005.grib").touch()
        assert icon.exists(ts)

    @unittest.mock.patch("satpy.Scene", autospec=True)
    def test_load(self, sS, icon, ts):
        icon.load(ts)
        sS.assert_called_once_with(
                filenames={
                    icon.base / "import" / "NWP_data" /
                    f"S_NWC_NWP_1900-01-01T00:00:00Z_{i:>03d}.grib"
                    for i in range(6)},
                reader="grib")


class TestNWCSAF:
    def test_get_path(self, nwcsaf, ts):
        ps = nwcsaf.get_path(ts)
        assert ps == [(nwcsaf.base / "1900" / "01" / "01"
                      / "S_NWC_CMIC_GOES16_NEW-ENGLAND-NR_"
                      "19000101T000000Z.nc")]

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
        import fogtools.db
        monkeypatch.setenv("SAFNWC", str(tmp_path))
        p = nwcsaf._get_dep_loc(abi)
        assert p == tmp_path / "import" / "Sat_data"
        p = nwcsaf._get_dep_loc(icon)
        assert p == tmp_path / "import" / "NWP_data"
        with pytest.raises(TypeError):
            nwcsaf._get_dep_loc(object())
        monkeypatch.delenv("SAFNWC")
        with pytest.raises(fogtools.db.FogDBError):
            nwcsaf._get_dep_loc(abi)

    def test_link(self, nwcsaf, abi, icon, ts, monkeypatch, tmp_path):
        monkeypatch.setenv("SAFNWC", str(tmp_path))
        abi._generated[ts] = [tmp_path / "abi" / "abi.nc"]
        nwcsaf.link(abi, ts)
        exp = tmp_path / "import" / "Sat_data" / "abi.nc"
        assert exp.is_symlink()
        assert exp.resolve() == tmp_path / "abi" / "abi.nc"
        nwcsaf.link(icon, ts)
        exp = (tmp_path / "import" / "NWP_data" /
               "S_NWC_NWP_1900-01-01T00:00:00Z_002.grib")
        assert exp.is_symlink()
        assert (exp.resolve() == icon.base / "import" / "NWP_data" /
                "S_NWC_NWP_1900-01-01T00:00:00Z_002.grib")

    @unittest.mock.patch("time.sleep", autospec=True)
    def test_wait_for_output(self, tisl, nwcsaf, ts, monkeypatch, tmp_path):
        import fogtools.db
        monkeypatch.setenv("SAFNWC", str(tmp_path))
        nwcsaf.is_running = unittest.mock.MagicMock()
        nwcsaf.is_running.return_value = False
        with pytest.raises(fogtools.db.FogDBError):
            nwcsaf.wait_for_output(ts)
        p = nwcsaf.get_path(ts)[0]
        p.parent.mkdir(exist_ok=True, parents=True)
        p.touch()
        nwcsaf.is_running.return_value = True
        nwcsaf.wait_for_output(ts)
        assert tisl.call_count == 0
        p.unlink()
        with pytest.raises(fogtools.db.FogDBError):
            nwcsaf.wait_for_output(ts)
            assert tisl.call_count == 60
        nwcsaf.is_running.return_value = False

    def test_ensure(self, nwcsaf, ts):
        nwcsaf.wait_for_output = unittest.mock.MagicMock()
        nwcsaf.ensure(ts)
        nwcsaf.wait_for_output.assert_called_once_with(ts)

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
    def test_get_path(self, synop, monkeypatch, tmp_path):
        monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
        p = synop.get_path(ts)
        assert p == tmp_path / "fogtools" / "store.parquet"

    @unittest.mock.patch("fogtools.isd.read_db", autospec=True)
    def test_load(self, fir, synop, ts, fake_df):
        fir.return_value = fake_df
        sel = synop.load(ts, tol=pandas.Timedelta("31min"))
        assert sel.shape == (5, 1)
        assert sel.index.get_level_values("DATE")[0] == pandas.Timestamp("18991231T2330Z")
        assert sel.index.get_level_values("DATE")[-1] == pandas.Timestamp("19000101T0030Z")
        assert sel.index.names == ["DATE", "LATITUDE", "LONGITUDE"]
        sel2 = synop.load(ts, tol=pandas.Timedelta("31min"))
        assert sel.equals(sel2)
        synop._db = None
        fir.return_value = fake_df.reset_index()
        sel3 = synop.load(ts, tol=pandas.Timedelta("31min"))
        assert sel.equals(sel3)

    @unittest.mock.patch("fogtools.isd.create_db", autospec=True)
    def test_store(self, fic, synop):
        synop.store(object())
        fic.assert_called_once_with()


class TestDEM:
    def test_get_path(self, dem):
        p = dem.get_path(object())
        assert p == pathlib.Path("/media/nas/x21308/DEM/USGS/merged-500.tif")

    @unittest.mock.patch("urllib.request.urlretrieve", autospec=True)
    @unittest.mock.patch("subprocess.run", autospec=True)
    @unittest.mock.patch("tempfile.NamedTemporaryFile", autospec=True)
    def test_store(self, tN, sr, uru, dem, tmp_path):
        dem.location = dem.dem_new_england = tmp_path / "fake.tif"
        mtf = tmp_path / "raspberry"
        tN.return_value.__enter__.return_value.name = str(tmp_path
                / "raspberry")
        dem.store(object())
        assert sr.call_count == 2
        c1 = unittest.mock.call(["gdal_merge.py", "-o", str(mtf)] +
                [str(mtf.parent / f"n{lat:>02d}w{lon:>03d}" / (("" if ext=="gpkg" else "USGS_1_") +
                         f"n{lat:>02d}w{lon:>03d}.{ext:s}"))
                    for lat in range(38, 49)
                    for lon in range(82, 66, -1)
                    for ext in ["tif", "jpg", "xml", "gpkg"]],
                check=True)
        c2 = unittest.mock.call(["gdalwarp", "-r", "bilinear", "-t_srs",
            "+proj=eqc +lat_ts=0 +lat_0=0 +lon_0=0 +x_0=0 +y_0=0 +ellps=WGS84 +units=m "
            "+no_defs +type=crs", "-tr", "500", "500", str(mtf), str(tmp_path /
            "fake.tif")], check=True)
        sr.assert_has_calls([c1, c2])
        dem.location = dem.dem_europe = "fribbulus xax"
        with pytest.raises(NotImplementedError):
            dem.store(object())


class TestFog:
    def test_get_path(self, fog, ts):
        p = fog.get_path(ts)
        assert p == [fog.base / "fog-19000101-0000.tif"]

    @unittest.mock.patch("satpy.Scene")
    def test_store(self, sS, fog, abi, ts):
        abi._generated[ts] = [pathlib.Path("/banana")]
        fog.store(ts)
        sS.return_value.resample.return_value.save_dataset.assert_called_once_with(
                "fls_day",
                fog.base / "fog-19000101-0000.tif")
