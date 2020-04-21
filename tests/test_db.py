import pathlib
import unittest.mock

import numpy.testing
import pandas
import pytest
import xarray
import satpy


@pytest.fixture(scope="function")
def db():
    import fogtools.db
    return fogtools.db.FogDB()


@pytest.fixture
def ts():
    return pandas.Timestamp("1900-01-01T00:00:00Z")


@pytest.fixture
def abi(tmp_path):
    import fogtools.db
    abi = fogtools.db._ABI()
    abi.base = tmp_path
    return abi


@pytest.fixture
def icon(tmp_path):
    import fogtools.db
    icon = fogtools.db._ICON()
    icon.base = tmp_path
    return icon


def test_init(db):
    assert db.sat is not None
    assert db.fog is not None


@pytest.mark.skip("not implemented yet")
def test_extend(db):
    ts = pandas.Timestamp("1900-01-01T00:00:00Z")
    db.extend(ts)


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

    def test_extract(self, abi, ts):
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
        abi.load = unittest.mock.MagicMock()
        abi.load.return_value = sc
        df = abi.extract(ts, numpy.array([10, 10]), numpy.array([10, 15]))
        numpy.testing.assert_array_equal(
                df.columns,
                ["raspberry", "cloudberry"])
        numpy.testing.assert_array_equal(df["raspberry"], [6, 7])
        numpy.testing.assert_array_equal(df["cloudberry"], [12, 13])


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


# concrete methods in abstract class still to be tested:
#   - ensure_deps
