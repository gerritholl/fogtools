"""Test the show-fog script
"""

from unittest.mock import patch, MagicMock

import xarray


@patch("argparse.ArgumentParser", autospec=True)
def test_get_parser(ap):
    import fogtools.processing.show_fog
    fogtools.processing.show_fog.get_parser()
    assert ap.return_value.add_argument.call_count == 4
    assert ap.return_value.add_mutually_exclusive_group.call_count == 2
    assert ap.return_value.add_mutually_exclusive_group.return_value.\
        add_argument.call_count == 4


@patch("fogtools.processing.show_fog.parse_cmdline", autospec=True)
@patch("fogtools.vis.get_fog_blend_for_sat", autospec=True)
@patch("fogpy.composites.Scene", autospec=True)
def test_main(fcS, fvg, fpsp, tmp_path, xrda):
    import fogtools.processing.show_fog
    from satpy import Scene, DatasetID
    fpsp.return_value = fogtools.processing.show_fog.get_parser().parse_args(
            ["/no/out/file",
             "--seviri", "/no/sat/files",
             "--nwcsaf", "/no/nwcsaf/files",
             "-a", "fribbulus xax"])
    m_im = MagicMock()
    m_sc = MagicMock()
    f_sc = Scene()
    f_sc[DatasetID("raspberry")] = xrda[0]
    f_sc[DatasetID("banana")] = xrda[1]
    f_sc[DatasetID("fls_day_extra")] = xarray.Dataset(
            {"a": xrda[0], "b": xrda[1]})
    m_sc.__getitem__.side_effect = f_sc.__getitem__
    m_sc.keys.side_effect = f_sc.keys
    fvg.return_value = (m_im, m_sc)
    fogtools.processing.show_fog.main()
    fvg.assert_called_once_with(
            "seviri_l1b_hrit",
            ["/no/sat/files"],
            "nwcsaf-geo",
            ["/no/nwcsaf/files"],
            "fribbulus xax",
            "overview")
    fvg.return_value[0].save.assert_called_once_with("/no/out/file")
    fvg.reset_mock()
    fvg.return_value[0].reset_mock()
    fpsp.return_value = fogtools.processing.show_fog.get_parser().parse_args(
            [str(tmp_path),
             "--abi", "/no/sat/files",
             "--cmsaf", "/no/nwcsaf/files",
             "-a", "fribbulus xax",
             "-i"])
    fogtools.processing.show_fog.main()
    fvg.return_value[0].save.assert_called_once_with(
            str(tmp_path / "fog_blend.tif"))
    fcS.return_value.save_datasets.assert_called_once_with(
            writer="cf",
            datasets={"a", "b"},
            filename=str(tmp_path / "intermediates.nc"))
    fvg.reset_mock()
    fvg.return_value[0].reset_mock()
    fcS.return_value.save_datasets.reset_mock()
    fpsp.return_value = fogtools.processing.show_fog.get_parser().parse_args(
            [str(tmp_path),
             "--seviri", "/no/sat/files",
             "--nwcsaf", "/no/nwcsaf/files",
             "-a", "fribbulus xax",
             "-d"])

    fogtools.processing.show_fog.main()
    m_sc.save_datasets.assert_called_once_with(
            filename=str(tmp_path / "{name:s}.tif"),
            datasets={"raspberry", "banana"})
