"""Test the show-fog script
"""

from unittest.mock import patch, MagicMock


@patch("argparse.ArgumentParser", autospec=True)
def test_get_parser(ap):
    import fogtools.processing.show_fog
    fogtools.processing.show_fog.get_parser()
    assert ap.return_value.add_argument.call_count == 6


@patch("fogtools.processing.show_fog.parse_cmdline", autospec=True)
@patch("fogtools.vis.get_fog_blend_for_sat", autospec=True)
@patch("fogpy.composites.Scene", autospec=True)
def test_main(fcS, fvg, fpsp, tmpdir):
    import fogtools.processing.show_fog
    fpsp.return_value = fogtools.processing.show_fog.get_parser().parse_args(
            ["/no/out/file",
             "--sat", "/no/sat/files",
             "--nwcsaf", "/no/nwcsaf/files",
             "-a", "fribbulux xax",
             "-m", "seviri_l1b_hrit"])
    fogtools.processing.show_fog.main()
    fvg.assert_called_once_with(
            "seviri_l1b_hrit",
            ["/no/sat/files"],
            ["/no/nwcsaf/files"],
            "fribbulux xax",
            "overview",
            return_extra=False)
    fvg.return_value.save.assert_called_once_with("/no/out/file")
    fvg.reset_mock()
    fpsp.return_value = fogtools.processing.show_fog.get_parser().parse_args(
            ["/no/out/file",
             "--sat", "/no/sat/files",
             "--nwcsaf", "/no/nwcsaf/files",
             "-a", "fribbulux xax",
             "-m", "seviri_l1b_hrit",
             "-e", "shadowlands.nc"])
    fvg.return_value = (MagicMock(), MagicMock())
    fogtools.processing.show_fog.main()
    fcS.return_value.save_datasets.assert_called_once_with(
            writer="cf",
            datasets=fvg.return_value[1].__getitem__
                        .return_value.data_vars.keys.return_value,
            filename="shadowlands.nc")
