"""Test the show-fog script
"""

from unittest.mock import patch, MagicMock


@patch("argparse.ArgumentParser", autospec=True)
def test_get_parser(ap):
    import fogtools.processing.show_fog
    fogtools.processing.show_fog.get_parser()
    assert ap.return_value.add_argument.call_count == 6


@patch("fogtools.processing.show_fog.get_parser", autospec=True)
@patch("fogtools.vis.get_fog_blend_from_seviri_nwcsaf", autospec=True)
@patch("fogpy.composites.Scene", autospec=True)
def test_main(fcS, fvg, fpsg, tmpdir):
    import fogtools.processing.show_fog
    fpsg.return_value.parse_args.return_value.seviri = ["/no/seviri/files"]
    fpsg.return_value.parse_args.return_value.nwcsaf = ["/no/nwcsaf/files"]
    fpsg.return_value.parse_args.return_value.outfile = "/no/out/file"
    fpsg.return_value.parse_args.return_value.area = "fribbulux xax"
    fpsg.return_value.parse_args.return_value.extra = None
    fogtools.processing.show_fog.main()
    fpsg.assert_called_once_with()
    fvg.assert_called_once_with(
            ["/no/seviri/files"],
            ["/no/nwcsaf/files"],
            "fribbulux xax",
            "overview",
            return_extra=False)
    fvg.return_value.save.assert_called_once_with("/no/out/file")
    fvg.reset_mock()
    fpsg.return_value.parse_args.return_value.extra = "shadowlands.nc"
    fvg.return_value = (MagicMock(), MagicMock())
    fogtools.processing.show_fog.main()
    fcS.return_value.save_datasets.assert_called_once_with(
            writer="cf",
            datasets=fvg.return_value[1].__getitem__
                        .return_value.data_vars.keys.return_value,
            filename="shadowlands.nc")
