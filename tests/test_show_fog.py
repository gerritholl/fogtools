"""Test the show-fog script
"""

import os
from unittest.mock import patch
import pandas
import pytest


@patch("argparse.ArgumentParser", autospec=True)
def test_get_parser(ap):
    import fogtools.processing.show_fog
    fogtools.processing.show_fog.get_parser()
    assert ap.return_value.add_argument.call_count == 4


@patch("fogtools.processing.show_fog.get_parser", autospec=True)
@patch("fogtools.vis.get_fog_blend_from_seviri_nwcsaf", autospec=True)
def test_main(fvg, fpsg, tmpdir):
    import fogtools.processing.show_fog
    fpsg.return_value.parse_args.return_value.seviri = ["/no/seviri/files"]
    fpsg.return_value.parse_args.return_value.nwcsaf = ["/no/nwcsaf/files"]
    fpsg.return_value.parse_args.return_value.outfile = "/no/out/file"
    fpsg.return_value.parse_args.return_value.area = "fribbulux xax"
    fogtools.processing.show_fog.main()
    fpsg.assert_called_once_with()
    fvg.assert_called_once_with(
            ["/no/seviri/files"],
            ["/no/nwcsaf/files"],
            "fribbulux xax",
            "overview")
    fvg.return_value.save.assert_called_once_with("/no/out/file")
