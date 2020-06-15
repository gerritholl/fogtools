"""Test the build-db script
"""

import pathlib
from unittest.mock import patch

import pandas


@patch("argparse.ArgumentParser", autospec=True)
def test_get_parser(ap):
    import fogtools.processing.build_db
    fogtools.processing.build_db.get_parser()
    assert ap.return_value.add_argument.call_count == 4


@patch("fogtools.processing.build_db.parse_cmdline", autospec=True)
@patch("fogtools.db.FogDB", autospec=True)
def test_main(fdF, fpbp, station, tmp_path):
    import fogtools.processing.build_db
    fpbp.return_value = fogtools.processing.build_db.get_parser().parse_args(
            ["/no/out/file",
             "--date", "198508131515"])
    fogtools.processing.build_db.main()
    fdF.return_value.extend.assert_called_with(
            pandas.Timestamp("198508131515"))
    fdF.return_value.store.assert_called_with(
            pathlib.Path("/no/out/file"))
    fpbp.return_value = fogtools.processing.build_db.get_parser().parse_args(
            ["/no/out/file",
             "--top-n", "1"])
    with patch("fogtools.isd.read_db") as fir:
        fir.return_value = station
        fogtools.processing.build_db.main()
    fdF.return_value.extend.assert_called_with(
            pandas.Timestamp("201901052200"), onerror="log")
    fdF.return_value.store.assert_called_with(
            pathlib.Path("/no/out/file"))
