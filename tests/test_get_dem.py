"""Test the get-dem script
"""

from unittest.mock import patch
import pathlib


@patch("argparse.ArgumentParser", autospec=True)
def test_get_parser(ap):
    import fogtools.processing.get_dem
    fogtools.processing.get_dem.get_parser()
    assert ap.return_value.add_argument.call_count == 3


@patch("fogtools.processing.get_dem.get_parser", autospec=True)
@patch("urllib.request.urlretrieve", autospec=True)
def test_main(uru, fpgg, tmpdir):
    import fogtools.processing.get_dem
    fpgg.return_value.parse_args.return_value.latrange = [40, 45]
    fpgg.return_value.parse_args.return_value.lonrange = [-75, -70]
    ptd = pathlib.Path(tmpdir)
    fpgg.return_value.parse_args.return_value.outdir = ptd
    fogtools.processing.get_dem.main()
    assert uru.call_count == 5*5*4
