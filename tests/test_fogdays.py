"""Test the fogfreq script
"""

from unittest.mock import patch
import pytest


@patch("argparse.ArgumentParser", autospec=True)
def test_get_parser(ap):
    import fogtools.analysis.fogrank
    fogtools.analysis.fogrank.get_parser()
    assert ap.return_value.add_argument.call_count == 3


@patch("fogtools.analysis.fogrank.get_parser")
@patch("fogtools.isd.read_db")
def test_main(fir, gp, db):
    import fogtools.analysis.fogrank
    gp.return_value.parse_args.return_value.n = 5
    gp.return_value.parse_args.return_value.v = 1000
    gp.return_value.parse_args.return_value.f = "csv"
    fir.return_value = db
    fogtools.analysis.fogrank.main()
    gp.return_value.parse_args.return_value.f = "markdown"
    fogtools.analysis.fogrank.main()
    gp.return_value.parse_args.return_value.f = "invalid"
    with pytest.raises(ValueError):
        fogtools.analysis.fogrank.main()
