"""Test the dlabi script
"""

import pandas
from unittest.mock import patch


@patch("argparse.ArgumentParser", autospec=True)
def test_get_parser(ap):
    import fogtools.processing.dlabi
    fogtools.processing.dlabi.get_parser()
    assert ap.return_value.add_argument.call_count == 3


@patch("fogtools.processing.dlabi.get_parser", autospec=True)
@patch("fogtools.abi.download_abi_day", autospec=True)
def test_main(fad, fpdg):
    import fogtools.processing.dlabi
    fpdg.return_value.parse_args.return_value.date = pandas.Timestamp(
            "1900-01-01")
    fpdg.return_value.parse_args.return_value.channels = [1, 2, 3]
    fpdg.return_value.parse_args.return_value.types = "CF"
    fogtools.processing.dlabi.main()
    fad.assert_called_once_with(
            pandas.Timestamp("1900-01-01"), [1, 2, 3], "CF")
