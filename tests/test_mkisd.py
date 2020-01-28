"""Test the mkisd script
"""

from unittest.mock import patch


@patch("argparse.ArgumentParser", autospec=True)
def test_get_parser(ap):
    import fogtools.processing.mkisd
    fogtools.processing.mkisd.get_parser()
    assert ap.return_value.add_argument.call_count == 3


@patch("fogtools.processing.mkisd.get_parser", autospec=True)
@patch("fogtools.isd.create_db", autospec=True)
def test_main(cd, pc):
    import fogtools.processing.mkisd
    pc.return_value.parse_args.return_value.out = "tofu"
    pc.return_value.parse_args.return_value.start = "19000101"
    pc.return_value.parse_args.return_value.end = "19191231"
    fogtools.processing.mkisd.main()
    pc.assert_called_once_with()
    cd.assert_called_once_with("tofu", "19000101", "19191231")
