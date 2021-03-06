"""Test the fogfreq script
"""

from unittest.mock import patch


@patch("argparse.ArgumentParser", autospec=True)
def test_get_parser(ap):
    import fogtools.analysis.fogrank
    fogtools.analysis.fogrank.get_parser()
    assert ap.return_value.add_argument.call_count == 6


@patch("fogtools.isd.read_db")
def test_main(fir, gb_db):
    import fogtools.analysis.fogrank
    fargp = fogtools.analysis.fogrank.get_parser().parse_args(
            ["-n", "5", "-v", "1000", "-p", "D", "-s", "D",
             "-a", "70", "-f", "csv"])
    fir.return_value = gb_db
    with patch("fogtools.analysis.fogrank.get_parser") as fafg:
        fafg.return_value.parse_args.return_value = fargp
        fogtools.analysis.fogrank.main()
    fargp = fogtools.analysis.fogrank.get_parser().parse_args(
            ["-n", "5", "-v", "1000", "-p", "D", "-s", "D",
             "-a", "70", "-f", "markdown"])
    with patch("fogtools.analysis.fogrank.get_parser") as fafg:
        fafg.return_value.parse_args.return_value = fargp
        fogtools.analysis.fogrank.main()
