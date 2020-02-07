"""Test the fogfreq script
"""

from unittest.mock import patch


@patch("argparse.ArgumentParser", autospec=True)
def test_get_parser(ap):
    import fogtools.analysis.fogfreq
    fogtools.analysis.fogfreq.get_parser()
    assert ap.return_value.add_argument.call_count == 1


@patch("fogtools.analysis.fogfreq.get_parser", autospec=True)
@patch("fogtools.plot.Visualiser", autospec=True)
def test_main(v, pc):
    import fogtools.analysis.fogfreq
    pc.return_value.parse_args.return_value.out = "tofu"
    fogtools.analysis.fogfreq.main()
    pc.assert_called_once_with()
    v.return_value.plot_fog_frequency.assert_called_once_with("tofu")
