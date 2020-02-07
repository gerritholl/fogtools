"""Test the fogfreq script
"""

from unittest.mock import patch


@patch("argparse.ArgumentParser", autospec=True)
def test_get_parser(ap):
    import fogtools.analysis.foghist2d
    fogtools.analysis.foghist2d.get_parser()
    assert ap.return_value.add_argument.call_count == 1


@patch("fogtools.analysis.foghist2d.get_parser", autospec=True)
@patch("fogtools.plot.Visualiser", autospec=True)
def test_main(v, pc):
    import fogtools.analysis.foghist2d
    pc.return_value.parse_args.return_value.out = "tofu"
    fogtools.analysis.foghist2d.main()
    pc.assert_called_once_with()
    v.return_value.plot_fog_dt_hist.assert_called_once_with("tofu")
