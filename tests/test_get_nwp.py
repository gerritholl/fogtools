"""Test the mkisd script
"""

import os
from unittest.mock import patch
import pandas
import pytest


@patch("argparse.ArgumentParser", autospec=True)
def test_get_parser(ap):
    import fogtools.processing.get_nwp
    fogtools.processing.get_nwp.get_parser()
    assert ap.return_value.add_argument.call_count == 1


@patch("fogtools.processing.get_nwp.get_parser", autospec=True)
@patch("subprocess.run", autospec=True)
def test_main(sr, fpgg, tmpdir):
    import fogtools.processing.get_nwp
    from fogtools.sky import SkyFailure
    fpgg.return_value.parse_args.return_value.date = pandas.Period(
            "19000101120000")
    os.environ.pop("SAFNWC", None)
    with pytest.raises(SystemExit):
        fogtools.processing.get_nwp.main()
    os.environ["SAFNWC"] = str(tmpdir)
    fpgg.reset_mock()
    # expecting failure because files not actually written
    with pytest.raises(SkyFailure):
        fogtools.processing.get_nwp.main()
    fpgg.assert_called_once_with()
    sr.assert_called_once()
