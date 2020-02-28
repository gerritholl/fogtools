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
@patch("fogtools.sky.get_and_send", autospec=True)
def test_main(fsg, fpgg, tmpdir):
    import fogtools.processing.get_nwp
    fpgg.return_value.parse_args.return_value.date = pandas.Timestamp(
            "19000101120000")
    with pytest.raises(SystemExit):
        fogtools.processing.get_nwp.main()
    os.environ["SAFNWC"] = str(tmpdir)
    fpgg.reset_mock()
    fogtools.processing.get_nwp.main()
    fpgg.assert_called_once_with()
    fsg.assert_called_once_with(str(tmpdir), pandas.Timestamp(
        "19000101120000"))
