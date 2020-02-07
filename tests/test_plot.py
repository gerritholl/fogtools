from unittest import mock
import pytest


@pytest.fixture
def v(db):
    from fogtools.plot import Visualiser
    with mock.patch("fogtools.isd.read_db") as fir:
        fir.return_value = db
        vi = Visualiser()
    return vi


@mock.patch("matplotlib.pyplot.subplots", autospec=True)
@mock.patch("fogtools.plot.write_multi", autospec=True)
@mock.patch("fogtools.isd.count_fogs_per_day", autospec=True)
def test_plot_fog_freq(fic, tpcw, mps, v):
    (f, a, vc) = (mock.MagicMock(), mock.MagicMock(), mock.MagicMock())
    fic.return_value.value_counts.return_value = vc
    mps.return_value = (f, a)
    fic.return_value.value_counts.return_value.compute.side_effect = \
        AttributeError
    v.plot_fog_frequency()
    assert fic.return_value.value_counts.return_value.sort_index.\
        return_value.cumsum.return_value.sort_index.return_value.\
        plot.call_count == 4
    assert vc.sort_index.return_value.cumsum.return_value.sort_index.\
        return_value.plot.call_count == 4


@mock.patch("matplotlib.pyplot.subplots", autospec=True)
@mock.patch("fogtools.plot.write_multi", autospec=True)
def test_plot_fog_dt_hist(tpcw, mps, v):
    (f, a) = (mock.MagicMock(), mock.MagicMock())
    mps.return_value = (f, a)
    v.plot_fog_dt_hist()
    assert a.hexbin.called_once()
