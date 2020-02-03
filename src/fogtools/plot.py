"""Various plotting utilities
"""

import datetime
import pathlib
import matplotlib.pyplot
from typhon.plots.common import write_multi
from . import isd


class Visualiser:
    def __init__(self):
        self.df = isd.read_db()

    def plot_fog_frequency(self):
        (f, a) = matplotlib.pyplot.subplots()
        for i in [250, 500, 750, 1000]:
            cnt = isd.count_fogs_per_day(self.df, i)
            vc = cnt.value_counts()
            # breakpoint()
            # need to compute before I can sort, but the value counts
            # will be small anyway
            try:
                vc = vc.compute()
            except AttributeError:  # not dask
                pass
            vc.sort_index(ascending=False).cumsum().sort_index().plot(
                    kind="line", label=f"vis < {i:d}", ax=a)
        a.set_xlabel("No. stations")
        a.set_ylabel("No. days")
        a.grid(which="major", axis="both")
        a.legend()
    #    a.set_xlim([0, 30])
    #    a.set_ylim([0, 150])
        write_multi(f, plotdir() / "fogs_per_day")

    def plot_fog_dt_hist(self):
        """2-D histogram of visibility - delta-temperature

        Plot a 2-dimensional histogram (hexbin) of visibility and
        the difference between temperature and dewpoint
        """

        (f, a) = matplotlib.pyplot.subplots()
        m = a.hexbin(
                self.df["vis"],
                self.df["temp"] - self.df["dew"],
                mincnt=1,
                extent=[0, 10000, 0, 10],
                norm=matplotlib.colors.LogNorm())
        f.colorbar(m)
        a.set_xlabel("Visibility [m]")
        a.set_ylabel("T - T_d [K]")
        a.set_title(r"Joint distribution visibility vs. \Delta dewpoint")
        write_multi(f, plotdir() / "dewpoint_2dhist")


def plotdir():
    pd = pathlib.Path("/media/nas/x21308/plots_and_maps/" +
                      datetime.datetime.now().strftime("%Y/%m/%d"))
    pd.mkdir(exist_ok=True, parents=True)
    return pd
