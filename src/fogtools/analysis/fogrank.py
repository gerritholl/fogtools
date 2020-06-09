"""Find days on which fog most common in New England.

Return a table of days on which fog is most common in New England
"""

import argparse
from .. import isd


def get_parser():
    parser = argparse.ArgumentParser(
            description=__doc__,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
            "-n", action="store", type=int,
            default=20, help="Write top n days")

    parser.add_argument(
            "-v", action="store", type=int,
            default=1000, help="Max visibility in metre")

    parser.add_argument(
            "-p", action="store", type=str,
            default="D", help="Frequency (pandas freq. string) for grouping")

    parser.add_argument(
            "-f", action="store", choices=["markdown", "csv"],
            default="markdown",
            help="How to present output")

    return parser


def print_fogs(top_n, vis, freq, form):
    """Display the most common fog time periods.

    Display to stdout a table of the top_n time periods at which the
    largest number of stations reported fog, as defined by reporting a
    visibility less than vis, grouped by periods of period, reported in
    form form.

    Args:
        top_n (int): Number to report.
        vis (number): Max visibility to consider.
        freq (str or Offset): Frequency to count fogs.
        form (str): Form to write, can be "markdown" or "csv".
    """
    df = isd.read_db()
    cnt = isd.count_fogs_per_time(df, freq, vis)
    selec = cnt.sort_values(ascending=False)[:top_n]
    if form == "markdown":
        print(selec.to_markdown(), end="\n")
    elif form == "csv":
        print(selec.to_csv(), end="")
    else:
        raise ValueError(f"Invalid format: {form:s}")


def main():
    p = get_parser().parse_args()
    print_fogs(p.n, p.v, p.p, p.f)
