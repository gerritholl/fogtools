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
            "-f", action="store", choices=["markdown", "csv"],
            default="markdown",
            help="How to present output")

    return parser


def print_fogs(n, v, f):
    df = isd.read_db()
    cnt = isd.count_fogs_per_day(df, v)
    selec = cnt.sort_values(ascending=False)[:n]
    if f == "markdown":
        print(selec.to_markdown(), end="\n")
    elif f == "csv":
        print(selec.to_csv(), end="")
    else:
        raise ValueError(f"Invalid format: {f:s}")


def main():
    p = get_parser().parse_args()
    print_fogs(p.n, p.v, p.f)
