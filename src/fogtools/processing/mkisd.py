"""Script to download ISD database
"""

from .. import isd


def mkisd():
    isd.create_db()


def main():
    mkisd()
