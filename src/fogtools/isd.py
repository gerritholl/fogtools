"""Routines related to Integrated Surface Database (ISD)

"""

import pandas

url_station_list = "ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-history.txt"

def get_stations():
    """Download the list of ISD stations
    """
    return pandas.read_fwf(url_station_list, skiprows=20,
                           parse_dates=["BEGIN", "END"])
