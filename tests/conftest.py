# -*- coding: utf-8 -*-
"""
    Dummy conftest.py for fogtools.

    If you don't know what this is for, just leave it empty.
    Read more about conftest.py under:
    https://pytest.org/latest/plugins.html
"""

import os
import io

import pytest
import pandas
import xarray
import numpy
import unittest.mock

csv_db_test_content = """,STATION,DATE,LATITUDE,LONGITUDE,ELEVATION,NAME,vis,temp,dew
1000,72047299999,2017-01-31 07:35:00,44.991894,-70.664625,556.26,"STEVEN A BEAN MUNICIPAL, ME US",11265,-17.0,-18.0
1001,72047299999,2017-01-31 07:55:00,44.991894,-70.664625,556.26,"STEVEN A BEAN MUNICIPAL, ME US",11265,-17.0,-19.0
1002,72047299999,2017-01-31 08:15:00,44.991894,-70.664625,556.26,"STEVEN A BEAN MUNICIPAL, ME US",16093,-17.0,-20.0
1003,72047299999,2017-01-31 08:35:00,44.991894,-70.664625,556.26,"STEVEN A BEAN MUNICIPAL, ME US",11265,-17.0,-20.0
1004,72047299999,2017-01-31 09:15:00,44.991894,-70.664625,556.26,"STEVEN A BEAN MUNICIPAL, ME US",16093,-18.0,-20.0
"""  # noqa: E501

csv_test_content = """"STATION","DATE","SOURCE","LATITUDE","LONGITUDE","ELEVATION","NAME","REPORT_TYPE","CALL_SIGN","QUALITY_CONTROL","WND","CIG","VIS","TMP","DEW","SLP","AA1","AA2","AY1","AY2","GF1","MW1","REM"
"94733099999","2019-01-03T22:00:00","4","-32.5833333","151.1666666","45.0","SINGLETON STP, AS","FM-12","99999","V020","050,1,N,0010,1","22000,1,9,N","025000,1,9,9","+0260,1","+0210,1","99999,9","24,0000,9,1",,"0,1,02,1","0,1,02,1","01,99,1,99,9,99,9,99999,9,99,9,99,9","01,1","SYN05294733 11/75 10502 10260 20210 60004 70100 333 70000="
"94733099999","2019-01-04T04:00:00","4","-32.5833333","151.1666666","45.0","SINGLETON STP, AS","FM-12","99999","V020","090,1,N,0021,1","22000,1,9,N","025000,1,9,9","+0378,1","+0172,1","99999,9","06,0000,9,1",,"0,1,02,1","0,1,02,1","03,99,1,99,9,99,9,99999,9,99,9,99,9","03,1","SYN04294733 11/75 30904 10378 20172 60001 70300="
"94733099999","2019-01-04T22:00:00","4","-32.5833333","151.1666666","45.0","SINGLETON STP, AS","FM-12","99999","V020","290,1,N,0057,1","99999,9,9,N","020000,1,9,9","+0339,1","+0201,1","99999,9","24,0000,9,1",,"0,1,02,1","0,1,02,1",,"02,1","SYN05294733 11970 02911 10339 20201 60004 70200 333 70000="
"94733099999","2019-01-05T22:00:00","4","-32.5833333","151.1666666","45.0","SINGLETON STP, AS","FM-12","99999","V020","200,1,N,0026,1","99999,9,9,N","000100,1,9,9","+0209,1","+0193,1","99999,9","24,0004,3,1",,"1,1,02,1","1,1,02,1","08,99,1,99,9,99,9,99999,9,99,9,99,9","51,1","SYN05294733 11/01 82005 10209 20193 69944 75111 333 70004="
"94733099999","2019-01-08T04:00:00","4","-32.5833333","151.1666666","45.0","SINGLETON STP, AS","FM-12","99999","V020","070,1,N,0026,1","22000,1,9,N","025000,1,9,9","+0344,1","+0213,1","99999,9","06,0000,9,1",,"2,1,02,1","2,1,02,1","04,99,1,99,9,99,9,99999,9,99,9,99,9","02,1","SYN04294733 11/75 40705 10344 20213 60001 70222="
"""  # noqa: E501

@pytest.fixture
def gb_db():
    with io.StringIO(csv_db_test_content) as f:
        return pandas.read_csv(f, parse_dates=["DATE"])


@pytest.fixture
def xrda():
    overview = xarray.DataArray(
            numpy.linspace(0, 100, 3*100*100).reshape(3, 100, 100),
            dims=("bands", "y", "x"),
            coords={"bands": ["R", "G", "B"]})
    fls_day = xarray.DataArray(
            numpy.linspace(0, 100, 4*100*100).reshape(4, 100, 100),
            dims=("bands", "y", "x"),
            coords={"bands": ["R", "G", "B", "A"]})
    return (overview, fls_day)


@pytest.fixture(autouse=True)
def tmpenv(monkeypatch, tmp_path):
    monkeypatch.setenv("SAFNWC", str(tmp_path))
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))


@pytest.fixture(scope="session", autouse=True)
def setUp(tmp_path_factory):
    os.environ["XDG_CACHE_HOME"] = str(
            tmp_path_factory.mktemp("xdg_cache_home"))
    os.environ["SAFNWC"] = str(tmp_path_factory.mktemp("safnwc"))


@pytest.fixture
def station():
    from fogtools.isd import dl_station
    with unittest.mock.patch("s3fs.S3FileSystem", autospec=True) as s3:
        s3.return_value.open.return_value = io.StringIO(csv_test_content)
        return dl_station(2019, "94733099999")
