# -*- coding: utf-8 -*-
"""
    Dummy conftest.py for fogtools.

    If you don't know what this is for, just leave it empty.
    Read more about conftest.py under:
    https://pytest.org/latest/plugins.html
"""

import io

import pytest
import pandas

csv_db_test_content = """,STATION,DATE,LATITUDE,LONGITUDE,ELEVATION,NAME,vis,temp,dew
1000,72047299999,2017-01-31 07:35:00,44.991894,-70.664625,556.26,"STEVEN A BEAN MUNICIPAL, ME US",11265,-17.0,-18.0
1001,72047299999,2017-01-31 07:55:00,44.991894,-70.664625,556.26,"STEVEN A BEAN MUNICIPAL, ME US",11265,-17.0,-19.0
1002,72047299999,2017-01-31 08:15:00,44.991894,-70.664625,556.26,"STEVEN A BEAN MUNICIPAL, ME US",16093,-17.0,-20.0
1003,72047299999,2017-01-31 08:35:00,44.991894,-70.664625,556.26,"STEVEN A BEAN MUNICIPAL, ME US",11265,-17.0,-20.0
1004,72047299999,2017-01-31 09:15:00,44.991894,-70.664625,556.26,"STEVEN A BEAN MUNICIPAL, ME US",16093,-18.0,-20.0
"""  # noqa: E501


@pytest.fixture
def db():
    with io.StringIO(csv_db_test_content) as f:
        return pandas.read_csv(f, parse_dates=["DATE"])
