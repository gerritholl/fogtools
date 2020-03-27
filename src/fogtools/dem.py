"""Tools related to DEM
"""

import urllib.request

src_uri_dir_pattern = ("https://prd-tnm.s3.amazonaws.com/StagedProducts"
                       "/Elevation/1/TIFF/{loc_lab:s}/")
src_uri_file_pattern = "USGS_1_{loc_lab:s}.{ext:s}"


def get_loc_lab(lat, lon):
    return ("sn"[lat > 0] + f"{abs(lat):>02d}"
            + "we"[lon > 0] + f"{abs(lon):>03d}")


def get_src_uri_dir(lat, lon):
    return src_uri_dir_pattern.format(loc_lab=get_loc_lab(lat, lon))


def get_src_uri_filename(lat, lon, tp):
    if tp in ("tif", "jpg", "xml"):
        return src_uri_file_pattern.format(
                loc_lab=get_loc_lab(lat, lon),
                ext=tp)
    elif tp == "gpkg":
        return get_loc_lab(lat, lon) + ".gpkg"
    else:
        raise ValueError(f"Unknown type: {tp!s}")


def get_src_uris(lat, lon):
    d = get_src_uri_dir(lat, lon)
    return [d + get_src_uri_filename(lat, lon, tp)
            for tp in ("tif", "jpg", "xml", "gpkg")]


def get_out_dir(lat, lon, basedir_out):
    return basedir_out / get_loc_lab(lat, lon)


def dl_usgs_dem(lat, lon, out_dir):
    for src_uri in get_src_uris(lat, lon):
        fn = src_uri.split("/")[-1]
        urllib.request.urlretrieve(src_uri, out_dir / fn)


def dl_usgs_dem_in_range(lat_from, lat_to, lon_from, lon_to, basedir_out):
    """Download all USGS 1-arc-second TIFF and metadata
    """

    for lat in range(lat_from, lat_to):
        for lon in range(lon_from, lon_to):
            out_dir = get_out_dir(lat, lon, basedir_out)
            dl_usgs_dem(lat, lon, out_dir)
