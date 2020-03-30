"""Tools related to DEM
"""

import urllib.request
import logging
import errno

src_uri_dir_pattern = ("https://prd-tnm.s3.amazonaws.com/StagedProducts"
                       "/Elevation/1/TIFF/{loc_lab:s}/")
src_uri_file_pattern = "USGS_1_{loc_lab:s}.{ext:s}"

logger = logging.getLogger(__name__)


def get_loc_lab(lat, lon):
    """Construct the USGS location label

    Construct a lat/lon location label such as n01e002 that is part of the
    labels used by the "Staged Products" HTTPS web directory access for USGS
    1-arcsecond DEM TIFF files.

    Args:
        lat (int): Latitude
        lon (int): Longitude

    Returns:
        str, label describing lat/lon cell
    """
    return ("sn"[lat > 0] + f"{abs(lat):>02d}"
            + "we"[lon > 0] + f"{abs(lon):>03d}")


def get_src_uri_dir(lat, lon):
    """Construct the directory part of the 1" USGS DEM URI

    Construct the directory part of the "Staged Products" HTTPS web directory
    access for the USGS 1-arcsecond DEM TIFF files for a particular grid cell.

    Args:
        lat (int): Latitude
        lon (int): Longitude

    Returns:
        str, URI to directory containing
    """
    return src_uri_dir_pattern.format(loc_lab=get_loc_lab(lat, lon))


def get_src_uri_filename(lat, lon, tp):
    """Get the URI that contains 1" USGS DEM for grid cell

    Get the URI that should contain the 1" cell from the USGS DEM by HTTPS web
    directory access.  For each grid cell there are four files: tif, jpg, xml,
    and gpkg.

    Args:
        lat (int): Latitude
        lon (int): Longitude
        tp (str): Type, must be "tif", "jpg", "xml", or "gpkg"

    Returns:
        str
    """
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


def dl_usgs_dem(lat, lon, out_dir, overwrite=False):
    for src_uri in get_src_uris(lat, lon):
        fn = src_uri.split("/")[-1]
        out = out_dir / fn
        if not overwrite and out.exists():
            logging.info(f"Already exists: {out!s}")
            continue
        logger.info(f"Downloading {src_uri!s} to {out!s}")
        try:
            urllib.request.urlretrieve(src_uri, out)
        except Exception:
            logging.error("Something went wrong downloading "
                          f"{src_uri!s} to {out!s}, deleting partial file")
            out.unlink(missing_ok=True)
            raise


def dl_usgs_dem_in_range(lat_from, lat_to, lon_from, lon_to, basedir_out):
    """Download all USGS 1-arc-second TIFF and metadata
    """

    for lat in range(lat_from, lat_to):
        for lon in range(lon_from, lon_to):
            out_dir = get_out_dir(lat, lon, basedir_out)
            out_dir.mkdir(parents=True, exist_ok=True)
            try:
                dl_usgs_dem(lat, lon, out_dir)
            except urllib.error.HTTPError as err:
                logging.error(f"Could not download for {lat:d}, {lon:d}: " +
                              f"Error {err.code:d}: {err.reason:s}")
                try:
                    out_dir.rmdir()
                except OSError as oerr:
                    if oerr.errno != errno.ENOTEMPTY:
                        raise
