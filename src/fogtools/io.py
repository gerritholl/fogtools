"""Various IO-related tools
"""

def _get_cache_dir(base=None):
    """Get directory to use for caching

    Get (and create, if necessary) directory to use for caching.

    Args:
        base (str or pathlib.Path):
            Directory in which to create cache dir.  If not given, use
            XDG_CACHE_HOME or otherwise ~/.cache.

    Returns:
        pathlib.Path object pointing to cache dir
    """
    cacheroot = (base or
                 os.environ.get("XDG_CACHE_HOME") or
                 pathlib.Path.home() / ".cache")
    cacheroot = pathlib.Path(cacheroot)
    cacheroot /= "fogtools"
    cacheroot.mkdir(parents=True, exist_ok=True)
    return cacheroot


def plotdir():
    pd = pathlib.Path("/media/nas/x21308/plots_and_maps/" +
                      datetime.datetime.now().strftime("%Y/%m/%d"))
    pd.mkdir(exist_ok=True, parents=True)
    return pd
