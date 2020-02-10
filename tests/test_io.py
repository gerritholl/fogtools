import tempfile
import os


def test_cache_dir():
    from fogtools.isd import _get_cache_dir
    with tempfile.TemporaryDirectory() as tmpdir:
        d = _get_cache_dir(tmpdir)
        assert d.exists()
        assert d.is_dir()
        assert str(d.parent) == tmpdir
        assert d.name == "fogtools"
    try:
        _environ = os.environ.copy()
        del os.environ["XDG_CACHE_HOME"]
        d = _get_cache_dir()
        assert d.exists()
        assert d.is_dir()
        assert d.parent.name == ".cache"
        assert d.name == "fogtools"
    finally:
        try:
            d.rmdir()
        except OSError:
            pass
        os.environ.clear()
        os.environ.update(_environ)
    try:
        _environ = os.environ.copy()
        os.environ["XDG_CACHE_HOME"] = os.environ.get("TMPDIR", "/tmp")
        d = _get_cache_dir()
        assert d.exists()
        assert d.is_dir()
        assert str(d.parent) == "/tmp"
        assert d.name == "fogtools"
    finally:
        try:
            d.rmdir()
        except OSError:
            pass
        os.environ.clear()
        os.environ.update(_environ)
