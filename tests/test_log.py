import datetime
import logging


def test_log_context(tmp_path):
    import fogtools.log
    tm = datetime.datetime(1985, 8, 13, 15)
    root = logging.getLogger()
    tofu = logging.getLogger("tofu")
    veggie = logging.getLogger("veggie")
    oh = logging.FileHandler(tmp_path / "test")
    tofu.addHandler(oh)
    with fogtools.log.LogToTimeFile(tm) as c:
        f = c.logfile
        tofu.debug("tofu")
        veggie.debug("veggie")
    with f.open("r") as fp:
        text = fp.read()
        assert "tofu" in text
        assert "veggie" in text
    with (tmp_path / "test").open("r") as fp:
        text = fp.read()
        assert "tofu" in text
        assert "veggie" not in text
