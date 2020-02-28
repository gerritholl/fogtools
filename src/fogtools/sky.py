"""Routines to interact with sky
"""

import pathlib
import subprocess
import logging
import itertools

import lxml.etree
import lxml.builder

from . import io as ftio

logger = logging.getLogger(__name__)


def make_icon_nwcsaf_filename(base, t, fs):
    """Generate filename for ICON data for NWCSAF

    According to the NWCSAF Interface Control Document
    <NWC/CDOP3/GEO/AEMET/SW/ICD/1> §4.3

    Args:

        base (str or Pathlib.Path):
            Path relative to which the files are generated

        t (pandas.Timestamp):
            Time for which to generate filename.

        fs: int
            Forecast timestep.
    """

    fn = t.strftime(f"S_NWC_NWP_%Y-%m-%dT%H:%M:%SZ_{fs:>03d}.grib")
    return pathlib.Path(base) / "import" / "NWP_data" / fn


def ensure_parents_exist(*args):
    """For all paths, ensure parent dirs exist
    """
    for p in args:
        p.parent.mkdir(exist_ok=True, parents=True)


class RequestBuilder:
    """Class to build a SKY request for ICON for NWCSAF
    """

    skycat = ("/Routine/ICON/Global/Regular Grid 13.0 km 90 Level"
              "/Main Run/Forecast")
    db = "roma"
    p_lev = [1000, 3000, 5000, 7000, 10000, 15000, 20000, 25000, 30000,
             40000, 50000, 70000, 85000, 92500, 100000]
    surf_props_t0 = ["HSURF", "FR_LAND"]
    surf_props_tx = ["T_2M", "TD_2M", "PS", "T_G", "TQV", "RELHUM_2M",
                     "QV_2M"]
    lvl_props = ["T", "RELHUM", "FI", "U", "V"]

    def __init__(self, base, start_time):
        self.E = lxml.builder.ElementMaker(
            namespace="http://dwd.de/sky",
            nsmap={"sky": "http://dwd.de/sky"})
        self.start_time = start_time
        self.base = base
        self.expected_output_files = set()

    def refdate(self):
        return self.E.referenceDate(
                self.E.value(self.start_time.strftime("%Y%m%d%H%M%S")))

    def step(self, st):
        return self.E.field(self.E.value(f"{st:>02d}"), name="STEP")

    def sort_order(self):
        return self.E.sort(
                self.E.order(name="FIRST_LEVEL"),
                self.E.order(name="PARAMETER_SHORTNAME"))

    def result(self):
        return self.E.result(
            self.E.binary(),
            self.E.info(level="countXML"))

    def transfer(self, fs):
        fn_name = make_icon_nwcsaf_filename(
                        self.base, self.start_time, fs)
        hitFile = ftio.get_cache_dir() / "ihits"
        infoFile = ftio.get_cache_dir() / "info"
        ensure_parents_exist(fn_name, hitFile, infoFile)
        self.expected_output_files |= {fn_name, hitFile, infoFile}
        t = self.E.transfer(
                self.E.file(
                    hitFile=str(hitFile),
                    infoFile=str(infoFile),
                    name=str(fn_name)))
        return t

    def edition(self):
        return self.E.field(self.E.value("2"), name="edit")

    def select_surf_anal_props(self):
        return self.E.select(
                self.refdate(),
                self.step(0),
                self.E.field(
                    *[self.E.value(sp) for sp in self.surf_props_t0],
                    name="PARAMETER_SHORTNAME"),
                self.edition(),
                category=self.skycat)

    def select_surf_forc_props(self, s):
        return self.E.select(
                self.refdate(),
                self.step(s),
                self.E.field(
                    *[self.E.value(sp) for sp in self.surf_props_tx],
                    name="PARAMETER_SHORTNAME"),
                self.edition(),
                category=self.skycat)

    def select_level_props(self, s):
        return self.E.select(
                self.refdate(),
                self.step(s),
                self.E.field(
                    *[self.E.value(lp) for lp in self.lvl_props],
                    name="PARAMETER_SHORTNAME"),
                self.E.field(
                    *[self.E.value(str(i)) for i in self.p_lev],
                    name="FIRST_LEVEL"),
                self.edition(),
                category=self.skycat)

    def select_read_store_forc(self, s, mode):
        return self.E.read(
                getattr(self, f"select_{mode:s}_props")(s),
                self.sort_order(),
                self.result(),
                self.transfer(s),
                database=self.db)

    def get_request_et(self):
        return self.E.requestCollection(
                self.E.read(
                    self.select_surf_anal_props(),
                    self.result(),
                    self.transfer(0),
                    database=self.db),
                *itertools.chain(*((
                    self.select_read_store_forc(i, "surf_forc"),
                    self.select_read_store_forc(i, "level"))
                        for i in range(6))),
                processing="sequential",
                ifErr="go",
                priority="1",
                validate="true",
                append="false")

    def get_request_ba(self):
        et = self.get_request_et()
        return lxml.etree.tostring(
                et, standalone=True,
                pretty_print=True).replace(b"'", b'"', 6)


def build_icon_request_for_nwcsaf(
        base,
        dt_now,
        ):
    """Generate request for ICON data for NWCSAF
    """

    rb = RequestBuilder(base, dt_now)
    return rb.get_request_ba()


def send_to_sky(b):
    """Send request to sky.

    Request might come from :func:`build_icon_request_for_nwcsaf`.

    Args:
        b (bytes):
            Request to send to sky.

    Returns:
        CalledProcess object from subprocess module
    """

    try:
        cp = subprocess.run(
                ["sky", "-v"], input=b, check=True, capture_output=True)
    except subprocess.CalledProcessError as e:
        logger.error(
                f"sky call failed with code {e.returncode:d}\n"
                "stdout\n"
                "------\n" +
                (e.stdout.decode("ascii") or "(empty)\n") +
                "stderr\n"
                "------\n" +
                (e.stderr.decode("ascii") or "(empty)\n") +
                "sky-command\n"
                "-----------\n" +
                b.decode("ascii"))
        raise
    return cp


def get_and_send(base, dt_now):
    rb = RequestBuilder(base, dt_now)
    ba = rb.get_request_ba()
    logger.info("Sending request to sky, expecting output files: " +
                ", ".join(sorted(str(x) for x in rb.expected_output_files)))
    logger.debug("Full request:\n" + ba.decode("ascii"))
    send_to_sky(ba)
    for eof in rb.expected_output_files:
        if not pathlib.Path(eof).exists():
            raise FileNotFoundError(eof)
