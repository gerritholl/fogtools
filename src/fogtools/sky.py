"""Routines to interact with sky
"""

import pathlib
import subprocess
import logging
import itertools
import pandas

import lxml.etree
import lxml.builder

from . import io as ftio

logger = logging.getLogger(__name__)


class SkyFailure(Exception):
    pass


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

    def __init__(self, base):
        """Initialise class to build sky request

        Args:
            base (str)
                Base NWCSAF directory
        """
        self.E = lxml.builder.ElementMaker(
            namespace="http://dwd.de/sky",
            nsmap={"sky": "http://dwd.de/sky"})
        self.base = base
        self.expected_output_files = set()

    def refdate(self, start_time):
        """Construct <referenceDate> tag

        Construct the sky <referenceDate> tag, see Sky Handbuch, Table 5

        Args:
            start_time (pandas.Timestamp)

        Returns: lxml.etree.Element
        """
        return self.E.referenceDate(
                self.E.value(start_time.strftime("%Y%m%d%H%M%S")))

    def step(self, st):
        """Construct <field name="step"><value>...</value></field> tag

        The step indicates the forecast time step from the analysis.
        See ``sky -d roma -I icrgl130l90_main_fc_rout``.

        Args:
            st: int
                Forecast timestep in hours

        Returns: lxml.etree.Element
        """
        return self.E.field(self.E.value(f"{st:>02d}"), name="STEP")

    def sort_order(self):
        """Construct <sort><order> tag.

        See §3.2.2 in Sky Handbuch

        Returns: lxml.etree.Element
        """
        return self.E.sort(
                self.E.order(name="FIRST_LEVEL"),
                self.E.order(name="PARAMETER_SHORTNAME"))

    def result(self):
        """Construct <result> tag.

        See §3.2.3 in Sky Handbuch
        """
        return self.E.result(
            self.E.binary(),
            self.E.info(level="countXML"))

    def transfer(self, start_time, fs):
        """Construct <transfer> tag

        See §3.2.4 in Sky Handbuch

        Args:
            start_time (pandas.Timestamp)
                Analysis time
            fs: int
                Forecast timestep in hours

        Returns: lxml.etree.Element
        """
        fn_name = make_icon_nwcsaf_filename(
                        self.base, start_time, fs)
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
        """Construct <field name="GRIB_EDITION"><value>2</value></field> tag

        Construct a tag describing what version of grib is wanted.
        See ``sky -d roma -I icrgl130l90_main_fc_rout``.

        Returns: lxml.etree.Element
        """
        return self.E.field(self.E.value("2"), name="GRIB_EDITION")

    def select_surf_anal_props(self, start_time, _=None):
        """Construct XML tree for of selection surface analysis properties

        Construct an XML tree for the selection of surface properties from
        analysis, as defined in self.surf_props_t0.  See §3.2.1 in the SKY
        Handbuch and
        https://www.dwd.de/DE/leistungen/bufr_erweiterungen_national/grib2_parameter_tab.pdf

        Args:
            start_time (pandas.Timestamp)
                Time of analysis

        Returns: lxml.etree.Element
        """
        return self.E.select(
                self.refdate(start_time),
                self.step(0),
                self.E.field(
                    *[self.E.value(sp) for sp in self.surf_props_t0],
                    name="PARAMETER_SHORTNAME"),
                self.edition(),
                category=self.skycat)

    def select_surf_forc_props(self, start_time, s):
        """Construct XML tree for selection of surface forecast properties

        Construct an XML tree for the selection of surface properties from
        forecast, as defined in self.surf_props_tx.  See §3.2.1 in the SKY
        Handbuch and
        https://www.dwd.de/DE/leistungen/bufr_erweiterungen_national/grib2_parameter_tab.pdf

        Args:
            start_time (pandas.Datetime)
                Time of analysis
            s (int)
                Forecast step in hours

        Returns: lxml.etree.Element
        """
        return self.E.select(
                self.refdate(start_time),
                self.step(s),
                self.E.field(
                    *[self.E.value(sp) for sp in self.surf_props_tx],
                    name="PARAMETER_SHORTNAME"),
                self.edition(),
                category=self.skycat)

    def select_level_props(self, start_time, s):
        """Construct XML tree for selection of level forecast properties

        Construct an XML tree for the selection of level properties from
        forecast, as defined in self.lvl_props, at the levels defined in
        self.p_lev.  See §3.2.1 in the SKY Handbuch and
        https://www.dwd.de/DE/leistungen/bufr_erweiterungen_national/grib2_parameter_tab.pdf

        Args:
            start_time (pandas.Datetime)
                Time of analysis
            s (int)
                Forecast step in hours

        Returns: lxml.etree.Element
        """
        return self.E.select(
                self.refdate(start_time),
                self.step(s),
                self.E.field(
                    *[self.E.value(lp) for lp in self.lvl_props],
                    name="PARAMETER_SHORTNAME"),
                self.E.field(
                    *[self.E.value(str(i)) for i in self.p_lev],
                    name="FIRST_LEVEL"),
                self.edition(),
                category=self.skycat)

    def select_read_store_forc(self, start_time, s, mode):
        """Construct XML tree for reading stuff

        Construct an XML tree to read stuff.  See Sky Handbuch §3.2.
        This is a wrapper adding a read tag around a select tag, then adding
        sort order, result, and transfer.

        Args:
            start_time (pandas.Timestamp)
                Time for analysis / forecast run
            s (int)
                Forecast step in hours
            mode (str)
                Can be ``"surf_anal"``, ``"surf_forc"``, or ``"level"``.

        Returns: lxml.etree.Element
        """

        return self.E.read(
                getattr(self, f"select_{mode:s}_props")(start_time, s),
                self.sort_order(),
                self.result(),
                self.transfer(start_time, s),
                database=self.db)

    def get_request_et(self, start_times):
        """Get full request as an XML Tree

        Args:
            start_time (pandas.Timestamp)
                Times for analysis run

        Returns: lxml.etree.Element
        """

        reqs = []
        for start_time in start_times:
            reqs.append(self.select_read_store_forc(
                start_time, 0, "surf_anal"))
            reqs.extend(itertools.chain(*((
                    self.select_read_store_forc(start_time, i, "surf_forc"),
                    self.select_read_store_forc(start_time, i, "level"))
                        for i in range(6))))
        return self.E.requestCollection(
                *reqs,
                processing="sequential",
                ifErr="go",
                priority="1",
                validate="true",
                append="false")

    def get_request_ba(self, start_times):
        """Get full request as a bytes array

        Args:
            start_time (pandas.Timestamp):
                Time for analysis run

        Returns: bytes
        """

        et = self.get_request_et(start_times)
        return lxml.etree.tostring(
                et, standalone=True,
                pretty_print=True).replace(b"'", b'"', 6)


def build_icon_request_for_nwcsaf(base, dt_now):
    """Generate request for ICON data for NWCSAF (one time)

    Args:
        base (pathlib.Path or str)
            NWCSAF base directory (without import/NWP_data)
        dt_now (pandas.Timestamp)
            Analysis time

    Returns: bytes
    """

    rb = RequestBuilder(base)
    return rb.get_request_ba([dt_now])


def sky_get_icon_for_day(base, p):
    """Generate request for ICON data for NWCSAF (all day)

    Args:
        base (pathlib.Path or str)
            NWCSAF base directory (without import/NWP_data)
        p (pandas.Period)
            Analysis date
    """

    if not p.freqstr == "D":
        raise ValueError(f"Expected period to cover a day, "
                         f"found {p.freqstr:s}")
    rb = RequestBuilder(base)
    return rb.get_request_ba(period2daterange(p))


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


def verify_period(p):
    """Verify that a period is OK for sky

    Returns nothing, but raises ValueError if invalid.
    """

    if p.end_time - p.start_time > pandas.Timedelta("5 days"):
        raise ValueError("Period exceeds 5 days, don't do that please")
    elif (p.start_time.hour % 6) != 0:
        raise ValueError(f"Starting hour must be 0, 6, 12, 18, got "
                         f"{p.start_time.hour:d}")
    elif (p.start_time.floor("H") != p.start_time):
        raise ValueError("Start time must be whole hour")


def period2daterange(p):
    # surely there must be a pandas built-in way to do this?
    return pandas.date_range(
            p.start_time,
            p.end_time,
            freq="6H")


def get_and_send(base, period):
    rb = RequestBuilder(base)
    ba = rb.get_request_ba(period2daterange(period))
    logger.info("Sending request to sky, expecting output files: " +
                ", ".join(sorted(str(x) for x in rb.expected_output_files)))
    logger.debug("Full request:\n" + ba.decode("ascii"))
    send_to_sky(ba)
    for eof in rb.expected_output_files:
        peof = pathlib.Path(eof)
        if not (peof.exists() and peof.stat().st_size > 0):
            raise SkyFailure(f"File absent or empty: {eof!s}, sky "
                             "apparently failed to find data.")
