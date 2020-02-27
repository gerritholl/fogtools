"""Routines to interact with sky
"""

import pathlib

import lxml.etree
import lxml.builder


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

    def refdate(self):
        return self.E.referenceDate(
                self.E.value(self.start_time.strftime("%Y%m%d%H%M%S")))

    def step(self, st):
        return self.E.field(self.E.value(f"{st:>02d}"))

    def sort_order(self):
        return self.E.sort(
                self.E.order(name="FIRST_LEVEL"),
                self.E.order(name="PARAMETER_SHORTNAME"))

    def result(self):
        return self.E.result(
            self.E.binary(),
            self.E.info(level="countXML"))

    def transfer(self, fs):
        return self.E.transfer(
                self.E.file(
                    hitFile="ihits",
                    infoFile="info",
                    name=str(make_icon_nwcsaf_filename(
                        self.base, self.start_time, fs))))

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

    def get_request(self):
        return self.E.requestCollection(
                self.E.read(
                    self.select_surf_anal_props(),
                    self.result(),
                    self.transfer(0),
                    database=self.db),
                self.E.read(
                    self.select_surf_forc_props(1),
                    self.sort_order(),
                    self.result(),
                    self.transfer(1),
                    database=self.db),
                self.E.read(
                    self.select_level_props(1),
                    self.sort_order(),
                    self.result(),
                    self.transfer(1),
                    database=self.db),
                processing="sequential",
                ifErr="go",
                priority="1",
                validate="true",
                append="false")


def make_icon_request_for_nwcsaf(
        base,
        dt_now,
        ):
    """Generate request for ICON data for NWCSAF
    """

    rb = RequestBuilder(base, dt_now)
    em = rb.get_request()
    return lxml.etree.tostring(
            em,
            standalone=True,
            pretty_print=True).replace(b"'", b'"', 6)
