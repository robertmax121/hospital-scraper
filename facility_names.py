"""The actual hospital on postings filed under a system name (2026-10-15).

Wage accuracy (Wages-accuracy-audit 2026-10-15, root cause 2 and item 12a;
Wages-public-sources step 1a): ten adapters stamped every posting with the
system's label, so the nightly posted-pay rollup (refresh_wage_live_pay),
which pins a posting to a hospital by hospital_name or by a city only one
hospital occupies, could not place their posted pay on any hospital row.
Measured 2026-10-15 on active rows (hospital_name = the system label):
UPMC 2,911 of 2,911, Ascension 3,328 of 3,328, Mass General Brigham 2,313 of
2,313, Novant 1,593 of 1,593, Tenet 2,980 of 2,980, Baptist Health South
Florida 1,316 of 1,316, MultiCare 1,359 of 1,359, Vanderbilt 878 of 878,
Guthrie 757 of 757, Corewell 1,951 of 2,123.

Each employer already names the facility; only where it does so differs:

  UPMC         Findly search payload: business_unit, refined by brand
               ("UPMC Susquehanna Soldiers+Sail" is UPMC Wellsboro)
  Novant       Jibe list row: location_name ("PHO - Presbyterian Medical Center")
  Guthrie      Oracle list row: workLocation LocationName ("HCM - Corning Hospital")
  Tenet        Oracle list row: workLocation LocationName ("Delray Medical Center")
  BHSF         Phenom list row: brand ("Boca Raton Regional Hospital")
  MultiCare    Workday list bulletFields: the hiring entity ("Covington Medical Center")
  Vanderbilt   Workday detail: jobRequisitionLocation ("VANDERBILT WILSON COUNTY
               HOSPITAL (LOC02029)"); the title or the body's Organization line
               ("VUH", "MCJCHV", "VCH") when that is all there is
  Corewell     Workday detail jobRequisitionLocation and the body's "Primary
               Location" line ("SITE - Dearborn Hospital - 18101 Oakwood Blvd -
               Dearborn"); the Phenom row's facility when it has one
  Ascension    the body's "Facility:" line ("Facility: Sacred Heart Bay Hospital")
  MGB          the body's "Site:" line, the legal entity ("Site: The General
               Hospital Corporation" is Massachusetts General Hospital)

Every raw value goes through one resolver per system: a small alias table
from the employer's spellings to one proper hospital name (the names
hospital_wages and the CMS hospitals table use, or contain), and the town the
hospital is in. A value that names no hospital (a clinic, a medical group, a
corporate office, "Multiple Ascension sites"), or a hospital the table does
not list yet, leaves the system label, as before: nothing is guessed. A brand
that is several hospitals ("Facility: Ascension St. Vincent") is read with
the posting's own town, and names none where that town has no such hospital.
A hospital is never taken from another state than the posting's own (unless
the posting sits at the head office default), and a name two hospitals share
needs the posting's state or town (review 2026-10-15: Chicago's "Ascension
Saint Joseph Hospital" had been read as Milwaukee's). hospital_system is never
touched.

City and state: a posting keeps its own location. The hospital's town is used
only when the posting has none, or when its location is the system head
office (the default the scraper stamps when a feed gives no town).

The body-derived systems (Ascension, MGB, Corewell, Vanderbilt) see the body
only on nights it is fetched; a known body is sent blank and the enrichment
trigger keeps the stored one (scraper._keep_stored_body). On those nights the
hospital stored on the row is kept (load_known_facilities), unless that
night's own fetch named none.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
import unicodedata

import sb_keys  # 2026-10-23: the DATA project key, one reader for the repo

logger = logging.getLogger("scraper.facility")

# ── Text normalisation ──────────────────────────────────────────────────────
# Stored Ascension bodies carry UTF-8 read as Latin-1 ("Columbia St. Mary\u00e2\u0080\u0099s").
_MOJIBAKE = (("\u00e2\u0080\u0099", "'"), ("\u00e2\u0080\u0098", "'"), ("\u00e2\u0080\u009c", '"'),
             ("\u00e2\u0080\u009d", '"'), ("\u00e2\u0080\u0093", "-"), ("\u00e2\u0080\u0094", "-"),
             ("\u00c2\u00a0", " "))


def clean_text(s) -> str:
    """Mojibake repaired, curly quotes straightened, whitespace collapsed."""
    s = str(s or "")
    for bad, good in _MOJIBAKE:
        s = s.replace(bad, good)
    s = unicodedata.normalize("NFKC", s)
    s = s.replace("\u2019", "'").replace("\u2018", "'").replace("\ufffd", "'")
    s = re.sub("[\u200b\u202f\u00a0]", " ", s)
    return " ".join(s.split())


def fkey(s) -> str:
    """Lookup key: lower case, "saint" -> "st", apostrophes dropped, every
    other non-alphanumeric run a space, and "the" / "inc" / "llc" /
    "corporation" removed ("The General Hospital Corporation" -> "general
    hospital"; "St. Mary's" -> "st marys")."""
    s = clean_text(s).lower().replace("'", "")
    s = re.sub(r"\bsaint\b", "st", s)
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\b(the|inc|llc|corporation|corp)\b", " ", s)
    return " ".join(s.split())


# ── Per-system tables ───────────────────────────────────────────────────────
# hospitals: proper name -> (town, state), the town the hospital is in.
# aliases:   fkey (after the system's prefixes are stripped) -> proper name,
#            or {fkey(town) or state code: proper name} for a brand that is
#            several hospitals ("Ascension St. Vincent" + Evansville).
# Every proper name is also its own alias.
class Spec:
    def __init__(self, system, db_label, labels, hq, hospitals, aliases=None, prefixes=(),
                 fallback=False, rules=(), title_overrides=()):
        self.system = system            # scraper emit name
        self.db_label = db_label        # hospital_system as stored (after HOSPITAL_SYSTEM_ALIASES)
        self.labels = frozenset(labels) | {system, db_label}
        self.hq = hq                    # (town, state) of the head office / the scraper's default
        self.hospitals = dict(hospitals)
        self.prefixes = tuple(prefixes)
        self.fallback = fallback        # body-derived: keep the stored hospital on nights without a body
        self.rules = tuple((re.compile(rx, re.I), name) for rx, name in rules)
        self.title_overrides = tuple((frm, re.compile(rx, re.I), to) for frm, rx, to in title_overrides)
        # A proper name as printed is looked up first, before the prefix is
        # stripped ("Ascension St. Francis Hospital" is Milwaukee's, while a
        # bare "St. Francis Hospital" depends on the state), but only in its own
        # state, and only where the posting's state is known when the stripped
        # name is also a brand of several hospitals (resolve).
        self.exact = {fkey(name): name for name in self.hospitals}
        self.aliases: dict = {}
        for name in self.hospitals:
            self.aliases[self._key(name)] = name
        for k, v in (aliases or {}).items():
            self.aliases[self._key(k)] = v
        for k, v in self.aliases.items():
            for name in (v.values() if isinstance(v, dict) else (v,)):
                if name not in self.hospitals:
                    raise ValueError(f"{system}: alias {k!r} names unknown hospital {name!r}")

    def _key(self, raw) -> str:
        k = fkey(raw)
        changed = True
        while changed:
            changed = False
            for p in self.prefixes:
                if k.startswith(p + " ") and len(k) > len(p) + 1:
                    k = k[len(p) + 1:]
                    changed = True
        return k


# ── UPMC (Findly search payload: business_unit, brand) ─────────────────────
_UPMC = {
    "UPMC Presbyterian": ("Pittsburgh", "PA"), "UPMC Shadyside": ("Pittsburgh", "PA"),
    "UPMC Mercy": ("Pittsburgh", "PA"), "UPMC St. Margaret": ("Pittsburgh", "PA"),
    "UPMC Passavant": ("Pittsburgh", "PA"), "UPMC Passavant Cranberry": ("Cranberry Township", "PA"),
    "UPMC East": ("Monroeville", "PA"), "UPMC McKeesport": ("McKeesport", "PA"),
    "Magee-Womens Hospital of UPMC": ("Pittsburgh", "PA"),
    "UPMC Children's Hospital of Pittsburgh": ("Pittsburgh", "PA"),
    "UPMC Western Psychiatric Hospital": ("Pittsburgh", "PA"),
    "UPMC Altoona": ("Altoona", "PA"), "UPMC Bedford": ("Everett", "PA"),
    "UPMC Carlisle": ("Carlisle", "PA"), "UPMC Hanover": ("Hanover", "PA"),
    "UPMC Harrisburg": ("Harrisburg", "PA"), "UPMC Community Osteopathic": ("Harrisburg", "PA"),
    "UPMC Lititz": ("Lititz", "PA"), "UPMC Memorial": ("York", "PA"),
    "UPMC West Shore": ("Mechanicsburg", "PA"), "UPMC Hamot": ("Erie", "PA"),
    "UPMC Horizon": ("Greenville", "PA"), "UPMC Jameson": ("New Castle", "PA"),
    "UPMC Kane": ("Kane", "PA"), "UPMC Northwest": ("Seneca", "PA"),
    "UPMC Somerset": ("Somerset", "PA"), "UPMC Greene": ("Waynesburg", "PA"),
    "UPMC Washington": ("Washington", "PA"), "UPMC Western Maryland": ("Cumberland", "MD"),
    "UPMC Chautauqua": ("Jamestown", "NY"), "UPMC Williamsport": ("Williamsport", "PA"),
    "UPMC Wellsboro": ("Wellsboro", "PA"), "UPMC Cole": ("Coudersport", "PA"),
    "UPMC Muncy": ("Muncy", "PA"), "UPMC Lock Haven": ("Lock Haven", "PA"),
    "UPMC Trinity": ("Steubenville", "OH"), "UPMC Trinity Twin City": ("Dennison", "OH"),
}
_UPMC_ALIASES = {
    "UPMC Bedford Memorial": "UPMC Bedford",
    "UPMC Chautauqua at WCA": "UPMC Chautauqua",
    "UPMC Susquehanna Williamsport": "UPMC Williamsport",
    "UPMC Susquehanna Soldiers+Sail": "UPMC Wellsboro",
    "UPMC Soldiers and Sailors": "UPMC Wellsboro",
    "UPMC Susquehanna Muncy": "UPMC Muncy",
    "UPMC Susquehanna Lock Haven": "UPMC Lock Haven",
    "UPMC Pinnacle Carlisle": "UPMC Carlisle",
    "UPMC Pinnacle Hanover": "UPMC Hanover",
    "UPMC Pinnacle Lititz": "UPMC Lititz",
    "UPMC Pinnacle Memorial": "UPMC Memorial",
    "UPMC Pinnacle Harrisburg": "UPMC Harrisburg",
    "UPMC Pinnacle West Shore": "UPMC West Shore",
    "UPMC Pinnacle Community Osteopathic": "UPMC Community Osteopathic",
    "Magee Womens Hospital": "Magee-Womens Hospital of UPMC",
    "UPMC Magee-Womens Hospital": "Magee-Womens Hospital of UPMC",
    "UPMC Children's Hospital": "UPMC Children's Hospital of Pittsburgh",
    "UPMC McKeesport Hospital": "UPMC McKeesport",
    "UPMC Western Psychiatric": "UPMC Western Psychiatric Hospital",
}
# A brand under a hospital's business unit that is not the hospital: a
# practice, a home-care or long-term-care arm, a services company.
_UPMC_UNIT_RX = re.compile(r"pediatrics|services|srvcs|medical group|long term|physician|reg health|waasc|"
                           r"clinic|practice|home|hospice|pharmacy|staffing|corporate|health plan", re.I)

# ── Novant (Jibe location_name "CODE - Name") ───────────────────────────────
_NOVANT = {
    "Novant Health New Hanover Regional Medical Center": ("Wilmington", "NC"),
    "Novant Health Forsyth Medical Center": ("Winston-Salem", "NC"),
    "Novant Health Presbyterian Medical Center": ("Charlotte", "NC"),
    "Novant Health Rowan Medical Center": ("Salisbury", "NC"),
    "Novant Health Huntersville Medical Center": ("Huntersville", "NC"),
    "Novant Health Hilton Head Hospital": ("Hilton Head Island", "SC"),
    "Novant Health East Cooper Medical Center": ("Mount Pleasant", "SC"),
    "Novant Health Coastal Carolina Medical Center": ("Hardeeville", "SC"),
    "Novant Health Brunswick Medical Center": ("Bolivia", "NC"),
    "Novant Health Kernersville Medical Center": ("Kernersville", "NC"),
    "Novant Health Mint Hill Medical Center": ("Mint Hill", "NC"),
    "Novant Health Charlotte Orthopedic Hospital": ("Charlotte", "NC"),
    "Novant Health Community Hospital - Stokes": ("Danbury", "NC"),
    "Novant Health Thomasville Medical Center": ("Thomasville", "NC"),
    "Novant Health Matthews Medical Center": ("Matthews", "NC"),
    "Novant Health Ballantyne Medical Center": ("Charlotte", "NC"),
    "Novant Health Pender Medical Center": ("Burgaw", "NC"),
    "Novant Health Clemmons Medical Center": ("Clemmons", "NC"),
    "Novant Health Medical Park Hospital": ("Winston-Salem", "NC"),
    "NHRMC Orthopedic Hospital": ("Wilmington", "NC"),
    "Novant Health Hemby Children's Hospital": ("Charlotte", "NC"),
    "Novant Health Betty H. Cameron Women's and Children's Hospital": ("Wilmington", "NC"),
}
# The site code before " - " (the same code on every posting of a site).
_NOVANT_CODES = {
    "NHRMC": "Novant Health New Hanover Regional Medical Center",
    "FMH": "Novant Health Forsyth Medical Center",
    "PHO": "Novant Health Presbyterian Medical Center",
    "RMC": "Novant Health Rowan Medical Center",
    "PHH": "Novant Health Huntersville Medical Center",
    "SCR-HHH1": "Novant Health Hilton Head Hospital",
    "SCR-ECH1": "Novant Health East Cooper Medical Center",
    "NHCCMC": "Novant Health Coastal Carolina Medical Center",
    "SCR-CCD1": "Novant Health Coastal Carolina Medical Center",
    "BCH": "Novant Health Brunswick Medical Center",
    "KVM": "Novant Health Kernersville Medical Center",
    "MTH": "Novant Health Mint Hill Medical Center",
    "POH": "Novant Health Charlotte Orthopedic Hospital",
    "STK-LBCH": "Novant Health Community Hospital - Stokes",
    "CGH": "Novant Health Thomasville Medical Center",
    "PHM": "Novant Health Matthews Medical Center",
    "BLC": "Novant Health Ballantyne Medical Center",
    "PENDERMEMORIAL": "Novant Health Pender Medical Center",
    "CLM": "Novant Health Clemmons Medical Center",
    "MPH": "Novant Health Medical Park Hospital",
    "ORTHOHOSPITAL": "NHRMC Orthopedic Hospital",
}
_NOVANT_ALIASES = {
    "New Hanover Regional Medical Center": "Novant Health New Hanover Regional Medical Center",
    "Forsyth Medical Center": "Novant Health Forsyth Medical Center",
    "Presbyterian Medical Center": "Novant Health Presbyterian Medical Center",
    "Rowan Medical Center": "Novant Health Rowan Medical Center",
    "Huntersville Medical Center": "Novant Health Huntersville Medical Center",
    "Hilton Head Hospital": "Novant Health Hilton Head Hospital",
    "East Cooper Medical Center": "Novant Health East Cooper Medical Center",
    "Coastal Carolina Medical Center": "Novant Health Coastal Carolina Medical Center",
    "Coastal Carolina Hospital": "Novant Health Coastal Carolina Medical Center",
    "Brunswick Medical Center": "Novant Health Brunswick Medical Center",
    "Kernersville Medical Center": "Novant Health Kernersville Medical Center",
    "Mint Hill Medical Center": "Novant Health Mint Hill Medical Center",
    "Charlotte Orthopedic Hospital": "Novant Health Charlotte Orthopedic Hospital",
    "NH Community Hospital - Stokes": "Novant Health Community Hospital - Stokes",
    "Community Hospital of Stokes": "Novant Health Community Hospital - Stokes",
    "Thomasville Medical Center": "Novant Health Thomasville Medical Center",
    "Matthews Medical Center": "Novant Health Matthews Medical Center",
    "Ballantyne Medical Center": "Novant Health Ballantyne Medical Center",
    "Pender Memorial Hospital": "Novant Health Pender Medical Center",
    "Pender Medical Center": "Novant Health Pender Medical Center",
    "Clemmons Medical Center": "Novant Health Clemmons Medical Center",
    "Medical Park Hospital": "Novant Health Medical Park Hospital",
    "Hemby Children's Hospital": "Novant Health Hemby Children's Hospital",
}

# ── Guthrie (Oracle workLocation "HCM - <site>") ────────────────────────────
_GUTHRIE = {
    "Guthrie Robert Packer Hospital": ("Sayre", "PA"),
    "Guthrie Robert Packer Hospital Towanda Campus": ("Towanda", "PA"),
    "Guthrie Lourdes Hospital": ("Binghamton", "NY"),
    "Guthrie Cortland Medical Center": ("Cortland", "NY"),
    "Guthrie Corning Hospital": ("Corning", "NY"),
    "Guthrie Troy Community Hospital": ("Troy", "PA"),
}
_GUTHRIE_ALIASES = {
    "Robert Packer Hospital": "Guthrie Robert Packer Hospital",
    "Robert Packer Hospital at Towanda Campus": "Guthrie Robert Packer Hospital Towanda Campus",
    "Towanda Memorial Hospital": "Guthrie Robert Packer Hospital Towanda Campus",
    "Lourdes Hospital": "Guthrie Lourdes Hospital",
    "Cortland Medical Center": "Guthrie Cortland Medical Center",
    "Cortland Med Center": "Guthrie Cortland Medical Center",
    "Cortland Regional Medical Center": "Guthrie Cortland Medical Center",
    "Corning Hospital": "Guthrie Corning Hospital",
    "Troy Community Hospital": "Guthrie Troy Community Hospital",
}

# ── Tenet (Oracle workLocation LocationName) ────────────────────────────────
_TENET = {
    "Piedmont Medical Center": ("Rock Hill", "SC"),
    "Piedmont Medical Center - Fort Mill": ("Fort Mill", "SC"),
    "St. Mary's Medical Center": ("West Palm Beach", "FL"),
    "Good Samaritan Medical Center": ("West Palm Beach", "FL"),
    "Delray Medical Center": ("Delray Beach", "FL"),
    "Palm Beach Gardens Medical Center": ("Palm Beach Gardens", "FL"),
    "West Boca Medical Center": ("Boca Raton", "FL"),
    "Florida Coast Medical Center": ("Port St. Lucie", "FL"),
    "Saint Vincent Hospital": ("Worcester", "MA"),
    "Framingham Union Hospital": ("Framingham", "MA"),
    "Leonard Morse Hospital": ("Natick", "MA"),
    "Abrazo West Campus": ("Goodyear", "AZ"),
    "Abrazo Arrowhead Campus": ("Glendale", "AZ"),
    "Abrazo Central Campus": ("Phoenix", "AZ"),
    "Abrazo Scottsdale Campus": ("Phoenix", "AZ"),
    "Abrazo Arizona Heart Hospital": ("Phoenix", "AZ"),
    "Abrazo Surprise Micro-Hospital": ("Surprise", "AZ"),
    "Abrazo Cave Creek Micro-Hospital": ("Cave Creek", "AZ"),
    "Carondelet St. Joseph's Hospital": ("Tucson", "AZ"),
    "Carondelet St. Mary's Hospital": ("Tucson", "AZ"),
    "Carondelet Holy Cross Hospital": ("Nogales", "AZ"),
    "Carondelet Marana Micro-Hospital": ("Marana", "AZ"),
    "Saint Francis Hospital - Memphis": ("Memphis", "TN"),
    "Saint Francis Hospital - Bartlett": ("Bartlett", "TN"),
    "Baptist Medical Center": ("San Antonio", "TX"),
    "Northeast Baptist Hospital": ("San Antonio", "TX"),
    "North Central Baptist Hospital": ("San Antonio", "TX"),
    "Mission Trail Baptist Hospital": ("San Antonio", "TX"),
    "Westover Hills Baptist Hospital": ("San Antonio", "TX"),
    "St. Luke's Baptist Hospital": ("San Antonio", "TX"),
    "Resolute Health Hospital": ("New Braunfels", "TX"),
    "The Hospitals of Providence Memorial Campus": ("El Paso", "TX"),
    "The Hospitals of Providence East Campus": ("El Paso", "TX"),
    "The Hospitals of Providence Sierra Campus": ("El Paso", "TX"),
    "The Hospitals of Providence Transmountain Campus": ("El Paso", "TX"),
    "Valley Baptist Medical Center - Harlingen": ("Harlingen", "TX"),
    "Valley Baptist Medical Center - Brownsville": ("Brownsville", "TX"),
    "Valley Baptist Micro-Hospital - Weslaco": ("Weslaco", "TX"),
    "Nacogdoches Medical Center": ("Nacogdoches", "TX"),
    "Doctors Medical Center of Modesto": ("Modesto", "CA"),
    "Doctors Hospital of Manteca": ("Manteca", "CA"),
    "Emanuel Medical Center": ("Turlock", "CA"),
    "Desert Regional Medical Center": ("Palm Springs", "CA"),
    "John F. Kennedy Memorial Hospital": ("Indio", "CA"),
    "Hi-Desert Medical Center": ("Joshua Tree", "CA"),
    "San Ramon Regional Medical Center": ("San Ramon", "CA"),
    "Detroit Receiving Hospital": ("Detroit", "MI"),
    "Harper University Hospital": ("Detroit", "MI"),
    "Sinai-Grace Hospital": ("Detroit", "MI"),
    "Children's Hospital of Michigan": ("Detroit", "MI"),
    "Huron Valley-Sinai Hospital": ("Commerce Township", "MI"),
    "Rehabilitation Institute of Michigan": ("Detroit", "MI"),
}
_TENET_ALIASES = {
    "Rock Hill Hospital, Piedmont Medical Network": "Piedmont Medical Center",
    "Fort Mill Hospital, Piedmont Medical Network": "Piedmont Medical Center - Fort Mill",
    "St. Marys Medical Center": "St. Mary's Medical Center",
    "Metrowest Medical Center - Framingham Union Campus": "Framingham Union Hospital",
    "Saint Francis Hospital": "Saint Francis Hospital - Memphis",
    "St Francis Hospital Memphis": "Saint Francis Hospital - Memphis",
    "Saint Francis Hospital-Bartlett": "Saint Francis Hospital - Bartlett",
    "Saint Luke's Hospital": "St. Luke's Baptist Hospital",
    "Resolute Baptist Hospital": "Resolute Health Hospital",
    "Carondelet St. Josephs Hospital": "Carondelet St. Joseph's Hospital",
    "Carondelet St. Marys Hospital": "Carondelet St. Mary's Hospital",
    "Thop Memorial Campus": "The Hospitals of Providence Memorial Campus",
    "Thop East Campus": "The Hospitals of Providence East Campus",
    "Thop Sierra Campus": "The Hospitals of Providence Sierra Campus",
    "Thop Transmountain Campus": "The Hospitals of Providence Transmountain Campus",
    "Valley Baptist - Harlingen": "Valley Baptist Medical Center - Harlingen",
    "Valley Baptist - Brownsville": "Valley Baptist Medical Center - Brownsville",
    "2101 Pease Street, Harlingen, TX 78550": "Valley Baptist Medical Center - Harlingen",
    "Nacogdoches Med Ctr S Campus": "Nacogdoches Medical Center",
    "Hdmc Holdings, LLC DBA Hi-Desert Medical Center": "Hi-Desert Medical Center",
    "Hidesert Medical Center": "Hi-Desert Medical Center",
    # "3990 John R. Street, Detroit, Mi 48201-2018" is not listed (review 2026-10-15): it is
    # the Detroit Medical Center's recruiting address, on 31 physician postings that are mostly
    # at Children's Hospital of Michigan (2603025559), so it names no one hospital.
    "Rehab Institute of Michigan": "Rehabilitation Institute of Michigan",
}

# ── Baptist Health South Florida (Phenom brand) ─────────────────────────────
_BHSF = {
    "Baptist Hospital of Miami": ("Miami", "FL"),
    "Baptist Children's Hospital": ("Miami", "FL"),
    "Boca Raton Regional Hospital": ("Boca Raton", "FL"),
    "Bethesda Hospital East": ("Boynton Beach", "FL"),
    "Bethesda Hospital West": ("Boynton Beach", "FL"),
    "South Miami Hospital": ("South Miami", "FL"),
    "Doctors Hospital": ("Coral Gables", "FL"),
    "Homestead Hospital": ("Homestead", "FL"),
    "West Kendall Baptist Hospital": ("Miami", "FL"),
    "Fishermen's Community Hospital": ("Marathon", "FL"),
    "Mariners Hospital": ("Tavernier", "FL"),
}
_BHSF_ALIASES = {
    # "Bethesda Hospital, Inc." runs both Boynton Beach campuses; East is the
    # main one, West when the title names it (title_overrides).
    "Bethesda Hospital": "Bethesda Hospital East",
}

# ── MultiCare (Workday bulletFields: the hiring entity) ─────────────────────
_MULTICARE = {
    "MultiCare Tacoma General Hospital": ("Tacoma", "WA"),
    "MultiCare Allenmore Hospital": ("Tacoma", "WA"),
    "MultiCare Mary Bridge Children's Hospital": ("Tacoma", "WA"),
    "MultiCare Good Samaritan Hospital": ("Puyallup", "WA"),
    "MultiCare Auburn Medical Center": ("Auburn", "WA"),
    "MultiCare Covington Medical Center": ("Covington", "WA"),
    "MultiCare Deaconess Hospital": ("Spokane", "WA"),
    "MultiCare Valley Hospital": ("Spokane Valley", "WA"),
    "MultiCare Capital Medical Center": ("Olympia", "WA"),
    "MultiCare Yakima Memorial Hospital": ("Yakima", "WA"),
}
_MULTICARE_ALIASES = {
    "Mary Bridge Children's Hospital & Health Network": "MultiCare Mary Bridge Children's Hospital",
    "Yakima Memorial": "MultiCare Yakima Memorial Hospital",
}

# ── Mass General Brigham (the body's "Site:" legal entity) ──────────────────
_MGB = {
    "Massachusetts General Hospital": ("Boston", "MA"),
    "Brigham and Women's Hospital": ("Boston", "MA"),
    "Brigham and Women's Faulkner Hospital": ("Boston", "MA"),
    "Newton-Wellesley Hospital": ("Newton", "MA"),
    "Salem Hospital": ("Salem", "MA"),
    "Wentworth-Douglass Hospital": ("Dover", "NH"),
    "Mass Eye and Ear": ("Boston", "MA"),
    "Martha's Vineyard Hospital": ("Oak Bluffs", "MA"),
    "Cooley Dickinson Hospital": ("Northampton", "MA"),
    "McLean Hospital": ("Belmont", "MA"),
    "Nantucket Cottage Hospital": ("Nantucket", "MA"),
    "Spaulding Rehabilitation Hospital Boston": ("Boston", "MA"),
    "Spaulding Hospital Cambridge": ("Cambridge", "MA"),
    "Spaulding Rehabilitation Hospital Cape Cod": ("East Sandwich", "MA"),
    "Shaughnessy-Kaplan Rehabilitation Hospital": ("Salem", "MA"),
}
_MGB_ALIASES = {
    "The General Hospital Corporation": "Massachusetts General Hospital",
    "The Brigham and Women's Hospital, Inc.": "Brigham and Women's Hospital",
    "Brigham and Women's Faulkner Hospital, Inc.": "Brigham and Women's Faulkner Hospital",
    "North Shore Medical Center, Inc.": "Salem Hospital",
    "Massachusetts Eye and Ear Infirmary": "Mass Eye and Ear",
    "Martha's Vineyard Hospital, Inc.": "Martha's Vineyard Hospital",
    "Cooley Dickinson Hospital, Inc.": "Cooley Dickinson Hospital",
    "The McLean Hospital Corporation": "McLean Hospital",
    "Rehabilitation Hospital of the Cape and Islands Corporation": "Spaulding Rehabilitation Hospital Cape Cod",
    "Shaughnessy-Kaplan Rehabilitation Hospital, Inc.": "Shaughnessy-Kaplan Rehabilitation Hospital",
    # One corporation, two hospitals: the posting's town says which.
    "The Spaulding Rehabilitation Hospital Corporation": {
        "boston": "Spaulding Rehabilitation Hospital Boston",
        "charlestown": "Spaulding Rehabilitation Hospital Boston",
        "cambridge": "Spaulding Hospital Cambridge"},
}

# ── Corewell (SITE line / jobRequisitionLocation / Phenom facility) ─────────
_COREWELL = {
    "Corewell Health William Beaumont University Hospital": ("Royal Oak", "MI"),
    "Corewell Health Beaumont Troy Hospital": ("Troy", "MI"),
    "Corewell Health Beaumont Grosse Pointe Hospital": ("Grosse Pointe", "MI"),
    "Corewell Health Butterworth Hospital": ("Grand Rapids", "MI"),
    "Corewell Health Blodgett Hospital": ("Grand Rapids", "MI"),
    "Corewell Health Helen DeVos Children's Hospital": ("Grand Rapids", "MI"),
    "Corewell Health Dearborn Hospital": ("Dearborn", "MI"),
    "Corewell Health Farmington Hills Hospital": ("Farmington Hills", "MI"),
    "Corewell Health Taylor Hospital": ("Taylor", "MI"),
    "Corewell Health Trenton Hospital": ("Trenton", "MI"),
    "Corewell Health Wayne Hospital": ("Wayne", "MI"),
    "Corewell Health Lakeland Hospital, St. Joseph": ("St. Joseph", "MI"),
    "Corewell Health Lakeland Hospital, Niles": ("Niles", "MI"),
    "Corewell Health Watervliet Hospital": ("Watervliet", "MI"),
    "Corewell Health Big Rapids Hospital": ("Big Rapids", "MI"),
    "Corewell Health Reed City Hospital": ("Reed City", "MI"),
    "Corewell Health Greenville Hospital": ("Greenville", "MI"),
    "Corewell Health Gerber Hospital": ("Fremont", "MI"),
    "Corewell Health Ludington Hospital": ("Ludington", "MI"),
    "Corewell Health Pennock Hospital": ("Hastings", "MI"),
    "Corewell Health Zeeland Hospital": ("Zeeland", "MI"),
}
# The SITE names ("Royal Oak Hospital") and the older Beaumont / Spectrum
# spellings; "Corewell Health" itself is a stripped prefix.
_COREWELL_ALIASES = {
    "Royal Oak Hospital": "Corewell Health William Beaumont University Hospital",
    "Beaumont Royal Oak": "Corewell Health William Beaumont University Hospital",
    "William Beaumont University Hospital Royal Oak": "Corewell Health William Beaumont University Hospital",
    "Troy Hospital": "Corewell Health Beaumont Troy Hospital",
    "Grosse Pointe Hospital": "Corewell Health Beaumont Grosse Pointe Hospital",
    "Helen Devos Childrens Hospital": "Corewell Health Helen DeVos Children's Hospital",
}

# ── Vanderbilt (jobRequisitionLocation, title, Organization line) ───────────
_VANDERBILT = {
    "Vanderbilt University Medical Center": ("Nashville", "TN"),
    "Monroe Carell Jr. Children's Hospital at Vanderbilt": ("Nashville", "TN"),
    "Vanderbilt Psychiatric Hospital": ("Nashville", "TN"),
    "Vanderbilt Stallworth Rehabilitation Hospital": ("Nashville", "TN"),
    "Vanderbilt Wilson County Hospital": ("Lebanon", "TN"),
    "Vanderbilt Bedford Hospital": ("Shelbyville", "TN"),
    "Vanderbilt Tullahoma-Harton Hospital": ("Tullahoma", "TN"),
    "Vanderbilt Clarksville Hospital": ("Clarksville", "TN"),
}
_VANDERBILT_ALIASES = {
    "Vanderbilt Hospital": "Vanderbilt University Medical Center",
    "Vanderbilt University Hospital": "Vanderbilt University Medical Center",
    "Monroe Carell Jr Children's Hospital": "Monroe Carell Jr. Children's Hospital at Vanderbilt",
    "Psychiatric Hospital": "Vanderbilt Psychiatric Hospital",
    "Vanderbilt Hospital Clarksville": "Vanderbilt Clarksville Hospital",
}
# The unit codes VUMC prints at the start of a title's site or a body's
# Organization line, and the hospital names its titles spell out.
_VANDERBILT_RULES = (
    (r"\bVUH\b|Vanderbilt University Hospital", "Vanderbilt University Medical Center"),
    (r"\bMCJCHV?\b|Monroe Carell", "Monroe Carell Jr. Children's Hospital at Vanderbilt"),
    (r"\bVWCH\b|Wilson County Hospital", "Vanderbilt Wilson County Hospital"),
    (r"\bVTHH\b|Tullahoma[- ]Harton", "Vanderbilt Tullahoma-Harton Hospital"),
    # "VBCH" (Vanderbilt Bedford County Hospital); a bare "VBH" is not read,
    # Vanderbilt Behavioral Health uses it too.
    (r"\bVBCH\b|Vanderbilt Bedford Hospital|Bedford County Hospital", "Vanderbilt Bedford Hospital"),
    (r"\bVPH\b|Vanderbilt Psychiatric Hospital", "Vanderbilt Psychiatric Hospital"),
    (r"\bVCH\b|Vanderbilt Clarksville Hospital|Vanderbilt Hospital Clarksville", "Vanderbilt Clarksville Hospital"),
    (r"Stallworth", "Vanderbilt Stallworth Rehabilitation Hospital"),
)

# ── Ascension (the body's "Facility:" line) ─────────────────────────────────
_ASC = {
    # Indiana (St. Vincent)
    "Ascension St. Vincent Indianapolis Hospital": ("Indianapolis", "IN"),
    "Ascension St. Vincent Women's Hospital": ("Indianapolis", "IN"),
    "Peyton Manning Children's Hospital at Ascension St. Vincent": ("Indianapolis", "IN"),
    "Ascension St. Vincent Seton Specialty Hospital": ("Indianapolis", "IN"),
    "Ascension St. Vincent Stress Center": ("Indianapolis", "IN"),
    "Ascension St. Vincent Castleton": ("Indianapolis", "IN"),
    "Ascension St. Vincent Carmel": ("Carmel", "IN"),
    "Ascension St. Vincent Heart Center": ("Carmel", "IN"),
    "Ascension St. Vincent Fishers": ("Fishers", "IN"),
    "Ascension St. Vincent Kokomo": ("Kokomo", "IN"),
    "Ascension St. Vincent Anderson": ("Anderson", "IN"),
    "Ascension St. Vincent Evansville": ("Evansville", "IN"),
    "Ascension St. Vincent Orthopedic Hospital": ("Newburgh", "IN"),
    "Ascension St. Vincent Warrick": ("Boonville", "IN"),
    "Ascension St. Vincent Clay": ("Brazil", "IN"),
    "Ascension St. Vincent Randolph": ("Winchester", "IN"),
    "Ascension St. Vincent Salem": ("Salem", "IN"),
    "Ascension St. Vincent Williamsport": ("Williamsport", "IN"),
    "Ascension St. Vincent Mercy": ("Elwood", "IN"),
    "Ascension St. Vincent Jennings": ("North Vernon", "IN"),
    "Ascension St. Vincent Dunn": ("Bedford", "IN"),
    # 2026-10-29 (facility scale): the micro hospital its postings name
    # ("Facility: Ascension St Vincent Plainfield Micro Hospital", Plainfield, IN)
    "Ascension St. Vincent Plainfield": ("Plainfield", "IN"),
    # Florida
    "Ascension St. Vincent's Riverside": ("Jacksonville", "FL"),
    "Ascension St. Vincent's Southside": ("Jacksonville", "FL"),
    "Ascension St. Vincent's Clay County": ("Middleburg", "FL"),
    "Ascension St. Vincent's St. Johns County": ("St. Johns", "FL"),
    "Ascension Sacred Heart Pensacola": ("Pensacola", "FL"),
    "Ascension Sacred Heart Bay": ("Panama City", "FL"),
    "Ascension Sacred Heart Emerald Coast": ("Miramar Beach", "FL"),
    "Ascension Sacred Heart Gulf": ("Port St. Joe", "FL"),
    "The Studer Family Children's Hospital at Ascension Sacred Heart": ("Pensacola", "FL"),
    # Maryland
    "Ascension Saint Agnes Hospital": ("Baltimore", "MD"),
    # Tennessee
    "Ascension Saint Thomas Midtown": ("Nashville", "TN"),
    "Ascension Saint Thomas West": ("Nashville", "TN"),
    "Ascension Saint Thomas Behavioral Health Hospital": ("Nashville", "TN"),
    "Ascension Saint Thomas Rutherford": ("Murfreesboro", "TN"),
    "Ascension Saint Thomas River Park": ("McMinnville", "TN"),
    "Ascension Saint Thomas Highlands": ("Sparta", "TN"),
    "Ascension Saint Thomas Stones River": ("Woodbury", "TN"),
    "Ascension Saint Thomas Hickman": ("Centerville", "TN"),
    "Ascension Saint Thomas Three Rivers": ("Waverly", "TN"),
    "Ascension Saint Thomas DeKalb": ("Smithville", "TN"),
    # Oklahoma
    "Ascension St. John Medical Center": ("Tulsa", "OK"),
    "Ascension St. John Broken Arrow": ("Broken Arrow", "OK"),
    "Ascension St. John Owasso": ("Owasso", "OK"),
    "Ascension St. John Sapulpa": ("Sapulpa", "OK"),
    "Ascension St. John Jane Phillips": ("Bartlesville", "OK"),
    "Ascension St. John Nowata": ("Nowata", "OK"),
    # Kansas
    "Ascension Via Christi St. Francis": ("Wichita", "KS"),
    "Ascension Via Christi St. Joseph": ("Wichita", "KS"),
    "Ascension Via Christi St. Teresa": ("Wichita", "KS"),
    "Ascension Via Christi Rehabilitation Hospital": ("Wichita", "KS"),
    "Ascension Via Christi Hospital Manhattan": ("Manhattan", "KS"),
    "Ascension Via Christi Hospital Pittsburg": ("Pittsburg", "KS"),
    # Wisconsin
    "Ascension Columbia St. Mary's Hospital Milwaukee": ("Milwaukee", "WI"),
    "Ascension Columbia St. Mary's Hospital Ozaukee": ("Mequon", "WI"),
    "Ascension St. Francis Hospital": ("Milwaukee", "WI"),
    "Ascension St. Joseph Hospital": ("Milwaukee", "WI"),
    "Ascension All Saints Hospital": ("Racine", "WI"),
    "Ascension Elmbrook Hospital": ("Brookfield", "WI"),
    "Ascension Franklin Hospital": ("Franklin", "WI"),
    "Ascension Wisconsin Hospital - Menomonee Falls": ("Menomonee Falls", "WI"),
    "Ascension St. Elizabeth Hospital": ("Appleton", "WI"),
    "Ascension Mercy Hospital": ("Oshkosh", "WI"),
    "Ascension Calumet Hospital": ("Chilton", "WI"),
    "Midwest Orthopedic Specialty Hospital": ("Franklin", "WI"),
    # Texas
    "Dell Seton Medical Center at The University of Texas": ("Austin", "TX"),
    "Dell Children's Medical Center": ("Austin", "TX"),
    "Ascension Seton Medical Center Austin": ("Austin", "TX"),
    "Ascension Seton Northwest": ("Austin", "TX"),
    "Ascension Seton Southwest": ("Austin", "TX"),
    "Ascension Seton Shoal Creek": ("Austin", "TX"),
    "Ascension Seton Hays": ("Kyle", "TX"),
    "Ascension Seton Williamson": ("Round Rock", "TX"),
    "Ascension Seton Bastrop": ("Bastrop", "TX"),
    "Ascension Seton Smithville": ("Smithville", "TX"),
    "Ascension Seton Highland Lakes": ("Burnet", "TX"),
    "Ascension Seton Edgar B. Davis": ("Luling", "TX"),
    "Ascension Providence": ("Waco", "TX"),
    # Illinois
    "Ascension Alexian Brothers": ("Elk Grove Village", "IL"),
    "Ascension Alexian Brothers Rehabilitation Hospital": ("Elk Grove Village", "IL"),
    "Ascension Saint Alexius": ("Hoffman Estates", "IL"),
    "Ascension Alexian Brothers Behavioral Health Hospital": ("Hoffman Estates", "IL"),
    "Ascension Saint Joseph - Chicago": ("Chicago", "IL"),
    "Ascension Saint Joseph - Joliet": ("Joliet", "IL"),
    # Michigan
    "Ascension Genesys Hospital": ("Grand Blanc", "MI"),
}
_ASC_IND = "Ascension St. Vincent Indianapolis Hospital"
_ASC_ST_VINCENT_TOWNS = {
    "indianapolis": _ASC_IND, "carmel": "Ascension St. Vincent Carmel",
    "fishers": "Ascension St. Vincent Fishers", "kokomo": "Ascension St. Vincent Kokomo",
    "anderson": "Ascension St. Vincent Anderson", "evansville": "Ascension St. Vincent Evansville",
    "boonville": "Ascension St. Vincent Warrick", "brazil": "Ascension St. Vincent Clay",
    "winchester": "Ascension St. Vincent Randolph", "salem": "Ascension St. Vincent Salem",
    "williamsport": "Ascension St. Vincent Williamsport", "elwood": "Ascension St. Vincent Mercy",
    "north vernon": "Ascension St. Vincent Jennings", "bedford": "Ascension St. Vincent Dunn",
}
_ASC_ST_THOMAS_TOWNS = {
    "murfreesboro": "Ascension Saint Thomas Rutherford", "mcminnville": "Ascension Saint Thomas River Park",
    "sparta": "Ascension Saint Thomas Highlands", "woodbury": "Ascension Saint Thomas Stones River",
    "centerville": "Ascension Saint Thomas Hickman", "waverly": "Ascension Saint Thomas Three Rivers",
    "smithville": "Ascension Saint Thomas DeKalb",
}
_ASC_ST_JOHN_TOWNS = {
    "tulsa": "Ascension St. John Medical Center", "broken arrow": "Ascension St. John Broken Arrow",
    "owasso": "Ascension St. John Owasso", "sapulpa": "Ascension St. John Sapulpa",
    "bartlesville": "Ascension St. John Jane Phillips", "nowata": "Ascension St. John Nowata",
}
_ASC_SACRED_HEART_TOWNS = {
    "pensacola": "Ascension Sacred Heart Pensacola", "panama city": "Ascension Sacred Heart Bay",
    "miramar beach": "Ascension Sacred Heart Emerald Coast", "port st joe": "Ascension Sacred Heart Gulf",
}
_ASC_VIA_CHRISTI_TOWNS = {
    "manhattan": "Ascension Via Christi Hospital Manhattan", "pittsburg": "Ascension Via Christi Hospital Pittsburg",
}
_ASC_SETON_TOWNS = {
    "kyle": "Ascension Seton Hays", "round rock": "Ascension Seton Williamson", "bastrop": "Ascension Seton Bastrop",
    "smithville": "Ascension Seton Smithville", "burnet": "Ascension Seton Highland Lakes",
    "luling": "Ascension Seton Edgar B. Davis",
}
_ASC_ALIASES = {
    # Indiana
    "St. Vincent Indianapolis Hospital": _ASC_IND, "St. Vincent Indianapolis": _ASC_IND,
    "St. Vincent 86th Street Campus": _ASC_IND, "St. Vincent Hospital Indianapolis": _ASC_IND,
    "St. Vincent's Indianapolis Hospital": _ASC_IND,
    # St. Mary's Medical Center is St. Vincent Evansville's old name.
    "St. Mary's Medical Center": {"evansville": "Ascension St. Vincent Evansville"},
    "St. Vincent Women and Infants Hospital": "Ascension St. Vincent Women's Hospital",
    "Women and Infants Tower": "Ascension St. Vincent Women's Hospital",
    "Women and Infant's Tower": "Ascension St. Vincent Women's Hospital",
    "St. Vincent Women's Hospital": "Ascension St. Vincent Women's Hospital",
    "Peyton Manning Children's Hospital": "Peyton Manning Children's Hospital at Ascension St. Vincent",
    "St. Vincent Seton Hospital": "Ascension St. Vincent Seton Specialty Hospital",
    "St. Vincent Seton Specialty Hospital": "Ascension St. Vincent Seton Specialty Hospital",
    "St. Vincent Castleton Micro Hospital": "Ascension St. Vincent Castleton",
    "St. Vincent Carmel Hospital": "Ascension St. Vincent Carmel",
    "St. Vincent Heart Hospital": "Ascension St. Vincent Heart Center",
    "St. Vincent Heart Center of Indiana": "Ascension St. Vincent Heart Center",
    "St. Vincent Fishers Hospital": "Ascension St. Vincent Fishers",
    "St. Vincent Kokomo Hospital": "Ascension St. Vincent Kokomo",
    "St. Vincent Anderson Hospital": "Ascension St. Vincent Anderson",
    "St. Vincent Anderson Regional Hospital": "Ascension St. Vincent Anderson",
    "St. Vincent Anderson, Ability to float to St. Vincent Fishers as well": "Ascension St. Vincent Anderson",
    "St. Vincent Evansville Hospital": "Ascension St. Vincent Evansville",
    "St. Vincent Evansville Orthopedic Hospital": "Ascension St. Vincent Orthopedic Hospital",
    "St. Vincent Warrick Hospital": "Ascension St. Vincent Warrick",
    "ASV Clay": "Ascension St. Vincent Clay", "St. Vincent Clay Hospital": "Ascension St. Vincent Clay",
    "ASV Randolph": "Ascension St. Vincent Randolph",
    "St. Vincent Randolph Hospital": "Ascension St. Vincent Randolph",
    "ASV Salem": "Ascension St. Vincent Salem", "ASV Williamsport": "Ascension St. Vincent Williamsport",
    "ASV Mercy": "Ascension St. Vincent Mercy", "ASV Jennings": "Ascension St. Vincent Jennings",
    "ASV Dunn": "Ascension St. Vincent Dunn",
    # 2026-10-29 (facility scale): spellings on stored Facility lines the table lacked
    "St. Vincent Plainfield Micro Hospital": "Ascension St. Vincent Plainfield",
    "St. Vincent Plainfield Hospital": "Ascension St. Vincent Plainfield",
    "Stress Center": {"IN": "Ascension St. Vincent Stress Center"},
    "St. Vincent": _ASC_ST_VINCENT_TOWNS, "St. Vincent Hospital": _ASC_ST_VINCENT_TOWNS,
    "St. Vincent Medical Center": _ASC_ST_VINCENT_TOWNS,
    # Florida
    "St. Vincent's Riverside Hospital": "Ascension St. Vincent's Riverside",
    "St. Vincent's Southside Hospital": "Ascension St. Vincent's Southside",
    "St. Vincent's Clay County Hospital": "Ascension St. Vincent's Clay County",
    "St. Vincent's St. Johns County Hospital": "Ascension St. Vincent's St. Johns County",
    "St. Vincent's St. Johns Hospital": "Ascension St. Vincent's St. Johns County",
    "St. Vincent's": {"middleburg": "Ascension St. Vincent's Clay County",
                      "st johns": "Ascension St. Vincent's St. Johns County"},
    "Sacred Heart Hospital Pensacola": "Ascension Sacred Heart Pensacola",
    "Sacred Heart Pensacola Hospital": "Ascension Sacred Heart Pensacola",
    "Sacred Heart Bay Hospital": "Ascension Sacred Heart Bay",
    "Sacred Heart Hospital Emerald Coast": "Ascension Sacred Heart Emerald Coast",
    "Sacred Heart Hospital on the Emerald Coast": "Ascension Sacred Heart Emerald Coast",
    "Sacred Heart Hospital on the Gulf": "Ascension Sacred Heart Gulf",
    "Studer Family Children's Hospital at Ascension Sacred Heart":
        "The Studer Family Children's Hospital at Ascension Sacred Heart",
    "Studer Family Children's Hospital": "The Studer Family Children's Hospital at Ascension Sacred Heart",
    "Sacred Heart Children's Hospital": "The Studer Family Children's Hospital at Ascension Sacred Heart",
    "Sacred Heart": _ASC_SACRED_HEART_TOWNS, "Sacred Heart Hospital": _ASC_SACRED_HEART_TOWNS,
    # Maryland
    "St. Agnes Hospital": "Ascension Saint Agnes Hospital", "St. Agnes Healthcare": "Ascension Saint Agnes Hospital",
    "St. Agnes Ascension": "Ascension Saint Agnes Hospital", "St. Agnes": "Ascension Saint Agnes Hospital",
    "Saint Agnes Hospital and local ambulatory sites": "Ascension Saint Agnes Hospital",
    # Tennessee
    "Saint Thomas Midtown Hospital": "Ascension Saint Thomas Midtown",
    "Saint Thomas Heart Midtown": "Ascension Saint Thomas Midtown",
    "Saint Thomas West Hospital": "Ascension Saint Thomas West",
    "Saint Thomas Rutherford Hospital": "Ascension Saint Thomas Rutherford",
    "Saint Thomas Heart West": "Ascension Saint Thomas West",     # 2026-10-29: the heart program at West
    "Saint Thomas River Park Hospital": "Ascension Saint Thomas River Park",
    "Saint Thomas": _ASC_ST_THOMAS_TOWNS,
    # Oklahoma
    "St. John's Medical Center": "Ascension St. John Medical Center",
    "St. John Broken Arrow Hospital": "Ascension St. John Broken Arrow",
    "Jane Phillips Medical Center": "Ascension St. John Jane Phillips",
    "John Jane Phillips": "Ascension St. John Jane Phillips",
    "St. John": _ASC_ST_JOHN_TOWNS,
    # Kansas
    "Via Christi St. Francis Hospital": "Ascension Via Christi St. Francis",
    "Via Christi Hospital St. Francis": "Ascension Via Christi St. Francis",
    "VC St. Francis Hospital": "Ascension Via Christi St. Francis",
    "Via Christi St. Joseph Hospital": "Ascension Via Christi St. Joseph",
    "Via Christi St. Teresa Hospital": "Ascension Via Christi St. Teresa",
    "Via Christi Rehab Hospital": "Ascension Via Christi Rehabilitation Hospital",
    "Via Christi Rehabilitation Hospital": "Ascension Via Christi Rehabilitation Hospital",
    "Via Christi Manhattan": "Ascension Via Christi Hospital Manhattan",
    "Via Christi Manhattan Hospital": "Ascension Via Christi Hospital Manhattan",
    "Via Christi Hospital": _ASC_VIA_CHRISTI_TOWNS, "Via Christi": _ASC_VIA_CHRISTI_TOWNS,
    "Via Christi Hospitals": _ASC_VIA_CHRISTI_TOWNS,
    # Wisconsin
    "Columbia St. Mary's Milwaukee": "Ascension Columbia St. Mary's Hospital Milwaukee",
    "Columbia St. Mary's Hospital Milwaukee": "Ascension Columbia St. Mary's Hospital Milwaukee",
    "Columbia St. Mary Milwaukee": "Ascension Columbia St. Mary's Hospital Milwaukee",
    "CSM Milwaukee": "Ascension Columbia St. Mary's Hospital Milwaukee",
    "Columbia St. Mary's Ozaukee": "Ascension Columbia St. Mary's Hospital Ozaukee",
    "Columbia St. Mary's Hospital Ozaukee Campus": "Ascension Columbia St. Mary's Hospital Ozaukee",
    "Columbia St. Mary - Ozaukee": "Ascension Columbia St. Mary's Hospital Ozaukee",
    "Columbia St. Mary's": {"milwaukee": "Ascension Columbia St. Mary's Hospital Milwaukee",
                            "mequon": "Ascension Columbia St. Mary's Hospital Ozaukee"},
    "Columbia St. Mary's Hospital": {"milwaukee": "Ascension Columbia St. Mary's Hospital Milwaukee",
                                     "mequon": "Ascension Columbia St. Mary's Hospital Ozaukee"},
    "St. Francis Hospital": {"WI": "Ascension St. Francis Hospital", "KS": "Ascension Via Christi St. Francis"},
    "St. Francis": {"WI": "Ascension St. Francis Hospital", "KS": "Ascension Via Christi St. Francis"},
    # Illinois has two (Chicago and Joliet): the posting's town says which, a
    # third Illinois town says none. Chicago's postings print "Facility:
    # Ascension Saint Joseph Hospital" (1303184, 1325997, 2025-425507).
    "St. Joseph Hospital": {"WI": "Ascension St. Joseph Hospital", "chicago": "Ascension Saint Joseph - Chicago",
                            "joliet": "Ascension Saint Joseph - Joliet"},
    "St. Joseph's Hospital": {"WI": "Ascension St. Joseph Hospital"},
    "St. Joseph": {"WI": "Ascension St. Joseph Hospital", "chicago": "Ascension Saint Joseph - Chicago",
                   "joliet": "Ascension Saint Joseph - Joliet"},
    "St. Joseph Hospital Chicago": "Ascension Saint Joseph - Chicago",
    "All Saints": "Ascension All Saints Hospital",
    "All Saints Spring Street Campus": "Ascension All Saints Hospital",
    "All Saints Spring St. Campus": "Ascension All Saints Hospital",
    "Elmbrook Memorial Hospital": "Ascension Elmbrook Hospital", "Elmbrook": "Ascension Elmbrook Hospital",
    "Franklin": {"WI": "Ascension Franklin Hospital"},
    "Franklin Hospital": {"WI": "Ascension Franklin Hospital"},
    "St. Elizabeth": {"WI": "Ascension St. Elizabeth Hospital"},
    "St. Elizabeth's Hospital": "Ascension St. Elizabeth Hospital",
    "Mercy Hospital": {"WI": "Ascension Mercy Hospital"},
    "Menomonee Falls Hospital": "Ascension Wisconsin Hospital - Menomonee Falls",
    # Texas
    "Dell Seton Medical Center": "Dell Seton Medical Center at The University of Texas",
    "Dell Seton Medical Center-UT": "Dell Seton Medical Center at The University of Texas",
    "Dells Children Medical Center": "Dell Children's Medical Center",
    "Seton Medical Center Austin": "Ascension Seton Medical Center Austin",
    "Seton Medical Center Hays": "Ascension Seton Hays",
    "Seton Medical Center-Hays": "Ascension Seton Hays",
    "Seton Medical Ctr Hays": "Ascension Seton Hays",
    "Seton Williamson Hospital": "Ascension Seton Williamson",
    "Seton Southwest Hospital": "Ascension Seton Southwest",
    "Seton Northwest Hospital": "Ascension Seton Northwest",
    "Seton Medical Center": {"austin": "Ascension Seton Medical Center Austin", "kyle": "Ascension Seton Hays"},
    "Seton": _ASC_SETON_TOWNS,
    "Providence": {"TX": "Ascension Providence"},
    "Providence Health Center": "Ascension Providence",
    # Illinois
    "Alexian Brothers Medical Center": "Ascension Alexian Brothers",
    "Alexian Brothers Behavioral Health": "Ascension Alexian Brothers Behavioral Health Hospital",
    "Alexian Rehab Hospital": "Ascension Alexian Brothers Rehabilitation Hospital",
    "Rehabilitation Hospital": {"IL": "Ascension Alexian Brothers Rehabilitation Hospital"},
    "Saint Alexius Hospital": "Ascension Saint Alexius",
    "Saint Alexius Medical Center": "Ascension Saint Alexius",
}

_SPECS = (
    Spec("UPMC", "UPMC", ("UPMC - Pittsburgh Medical Center",), ("Pittsburgh", "PA"),
         _UPMC, _UPMC_ALIASES),
    Spec("Novant Health", "Novant Health", (), ("Winston-Salem", "NC"),
         _NOVANT, _NOVANT_ALIASES, prefixes=("novant health", "nh")),
    Spec("Guthrie Health", "Guthrie", (), ("Sayre", "PA"),
         _GUTHRIE, _GUTHRIE_ALIASES, prefixes=("hcm", "tgc", "guthrie")),
    Spec("Tenet Healthcare", "Tenet Healthcare", ("Tenet Health",), ("Dallas", "TX"),
         _TENET, _TENET_ALIASES),
    Spec("Baptist Health (FL)", "Baptist Health South Florida", (), ("Coral Gables", "FL"),
         _BHSF, _BHSF_ALIASES, prefixes=("baptist health",),
         title_overrides=(("Bethesda Hospital East", r"\bBethesda(?: Hospital)? West\b", "Bethesda Hospital West"),)),
    Spec("MultiCare Health", "MultiCare", (), ("Tacoma", "WA"),
         _MULTICARE, _MULTICARE_ALIASES, prefixes=("multicare",)),
    Spec("Mass General Brigham", "Mass General Brigham", (), ("Boston", "MA"),
         _MGB, _MGB_ALIASES, fallback=True),
    Spec("Corewell Health", "Corewell Health", (), ("Grand Rapids", "MI"),
         _COREWELL, _COREWELL_ALIASES, prefixes=("corewell health",), fallback=True),
    Spec("Vanderbilt (VUMC)", "Vanderbilt", ("Vanderbilt Health",), ("Nashville", "TN"),
         _VANDERBILT, _VANDERBILT_ALIASES, fallback=True, rules=_VANDERBILT_RULES),
    Spec("Ascension Health", "Ascension", (), ("St. Louis", "MO"),
         _ASC, _ASC_ALIASES, prefixes=("ascension",), fallback=True),
)
SPECS: dict[str, Spec] = {}
for _s in _SPECS:
    SPECS[_s.system] = _s
    SPECS.setdefault(_s.db_label, _s)
FACILITY_SYSTEMS = frozenset(s.system for s in _SPECS)
FALLBACK_DB_LABELS = tuple(s.db_label for s in _SPECS if s.fallback)


def spec_for(system) -> Spec | None:
    return SPECS.get((system or "").strip())


# ── Specs from the per-family modules (2026-10-29) ──────────────────────────
# The owner, 2026-10-29: every hospital inside a multi-hospital system named
# and placed correctly. A per-family module (facilities_oracle_other) builds
# its Specs from its own tables and registers them here, so list_facility,
# apply_facility and the run log treat them as they treat the ten above.
# FACILITY_SYSTEMS and FALLBACK_DB_LABELS keep the ten; registered() lists
# the rest. A registered Spec is a list-value Spec (fallback False): no body
# is read for it and nothing stored is kept.
_REGISTERED: list = []


def register(spec, emit_names=()) -> None:
    """Add a Spec from a per-family module under its scraper emit name, its
    stored label and any other emit name the system's adapters use
    ("Northwell Health (CX_1)"). A name one of the ten already holds is not
    taken over."""
    for name in (spec.system, *emit_names):
        if name and name not in SPECS:
            SPECS[name] = spec
    SPECS.setdefault(spec.db_label, spec)
    _REGISTERED.append(spec)


def registered() -> tuple:
    return tuple(_REGISTERED)


# ── Resolver ────────────────────────────────────────────────────────────────
_STATE_RX = re.compile(r"[A-Z]{2}")


def _at_hq(spec, city, st) -> bool:
    """The posting sits at the system head office, the town the scraper
    stamps when a feed gives none."""
    hq_town, hq_st = spec.hq
    jc = (city or "").strip()
    return bool(jc) and fkey(jc) == fkey(hq_town) and (st or "") in ("", hq_st)


def _posting_state(spec, city, state) -> str:
    """The posting's two-letter state when it says where the work is: ""
    when blank, not a state code, or the head office default."""
    st = (state or "").strip().upper()
    if not _STATE_RX.fullmatch(st) or _at_hq(spec, city, st):
        return ""
    return st


def resolve(system, raw, city="", state="", title=""):
    """(hospital name, its town, its state) for one raw facility value, or
    None when the value names no hospital of this system.

    2026-10-15 (review): a hospital is never taken from another state than
    the posting's own. Chicago postings print "Facility: Ascension Saint
    Joseph Hospital", which is spelled like Milwaukee's "Ascension St.
    Joseph Hospital"; the posting's state (IL) rules Milwaukee out and the
    brand table reads it with the posting's town. Where the posting gives no
    state (or only the head office), a proper name that is also a brand of
    several hospitals ("St. Joseph Hospital", "St. Francis Hospital") names
    none unless its town picks one."""
    spec = spec_for(system)
    if spec is None:
        return None
    raw = clean_text(raw)
    if not raw:
        return None
    known_st = _posting_state(spec, city, state)
    st = (state or "").strip().upper()
    k = spec._key(raw)
    alias = spec.aliases.get(k)
    if alias is None and k.endswith(" hospital"):
        # "Ascension Saint Thomas Stones River Hospital", "Ascension Sacred
        # Heart Gulf Hospital": the proper name with "Hospital" added.
        alias = spec.aliases.get(k[:-len(" hospital")])
    exact = spec.exact.get(fkey(raw))
    cands = []
    if exact and not (isinstance(alias, dict) and not known_st):
        cands.append(exact)
    if alias is not None:
        cands.append(alias)
    name = None
    for v in cands:
        if isinstance(v, dict):
            # A brand that is several hospitals: the posting's town (or state)
            # says which, and a town the table does not list says none.
            v = v.get(fkey(city)) or v.get(st)
        if v and (not known_st or spec.hospitals[v][1] == known_st):
            name = v
            break
    if name is None and not cands:
        for rx, rname in spec.rules:
            if rx.search(raw):
                if not known_st or spec.hospitals[rname][1] == known_st:
                    name = rname
                break
    if not name:
        return None
    for frm, rx, to in spec.title_overrides:
        if name == frm and rx.search(title or ""):
            name = to
    town, hst = spec.hospitals[name]
    return (name, town, hst)


def canonical_name(system, name) -> str:
    """The proper hospital name `name` spells (curly quotes, case, "Saint"),
    or "" when it is not one of this system's proper names."""
    spec = spec_for(system)
    if spec is None:
        return ""
    return spec.exact.get(fkey(name), "") if clean_text(name) else ""


def is_hospital_name(system, name) -> bool:
    """True when `name` is one of this system's proper hospital names (what
    resolve gives, not an alias of one), wherever the posting is."""
    return bool(canonical_name(system, name))


def on_system_label(job) -> bool:
    """True when the row carries no facility of its own: blank or one of the
    system's labels (what the adapters stamp when the feed names none)."""
    spec = spec_for(job.hospital_system)
    cur = clean_text(job.hospital_name)
    return spec is not None and (not cur or cur in spec.labels)


# ── Raw values from each source ─────────────────────────────────────────────
_UPMC_GENERIC_BRANDS = {"", "hospital", "organization"}


def upmc_facility(brand, business_unit, city="", state="") -> str:
    """UPMC: the brand when it names a hospital (it splits the Susquehanna
    and Passavant units), else the business unit, unless the brand is a
    practice or service line under that unit ("Children's Community
    Pediatrics" under UPMC Children's). "" when neither is a hospital, or
    when the hospital is in another state than the posting (review
    2026-10-15: "Behavioral Health Navigator I - UPMC Western Maryland" in
    Cumberland, MD sits in UPMC Western Psychiatric Hospital's unit)."""
    b, bu = clean_text(brand), clean_text(business_unit)
    hit = resolve("UPMC", b, city, state) if b else None
    if hit:
        return hit[0]
    if b and fkey(b) not in _UPMC_GENERIC_BRANDS and fkey(b) != fkey(bu) and _UPMC_UNIT_RX.search(b):
        return ""
    hit = resolve("UPMC", bu, city, state) if bu else None
    return hit[0] if hit else ""


_NOVANT_CODE_RX = re.compile(r"^\s*([A-Z0-9][A-Z0-9&_.-]*(?: [A-Z0-9&_.-]+)*)\s+-\s+(.+)$")


def novant_facility(location_name) -> str:
    """Novant: the Jibe location_name "PHO - Presbyterian Medical Center" by
    its site code, else by its name ("Hilton Head Hospital" has no code)."""
    raw = clean_text(location_name)
    if not raw:
        return ""
    m = _NOVANT_CODE_RX.match(raw)
    if m:
        name = _NOVANT_CODES.get(m.group(1).strip().upper())
        if name:
            return name
        hit = resolve("Novant Health", m.group(2))
        return hit[0] if hit else ""
    hit = resolve("Novant Health", raw)
    return hit[0] if hit else ""


def list_facility(system, raw, city="", state="", title="") -> str:
    """The hospital a list value names ("" when none): the Oracle work
    location (Guthrie, Tenet), the Phenom brand (BHSF), a Workday bullet
    (MultiCare)."""
    hit = resolve(system, raw, city, state, title)
    return hit[0] if hit else ""


def bullet_facility(system, bullets, city="", state="", title="") -> str:
    """The first Workday bulletField that names a hospital (MultiCare's
    hiring entity rides beside the town and the requisition number)."""
    for b in bullets or []:
        if isinstance(b, str):
            name = list_facility(system, b, city, state, title)
            if name:
                return name
    return ""


_SITE_RX = re.compile(r"SITE\s*-\s*(.+?)\s*-\s")
_LOC_CODE_RX = re.compile(r"\s*\(LOC\d+\)\s*$", re.I)


def descriptor_facility(system, descriptor, city="", state="", title=""):
    """A Workday jobRequisitionLocation descriptor: Corewell "SITE - Dearborn
    Hospital - 18101 Oakwood Blvd - Dearborn", Vanderbilt "VANDERBILT
    WILSON COUNTY HOSPITAL (LOC02029)". (name, town, state) or None."""
    d = clean_text(descriptor)
    if not d:
        return None
    m = _SITE_RX.search(d + " ")
    if m:
        d = m.group(1)
    d = _LOC_CODE_RX.sub("", d)
    return resolve(system, d, city, state, title)


_FACILITY_LINE_RX = re.compile(r"(?mi)^[ \t]*Facility(?:[ \t]+Name)?[ \t]*:[ \t]*([^\n|]+)")
_SITE_LINE_RX = re.compile(r"(?mi)^[ \t]*Site[ \t]*:[ \t]*([^\n|]+)")
_PRIMARY_SITE_RX = re.compile(r"(?m)^[ \t]*(SITE\s*-[^\n]+)")
_ORG_LINE_RX = re.compile(r"(?i)Organization:[ \t]*\n?[ \t]*([^\n]+)")


def text_candidates(system, description, title=""):
    """Raw facility values a body (and title) carries, best first."""
    spec = spec_for(system)
    if spec is None:
        return []
    body = description or ""
    if spec.db_label == "Ascension":
        return [m.group(1) for m in _FACILITY_LINE_RX.finditer(body)][:1]
    if spec.db_label == "Mass General Brigham":
        return [m.group(1) for m in _SITE_LINE_RX.finditer(body)][:1]
    if spec.db_label == "Corewell Health":
        return [m.group(1) for m in _PRIMARY_SITE_RX.finditer(body)][:1]
    if spec.db_label == "Vanderbilt":
        out = [title] if title else []
        m = _ORG_LINE_RX.search(body)
        if m:
            out.append(m.group(1))
        return out
    return []


def text_facility(system, description, title="", city="", state=""):
    """(name, town, state) from the body / title, or None."""
    spec = spec_for(system)
    if spec is None:
        return None
    read = descriptor_facility if spec.db_label == "Corewell Health" else resolve
    for raw in text_candidates(system, description, title):
        hit = read(system, raw, city, state, title)
        if hit:
            return hit
    return None


# ── Stored hospitals (body-derived systems) ─────────────────────────────────
_KNOWN: dict = {}
KNOWN_FACILITY_PAGE = 1000
KNOWN_FACILITY_MAX_ROWS = 50000
SEEN_ATTR = "_facility_seen"     # set on a Job whose own fetch named no hospital
# Under this many characters the row carries no body tonight: blank (the
# stored body is kept, _keep_stored_body) or a list teaser (Phenom's run
# 200-300). A fetched Ascension / MGB / Corewell / VUMC body runs 2,000-8,000.
NO_BODY_CHARS = 1000


def set_known_facilities(rows) -> int:
    """{(stored hospital_system, job_id): hospital_name} for rows whose
    stored name is a hospital of that system. Returns the map's size."""
    global _KNOWN
    out = {}
    for r in rows or []:
        sysname, jid, name = r.get("hospital_system"), r.get("job_id"), clean_text(r.get("hospital_name"))
        spec = spec_for(sysname)
        if spec is None or not spec.fallback or not name or jid in (None, "") or name in spec.labels:
            continue
        proper = canonical_name(sysname, name)
        if proper:
            out[(spec.db_label, str(jid))] = proper
    _KNOWN = out
    return len(out)


def known_facility(system, job_id) -> str:
    spec = spec_for(system)
    if spec is None:
        return ""
    return _KNOWN.get((spec.db_label, str(job_id)), "")


def load_known_facilities() -> int:
    """Read-only PostgREST passes (the same SUPABASE_URL / key the upsert
    uses) over the active rows of the body-derived systems, one system at a
    time, keyset-paged on id. Without credentials the map stays empty and
    nothing is kept.

    2026-10-15 (review): one system per pass (hospital_system=eq.<label>).
    The one pass over all four (hospital_system=in.(...) ordered by id) was
    planned as a walk of the whole primary key: 402,593 rows filtered out
    and 10.0 s for the first page on a cold cache, over PostgREST's 8 s
    statement timeout. Per system the planner uses idx_system: 11 pages in
    all on 2026-10-15, each under 0.5 s. A system that fails keeps the rows
    read before it; the other systems still load."""
    sb_url = os.environ.get("SUPABASE_URL", "")
    sb_key = sb_keys.supabase_key()
    if not sb_url or not sb_key:
        set_known_facilities([])
        return 0
    import urllib.parse as _urlparse
    import urllib.request as _urlreq
    rows = []
    for label in FALLBACK_DB_LABELS:
        flt = _urlparse.quote(f"eq.{label}", safe=".")
        last, got = 0, 0
        try:
            while len(rows) < KNOWN_FACILITY_MAX_ROWS:
                u = (f"{sb_url.rstrip('/')}/rest/v1/hospital_jobs?select=id,hospital_system,job_id,hospital_name"
                     f"&is_active=is.true&hospital_system={flt}&id=gt.{last}&order=id.asc&limit={KNOWN_FACILITY_PAGE}")
                rq = _urlreq.Request(u, headers={"apikey": sb_key, "Authorization": f"Bearer {sb_key}"})
                chunk = None
                for attempt in range(3):
                    try:
                        with _urlreq.urlopen(rq, timeout=60) as resp:
                            chunk = json.loads(resp.read().decode())
                        break
                    except Exception:
                        if attempt == 2:
                            raise
                        time.sleep(3 * (attempt + 1))
                if not chunk:
                    break
                rows.extend(chunk)
                got += len(chunk)
                last = chunk[-1]["id"]
                if len(chunk) < KNOWN_FACILITY_PAGE:
                    break
        except Exception as e:
            logger.warning(f"Known facilities: {label} load stopped after {got:,} rows ({e}); "
                           f"its rows past that keep a hospital only on nights their body is read")
    try:
        return set_known_facilities(rows)
    except Exception as e:          # never let this read end the run (it starts run_all)
        logger.warning(f"Known facilities: not installed ({e}); no stored hospital is kept tonight")
        set_known_facilities([])
        return 0


# ── Applying it to a Job (finalize_jobs, before normalize_job) ──────────────
def _place(job, spec, town, st) -> bool:
    """The hospital's town on a posting that has none, or that sits at the
    system head office. Returns True when the location changed."""
    if not (town and st):
        return False
    jc = (job.city or "").strip()
    js = (job.state or "").strip().upper()
    at_hq = _at_hq(spec, jc, js)
    blank = not jc and js in ("", st)
    if not (at_hq or blank) or (fkey(jc) == fkey(town) and js == st):
        return False
    job.city, job.state, job.location = town, st, f"{town}, {st}"
    return True


def apply_facility(job) -> str:
    """Store the hospital on one Job of a FACILITY_SYSTEMS system. Returns
    what happened: "list" (the adapter's value, canonicalised), "text" (the
    body or title), "kept" (the stored hospital, no body tonight), "none",
    or "" for any other system. hospital_system is never changed."""
    spec = spec_for(job.hospital_system)
    if spec is None:
        return ""
    cur = clean_text(job.hospital_name)
    how, hit = "none", None
    if cur and cur not in spec.labels:
        hit = resolve(job.hospital_system, cur, job.city, job.state, job.title)
        if not hit:
            return "other"        # an adapter's own facility name we do not list: left as it is
        how = "list"
    if hit is None:
        hit = text_facility(job.hospital_system, job.description, job.title, job.city, job.state)
        if hit:
            how = "text"
    if hit is None and spec.fallback and not getattr(job, SEEN_ATTR, False) \
            and len((job.description or "").strip()) < NO_BODY_CHARS:
        # The stored name is one this code wrote: taken as it is, unless the
        # posting now sits in another state than that hospital.
        name = canonical_name(job.hospital_system, known_facility(job.hospital_system, job.job_id))
        if name:
            town, hst = spec.hospitals[name]
            ks = _posting_state(spec, job.city, job.state)
            if not ks or ks == hst:
                hit, how = (name, town, hst), "kept"
    if hit is None:
        return "none"
    job.hospital_name = hit[0]
    _place(job, spec, hit[1], hit[2])
    return how


def apply_facilities(jobs) -> dict:
    """apply_facility over a run's rows. Returns {system: {how: rows}}."""
    stats: dict = {}
    for job in jobs:
        how = apply_facility(job)
        if how:
            d = stats.setdefault(job.hospital_system, {})
            d[how] = d.get(how, 0) + 1
    return stats


def mark_seen(job) -> None:
    """The row's own fetch tonight named no hospital: do not fall back to
    the stored one."""
    try:
        setattr(job, SEEN_ATTR, True)
    except Exception:
        pass
