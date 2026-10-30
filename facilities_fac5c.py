"""The hospital or named site on the rows of the systems phase 5 group C found still on the system name (2026-10-29).

The owner, 2026-10-29: "i thought you were gonna edit all the large
multihospital systems". His rules stand: (1) cards and job pages show the
HOSPITAL or named site ("Apply to <site>"); the system name and logo stay on
the job page and the system hub (hospital_system is never touched here: the
upsert key, the hub, the filters, the logo); (2) named sites (hospitals,
named clinics, centers, agencies' named branches, named communities) show
their own name; generic offices, home-based, remote, multi-site pools and
regional roles show the system name with the town; a town or a site is never
guessed (a town-to-site directory is no evidence: the posting itself must
name the site; a town with exactly one hospital of the system takes it only
under the phase 2 town-table rule of facilities_town, never for a clinic, an
office or a remote posting).

What the stored label rows of these systems carry (read 2026-10-29, after
the phase 2, 3 and 4 SQL), and who names them:

  TOWN TABLES (registered in facilities_town at import; its rule decides:
  remote, clinic, office, provider-group, care-site and multi-site postings
  keep the label, a town with two hospitals needs a title naming one, a
  stamped default town is no evidence):
    Appalachian Regional Healthcare (iCIMS)  14 hospitals, each alone in its
                        town (CMS list); Lexington is the head office.
    DCH Health System (iCIMS)  Tuscaloosa, Northport, Fayette.
    Shriners Children's (Workday)  one site per town (CMS list, the 2021
                        names the system's own pages use: "Shriners
                        Children's Ohio" in Dayton ...).
    St. Charles Health  Bend, Redmond, Prineville, Madras.
    Children's Wisconsin  Fox Valley Hospital (Neenah); Milwaukee is kept
                        (child welfare, community services and the head
                        office hire there as much as the hospital), and its
                        child welfare / family services titles are no
                        hospital's anywhere.
    Premier Health      Miami Valley Hospital and Miami Valley Hospital North
                        share Dayton (the stamped default); Centerville,
                        Middletown and Troy hold one each.
    Hendrick Health     Abilene holds Hendrick Medical Center (North campus)
                        and Hendrick Medical Center South; Brownwood one.
    Sharp HealthCare    La Mesa, Chula Vista, Coronado one each; San Diego
                        (the Workday tenant's stamp) holds Memorial, Mary
                        Birch and Mesa Vista.
    Mary Washington Healthcare  Stafford one; Fredericksburg holds Mary
                        Washington Hospital and Snowden at Fredericksburg.
    UW Medicine         Seattle holds Harborview, UW Medical Center -
                        Montlake and - Northwest; Renton (Valley) is kept:
                        UW's central lab and offices sit there too.
    Children's Hospital of Philadelphia  King of Prussia holds CHOP's
                        Middleman Family Pavilion (the Workday site "King of
                        Prussia Hospital"); Philadelphia is kept: the
                        system's name is the main campus's own name.
    Stormont Vail Health  Junction City holds the Flint Hills Campus (titles
                        "(Flint Hills)"); Topeka is kept: the system's name
                        is the main hospital's own (Stormont Vail Hospital).
    Cape Cod Healthcare  Hyannis (Cape Cod Hospital) and Falmouth (Falmouth
                        Hospital). Every row is stamped Hyannis (Taleo gives
                        no town): only a town the title names in brackets
                        counts ("Urgent Care Receptionist (Sandwich)").

  TITLE / BODY / LINK READERS (this module's pass, before the town tables):
    Hendrick Health     an Abilene title ending "- North" / "- South" ("RN
                        -CCU / ICU - South"): the campus. "Hendrick Clinic
                        South ..." is a clinic (the label).
    Premier Health      "MVH", "MVH North", "Miami Valley South", "UVMC",
                        "Upper Valley", "Atrium" in the title.
    Temple Health       "Episcopal", "Jeanes", "Fox Chase" / "FCCC",
                        "Chestnut Hill", "Women and Families" in a
                        Philadelphia title (the town holds all six sites).
    Sharp HealthCare    "Sharp Grossmont", "Sharp Memorial", "Sharp Mary
                        Birch", "Sharp Mesa Vista", "Sharp Chula Vista",
                        "Sharp Coronado" in the title; Rees-Stealy (the
                        medical group), Corporate, the Metro Campus and the
                        Outpatient Pavilion keep the label.
    UW Medicine         "Harborview", "UWMC Montlake", "UWMC Northwest",
                        "Valley Medical Center" in the title; "UW Medicine
                        Primary Care at ..." is a clinic; "UW Medical Center"
                        alone names no campus.
    Bellin Health       the "Location:" line's street: 744 S Webster Ave is
                        Bellin Hospital, 301 E St Joseph St the Bellin
                        Psychiatric Center (their published addresses);
                        other streets are clinics and offices.
    Stanford Health Care  the Workday site "5555 W Las Positas Blvd -
                        PLEASANTON" is Stanford Health Care Tri-Valley (its
                        street address); Palo Alto rows keep the label, the
                        main hospital's licensed name (CMS 050441).
    Cape Cod Healthcare  a title naming "VNA of Cape Cod" (the system's home
                        health agency), "Heritage at Falmouth" or "JML Care
                        Center" (its Falmouth senior campus); a bracketed
                        town in the title gives the town.
    Norton Sound Regional Hospital  a title ending in a village of its
                        service area ("Clinician I - Unalakleet"): the
                        village is the town (a village clinic keeps the
                        system name: decision 2); rows were stamped Nome.

Nights without a body: the town tables' stored decisions (facilities_town.
STORED) stand for every system with a table here, and this pass leaves such
a row to them, so a row never flips between nights. The Stanford and Norton
Sound readers read only the link and the title, which every night carries.

Shared with the stored-row change list (claud_outputinput/
facility_scale_2026-10-29/phase5/c_work/changes.py), so the database holds
what the scraper writes on the next run.
"""
from __future__ import annotations

import logging
import re

import facilities_town as FT
import facilities_workday
from city_utils import known_town

logger = logging.getLogger("scraper.facility_fac5c")

clean_text = FT.clean_text
_h = FT._h

ARH = "Appalachian Regional Healthcare"
DCH = "DCH Health System"
SHRINERS = "Shriners Children's"
STCHARLES = "St. Charles Health"
CHW = "Children's Wisconsin"
PREMIER = "Premier Health"
HENDRICK = "Hendrick Health"
SHARP = "Sharp HealthCare"
MWHC = "Mary Washington Healthcare"
UWM = "UW Medicine"
CHOP = "Children's Hospital of Philadelphia"
STORMONT = "Stormont Vail Health"
CAPECOD = "Cape Cod Healthcare"
TEMPLE = "Temple Health"
BELLIN = "Bellin Health"
STANFORD = "Stanford Health Care"
NORTON = "Norton Sound Regional Hospital"


# ══════════════════════════════════════════════════════════════════════════
#  1. TOWN TABLES (facilities_town's rule)
# ══════════════════════════════════════════════════════════════════════════
ARH_TOWNS = FT._spec(ARH, [
    _h("Hazard ARH Regional Medical Center", "Hazard", "KY", "cms 180029", "=US-Hazard"),
    _h("Highlands ARH Regional Medical Center", "Prestonsburg", "KY", "cms 180005"),
    _h("Beckley ARH Hospital", "Beckley", "WV", "cms 510062"),
    _h("Harlan ARH Hospital", "Harlan", "KY", "cms 180050"),
    _h("Tug Valley ARH Regional Medical Center", "South Williamson", "KY", "cms 180069"),
    _h("Paintsville ARH Hospital", "Paintsville", "KY", "cms 180078"),
    # CMS 181328 still lists the old name, Knox County Hospital; ARH has run
    # it as Barbourville ARH Hospital since it took the hospital over
    _h("Barbourville ARH Hospital", "Barbourville", "KY", "cms 181328"),
    _h("Whitesburg ARH Hospital", "Whitesburg", "KY", "cms 180002"),
    _h("Middlesboro ARH Hospital", "Middlesboro", "KY", "cms 180020"),
    _h("McDowell ARH Hospital", "McDowell", "KY", "cms 181331"),
    _h("Morgan County ARH Hospital", "West Liberty", "KY", "cms 181307"),
    _h("Mary Breckinridge ARH Hospital", "Hyden", "KY", "cms 181316"),
    _h("ARH Our Lady of the Way", "Martin", "KY", "cms 181305"),
    _h("Summers County ARH Hospital", "Hinton", "WV", "cms 511310"),
])

DCH_TOWNS = FT._spec(DCH, [
    _h("DCH Regional Medical Center", "Tuscaloosa", "AL", "cms 010092"),
    _h("Northport Medical Center", "Northport", "AL", "cms 010145"),
    _h("Fayette Medical Center", "Fayette", "AL", "cms 010045"),
])

SHRINERS_TOWNS = FT._spec(SHRINERS, [
    _h("Shriners Children's Portland", "Portland", "OR", "cms 383300"),
    _h("Shriners Children's Northern California", "Sacramento", "CA", "cms 053311"),
    _h("Shriners Children's St. Louis", "St. Louis", "MO", "cms 263304"),
    _h("Shriners Children's Spokane", "Spokane", "WA", "cms 503302"),
    _h("Shriners Children's Chicago", "Chicago", "IL", "cms 143302"),
    _h("Shriners Children's Texas", "Galveston", "TX", "cms 453311"),
    _h("Shriners Children's Ohio", "Dayton", "OH", "cms 363308"),
    _h("Shriners Children's Philadelphia", "Philadelphia", "PA", "cms 393309"),
    _h("Shriners Children's Hawaii", "Honolulu", "HI", "cms 123301"),
    _h("Shriners Children's Southern California", "Pasadena", "CA", "cms 053310"),
    _h("Shriners Children's Boston", "Boston", "MA", "cms 223304"),
    _h("Shriners Children's Salt Lake City", "Salt Lake City", "UT", "cms 463302"),
    _h("Shriners Children's Greenville", "Greenville", "SC", "cms 423300"),
    _h("Shriners Children's New England", "Springfield", "MA", "cms 223303"),
    _h("Shriners Children's Shreveport", "Shreveport", "LA", "cms 193301"),
    _h("Shriners Children's Florida", "Tampa", "FL", "cms 103303"),
    _h("Shriners Children's Twin Cities", "Woodbury", "MN", "site"),
    _h("Shriners Children's Erie", "Erie", "PA", "site"),
    _h("Shriners Children's Lexington", "Lexington", "KY", "site"),
])

STCHARLES_TOWNS = FT._spec(STCHARLES, [
    _h("St. Charles Bend", "Bend", "OR", "cms 380047"),
    _h("St. Charles Redmond", "Redmond", "OR", "cms 380040"),
    _h("St. Charles Prineville", "Prineville", "OR", "cms 381313"),
    _h("St. Charles Madras", "Madras", "OR", "cms 381324"),
])

CHW_TOWNS = FT._spec(CHW, [
    _h("Children's Wisconsin Milwaukee Hospital", "Milwaukee", "WI", "site"),
    _h("Children's Wisconsin Fox Valley Hospital", "Neenah", "WI", "site"),
], default=("Milwaukee", "WI"), keep={
    "Milwaukee, WI": "the system's child welfare, community services and head office hire in Milwaukee as much as "
                     "the hospital: the town alone does not tell"})
# its child welfare and family services and its regional roles are no
# hospital's, whatever town they sit in
FT.CARE_SITE_BY_SYSTEM[CHW] = re.compile(
    r"\b(?:child\s+welfare|foster|family\s+(?:support|preservation|services?)|case\s*(?:worker|aide)|"
    r"northeast\s+wisconsin|community\s+services?)\b", re.I)

PREMIER_SITES = {
    "mvh": ("Miami Valley Hospital", "Dayton", "OH", "cms 360051"),
    "mvhn": ("Miami Valley Hospital North", "Dayton", "OH", "site (the board names it)"),
    "mvhs": ("Miami Valley Hospital South", "Centerville", "OH", "site (the board names it)"),
    "amc": ("Atrium Medical Center", "Middletown", "OH", "cms 360076"),
    "uvmc": ("Upper Valley Medical Center", "Troy", "OH", "cms 360174"),
}
PREMIER_TOWNS = FT._spec(PREMIER, [_h(*v) for v in PREMIER_SITES.values()], default=("Dayton", "OH"))

HENDRICK_SITES = {
    "north": ("Hendrick Medical Center", "Abilene", "TX", "cms 450229"),
    "south": ("Hendrick Medical Center South", "Abilene", "TX", "cms 450558"),
    "brownwood": ("Hendrick Medical Center Brownwood", "Brownwood", "TX", "cms 450587"),
}
HENDRICK_TOWNS = FT._spec(HENDRICK, [_h(*v) for v in HENDRICK_SITES.values()])

SHARP_SITES = {
    "grossmont": ("Sharp Grossmont Hospital", "La Mesa", "CA", "cms 050026"),
    "chula": ("Sharp Chula Vista Medical Center", "Chula Vista", "CA", "cms 050222"),
    "coronado": ("Sharp Coronado Hospital", "Coronado", "CA", "cms 050234"),
    "memorial": ("Sharp Memorial Hospital", "San Diego", "CA", "cms 050100"),
    "birch": ("Sharp Mary Birch Hospital for Women & Newborns", "San Diego", "CA", "cms 050722"),
    "mesavista": ("Sharp Mesa Vista Hospital", "San Diego", "CA", "cms 054145"),
}
SHARP_TOWNS = FT._spec(SHARP, [_h(*v) for v in SHARP_SITES.values()], default=("San Diego", "CA"))

# FORCE_LOCATION_OVERRIDE puts every Mary Washington row in Fredericksburg
# unless facilities_workday.placed() knows the place: the hospitals are
# listed in its table too (a Workday site spelled as the hospital names it).
facilities_workday._t(MWHC, """
Stafford-Hospital | Stafford Hospital | Stafford | VA | cms 490140
Mary-Washington-Hospital | Mary Washington Hospital | Fredericksburg | VA | cms 490022
Snowden-at-Fredericksburg | Snowden at Fredericksburg | Fredericksburg | VA | cms 494028
""")
MWHC_TOWNS = FT._spec(MWHC, [
    _h("Mary Washington Hospital", "Fredericksburg", "VA", "cms 490022"),
    _h("Snowden at Fredericksburg", "Fredericksburg", "VA", "cms 494028"),
    _h("Stafford Hospital", "Stafford", "VA", "cms 490140"),
], default=("Fredericksburg", "VA"))

UW_SITES = {
    "harborview": ("Harborview Medical Center", "Seattle", "WA", "cms 500064"),
    "montlake": ("UW Medical Center - Montlake", "Seattle", "WA", "cms 500008"),
    "northwest": ("UW Medical Center - Northwest", "Seattle", "WA", "cms 500001"),
    "valley": ("Valley Medical Center", "Renton", "WA", "cms 500088"),
}
UW_TOWNS = FT._spec(UWM, [_h(*v) for v in UW_SITES.values()], keep={
    "Renton, WA": "UW Medicine's central laboratory and offices sit in Renton; Valley Medical Center hires on its "
                  "own board: only a title naming Valley counts"})

CHOP_TOWNS = FT._spec(CHOP, [
    _h("CHOP Philadelphia Campus", "Philadelphia", "PA", "cms 393303"),
    # the Workday site "King of Prussia Hospital": CHOP's inpatient hospital
    # at 550 S Goddard Blvd, King of Prussia, the Middleman Family Pavilion
    _h("CHOP Middleman Family Pavilion", "King of Prussia", "PA", "site"),
], keep={"Philadelphia, PA": "main campus: the system's name is the main campus's own name"})

STORMONT_TOWNS = FT._spec(STORMONT, [
    _h("Stormont Vail Hospital", "Topeka", "KS", "cms 170086"),
    _h("Stormont Vail Health Flint Hills Campus", "Junction City", "KS", "site"),
], default=("Topeka", "KS"),
   keep={"Topeka, KS": "main hospital: the system's name is the hospital's own (Stormont Vail Hospital)"})

CAPECOD_SITES = {
    "hyannis": ("Cape Cod Hospital", "Hyannis", "MA", "cms 220012"),
    "falmouth": ("Falmouth Hospital", "Falmouth", "MA", "cms 220135"),
}
# Every Cape Cod row is stamped Hyannis (the default), so the stamp is never
# evidence; a bracketed title town is ("(Falmouth)" goes to the town table;
# "(Hyannis)", the stamp's own town, is decided in capecod_read by the same
# tests), and a row whose title names no town is left alone (job._town_skip).
CAPECOD_TOWNS = FT._spec(CAPECOD, [_h(*v) for v in CAPECOD_SITES.values()], default=("Hyannis", "MA"))

TOWN_SPECS = (ARH_TOWNS, DCH_TOWNS, SHRINERS_TOWNS, STCHARLES_TOWNS, CHW_TOWNS, PREMIER_TOWNS, HENDRICK_TOWNS,
              SHARP_TOWNS, MWHC_TOWNS, UW_TOWNS, CHOP_TOWNS, STORMONT_TOWNS, CAPECOD_TOWNS)


# ══════════════════════════════════════════════════════════════════════════
#  2. TITLE / BODY / LINK READERS (this module's pass)
# ══════════════════════════════════════════════════════════════════════════
def _skip(system, title, description) -> bool:
    """A posting no one site holds, whatever it names (facilities_town's
    tests): remote, a care site, a multi-site pool, an office line, a
    system-office executive."""
    return (FT.is_remote(title, description) or FT.is_care_site(system, title)
            or FT.is_multi_site(title, description) or FT.where_not_hospital(description)
            or bool(FT.SYSTEM_ROLE_RX.search(clean_text(title))))


def _one(hits):
    names = {h[0] for h in hits}
    return hits[0] if len(names) == 1 else None


def _at(site, city, state, default=None) -> bool:
    """The posting sits in the site's own town, has no town, or sits at the
    stamped default (then the title, which names the site, gives the town)."""
    jc, js = clean_text(city), clean_text(state).upper()
    if not jc:
        return True
    if FT.town_key(jc) == FT.town_key(site[1]) and js in ("", site[2]):
        return True
    return bool(default) and jc.lower() == default[0].lower() and js in ("", default[1])


# Hendrick: "- North" / "- South" (or ", North", "OR North", "South Aug - Dec")
_HENDRICK_RX = re.compile(r"(?:^|[\s,/(–-])(North|South)(?=\s*(?:$|[-–),]|\s+(?:Aug|Jan|Nights?|Days|Full|Pool|BSN)\b))",
                          re.I)


def hendrick_read(title, description, city, state):
    t = clean_text(title)
    if FT.town_key(city) != "abilene" or FT._offsite_by_title(t, description, HENDRICK_TOWNS):
        return None
    hits = [HENDRICK_SITES[m.group(1).lower()] for m in _HENDRICK_RX.finditer(t)]
    return _one(hits) if hits else None


_PREMIER_RX = (
    (re.compile(r"\b(?:MVH[\s-]*NORTH|MVHN|MIAMI\s+VALLEY(?:\s+HOSPITAL)?\s+NORTH)\b", re.I), "mvhn"),
    (re.compile(r"\b(?:MVH[\s-]*SOUTH|MVHS|MIAMI\s+VALLEY(?:\s+HOSPITAL)?\s+SOUTH)\b", re.I), "mvhs"),
    (re.compile(r"\b(?:MVH|MIAMI\s+VALLEY\s+HOSPITAL)\b(?![\s-]*(?:NORTH|SOUTH|N\b|S\b))", re.I), "mvh"),
    (re.compile(r"\b(?:UVMC|UPPER\s+VALLEY(?:\s+MEDICAL\s+CENTER)?)\b", re.I), "uvmc"),
    (re.compile(r"\bATRIUM(?:\s+MEDICAL\s+CENTER)?\b", re.I), "amc"),
)
_FACILITY_LINE_RX = re.compile(r"(?im)^[ \t*•-]*Facility[ \t]*:")


def premier_read(title, description, city, state):
    t = clean_text(title)
    # a "Facility:" line is facilities_text's to read (it names an office or
    # a region when it names no hospital)
    if _FACILITY_LINE_RX.search(str(description or "")) or FT._offsite_by_title(t, description, PREMIER_TOWNS):
        return None
    hits = [PREMIER_SITES[k] for rx, k in _PREMIER_RX if rx.search(t)]
    hit = _one(hits) if hits else None
    if hit is None or not _at(hit, city, state, PREMIER_TOWNS.default):
        return None
    return hit


_TEMPLE_RX = (
    (re.compile(r"\bEpiscopal\b", re.I), "Temple University Hospital - Episcopal Campus"),
    (re.compile(r"\bJeanes\b", re.I), "Temple University Hospital - Jeanes Campus"),
    (re.compile(r"\bFox\s+Chase\b|\bFCCC\b", re.I), "Fox Chase Cancer Center"),
    (re.compile(r"\bChestnut\s+Hill\b", re.I), "Temple Health - Chestnut Hill Hospital"),
    (re.compile(r"\bWomen\s*(?:&|and)\s*Families\b|\bTWFH\b", re.I), "Temple Women & Families Hospital"),
)


def temple_read(title, description, city, state):
    t = clean_text(title)
    spec = FT.SPECS.get(TEMPLE)
    if spec is None or FT._offsite_by_title(t, description, spec):
        return None
    names = {n for rx, n in _TEMPLE_RX if rx.search(t)}
    if len(names) != 1:
        return None
    h = spec.names.get(FT.name_key(next(iter(names))))
    if h is None or not _at(h, city, state):
        return None
    return h[0], h[1], h[2], h[3]


_SHARP_NOT = re.compile(r"\bRees[\s-]*Stealy\b|\bSharp\s+Corporate\b|\bMetro\s+Campus\b|\bOutpatient\s+Pavilion\b|"
                        r"\bSpecialty\s+Pharmacy\b|\bHospiceCare\b|\bHealth\s+Plan\b|\bMcDonald\s+Center\b", re.I)
_SHARP_RX = (
    (re.compile(r"\b(?:Sharp\s+)?Grossmont(?:\s+Hospital)?\b", re.I), "grossmont"),
    (re.compile(r"\bSharp\s+Memorial(?:\s+Hospital)?\b", re.I), "memorial"),
    (re.compile(r"\b(?:Sharp\s+)?Mary\s+Birch\b", re.I), "birch"),
    (re.compile(r"\b(?:Sharp\s+)?Mesa\s+Vista(?:\s+Hospital)?\b", re.I), "mesavista"),
    (re.compile(r"\bSharp\s+Chula\s+Vista\b|\bChula\s+Vista\s+Medical\s+Center\b", re.I), "chula"),
    (re.compile(r"\bSharp\s+Coronado\b|\bCoronado\s+Hospital\b", re.I), "coronado"),
)


def sharp_read(title, description, city, state):
    t = clean_text(title)
    if _SHARP_NOT.search(t) or FT._offsite_by_title(t, description, SHARP_TOWNS):
        return None
    hits = [SHARP_SITES[k] for rx, k in _SHARP_RX if rx.search(t)]
    hit = _one(hits) if hits else None
    if hit is None or not _at(hit, city, state, SHARP_TOWNS.default):
        return None
    return hit


_UW_RX = (
    (re.compile(r"\bHarborview\b(?!\s+Eye\s+Institute)", re.I), "harborview"),
    (re.compile(r"\b(?:UW\s*Medical\s+Center|UWMC)\s*[-–,]?\s*Montlake\b|\bMontlake\b", re.I), "montlake"),
    (re.compile(r"\b(?:UW\s*Medical\s+Center|UWMC)\s*[-–,]?\s*(?:Northwest|NW)\b|\bNorthwest\s+Hospital\b", re.I),
     "northwest"),
    (re.compile(r"\bValley\s+Medical\s+Center\b", re.I), "valley"),
)
_UW_CLINIC = re.compile(r"\bPrimary\s+Care\b|\bEye\s+Institute\b|\bClinics?\b|\bUrgent\s+Care\b", re.I)


def uw_read(title, description, city, state):
    t = clean_text(title)
    if _UW_CLINIC.search(t) or FT._offsite_by_title(t, description, UW_TOWNS):
        return None
    hits = [UW_SITES[k] for rx, k in _UW_RX if rx.search(t)]
    hit = _one(hits) if hits else None
    if hit is None or not _at(hit, city, state):
        return None
    return hit


# Bellin: "Location: 744 S Webster Ave, Green Bay, WI 54301" (also inside a
# "Job Specifics:" line)
_BELLIN_LOC_RX = re.compile(r"(?im)^[ \t*•-]*(?:Job\s+Specifics[ \t]*:[ \t]*)?Location[ \t]*:[ \t]*([^\n]{4,120})$")
_BELLIN_ADDR = (
    (re.compile(r"^744\s+S(?:outh)?\.?\s+Webster\s+Ave", re.I), "Bellin Hospital"),
    (re.compile(r"^301\s+E(?:ast)?\.?\s+St\.?\s+Joseph\s+St", re.I), "Bellin Psychiatric Center"),
)


def bellin_read(title, description, city, state):
    spec = FT.SPECS.get(BELLIN)
    m = _BELLIN_LOC_RX.search(str(description or "")[:6000])
    if spec is None or not m or FT._offsite_by_title(clean_text(title), description, spec):
        return None
    v = clean_text(m.group(1))
    names = {n for rx, n in _BELLIN_ADDR if rx.search(v)}
    if len(names) != 1 or not re.search(r"\bGreen\s+Bay\b", v, re.I):
        return None
    h = spec.names.get(FT.name_key(next(iter(names))))
    if h is None or not _at(h, city, state):
        return None
    return h[0], h[1], h[2], h[3]


STANFORD_LINK = {
    "5555wlaspositasblvdpleasanton": ("Stanford Health Care Tri-Valley", "Pleasanton", "CA",
                                      "cms 050283; address 5555 W Las Positas Blvd, Pleasanton"),
}


def _link_site(url) -> str:
    u = str(url or "")
    if "/job/" not in u:
        return ""
    seg = u.split("/job/", 1)[1].split("/", 1)[0]
    return re.sub(r"[^a-z0-9]", "", seg.lower())


def stanford_read(title, description, city, state, url=""):
    hit = STANFORD_LINK.get(_link_site(url))
    if hit is None or FT.REMOTE_RX.search(clean_text(title)):
        return None
    return hit


CAPECOD_NAMED = (
    (re.compile(r"\bVNA\s+of\s+Cape\s+Cod\b", re.I), ("VNA of Cape Cod", "", "MA", "title: the system's home health agency")),
    (re.compile(r"\bHeritage\s+at\s+Falmouth\b", re.I), ("Heritage at Falmouth", "Falmouth", "MA", "title (its own name)")),
    (re.compile(r"\bJML\s+Care\s+Center\b", re.I), ("JML Care Center", "Falmouth", "MA", "address 184 Ter Heun Dr, Falmouth")),
)
_BRACKET_RX = re.compile(r"\(([^()]{3,40})\)")
CAPECOD_TOWN_FIX = {"masphee": "Mashpee"}
# Cape villages (USPS place names) the known-town list does not carry
CAPECOD_VILLAGES = {FT.town_key(v): v for v in (
    "Osterville", "Centerville", "Cotuit", "Marstons Mills", "West Barnstable", "Hyannis Port", "West Yarmouth",
    "South Yarmouth", "Yarmouth Port", "East Falmouth", "North Falmouth", "West Falmouth", "Woods Hole",
    "South Dennis", "Dennis Port", "East Sandwich", "Buzzards Bay", "Pocasset", "Sagamore", "East Harwich",
    "Harwich Port", "North Eastham")}


def capecod_title_town(title) -> str:
    """The Massachusetts town a bracketed part of the title names
    ("(Sandwich)", "(Per Diem) (Mashpee)"), else ""."""
    found = []
    for m in _BRACKET_RX.finditer(clean_text(title)):
        for part in m.group(1).split(","):
            p = part.strip()
            p = CAPECOD_TOWN_FIX.get(p.lower(), p)
            t = (known_town(p, "MA") or CAPECOD_VILLAGES.get(FT.town_key(p), "")) if p else ""
            if t:
                found.append(t)
    return found[0] if len(set(found)) == 1 else ""


def capecod_read(job):
    """(name, city, state) or None, and sets job._town_skip when the row
    keeps the Hyannis stamp (no title town): the town table must not take
    the stamp as evidence."""
    t = clean_text(job.title)
    town = capecod_title_town(t)
    for rx, site in CAPECOD_NAMED:
        if rx.search(t) and not FT.is_remote(t, job.description):
            name, city, st, _src = site
            if town:
                return name, town, "MA"
            if city:
                return name, city, st
            return name, clean_text(job.city), clean_text(job.state).upper()
    if town and FT.town_key(town) == FT.town_key(CAPECOD_TOWNS.default[0]):
        # the title names the stamp's own town: facilities_town would read
        # the row as stamped, so its town test runs here (Hyannis holds one
        # hospital, Cape Cod Hospital; a clinic, office, remote or care-site
        # posting keeps the label)
        if (_skip(CAPECOD, t, job.description) or FT.is_offsite(t, job.description, CAPECOD_TOWNS)
                or FT.name_key("Falmouth Hospital") in FT.name_key(t)):
            job._town_skip = True
            return CAPECOD, town, "MA"
        return CAPECOD_SITES["hyannis"][:3]
    if town:
        return CAPECOD, town, "MA"
    if clean_text(job.city).lower() in ("", "hyannis"):
        job._town_skip = True
    return None


NORTON_VILLAGES = ("Brevig Mission", "Council", "Diomede", "Elim", "Gambell", "Golovin", "Koyuk", "Savoonga",
                   "Shaktoolik", "Shishmaref", "Solomon", "St. Michael", "Stebbins", "Teller", "Unalakleet", "Wales",
                   "White Mountain")
_NORTON_ALIAS = {"brevig": "Brevig Mission", "st michael": "St. Michael", "saint michael": "St. Michael"}
_NORTON_RX = re.compile(r"[-,–]\s*([A-Za-z. ]{3,20}?)\s*$")


def norton_read(title, description, city, state):
    """The label at the village a title ends with ("Clinician I -
    Unalakleet"), for a row stamped Nome (or with no town)."""
    if clean_text(city).lower() not in ("", "nome"):
        return None
    m = _NORTON_RX.search(clean_text(title))
    if not m:
        return None
    v = m.group(1).strip()
    k = re.sub(r"[^a-z ]", "", v.lower()).strip()
    village = _NORTON_ALIAS.get(k) or next((x for x in NORTON_VILLAGES if x.lower().replace(".", "") == k), "")
    if not village:
        return None
    return NORTON, village, "AK", "title"


READERS = {HENDRICK: hendrick_read, PREMIER: premier_read, TEMPLE: temple_read, SHARP: sharp_read, UWM: uw_read,
           BELLIN: bellin_read, NORTON: norton_read}
SYSTEMS = frozenset(READERS) | {STANFORD, CAPECOD}


def decide(system, hospital_name, city, state, title="", description="", url=""):
    """(hospital_name, city, state, how) for one label row of a reader
    system, or (None, None, None, how). Pure, apart from Cape Cod (see
    apply_facility). how: "title", "address", "link", "town" (the label at
    the posting's own town), "" (nothing)."""
    s = (system or "").strip()
    if s not in SYSTEMS or clean_text(hospital_name) not in ("", s):
        return None, None, None, ""
    if s == STANFORD:
        hit = stanford_read(title, description, city, state, url)
        return (hit[0], hit[1], hit[2], "link") if hit else (None, None, None, "")
    if s != NORTON and _skip(s, title, description):
        return None, None, None, ""
    hit = READERS[s](title, description, city, state)
    if not hit:
        return None, None, None, ""
    how = "town" if hit[0] == s else ("address" if s == BELLIN else "title")
    return hit[0], hit[1], hit[2], how


def _set(job, name, city, state) -> None:
    job.hospital_name = name
    if city and (city, state) != (clean_text(job.city), clean_text(job.state).upper()):
        job.city, job.state, job.location = city, state, f"{city}, {state}"


def apply_facility(job) -> str:
    s = (job.hospital_system or "").strip()
    if s not in SYSTEMS or clean_text(job.hospital_name) not in ("", s):
        return ""
    if len(str(job.description or "").strip()) < FT.NO_BODY_CHARS and s in FT.SPECS \
            and (s, str(job.job_id)) in FT.STORED:
        return "stored"           # the town table's stored decision stands tonight
    if s == CAPECOD:
        hit = capecod_read(job)
        if not hit:
            return "stamp"
        _set(job, *hit)
        return "town" if hit[0] == s else "title"
    name, city, state, how = decide(s, job.hospital_name, job.city, job.state, job.title, job.description, job.url)
    if not name:
        return how or "none"
    _set(job, name, city, state)
    return how


ROW_ERRORS: dict = {}


def apply_facilities(jobs) -> dict:
    """apply_facility over a run's rows (finalize_jobs, after the phase 4
    passes, before the town tables). A row apply_facility raises on keeps
    the name and town it had and is counted ("error"). Returns {system:
    {how: rows}}."""
    if not FT._STORED_LOADED[0]:
        try:
            FT.load_stored()
        except Exception:
            FT.set_stored([])
    stats: dict = {}
    for job in jobs:
        before = (job.hospital_name, job.city, job.state, job.location)
        try:
            how = apply_facility(job)
        except Exception as e:
            job.hospital_name, job.city, job.state, job.location = before
            n = ROW_ERRORS.get(job.hospital_system, 0) + 1
            ROW_ERRORS[job.hospital_system] = n
            if n <= 3:
                logger.warning(f"Phase 5 C: one {job.hospital_system} row kept its name ({type(e).__name__}: {e})")
            how = "error"
        if how:
            d = stats.setdefault(job.hospital_system, {})
            d[how] = d.get(how, 0) + 1
    return stats
