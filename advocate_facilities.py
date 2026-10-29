"""One clean name and the real town per Advocate Health facility (2026-10-29).

The owner, 2026-10-29: "Need to sort the advocate health hospitals and get
contacts ... retro assigning the applies to individual hospitals within a
system." Every Advocate Health row carried the system label in hospital_name
(5,524 of 5,524 active rows on 2026-10-29), and 2,922 of them sat in
Charlotte, NC: the tenant default (SYSTEM_LOCATION_DEFAULTS) filled every row
whose location names a hospital by its street alone. Ex: R272624, a CNA job
in the emergency department of Advocate Sherman Hospital, Elgin, IL, was
stored as "Advocate Health", Charlotte, NC.

Advocate's Workday board (aah.wd5, site External) names each posting's site
in the location descriptor, "<site> - <street>[, <town>, <ST>]":
  "Advocate Sherman Hospital - 1425 N Randall Rd"              (IL, no town)
  "Aurora St Lukes Medical Center - 2900 W Oklahoma Ave"       (WI, no town)
  "Atrium Health Cleveland - 201 E Grover St, Shelby, NC"      (town given)
  "Chicago, IL - 3000 N Halsted St"                            (a clinic or office: town only)
The list's locationsText is that descriptor, or "2 Locations" on a posting
with more than one site; the externalPath's first segment is always the
PRIMARY site's descriptor slugged ("/job/Advocate-Sherman-Hospital---1425-N-
Randall-Rd/..."; read off the CXS detail payloads 2026-10-29, where
jobPostingInfo.location is that same primary site and additionalLocations
lists the others).

  site_key(descriptor)       the site part of a descriptor (before its
                             street), letters and digits only, lower case
  segment_key(segment)       the same key from an externalPath segment
  facility(loc, path)        (hospital_name, city, state) for one posting:
                             the descriptor when locationsText is one, else
                             the externalPath segment (the primary site). A
                             named hospital or center in FACILITIES gives its
                             one clean name and the town of its own street
                             address; one of the system's own offices
                             (OFFICES) gives the system label with its town;
                             anything else (a "Town, ST - street" clinic,
                             "Remote", a county, a site this table does not
                             know yet) gives None and the row is placed as
                             before
  unknown_site(loc, path)    the site text of a descriptor that names a site
                             but is not in the table ("" otherwise), for the
                             run log

Every town below is the facility's own street address: the CMS hospital list
in the database (hospitals.cms_facility_id given), else the town the board's
descriptor itself names, else the facility's published address, each checked
against the town the postings' own text names (2026-10-29: Sherman "Elgin,
IL" on 9 bodies, South Shore "Cudahy" on 17, Mount Pleasant on 23, Fond du Lac
on 13, Hazel Crest, Oak Lawn ...). The names are each facility's public name,
written once: the board's "St Lukes" is "St. Luke's", Wake Forest Baptist's
campuses carry their full names ("High Point Medical Center" is "Atrium
Health Wake Forest Baptist High Point Medical Center", as the CRM's contact
labels write them), and the system's own tags ("(Advocate Health Use Only)")
are dropped. A town the board writes ("Winston Salem") keeps the board's
spelling, so no row whose town was already right moves.

hospital_system stays "Advocate Health" (the upsert key, the hub, the filters
and the logo). Shared with the stored-row cleanup
(claud_outputinput/facility_scale_2026-10-29/advocate/make_sql.py), so the
database holds what the scraper keeps.
"""
from __future__ import annotations

import re

SYSTEM = "Advocate Health"

# site key -> (hospital_name, city, state, source). Source: "cms <id>" (the
# database's CMS hospital list), "board" (the descriptor names the town) or
# "address" (the facility's published street address).
FACILITIES: dict[str, tuple[str, str, str, str]] = {}


def _add(board_site: str, name: str, city: str, state: str, source: str) -> None:
    FACILITIES[re.sub(r"[^a-z0-9]", "", board_site.lower())] = (name, city, state, source)


# ── Advocate, Illinois (descriptors carry no town) ─────────────────────────
_add("Advocate Lutheran General Hospital", "Advocate Lutheran General Hospital", "Park Ridge", "IL", "cms 140223")
_add("Advocate Condell Medical Center", "Advocate Condell Medical Center", "Libertyville", "IL", "cms 140202")
_add("Advocate Christ Hospital & Medical Center", "Advocate Christ Medical Center", "Oak Lawn", "IL", "cms 140208")
_add("Advocate Illinois Masonic Medical Center", "Advocate Illinois Masonic Medical Center", "Chicago", "IL", "cms 140182")
_add("Advocate Good Shepherd Hospital", "Advocate Good Shepherd Hospital", "Barrington", "IL", "cms 140291")
_add("Advocate Good Samaritan Hospital", "Advocate Good Samaritan Hospital", "Downers Grove", "IL", "cms 140288")
_add("Advocate Sherman Hospital", "Advocate Sherman Hospital", "Elgin", "IL", "cms 140030")
# both of the board's Trinity sites (2320 E 93rd St, 8000 S Lake Shore Dr) are in Chicago
_add("Advocate Trinity Hospital", "Advocate Trinity Hospital", "Chicago", "IL", "cms 140048")
_add("Advocate South Suburban Hospital", "Advocate South Suburban Hospital", "Hazel Crest", "IL",
     "address 17800 S Kedzie Ave, Hazel Crest")
# the children's hospital's two campuses: 4440 W 95th St (the Christ campus) and 1675 Dempster St
_add("Advocate Children's Hospital", "Advocate Children's Hospital Oak Lawn", "Oak Lawn", "IL",
     "address 4440 W 95th St, Oak Lawn")
_add("Advocate Children's Hospital - Park Ridge", "Advocate Children's Hospital Park Ridge", "Park Ridge", "IL",
     "address 1675 Dempster St, Park Ridge")

# ── Aurora, Wisconsin (descriptors carry no town) ──────────────────────────
_add("Aurora St Lukes Medical Center", "Aurora St. Luke's Medical Center", "Milwaukee", "WI", "cms 520138")
_add("Aurora St Lukes South Shore", "Aurora St. Luke's South Shore", "Cudahy", "WI", "address 5900 S Lake Dr, Cudahy")
_add("Aurora Sinai Medical Center", "Aurora Sinai Medical Center", "Milwaukee", "WI", "address 945 N 12th St, Milwaukee")
_add("Aurora Medical Center West Allis", "Aurora West Allis Medical Center", "West Allis", "WI", "cms 520139")
_add("Aurora Medical Center Grafton", "Aurora Medical Center Grafton", "Grafton", "WI", "cms 520207")
_add("Aurora BayCare Medical Center", "Aurora BayCare Medical Center", "Green Bay", "WI", "cms 520193")
_add("Aurora Medical Center Sheboygan County", "Aurora Medical Center Sheboygan County", "Sheboygan", "WI",
     "cms 520035")
# the board's older Sheboygan site (2629 N 7th St, CMS 520035's listed street), its own name on the board
_add("Aurora Medical Center Sheboygan", "Aurora Medical Center Sheboygan", "Sheboygan", "WI",
     "address 2629 N 7th St, Sheboygan")
_add("Aurora Medical Center Oshkosh", "Aurora Medical Center Oshkosh", "Oshkosh", "WI", "cms 520198")
_add("Aurora Medical Center Oshkosh: Fond du Lac Campus", "Aurora Medical Center Fond du Lac", "Fond du Lac", "WI",
     "address 210 Wisconsin American Dr, Fond du Lac")
_add("Aurora Medical Center Summit", "Aurora Medical Center Summit", "Summit", "WI", "cms 520206")
_add("Aurora Medical Center Kenosha", "Aurora Medical Center Kenosha", "Kenosha", "WI", "cms 520189")
_add("Aurora Medical Center Manitowoc County", "Aurora Medical Center Manitowoc County", "Two Rivers", "WI",
     "cms 520034")
_add("Aurora Medical Center Bay Area", "Aurora Medical Center Bay Area", "Marinette", "WI", "cms 520113")
_add("Aurora Medical Center Mount Pleasant", "Aurora Medical Center Mount Pleasant", "Mount Pleasant", "WI",
     "address 13250 Washington Ave, Mount Pleasant")
_add("Aurora Medical Center Burlington", "Aurora Medical Center Burlington", "Burlington", "WI", "cms 520059")
_add("Aurora Medical Center Lakeland", "Aurora Medical Center Lakeland", "Elkhorn", "WI", "cms 520102")
_add("Aurora Medical Center Washington County", "Aurora Medical Center Washington County", "Hartford", "WI",
     "cms 520038")
_add("Aurora Psychiatric Hospital", "Aurora Psychiatric Hospital", "Wauwatosa", "WI", "cms 524000")

# ── Atrium Health and Wake Forest Baptist (the descriptor names the town) ──
_add("Atrium Health Wake Forest Baptist", "Atrium Health Wake Forest Baptist Medical Center", "Winston Salem", "NC",
     "board; cms 340047")
_add("Carolinas Medical Center", "Atrium Health Carolinas Medical Center", "Charlotte", "NC", "board; cms 340113")
_add("Atrium Health Navicent The Medical Center", "Atrium Health Navicent The Medical Center", "Macon", "GA",
     "board; cms 110107")
_add("Atrium Health Cabarrus", "Atrium Health Cabarrus", "Concord", "NC", "board; cms 340001")
_add("Atrium Health Floyd Medical Center", "Atrium Health Floyd Medical Center", "Rome", "GA", "board; cms 110054")
_add("Atrium Health Union", "Atrium Health Union", "Monroe", "NC", "board; cms 340130")
_add("Atrium Health Union West", "Atrium Health Union West", "Matthews", "NC", "board")
_add("Atrium Health Pineville", "Atrium Health Pineville", "Charlotte", "NC", "board; cms 340098")
_add("Atrium Health Pineville Rehabilitation Hospital", "Atrium Health Pineville Rehabilitation Hospital",
     "Charlotte", "NC", "board")
_add("Atrium Health University City", "Atrium Health University City", "Charlotte", "NC", "board; cms 340166")
_add("Atrium Health Mercy", "Atrium Health Mercy", "Charlotte", "NC", "board")
_add("Atrium Health Cleveland", "Atrium Health Cleveland", "Shelby", "NC", "board; cms 340021")
_add("Atrium Health Kings Mountain", "Atrium Health Kings Mountain", "Kings Mountain", "NC", "board")
_add("Atrium Health Stanly", "Atrium Health Stanly", "Albemarle", "NC", "board; cms 340119")
_add("Atrium Health Lincoln", "Atrium Health Lincoln", "Lincolnton", "NC", "board; cms 340145")
_add("Atrium Health Lake Norman", "Atrium Health Lake Norman", "Cornelius", "NC", "board")
_add("Atrium Health Huntersville", "Atrium Health Huntersville", "Huntersville", "NC", "board")
_add("Atrium Health Anson", "Atrium Health Anson", "Wadesboro", "NC", "board; cms 340084")
_add("Atrium Health Behavioral Health Charlotte", "Atrium Health Behavioral Health Charlotte", "Charlotte", "NC",
     "board")
_add("Atrium Behavioral Health Davidson", "Atrium Behavioral Health Davidson", "Davidson", "NC", "board")
_add("Carolinas Rehabilitation NorthEast", "Carolinas Rehabilitation NorthEast", "Concord", "NC", "board")
_add("High Point Medical Center", "Atrium Health Wake Forest Baptist High Point Medical Center", "High Point", "NC",
     "board; cms 340004")
_add("Lexington Medical Center", "Atrium Health Wake Forest Baptist Lexington Medical Center", "Lexington", "NC",
     "board; cms 340096")
_add("Davie Medical Center", "Atrium Health Wake Forest Baptist Davie Medical Center", "Bermuda Run", "NC", "board")
_add("Wilkes Medical Center", "Atrium Health Wake Forest Baptist Wilkes Medical Center", "North Wilkesboro", "NC",
     "board; cms 340064")
# the one Wake Forest Baptist affiliate whose descriptor carries no town (180 Parkwood Dr)
_add("Hugh Chatham Memorial Hospital", "Hugh Chatham Memorial Hospital", "Elkin", "NC", "cms 340097")
_add("Alleghany Health Memorial Hospital", "Alleghany Memorial Hospital", "Sparta", "NC", "board; cms 341320")
_add("Scotland Memorial Hospital (Advocate Health Use Only)", "Scotland Memorial Hospital", "Laurinburg", "NC",
     "board; cms 340008")
_add("Atrium Health Navicent Baldwin", "Atrium Health Navicent Baldwin", "Milledgeville", "GA", "board; cms 110150")
_add("Navicent Rehabilitation Hospital", "Atrium Health Navicent Rehabilitation Hospital", "Macon", "GA", "board")
_add("Atrium Health Navicent Peach", "Atrium Health Navicent Peach", "Byron", "GA", "board; cms 111310")
_add("Atrium Health Floyd Polk Medical Center", "Atrium Health Floyd Polk Medical Center", "Cedartown", "GA",
     "board; cms 111330")
_add("Atrium Health Floyd Cherokee Medical Center", "Atrium Health Floyd Cherokee Medical Center", "Centre", "AL",
     "board; cms 010022")

# The system's own offices (AAO: the Advocate Aurora operations offices):
# the row keeps the system label and takes the office's town.
OFFICES: dict[str, tuple[str, str]] = {
    "aaooakbrook": ("Oak Brook", "IL"),      # 2025 Windsor Dr, Oak Brook
    "aaomilwaukee": ("Milwaukee", "WI"),     # 750 W Virginia St, Milwaukee
}

_N_LOCATIONS_RX = re.compile(r"^\s*\d+\s+locations?\s*$", re.I)
# "Chicago, IL", "Kenosha County, WI", "Chicago Southeast, IL,": a town or an
# area, never a facility.
_PLACE_RX = re.compile(r"^[A-Za-z .'/-]+,\s*[A-Z]{2}\b")
_SEG_PLACE_RX = re.compile(r"^[A-Za-z-]+-[A-Z]{2}$")


def _key(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def site_text(descriptor: str) -> str:
    """The site part of a location descriptor: everything before its last
    " - " ("Advocate Children's Hospital - Park Ridge - 1675 Dempster St" ->
    "Advocate Children's Hospital - Park Ridge"); "" when there is no street
    part ("Remote", "Kenosha County, WI")."""
    parts = re.split(r"\s+-\s+", (descriptor or "").strip())
    return " - ".join(parts[:-1]).strip() if len(parts) > 1 else ""


def site_key(descriptor: str) -> str:
    return _key(site_text(descriptor))


def path_segment(external_path: str) -> str:
    m = re.match(r"^/?job/([^/]+)/", external_path or "")
    return m.group(1) if m else ""


def segment_key(segment: str) -> str:
    """The site key of an externalPath segment (the slug of the descriptor:
    " - " is "---", "&" is dropped): every chunk but the last (the street)."""
    chunks = [c for c in re.split(r"-{3,}", segment or "") if c.strip("-")]
    return _key("".join(chunks[:-1])) if len(chunks) > 1 else ""


def _site(loc: str, external_path: str) -> tuple[str, str]:
    """(site key, site text) of a posting's primary site: the descriptor when
    locationsText is one, else the externalPath segment."""
    loc = (loc or "").strip()
    if loc and not _N_LOCATIONS_RX.match(loc):
        return site_key(loc), site_text(loc)
    seg = path_segment(external_path)
    chunks = [c for c in re.split(r"-{3,}", seg) if c.strip("-")]
    return segment_key(seg), (" ".join(chunks[:-1]).replace("-", " ") if len(chunks) > 1 else "")


def facility(loc: str, external_path: str = "") -> tuple[str, str, str] | None:
    """(hospital_name, city, state) for one posting, or None to place the row
    as before (see the module note)."""
    key, _text = _site(loc, external_path)
    if not key:
        return None
    hit = FACILITIES.get(key)
    if hit:
        return hit[0], hit[1], hit[2]
    office = OFFICES.get(key)
    if office:
        return SYSTEM, office[0], office[1]
    return None


def unknown_site(loc: str, external_path: str = "") -> str:
    """A site the descriptor names that neither table knows ("" for a known
    site, a town or area, or no site at all)."""
    key, text = _site(loc, external_path)
    if not key or key in FACILITIES or key in OFFICES:
        return ""
    if _PLACE_RX.match(text) or _SEG_PLACE_RX.match(text.replace(" ", "-")):
        return ""
    return text


def names() -> list[str]:
    """Every facility name the table gives, once."""
    return sorted({v[0] for v in FACILITIES.values()})
