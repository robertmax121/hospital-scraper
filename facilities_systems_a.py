"""Hospitals inside the big systems still on the system label, phase 4 part A
(2026-10-29, facility scale): Indian Health Service, Emory Healthcare, The
University of Kansas Health System, University of Rochester, Cooper
University Health Care, Northern Light Health and University of Mississippi
Medical Center. None of them had a facility rule on main (c8173ca) beyond
Emory's classic-portal title / "Location:" read (facilities_icims), KU's and
Rochester's Workday location maps (scraper.WD_FACILITY_MAP); their label rows
(hospital_name = hospital_system) are what those leave.

The owner, 2026-10-29: every hospital inside a multi-hospital system is named
and placed correctly on the site and in the dashboard ("i thought you were
gonna edit all the large multihospital systems"). Decisions: (1) cards and job
pages show the HOSPITAL or site ("Apply to <site>"), the system name and logo
stay on the job page and the system hub; (2) named sites (hospitals, named
clinics, centers, agencies' named branches) show their own name; generic
offices, home-based and remote postings show the system name with the town;
NEVER guess a town or a site (a directory that maps a town to a branch is no
evidence unless the posting itself names that branch).

Where each system's posting names its site, and what this module does:

  Northern Light Health (Infor)  The body: an entity line, then
      "Position is located: <site>" ("Northern Light Maine Coast Hospital",
      "Mercy Fore River", "Mayo Hospital, Dover-Foxcroft, ME", "Cianchette
      Professional Blding"). The located line decides (read_northern_light):
      a table site is that site; any other value (an office building, a
      street, a practice, "Various locations") keeps the system label with
      the row's town. Only a missing line, a placeholder or a bare town
      ("{ Pittsfield, ME }") lets the entity line speak, and an entity names
      its hospital only for a posting in that hospital's own town whose
      title is no clinic's, office's or remote job's.
  The University of Kansas Health System (Workday)  The body opens
      "Position Title / <title> / <shift> / <work location>" ("Bell
      Hospital", "Olathe Hospital", "Cambridge Tower A"). A work location
      that is one of the system's hospitals names it (read_ku); a building
      that is no hospital (a medical pavilion, a tower, an office) keeps the
      label; a posting with several work locations names none. A body that
      names no work location (or no body and no stored site) falls back on
      the posting's own Workday site, the first segment of its link
      ("/job/Olathe-Hospital/..."), when that is a hospital's. No town
      table: half the work locations are clinics and offices in the
      hospitals' own towns, so a town names nothing here.
  University of Rochester (Workday)  The label rows are the University's own
      jobs (the School of Medicine and Dentistry, River Campus, the Laser
      Lab, dining, facilities): the system name IS their employer, and the
      hospitals' own rows are named by WD_FACILITY_MAP already. What is
      wrong is the town: the Workday location is a building ("Wilson
      Commons", "Laboratory for Laser Energetics", "2337 Clinton Avenue
      South") and the scraper stored it as the town. The posting's own "Job
      Location (Full Address):" line gives the real one ("475 Alumni Rd,
      Rochester, New York, ..."): rochester_town writes it on a label row
      whose town is no known town (a real town is never changed).
  Emory Healthcare, Cooper University Health Care, University of Mississippi
  Medical Center: no field names the site (Emory's classic iCIMS bodies rarely carry a "Location:" line;
      Cooper's never; UMMC's "Organization:" is a unit or a department). They
      get town tables (facilities_town TownSpecs, registered here): the one
      hospital of the system in the posting's own town, with every guard of
      that module (clinic, office, remote, provider, care-site, multi-site
      titles keep the label; a town with two of the system's hospitals is
      placed only when the posting names one). University of Mississippi
      Medical Center is the Jackson campus's own name, so Jackson rows are
      already right and stay; Cooper's postings call the system "Cooper
      Norcross Health, formerly Cooper University Health Care" and its
      hospitals by their current names, which are written.
  Indian Health Service (USAJOBS)  The stored row keeps only the first 500
      characters of the qualifications, and the national announcements list
      60 duty towns each with no facility (1,020 of the 1,163 label rows).
      A one-town announcement's JobSummary often names its facility ("This
      position is located in the Emergency Department at the Northern Navajo
      Medical Center, Shiprock, NM"; "the Chinle Service Unit in Chinle,
      AZ"). ihs_summary_facility reads that, against IHS_FACILITIES (each
      with its own town), for the row in that facility's own town only. The
      town alone never names a facility (an IHS town can hold a hospital,
      health centers and stations, many of them tribally run).

Nights without a body: the scraper sends a known body blank
(scraper._keep_stored_body); the site (and for Rochester the town) stored on
the row is kept (load_known_facilities), so a row never flips between
nights. The town tables keep their own stored decisions (facilities_town).

hospital_system is never touched (the upsert key, the hub, the filters, the
logo). Shared with the stored-row change list (claud_outputinput/
facility_scale_2026-10-29/phase4/sa_work/changes.py), so the database holds
what the scraper writes on the next run.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time

import facilities_text as T
import facilities_town as TOWN
import sb_keys  # the DATA project key, one reader for the repo
from city_utils import known_town

logger = logging.getLogger("scraper.facility_sa")

S = T.S
key = T.key

IHS = "Indian Health Service"
EMORY = "Emory Healthcare"
KU = "The University of Kansas Health System"
ROCHESTER = "University of Rochester"
COOPER = "Cooper University Health Care"
NL = "Northern Light Health"
UMMC = "University of Mississippi Medical Center"
SYSTEMS = frozenset({IHS, EMORY, KU, ROCHESTER, COOPER, NL, UMMC})


def offsite_title(title, description="") -> bool:
    """A clinic's, a practice's, an office's, a provider group's, a
    senior-care site's or a remote posting, by its title or its own
    "Department:" / "Location:" lines (the town module's words and tests):
    an entity line does not name the hospital for it. The entity is the
    employer; its physicians and practices work in its clinics too."""
    t = TOWN.clean_text(title)
    return bool(TOWN.REMOTE_RX.search(t) or TOWN.CARE_SITE_RX.search(t)
                or TOWN._offsite_by_title(title, description))


# ═════════════════════════ NORTHERN LIGHT HEALTH ═══════════════════════════
# Every site a label row's "Position is located:" line or entity line names
# (the 792 active label rows, 2026-10-29), with its own town: "cms <id>" the
# CMS hospital list (public.hospitals), "board" every posting naming the site
# sits in that town. Mercy's inpatient hospital is the Fore River campus
# (175 Fore River Pkwy, Portland); the CMS file still carries the old State
# Street address, same town. The home care and hospice branches are the
# agency's named branches (decision 2), spelled as the postings spell them.
NL_SITES = (
    S("Northern Light Eastern Maine Medical Center", "Bangor", "ME", "cms 200033", "Eastern Maine Medical Center",
      "EMMC", "Northern Light Health Eastern Maine Medical Center"),
    S("Northern Light Acadia Hospital", "Bangor", "ME", "cms 204006", "Acadia", "Acadia Hospital",
      "Northern Light Acadia"),
    S("Northern Light Maine Coast Hospital", "Ellsworth", "ME", "cms 200050", "Maine Coast Hospital"),
    S("Northern Light Mercy Hospital", "Portland", "ME", "cms 200008", "Mercy Hospital", "Mercy Fore River",
      "Northern Light Mercy Hospital Fore River"),
    S("Northern Light A.R. Gould Hospital", "Presque Isle", "ME", "cms 200018", "A.R. Gould Hospital",
      "Northern Light A.R. Gould", "Northern Light AR Gould Hospital", "AR Gould Hospital"),
    S("Northern Light Mayo Hospital", "Dover-Foxcroft", "ME", "cms 201309", "Mayo Hospital", "NL Mayo Hospital"),
    S("Northern Light Blue Hill Hospital", "Blue Hill", "ME", "cms 201300", "Blue Hill Hospital",
      "Northern Light Blue Hill Memorial Hospital", "Blue Hill Memorial Hospital"),
    S("Northern Light Sebasticook Valley Hospital", "Pittsfield", "ME", "cms 201313", "Sebasticook Valley Hospital"),
    S("Northern Light CA Dean Hospital", "Greenville", "ME", "cms 201301", "Northern Light C A Dean Hospital",
      "Northern Light C.A. Dean Hospital", "CA Dean Hospital", "C.A. Dean Hospital", "C A Dean Hospital"),
    # named sites that are no hospital (decision 2: their own name)
    S("Northern Light Pediatric Health Center", "Bangor", "ME", "board"),
    S("Eleanor Widener Dixon Memorial Clinic", "Gouldsboro", "ME", "board", "Eleanor Widener Dixon Mem Clnc"),
    S("Northern Light Home Care and Hospice Presque Isle", "Presque Isle", "ME", "board"),
    S("Northern Light Home Care and Hospice Ellsworth", "Ellsworth", "ME", "board"),
    S("Northern Light Home Care and Hospice Waterville", "Waterville", "ME", "board"),
    S("Northern Light Home Care and Hospice Houlton", "Houlton", "ME", "board"),
    S("Northern Light Home Care and Hospice Fort Kent", "Fort Kent", "ME", "board"),
)
NL_SPEC = T.Spec(NL, None, (), NL_SITES)
# the hospitals an entity line may name (a home care branch or a clinic is
# never read off an entity line: the entity is "Northern Light Home Care and
# Hospice" without a town)
NL_HOSPITALS = frozenset(site.name for site, _sp in NL_SITES[:9])

_NL_LOCATED_RX = re.compile(r"^[ \t*•\-]*Position\s+is\s+located\s*:[ \t]*(.*?)[ \t]*$", re.I | re.M)
_PLACEHOLDER_RX = re.compile(r"^(?:location|n/?a|tbd|none|various)?$", re.I)
_NL_LEAD_LINES = 5


_NL_SITE_WORD_RX = re.compile(
    r"\b(?:hospital|center|centre|ctr|clinic|clnc|building|bldg|blding|office|plaza|street|st|ave|avenue|road|rd|"
    r"drive|lane|campus|practices?|medicine|svcs|services|health|care|surgery|residence|warehouse|wrhouse|"
    r"various|remote)\b", re.I)


def _nl_bare_town(value) -> bool:
    """"Pittsfield, ME", "{ Greenville, Me}", "Unity, ME", "Pittsfield": a
    town of Maine and nothing else (the posting names no site). A value with
    ", ME" counts when it has no site word; one without it only when it is a
    known town of Maine."""
    v = re.sub(r"^[{\s]+|[}\s]+$", "", value or "")
    town = re.sub(r"\s*,\s*(?:ME|Maine)\.?$", "", v, flags=re.I).strip()
    if not town or re.search(r"\d", town) or _NL_SITE_WORD_RX.search(town):
        return False
    if known_town(town, "ME"):
        return True
    return town != v and bool(re.fullmatch(r"[A-Za-z][A-Za-z .'-]{1,30}", town)) and len(town.split()) <= 3


def nl_located(description):
    """The "Position is located:" value (braces and space trimmed), "" for a
    placeholder ("{ Location}"), or None when the body has no such line."""
    m = _NL_LOCATED_RX.search(str(description or ""))
    if not m:
        return None
    v = T.clean_text(m.group(1))
    v = re.sub(r"^[{\s]+|[}\s]+$", "", v).strip()
    # a hyphen with a space on one side is a separator ("Mayo Hospital-
    # Dover-Foxcroft, ME" is "Mayo Hospital - Dover-Foxcroft, ME")
    v = re.sub(r"\s+-\s*|\s*-\s+", " - ", v)
    return "" if _PLACEHOLDER_RX.match(v) else v


def read_northern_light(description):
    """(Site or None, kind, what, raw) for one Northern Light body. what:
    "site", "office" (the located line names a work site that is no table
    site), "unknown" (an entity line that names no table hospital), "none"."""
    body = str(description or "")
    if not body.strip():
        return None, "", "none", ""
    v = nl_located(body)
    if v:
        site = T.match_value(NL_SPEC, v)
        if site is not None:
            return site, "site", "site", v
        if not _nl_bare_town(v):
            return None, "", "office", v
    # no located line, a placeholder or a bare town: the entity line
    lines = [ln.strip() for ln in T.clean_text_lines(body) if ln.strip()][:_NL_LEAD_LINES]
    for ln in lines:
        lk = T.lead_key(ln) + " "
        for k, site in NL_SPEC.lead:
            if site.name in NL_HOSPITALS and lk.startswith(k + " "):
                return site, "entity", "site", ln
    first = next((ln for ln in lines if ln.lower().startswith("northern light")), "")
    return None, "", ("unknown" if first else "none"), first


# ══════════════════ THE UNIVERSITY OF KANSAS HEALTH SYSTEM ══════════════════
# The work locations of the body's header that are one of the system's
# hospitals, with the hospital's own name and town (CMS hospital list). "Bell
# Hospital" is the University of Kansas Hospital's main building (4000
# Cambridge St, Kansas City, KS). Every other work location of the 2026-10-29
# label rows is an outpatient pavilion, a tower, an office or a clinic.
KU_SITES = (
    S("University of Kansas Hospital", "Kansas City", "KS", "cms 170040", "Bell Hospital"),
    S("University of Kansas Health System Olathe Hospital", "Olathe", "KS", "cms 170049", "Olathe Hospital"),
    S("University of Kansas Health System Great Bend Campus", "Great Bend", "KS", "cms 170191",
      "Great Bend Hospital"),
    S("University of Kansas Health System St. Francis Campus", "Topeka", "KS", "cms 170016",
      "St Francis Campus", "St. Francis Campus"),
    S("Liberty Hospital", "Liberty", "MO", "cms 260177"),
    S("Miami County Medical Center", "Paola", "KS", "cms 170109", "Paola Hospital"),
    S("Rehabilitation Hospital of Overland Park", "Overland Park", "KS",
      "board: the posting's own work location, Overland Park"),
)
KU_SPEC = T.Spec(KU, ("Kansas City", "KS"), (), KU_SITES)
_KU_SHIFT_RX = re.compile(
    r"^(?:(?:weekend\s+)?(?:days?|nights?|evenings?|varies|variable|mid\s+shift|rotating|on\s+call|weekends?)"
    r"(?:\s*-\s*(?:full\s+time|part\s+time|prn|per\s+diem|temporary|seasonal))?\s*(?:,\s*|$))+$", re.I)


def ku_locations(description):
    """The work locations of a KU body's header ("Position Title", the title,
    a shift line, then the work location(s), then "Position Summary"): a list
    (empty when the header names none), or None without a header."""
    lines = [ln.strip() for ln in T.clean_text_lines(description) if ln.strip()]
    i = next((k for k, ln in enumerate(lines[:6]) if ln.lower() == "position title"), None)
    if i is None:
        return None
    j = next((k for k in range(i + 1, min(i + 8, len(lines))) if lines[k].lower().startswith("position summary")),
             None)
    if j is None:
        return None
    vals = [ln for ln in lines[i + 2:j] if not _KU_SHIFT_RX.match(ln)]
    out = []
    for v in vals:
        # several work locations are listed with ", " ("Bell Hospital, Cambridge
        # Tower A"); a street's own comma is not a list ("12000 W. 151st St.")
        out += [p.strip() for p in re.split(r",\s+(?=[A-Z0-9])", v) if p.strip()]
    return out


def read_ku(description):
    """(Site or None, kind, what, raw): "site" (one work location, a table
    hospital), "office" (one work location that is no hospital), "multi"
    (several work locations), "none" (no work location in the body)."""
    locs = ku_locations(description)
    if not locs:
        return None, "", "none", ""
    if len(locs) > 1:
        return None, "", "multi", ", ".join(locs)
    site = KU_SPEC.table.get(key(locs[0]))
    if site is not None:
        return site, "site", "site", locs[0]
    return None, "", "office", locs[0]


def _slug(text) -> str:
    return "-".join(re.findall(r"[a-z0-9]+", str(text or "").lower()))


# the Workday link's site segment of each KU hospital spelling ("Olathe-Hospital")
_KU_URL_SITES: dict = {}
for _site, _spellings in KU_SITES:
    for _sp in (_site.name,) + tuple(_spellings):
        _KU_URL_SITES[_slug(_sp)] = _site


def ku_url_site(url):
    """The KU hospital the posting's own Workday link names as its primary
    site ("/job/Olathe-Hospital/RN_R-1"), or None (a town, "Remote", a
    pavilion, a clinic: "/job/Liberty-Hospital-Sports-Medicine/...")."""
    m = re.search(r"/job/([^/?#]+)/", str(url or ""))
    return _KU_URL_SITES.get(m.group(1).lower()) if m else None


# ═══════════════════════════ UNIVERSITY OF ROCHESTER ════════════════════════
_ROCH_ADDR_RX = re.compile(r"Job\s+Location\s*\(Full\s+Address\)\s*:?\s*\n+\s*([^\n]+)", re.I)
_STATE_ABBR = {v: k for k, v in TOWN._STATE_NAMES.items()}
_TOWN_WORDS_RX = re.compile(r"^[A-Za-z][A-Za-z .'-]{1,40}$")


def rochester_address_town(description):
    """(town, ST) from the posting's "Job Location (Full Address):" line
    ("475 Alumni Rd, Rochester, New York, United States of America, 14627"),
    or None (no line, a remote placeholder, a shape that names no town)."""
    m = _ROCH_ADDR_RX.search(str(description or ""))
    if not m:
        return None
    line = T.clean_text(m.group(1))
    if re.search(r"\bremote\b", line, re.I):
        return None          # "Remote Work - New York, Albany, New York, 12224": no work town
    parts = [p.strip() for p in line.split(",") if p.strip()]
    parts = [p for p in parts if p.lower() not in ("united states of america", "united states", "usa", "us")]
    if len(parts) < 3 or not re.fullmatch(r"\d{5}(?:-\d{4})?", parts[-1]):
        return None
    st = _STATE_ABBR.get(parts[-2].lower())
    town = parts[-3]
    if not st or not _TOWN_WORDS_RX.match(town) or re.search(r"\d", town):
        return None
    return town, st


def _is_town(city, state) -> bool:
    c = T.clean_text(city)
    return bool(c) and bool(known_town(c, (state or "").strip().upper() or None))


# ════════════════════════════ INDIAN HEALTH SERVICE ═════════════════════════
# The IHS facilities a JobSummary names, each with its own town ("cms <id>"
# the CMS hospital list; "posting" the facility's own postings name it in
# that town: the hiring-organization lines of the stored rows, 2026-10-29,
# and the job pages read that day). Spellings are whole phrases, matched
# case-insensitively with any spacing or punctuation between words.
def _f(name, city, state, source, *spellings):
    return name, city, state, source, (name,) + spellings


IHS_FACILITIES = (
    _f("Northern Navajo Medical Center", "Shiprock", "NM", "cms 320059"),
    _f("Shiprock Service Unit", "Shiprock", "NM", "posting"),
    _f("Gallup Indian Medical Center", "Gallup", "NM", "cms 320061"),
    _f("Gallup Service Unit", "Gallup", "NM", "posting"),
    _f("Crownpoint Healthcare Facility", "Crownpoint", "NM", "cms 320062", "Crownpoint Health Care Facility",
       "Crownpoint Comprehensive Health Care Facility"),
    _f("Crownpoint Service Unit", "Crownpoint", "NM", "posting"),
    _f("Zuni Comprehensive Community Health Center", "Zuni", "NM", "cms 320060",
       "Zuni Comprehensive Community Center"),
    _f("Acoma-Canoncito-Laguna Hospital", "Acoma", "NM", "cms 320070", "Acoma Canoncito Laguna Hospital",
       "ACL Hospital"),
    _f("Mescalero Indian Hospital", "Mescalero", "NM", "cms 320058", "Mescalero Public Health Service Indian Hospital"),
    _f("Mescalero Service Unit", "Mescalero", "NM", "posting"),
    _f("Santa Fe Indian Hospital", "Santa Fe", "NM", "cms 320057", "Santa Fe PHS Indian Hospital"),
    _f("Santa Fe Indian Health Center", "Santa Fe", "NM", "posting"),
    _f("Chinle Comprehensive Health Care Facility", "Chinle", "AZ", "cms 030084", "Chinle Comprehensive Health Care"),
    _f("Chinle Service Unit", "Chinle", "AZ", "posting"),
    _f("Pinon Health Center", "Pinon", "AZ", "posting"),
    _f("Kayenta Health Center", "Kayenta", "AZ", "posting"),
    _f("Kayenta Service Unit", "Kayenta", "AZ", "posting"),
    _f("Phoenix Indian Medical Center", "Phoenix", "AZ", "cms 030078"),
    _f("Whiteriver Indian Hospital", "Whiteriver", "AZ", "cms 030113", "Whiteriver PHS Indian Hospital"),
    _f("Whiteriver Service Unit", "Whiteriver", "AZ", "posting"),
    _f("Hopi Health Care Center", "Polacca", "AZ", "cms 031305"),
    _f("Parker Indian Health Center", "Parker", "AZ", "cms 031307"),
    _f("Colorado River Service Unit", "Parker", "AZ", "posting"),
    _f("Sells Indian Hospital", "Sells", "AZ", "cms 030074", "Sells Hospital"),
    _f("Fort Yuma Service Unit", "Winterhaven", "CA", "posting"),
    _f("Pine Ridge Hospital", "Pine Ridge", "SD", "cms 430098", "Pine Ridge IHS Hospital", "Pine Ridge Indian Hospital"),
    _f("Pine Ridge Service Unit", "Pine Ridge", "SD", "posting"),
    _f("Kyle Health Center", "Kyle", "SD", "posting"),
    _f("Rosebud Hospital", "Rosebud", "SD", "cms 430084", "Rosebud Indian Hospital", "Rosebud IHS Hospital"),
    _f("Rosebud Service Unit", "Rosebud", "SD", "posting"),
    _f("Cheyenne River Health Center", "Eagle Butte", "SD", "cms 430083", "Eagle Butte Hospital",
       "Eagle Butte Indian Hospital"),
    _f("Cheyenne River Service Unit", "Eagle Butte", "SD", "posting"),
    _f("Quentin N. Burdick Memorial Health Care Facility", "Belcourt", "ND", "cms 350063",
       "Quentin N Burdick Memorial Health Care Facility", "Quentin N. Burdick Memorial Healthcare Facility",
       "Quentin Burdick Memorial Health Care Facility"),
    _f("Turtle Mountain Service Unit", "Belcourt", "ND", "posting"),
    _f("Fort Yates Hospital", "Fort Yates", "ND", "cms 350064", "Fort Yates Indian Hospital"),
    _f("Standing Rock Service Unit", "Fort Yates", "ND", "posting"),
    _f("Blackfeet Community Hospital", "Browning", "MT", "cms 270074"),
    _f("Blackfeet Service Unit", "Browning", "MT", "posting"),
    _f("Crow/Northern Cheyenne Hospital", "Crow Agency", "MT", "cms 271339", "Crow Northern Cheyenne Hospital"),
    _f("Crow Service Unit", "Crow Agency", "MT", "posting"),
    _f("Fort Belknap Hospital", "Harlem", "MT", "cms 271315", "Fort Belknap Indian Hospital"),
    _f("Fort Belknap Service Unit", "Harlem", "MT", "posting"),
    _f("Red Lake Hospital", "Red Lake", "MN", "cms 240206", "Red Lake Indian Hospital", "Red Lake IHS Hospital"),
    _f("Red Lake Service Unit", "Red Lake", "MN", "posting"),
    _f("Cass Lake Indian Hospital", "Cass Lake", "MN", "cms 241358", "Cass Lake Hospital",
       "Cass Lake Indian Health Services Hospital"),
    _f("Cass Lake Service Unit", "Cass Lake", "MN", "posting"),
    _f("Omaha-Winnebago Hospital", "Winnebago", "NE", "posting", "Omaha Winnebago Hospital"),
    _f("Omaha-Winnebago Service Unit", "Winnebago", "NE", "posting", "Omaha Winnebago Service Unit"),
    _f("Great Plains Area Drug Dependency Unit", "Winnebago", "NE", "posting"),
    _f("Claremore Indian Hospital", "Claremore", "OK", "cms 370173"),
    _f("Lawton Indian Hospital", "Lawton", "OK", "cms 370170"),
    _f("Pawnee Indian Health Center", "Pawnee", "OK", "posting"),
    _f("Wewoka Indian Health Center", "Wewoka", "OK", "posting"),
    _f("Haskell Indian Health Center", "Lawrence", "KS", "posting"),
    _f("Uintah and Ouray Service Unit", "Fort Duchesne", "UT", "posting", "Uintah & Ouray Service Unit"),
    _f("Elko Service Unit", "Elko", "NV", "posting"),
)


def _phrase_rx(text):
    words = re.findall(r"[A-Za-z0-9]+", text)
    return re.compile(r"(?<![A-Za-z0-9])" + r"[\s.,'’&/-]*".join(map(re.escape, words)) + r"(?![A-Za-z0-9])", re.I)


_IHS_RX = tuple((name, city, st, src, tuple(_phrase_rx(sp) for sp in spellings))
                for name, city, st, src, spellings in IHS_FACILITIES)


def ihs_text_facilities(text) -> list:
    """The IHS_FACILITIES a text names: (name, city, state), in table order,
    each once; a spelling inside a longer spelling of another facility that
    the text also names does not count ("Chinle Service Unit" is not also
    "Chinle Comprehensive Health Care" unless both are written)."""
    t = str(text or "")
    hits = []
    spans = []
    for name, city, st, _src, rxs in sorted(_IHS_RX, key=lambda r: -max(len(x.pattern) for x in r[4])):
        for rx in rxs:
            m = rx.search(t)
            if m and not any(a <= m.start() and m.end() <= b for a, b in spans):
                hits.append((name, city, st))
                spans.append((m.start(), m.end()))
                break
    order = {f[0]: i for i, f in enumerate(IHS_FACILITIES)}
    return sorted(set(hits), key=lambda h: order[h[0]])


def ihs_summary_facility(m: dict, city: str, state: str):
    """The IHS facility a USAJOBS announcement's JobSummary names for its row
    at (city, state), or None. The facility must be in the row's own town;
    a summary naming two facilities of that town names none."""
    details = ((m or {}).get("UserArea") or {}).get("Details") or {}
    found = ihs_text_facilities(details.get("JobSummary"))
    here = [h for h in found if h[2] == (state or "").strip().upper() and TOWN.town_key(h[1]) == TOWN.town_key(city)]
    if len({h[0] for h in here}) != 1:
        return None
    return here[0][0]


# ═══════════════════════════ TOWN TABLES (facilities_town) ══════════════════
# Every hospital of the system with the town of its own street address. A
# town with two of them is shared (named only when the posting names one); a
# town in keep is never mapped.
_h = TOWN._h
EMORY_TOWNS = TOWN._spec(EMORY, [
    _h("Emory University Hospital", "Atlanta", "GA", "cms 110010"),
    _h("Emory University Hospital Midtown", "Atlanta", "GA", "cms 110078"),
    _h("Emory Saint Joseph's Hospital", "Atlanta", "GA", "cms 110082"),
    _h("Emory Rehabilitation Hospital", "Atlanta", "GA", "cms 113031"),
    _h("Emory Decatur Hospital", "Decatur", "GA", "cms 110076"),
    _h("Emory Long Term Acute Care", "Decatur", "GA", "cms 112006"),
    _h("Emory Hillandale Hospital", "Lithonia", "GA", "cms 110226"),
    _h("Emory Johns Creek Hospital", "Johns Creek", "GA", "cms 110230"),
    # spelled as facilities_icims spells it (CMS: Emory Houston Hospital Warner Robins)
    _h("Emory Warner Robins Hospital", "Warner Robins", "GA", "cms 110069"),
    _h("Emory University Orthopaedics & Spine Hospital", "Tucker", "GA", "site (1455 Montreal Rd, Tucker)"),
    _h("St. Francis Hospital - Emory Healthcare", "Columbus", "GA", "cms 110129"),
], aliases={
    "Emory University Hospital Midtown": ("Emory Midtown", "EUHM", "Midtown Hospital"),
    "Emory Saint Joseph's Hospital": ("Emory St Joe's", "Emory St Joes", "ESJH", "St Joe's", "Saint Joseph's"),
    "Emory Warner Robins Hospital": ("Emory Hospital Warner Robins", "Emory Houston Hospital Warner Robins"),
    "Emory Decatur Hospital": ("EDH",),
    "Emory Johns Creek Hospital": ("EJCH", "Emory John's Creek Hospital"),
# review A, 2026-10-29: "This role will lead communications in both Warner
# Robins, Ga and Perry, Ga." is no one hospital's job
}, office_line=r"(?i)\bwarner\s+robins,?\s+(?:ga\.?,?\s+|georgia,?\s+)?(?:and|&)\s+perry\b", entity_chars=12000)
# Review A, 2026-10-29: postings no one hospital holds, read in the whole
# stored body (the "About Us" boilerplate pushes them past the head): an
# anaesthesia team for every site ("CRNA JOB DESCRIPTION - Camden, CDHI, VSC,
# MSC, BMA, CAPE (all locations)"), a remote option ("For 100% remote RNs"),
# home visits and the Mobile Integrated Health team in patients' homes.
P4_BODY_CHARS = 12000
COOPER_OFFICE_BODY = (r"(?i)\(\s*all\s+locations\s*\)|\b100%\s+remote\b|\bfully\s+remote\b|"
                      r"\bhome[- ]based\s+primary\s+care\b|\bin[\s\-‐‑]home\s+(?:care|assessments?)\b|"
                      r"\bfield[- ]based\b")
COOPER_TOWNS = TOWN._spec(COOPER, [
    # the postings' own names since the rename ("Cooper Norcross Health,
    # formerly Cooper University Health Care ... its 663-bed flagship Cooper
    # Norcross University Hospital in Camden, its 229-bed Cooper Norcross
    # University Hospital Cape Regional in Cape May Court House")
    _h("Cooper Norcross University Hospital", "Camden", "NJ", "cms 310014 (Cooper University Hospital)"),
    _h("Cooper Norcross University Hospital Cape Regional", "Cape May Court House", "NJ",
       "cms 310011 (Cape Regional Medical Center)", "=Capa May Court House"),
    _h("Recovery Village Cherry Hill at Cooper", "Cherry Hill", "NJ",
       "site (the postings' list of the system's four hospitals)"),
], keep={
    "Cherry Hill, NJ": "the Recovery Village is run with American Recovery Services; Cooper's own Cherry Hill "
                       "postings are its offices and practices",
}, sites=[
    ("Children's Hospital at Cooper Norcross Health", "Camden", "NJ",
     ("Children's Hospital at Cooper Norcross Health", "Children's Hospital at Cooper",
      "Children's Regional Hospital at Cooper")),
    ("MD Anderson Cancer Center at Cooper", "Camden", "NJ", ("MD Anderson Cancer Center at Cooper", "MD Anderson")),
], aliases={
    "Cooper Norcross University Hospital Cape Regional": ("Cape Regional", "Cape May", "Cape"),
}, office_line=COOPER_OFFICE_BODY, entity_chars=P4_BODY_CHARS)
# No town table for KU: its bodies name the work location (1,034 of the
# 1,039 label rows on 2026-10-29) and 510 of those are pavilions, towers,
# offices and clinics in the hospitals' own towns (Kansas City's Medical
# Pavilion and Cambridge Tower A, Liberty's Sports Medicine and Urgent Care),
# so a town would give the hospital to a clinic's posting on a night its body
# is not read.
UMMC_TOWNS = TOWN._spec(UMMC, [
    # The Jackson campus is "University of Mississippi Medical Center" itself
    # (CMS 250001): its rows already carry their hospital's name and stay.
    _h("UMMC Grenada", "Grenada", "MS", "cms 250168"),
    _h("UMMC Holmes County", "Lexington", "MS", "cms 251319"),
    _h("UMMC Greenwood", "Greenwood", "MS", "cms 250099 (Greenwood Leflore Hospital; the postings call it UMMC Greenwood)"),
], default=("Jackson", "MS"), keep={
    "Jackson, MS": "the system's own name is the Jackson campus's name (University of Mississippi Medical Center)",
}, aliases={
    "UMMC Holmes County": ("Holmes County Hospital",),
    "UMMC Grenada": ("Grenada Hospital",),
})
TOWN_SYSTEMS = frozenset({EMORY, COOPER, UMMC})


def _also_named(spec, name, *spellings) -> None:
    """Short names the system's own titles give a hospital ("CRNA / CAA
    Inpatient - Emory Midtown"): they name it like the part of its name that
    says more than its town does (facilities_town._names: a clinic's or an
    office's title is not named by them)."""
    if name not in spec.named_keys:
        raise ValueError(f"{spec.system}: {name!r} is not in the town table")
    for sp in spellings:
        k = TOWN.name_key(sp)
        spec.keys[name].add(k)
        spec.named_keys[name].add(k)


# A title for Warner Robins and the Perry hospital at once ("Associate Chief
# Quality Officer, Warner Robins & Perry") is no one hospital's job: it keeps
# the system name (facilities_town reads the per-system pattern as a site
# that is no single hospital).
TOWN.CARE_SITE_BY_SYSTEM.setdefault(EMORY, re.compile(r"\bWarner\s+Robins\s*(?:&|and|/)\s*Perry\b", re.I))
# Review A, 2026-10-29: Cooper titles that are no Camden or Cape May hospital
# job: home visits and the Mobile Integrated Health / community paramedicine
# team, a title naming the surgery centres or other towns ("Perioperative
# Services Educator ... - Voorhees and Moorestown", "Hospitalist Cape May and
# Inspira"), and the business offices (patient accounts, collections, prior
# authorization, provider compensation, sponsored programs).
TOWN.CARE_SITE_BY_SYSTEM.setdefault(COOPER, re.compile(
    r"\b(?:home\s+visits?|mobile\s+integrated|community\s+paramedic\w*|community\s+outreach|voorhees|moorestown|"
    r"willingboro|mount\s+laurel|sewell|inspira|patient\s+accounts?|collections?\s+specialist|"
    r"authorization\s+specialist|provider\s+compensation|sponsored\s+programs?)\b", re.I))

_also_named(EMORY_TOWNS, "Emory University Hospital Midtown", "Emory Midtown", "EUHM")
_also_named(EMORY_TOWNS, "Emory Saint Joseph's Hospital", "Emory Saint Joseph's", "Emory St Joseph's", "ESJH")
_also_named(EMORY_TOWNS, "Emory Warner Robins Hospital", "Emory Hospital Warner Robins")
_also_named(COOPER_TOWNS, "Cooper Norcross University Hospital Cape Regional", "Cape Regional")
_also_named(UMMC_TOWNS, "UMMC Holmes County", "Holmes County Hospital")


# ════════════════════ STORED NAMES (nights without a body) ═══════════════════
_KNOWN: dict = {}          # (system, job_id) -> (hospital_name, city, state)
KNOWN_PAGE = 1000
KNOWN_MAX_ROWS = 20000
KNOWN_SYSTEMS = (NL, KU, ROCHESTER)


def set_known_facilities(rows) -> int:
    """{(hospital_system, job_id): (hospital_name, city, state)} for the
    stored active rows of NL (a site name of its table), KU (every row: the
    label is a stored decision too, so a body-less night never falls back on
    the link) and Rochester (its label rows, for their town)."""
    global _KNOWN
    out = {}
    for r in rows or []:
        system = (r.get("hospital_system") or "").strip()
        jid = r.get("job_id")
        if system not in KNOWN_SYSTEMS or jid in (None, ""):
            continue
        name = T.clean_text(r.get("hospital_name"))
        if system == NL and name not in NL_SPEC.names:
            continue
        if system == KU and name != KU and name not in KU_SPEC.names:
            continue
        if system == ROCHESTER and name != ROCHESTER:
            continue
        out[(system, str(jid))] = (name, T.clean_text(r.get("city")), T.clean_text(r.get("state")).upper())
    _KNOWN = out
    return len(out)


def known_facility(system, job_id):
    return _KNOWN.get(((system or "").strip(), str(job_id)))


def load_known_facilities() -> int:
    """Read-only PostgREST passes (the SUPABASE_URL / key the upsert uses)
    over the active rows of KNOWN_SYSTEMS, keyset-paged on id. Without
    credentials the map stays empty and nothing is kept. Never raises."""
    sb_url = os.environ.get("SUPABASE_URL", "")
    sb_key = sb_keys.supabase_key()
    if not sb_url or not sb_key:
        set_known_facilities([])
        return 0
    import urllib.parse as _urlparse
    import urllib.request as _urlreq
    rows = []
    for system in KNOWN_SYSTEMS:
        flt = _urlparse.quote(f"eq.{system}", safe=".")
        last, got = 0, 0
        try:
            while len(rows) < KNOWN_MAX_ROWS:
                u = (f"{sb_url.rstrip('/')}/rest/v1/hospital_jobs?select=id,hospital_system,job_id,hospital_name,"
                     f"city,state&is_active=is.true&hospital_system={flt}&id=gt.{last}&order=id.asc"
                     f"&limit={KNOWN_PAGE}")
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
                if len(chunk) < KNOWN_PAGE:
                    break
        except Exception as e:
            logger.warning(f"Systems A (phase 4): {system} load stopped after {got:,} rows ({e}); "
                           f"its rows past that keep a site only on nights their body is read")
    try:
        return set_known_facilities(rows)
    except Exception as e:          # never let this read end the run
        logger.warning(f"Systems A (phase 4): stored names not installed ({e})")
        set_known_facilities([])
        return 0


# ═══════════════ APPLYING IT (finalize_jobs, before the town tables) ═════════
UNKNOWN: dict = {}       # system -> {raw value: rows} named by no table entry, this run
NO_BODY_CHARS = T.NO_BODY_CHARS


def _set(job, name, city, state) -> None:
    job.hospital_name = name
    if (city, state) != ((job.city or "").strip(), (job.state or "").strip().upper()):
        job.city, job.state, job.location = city, state, f"{city}, {state}"


def _kept(job, spec):
    """The stored site on a night without a body; True when one was set."""
    hit = known_facility(spec.system, job.job_id)
    if not hit or hit[0] not in spec.names:
        return False
    site = spec.names[hit[0]]
    placed = T.place(spec, site, "entity", job.city, job.state)
    if placed is None and hit[1]:
        # the stored row sat in the site's own town (a row the body moved
        # off the stamped default): that town again
        placed = T.place(spec, site, "entity", hit[1], hit[2])
    if placed is None:
        return False
    _set(job, hit[0] if hit[0] != site.name else placed[0], placed[1], placed[2])
    return True


def _count_unknown(system, raw) -> None:
    if raw:
        d = UNKNOWN.setdefault(system, {})
        d[raw[:80]] = d.get(raw[:80], 0) + 1


def _nl_one(job) -> str:
    site, kind, what, raw = read_northern_light(job.description)
    if site is not None:
        if kind == "entity" and offsite_title(job.title, job.description):
            return "offsite"
        placed = T.place(NL_SPEC, site, kind, job.city, job.state, raw)
        if placed is None:
            return "elsewhere"
        _set(job, *placed)
        return "text" if kind == "site" else "entity"
    if len((job.description or "").strip()) < NO_BODY_CHARS:
        return "kept" if _kept(job, NL_SPEC) else "none"
    if what in ("office", "unknown"):
        _count_unknown(NL, raw)
    return what


def _ku_url(job) -> str:
    """The hospital the posting's own Workday link names as its primary site,
    for a row whose body names no work location; "" when it names none."""
    site = ku_url_site(job.url)
    if site is None:
        return ""
    placed = T.place(KU_SPEC, site, "site", job.city, job.state)
    if placed is None:
        return "elsewhere"
    _set(job, *placed)
    return "url"


# Review A, 2026-10-29: a KU posting worked remotely or hybrid ("Director of
# Claims & Senior Counsel - Hybrid", "Choose from traditional on-site,
# hybrid, or fully remote work arrangements") is no one hospital's (decision
# 2), whatever its work location line names. "Hospitalist (Hybrid Shift)" and
# "Core Lab Hybrid 8HR/10HR" are schedules, not remote work.
KU_REMOTE_TITLE_RX = re.compile(r"(?:[-–(]\s*hybrid\s*\)?\s*$)|\bremote\b", re.I)
KU_REMOTE_BODY_RX = re.compile(r"\b(?:fully\s+remote|100%\s+remote|remote\s+work\s+arrangements?|work\s+from\s+home)\b",
                               re.I)


def ku_remote(title, description) -> bool:
    return bool(KU_REMOTE_TITLE_RX.search(T.clean_text(title))
                or KU_REMOTE_BODY_RX.search(str(description or "")[:12000]))


def _ku_one(job) -> str:
    if ku_remote(job.title, job.description):
        return "remote"
    site, kind, what, raw = read_ku(job.description)
    if site is not None:
        placed = T.place(KU_SPEC, site, kind, job.city, job.state, raw)
        if placed is None:
            return "elsewhere"
        _set(job, *placed)
        return "text"
    if len((job.description or "").strip()) < NO_BODY_CHARS:
        # no body tonight: a stored row keeps its stored decision (a site, or
        # the label a pavilion's posting was left on); only a row the
        # database does not hold yet falls back on its link's site
        if known_facility(KU, job.job_id) is not None:
            return "kept" if _kept(job, KU_SPEC) else "kept-label"
        return _ku_url(job) or "none"
    if what == "none":
        return _ku_url(job) or "none"
    if what == "office":
        _count_unknown(KU, raw)
    return what


def rochester_town(job) -> str:
    """The posting's own town on a University of Rochester label row whose
    town is a building ("Wilson Commons") or blank. Returns "town" (fixed),
    "kept" (the stored town, no body tonight), "town-ok", "no-address"."""
    if _is_town(job.city, job.state):
        return "town-ok"
    got = rochester_address_town(job.description)
    if got is None and len((job.description or "").strip()) < NO_BODY_CHARS:
        hit = known_facility(ROCHESTER, job.job_id)
        if hit and hit[1] and _is_town(hit[1], hit[2]) and hit[2] == (job.state or hit[2]).strip().upper():
            _set(job, job.hospital_name, hit[1], hit[2])
            return "kept"
        return "no-address"
    if got is None:
        return "no-address"
    town, st = got
    if (job.state or "").strip() and (job.state or "").strip().upper() != st:
        return "other-state"
    if TOWN.town_key(town) == TOWN.town_key(job.city):
        return "town-ok"
    _set(job, job.hospital_name, town, st)
    return "town"


def apply_facility(job) -> str:
    """One Job of NL, KU or Rochester still on the system label. Returns how,
    "other" (already a site), or "" for any other system. hospital_system is
    never changed."""
    system = (job.hospital_system or "").strip()
    if system not in (NL, KU, ROCHESTER):
        return ""
    if T.clean_text(job.hospital_name) != system:
        return "other"
    if system == NL:
        return _nl_one(job)
    if system == KU:
        return _ku_one(job)
    return rochester_town(job)


# review B 2026-10-29 (facility release 3): one row nobody foresaw must not
# end the pass for every row after it. The row that raises keeps the name and
# town it came with and counts as "error" in the pass's stats.
_ROW_FIELDS = ("hospital_name", "city", "state", "location")


def _row_guard(fn, job, *args):
    keep = [(a, getattr(job, a)) for a in _ROW_FIELDS if hasattr(job, a)]
    try:
        return fn(job, *args)
    except Exception:
        for a, v in keep:
            try:
                setattr(job, a, v)
            except Exception:
                pass
        return "error"


def apply_facilities(jobs) -> dict:
    """apply_facility over a run's rows. Returns {system: {how: rows}}."""
    stats: dict = {}
    for job in jobs:
        how = _row_guard(apply_facility, job)
        if how:
            d = stats.setdefault(job.hospital_system, {})
            d[how] = d.get(how, 0) + 1
    return stats


def unknown_summary(top=4) -> str:
    parts = []
    for system, d in sorted(UNKNOWN.items()):
        vals = sorted(d.items(), key=lambda kv: -kv[1])[:top]
        parts.append(f"{system}: " + "; ".join(f"{v} ({n})" for v, n in vals))
    return " | ".join(parts)
