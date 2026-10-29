"""Named branches on the home health, hospice and private duty chains (2026-10-29, phase 3).

The owner, 2026-10-29: every site inside a multi-site employer is named and
placed correctly on the site and in the dashboard, and applies are filed under
the site they were for. His decisions: (1) cards and job pages show the SITE
("Apply to <site>"); the system name and logo stay on the job page and the
system hub; (2) named sites (hospitals, named clinics and centers, an agency's
named branch) show their own name; generic offices, home-based and remote jobs
show the system name with the town; never guess a town. At 17:30 UTC: "i
thought you were gonna edit all the large multihospital systems" (phase 2 had
left the home health, hospice and private duty chains on their label).

Where each chain's posting names its branch (read 2026-10-29 off each board's
own feed, a few requests per host; the stored rows of every chain read too):

  BAYADA Home Health    Greenhouse job.departments (the boards-api list, read
  Care                  every night): the service office, "Millville (MIL) -
                        52", "Lehigh Valley Home Health (LVV) - 418",
                        "Asheville Skilled Nursing and Pediatrics (AVL) - 256".
                        1,809 of the board's 2,613 postings sit in one; the
                        rest sit in a regional division ("Violet Division
                        (VIO) - 169"), a sales team, a service unit or a
                        closed office, which names no branch.
  Amedisys              Jibe list row location_name, the care center: "<number>
                        <town>, <ST>[ <acquired brand>]" ("2214 Sumter, SC
                        Winyah", "5050 Largo, MD-HSP"); tags1 the line
                        (Homecare / Hospice).
  AccentCare            Jibe list row location_name, the branch: "<town> <ST>
                        <line>" ("Fort Worth TX HH", "Tampa FL HOS",
                        "Mease Manor Continuing Care FL HOS IPC"); tags7 the
                        line (Home Health / Hospice / Personal Care).
  VITAS Healthcare      Oracle requisition workLocation LocationName, the
                        hospice program and its office: "Broward North - Main",
                        "Lake-Sumter - Main", "Brevard - Melbourne";
                        "Corporate - ..." and "CCC - ..." are no program.
  CenterWell            Workday detail jobRequisitionLocation, the branch or
                        center: "CWHH Ballston Lk", "CWHH Hope", "CW Marietta"
                        (CenterWell Home Health); "CON Kirby" (a Conviva
                        primary care center, an internal short name, not
                        read). The list says only the town; the detail is read
                        for new postings and re-read once (REREAD_*). A few
                        stored bodies say it too: "On-site at CWHH Seattle, WA
                        branch location".
  BrightSpring Health   the body's "Our Company" line (iCIMS), the operating
  Services              company: "Adoration Health", "PharMerica", "Haven
                        Hospice", "Rehab Without Walls Neuro Rehabilitation"
                        (BRIGHTSPRING_BRANDS); a teaser without that line can
                        open with it ("Adoration Home Health is seeking ...").
  Compassus             the body's "Company:" line (Workday), the joint
                        venture: "Providence at Home with Compassus",
                        "Ascension at Home together with Compassus" ... plain
                        "Compassus" is the system itself.

Chains whose postings name no branch keep the system name with the posting's
own town (decision 2): Aveanna (the card's "Line of Business" is a service
line, the JSON-LD names only the town), Enhabit ("Primary Location: Nampa,
Idaho", "Patient Coverage Area"), Maxim (TalentBrew, the town only), Gentiva
(the stored text names none; its iCIMS job page answers this machine with a
CAPTCHA, so it was not read). Elara Caring names no branch either; a site it
writes "<town> - <ST> <suffix>" ("Abilene - TX Hospital", "Valparaiso - IN
SHH") parsed as no town, so elara_town reads it, only as a known town of the
row's state. A "N Locations" posting (most of Elara's 32 blank towns on
10-29: "10 Locations", primary site "Greenlawn - NY") keeps no town, the
2026-10-13 rule: a posting at several sites is in no one town.

Names. A branch the feed names by its place is "<brand>[ <line>] - <place>",
the owner's own example "BAYADA Home Health Care - Asheville": the office's
own name without its internal code ("BAYADA Home Health Care - Lehigh Valley
Home Health"), the care center's town ("Amedisys Hospice - Largo"), the
program ("VITAS Healthcare - Broward North"). A joint venture's own name
stands alone ("Valley Health at Home by BAYADA", "Providence at Home with
Compassus"); an operating company is its own name ("Haven Hospice"). The line
(Home Health, Hospice, Personal Care) comes from the feed's own line field or
code, so a town's home health and hospice branches stay apart.

Towns. The row keeps the posting's own town; a branch's place is never
written as the town (the branch office's town is not the posting's: an
AccentCare "Concord CA" branch posts Walnut Creek jobs). Only an Elara
single-site town the generic parse missed is filled, from the posting's own
location text.

Nights without the field. The list-level chains (BAYADA, Amedisys,
AccentCare, VITAS) carry their field every night. The body and detail chains
(BrightSpring, Compassus, CenterWell) see it only when the body or the detail
is read that night; on other nights the name stored on the row is kept
(load_known_facilities), as facilities_text does, unless that night's own
body or detail names none. Only names this module writes are ever kept.

hospital_system is never touched (the upsert key, the hub, the filters, the
logo). Shared with the stored-row change list
(claud_outputinput/facility_scale_2026-10-29/phase3/home_health/make_changes.py),
so the database holds what the scraper keeps.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
import zlib
from datetime import datetime

import sb_keys  # the DATA project key, one reader for the repo
from city_utils import STATE_CODES, known_town, town_key
from facility_names import clean_text

logger = logging.getLogger("scraper.facility_home_health")

BAYADA = "BAYADA Home Health Care"
AMEDISYS = "Amedisys"
ACCENTCARE = "AccentCare"
VITAS = "VITAS Healthcare"
CENTERWELL = "CenterWell"
BRIGHTSPRING = "BrightSpring Health Services"
COMPASSUS = "Compassus"
ELARA = "Elara Caring"

JIBE_SYSTEMS = frozenset({AMEDISYS, ACCENTCARE})
ORACLE_SYSTEMS = frozenset({VITAS})
GREENHOUSE_SYSTEMS = frozenset({BAYADA})
WD_REQ_LOCATION = frozenset({CENTERWELL})       # the detail's jobRequisitionLocation names the branch
WD_TOWN_SYSTEMS = frozenset({ELARA})            # a lost town read back from the location text
KEEP_SYSTEMS = frozenset({BRIGHTSPRING, COMPASSUS, CENTERWELL})   # body / detail chains: stored name kept
SYSTEMS = GREENHOUSE_SYSTEMS | JIBE_SYSTEMS | ORACLE_SYSTEMS | KEEP_SYSTEMS

_NAMEISH = re.compile(r"^[A-Za-z][A-Za-z.' &/()-]*[A-Za-z.)]$")
_LINE_NAMES = {"home health": "Home Health", "homecare": "Home Health", "home care": "Home Health",
               "hospice": "Hospice", "personal care": "Personal Care", "palliative": "Palliative"}


def _words(s) -> str:
    return " ".join(clean_text(s).split())


def _place_case(place: str) -> str:
    """"LANCASTER" -> "Lancaster", "RANKIN COUNTY" -> "Rankin County"; mixed
    case stays as printed."""
    p = _words(place)
    return p.title() if p.isupper() else p


def _plausible_place(place: str) -> bool:
    """A place-like name: letters (with . ' & / ( ) -), one to five words,
    no digits, no code-only word."""
    p = _words(place)
    if not p or len(p) > 48 or not _NAMEISH.match(p) or len(p.split()) > 5:
        return False
    return not re.search(r"\b(?:remote|corporate|nationwide|virtual|work at home|telework|office|hub|"
                         r"regional|region|division|admin|intake|support|general area)\b", p, re.I)


def _line(raw) -> str:
    """The service line a feed field names ("Homecare" -> "Home Health"), or ""."""
    vals = raw if isinstance(raw, (list, tuple)) else [raw]
    for v in vals:
        k = _words(v).lower()
        if k in _LINE_NAMES:
            return _LINE_NAMES[k]
    return ""


# ── BAYADA (Greenhouse job.departments: the service office) ────────────────
_BAYADA_CODE_RX = re.compile(r"\s*\(([A-Z0-9]{2,4})\)\s*(?:-\s*\d+)?\s*$")
_BAYADA_TAG_RX = re.compile(r"\s*\((?:mirror(?: office)?|closed)\)", re.I)
# Departments that are no service office: a regional division ("Violet
# Division"), a region, a sales team, a service unit, a support function, an
# acquisition holding office.
_BAYADA_NOT_OFFICE_RX = re.compile(
    r"\b(?:division|region|sales|unit|payroll|recruiting|enterprise|legal services|acquisition|"
    r"digital|service office|support|audit|unallocated|shared)\b", re.I)


def bayada_office(dept_name) -> str:
    """The service office a BAYADA department names, without its code
    ("Millville (MIL) - 52" -> "Millville"), or "" for a department that is
    no office (a division, a region, a sales team, a closed office, a
    department printed without its office code)."""
    raw = _words(dept_name)
    if not raw or re.search(r"\(closed\)", raw, re.I):
        return ""
    mirror = bool(re.search(r"\(mirror", raw, re.I))
    m = _BAYADA_CODE_RX.search(raw)
    if not m:
        return ""
    name = _BAYADA_TAG_RX.sub("", raw[:m.start()]).strip(" -")
    name = _words(name)
    if not name or _BAYADA_NOT_OFFICE_RX.search(name):
        return ""
    if mirror and "bayada" not in name.lower():
        return ""       # a mirror listing of another office ("CMRHC Cape May (Mirror Office)")
    return name


def bayada_branch(departments) -> str:
    """hospital_name for one boards-api job of BAYADA: "BAYADA Home Health
    Care - <office>" ("BAYADA Home Health Care - Asheville"), the joint
    venture's own name when it carries BAYADA's ("Valley Health at Home by
    BAYADA"), or "" (no office, or two different ones)."""
    names = set()
    for d in departments or []:
        if isinstance(d, dict):
            n = bayada_office(d.get("name"))
            if n:
                names.add(n)
    if len(names) != 1:
        return ""
    name = names.pop()
    return name if "bayada" in name.lower() else f"{BAYADA} - {name}"


def greenhouse_branch(system, job: dict) -> str:
    if system == BAYADA:
        return bayada_branch((job or {}).get("departments"))
    return ""


# ── Amedisys / AccentCare (Jibe location_name) ──────────────────────────────
_STATE_TOKEN = "|".join(sorted(STATE_CODES))
_AMED_RX = re.compile(r"^\s*(\d{4})\s+(.+?)\s*$")
_AMED_PLACE_RX = re.compile(rf"^(?P<place>.+?)\s*(?:,\s*|\s+)(?P<st>{_STATE_TOKEN})(?=$|[\s-])(?P<rest>.*)$")
_AMED_HOSPICE_RX = re.compile(r"\b(?:HOS|HSP|Hospice)\b", re.I)


def _named_place(place: str, st: str, city: str = "") -> str:
    """The branch's place as a name: the known spelling of that town, the
    row's own town when the place is that town or a cut of it ("The Village"
    for The Villages), else the printed place when it is place-like."""
    p = _words(place)
    if not p:
        return ""
    kt = known_town(p, st) if st else None
    if kt:
        return _place_case(p) if ("'" in p and "'" not in kt) else kt   # "Lee's Summit", not the list's "Lees Summit"
    k = town_key(p)
    c = _words(city)
    if c and k and (town_key(c) == k or (len(k) >= 6 and town_key(c).startswith(k))):
        return known_town(c, st) or _place_case(c)
    return _place_case(p) if _plausible_place(p) else ""


def amedisys_branch(location_name, line_tags=None, city: str = "") -> str:
    """"Amedisys Home Health - Sumter" for "2214 Sumter, SC Winyah" (tags1
    Homecare), "Amedisys Hospice - Largo" for "5050 Largo, MD-HSP"; "" when
    the value carries no care-center number or no state."""
    m = _AMED_RX.match(_words(location_name))
    if not m:
        return ""
    rest = m.group(2)
    rest = re.sub(r"^Amedisys\b.*?\bof\s+", "", rest, flags=re.I)   # "Amedisys Hme Hth of Butler, PA"
    pm = _AMED_PLACE_RX.match(rest)
    if not pm:
        return ""
    place = _named_place(pm.group("place"), pm.group("st"), city)
    if not place:
        return ""
    line = _line(line_tags)
    if not line:
        line = "Hospice" if _AMED_HOSPICE_RX.search(pm.group("rest")) else ""
    return f"{AMEDISYS} {line} - {place}" if line else f"{AMEDISYS} - {place}"


_ACC_CODES = {"HH": "Home Health", "HOS": "Hospice", "HOSP": "Hospice", "HOSPICE": "Hospice",
              "PCS": "Personal Care", "PC": "Personal Care", "PP": "", "IPC": "", "ACQ": ""}
_ACC_GLUED_RX = re.compile(rf"^(?P<place>[A-Za-z][A-Za-z.' ]*?[a-z])(?P<st>{_STATE_TOKEN})$")


def accentcare_branch(location_name, line_tags=None, city: str = "") -> str:
    """"AccentCare Home Health - Fort Worth" for "Fort Worth TX HH",
    "AccentCare Hospice - Houston North" for "Houston TX North HOS",
    "AccentCare Hospice - Mease Manor Continuing Care" for its inpatient unit
    ("... FL HOS IPC"); "" for a remote or unreadable value."""
    raw = _words(location_name)
    if not raw or re.match(r"^remote\b", raw, re.I):
        return ""
    toks = raw.split()
    idx, st = None, ""
    for i, t in enumerate(toks):
        if i > 0 and t in STATE_CODES:
            idx, st = i, t
            break
    if idx is None:
        g = _ACC_GLUED_RX.match(raw)           # "GiddingsTX"
        if not g:
            return ""
        place_toks, after, st = g.group("place").split(), [], g.group("st")
    else:
        place_toks, after = toks[:idx], toks[idx + 1:]
    line, qual = "", []
    for t in after:
        u = t.upper().strip(".,")
        if u in _ACC_CODES:
            line = line or _ACC_CODES[u]
        elif re.fullmatch(r"\d{5}", u):
            continue
        else:
            qual.append(t)
    # "MERIDIAN HOSPICE MS": the line word sits in the place
    while place_toks and place_toks[-1].upper() in ("HOSPICE", "HOS", "HH"):
        line = line or _ACC_CODES[place_toks[-1].upper()]
        place_toks = place_toks[:-1]
    place = " ".join(place_toks).replace("(", "").replace(")", "")
    named = _named_place(place, st, city)
    if not named:
        return ""
    if qual:
        q = " ".join(qual)
        if not _plausible_place(q):
            return ""
        named = f"{named} {_place_case(q)}"
    line = line or _line(line_tags)
    return f"{ACCENTCARE} {line} - {named}" if line else f"{ACCENTCARE} - {named}"


def jibe_branch(system, j: dict) -> str:
    """hospital_name for one Jibe /api/jobs row of a chain here, or ""."""
    j = j or {}
    if system == AMEDISYS:
        return amedisys_branch(j.get("location_name"), j.get("tags1"), j.get("city") or "")
    if system == ACCENTCARE:
        return accentcare_branch(j.get("location_name"), j.get("tags7"), j.get("city") or "")
    return ""


# ── VITAS (Oracle workLocation LocationName: "<program> - <office>") ────────
_VITAS_NOT_PROGRAM_RX = re.compile(r"^(?:corporate|ccc|remote|home office|national)\b", re.I)


def vitas_program(location_name) -> str:
    """"VITAS Healthcare - Broward North" for "Broward North - Main"; "" for
    the corporate offices and the call center ("Corporate - Miramar HR &
    Legal", "CCC - C500, CC After-Hours")."""
    raw = _words(location_name)
    if not raw:
        return ""
    program = _words(re.split(r"\s+-\s+", raw, maxsplit=1)[0])
    if not program or _VITAS_NOT_PROGRAM_RX.match(program) or re.search(r"\d", program):
        return ""
    if not re.fullmatch(r"[A-Za-z][A-Za-z.' /&-]{1,40}", program):
        return ""
    return f"{VITAS} - {_place_case(program)}"


def oracle_branch(system, j: dict) -> str:
    """hospital_name for one Oracle requisition of a chain here, or ""."""
    if system != VITAS:
        return ""
    wl = (j or {}).get("workLocation", (j or {}).get("WorkLocation"))
    if isinstance(wl, dict):
        wl = wl.get("items", [wl])
    for w in wl or []:
        if isinstance(w, dict):
            name = str(w.get("LocationName") or "").strip()
            if name:
                return vitas_program(name)
    return ""


# ── CenterWell (Workday jobRequisitionLocation "CWHH Ballston Lk") ──────────
REQ_LOC_ATTR = "_hh_req_location"     # set on a Job whose detail was read tonight
_CW_PREFIX_RX = re.compile(r"^\s*(CWHH|CW)\b[\s-]+(.+?)\s*$", re.I)
_CW_ABBR = {"lk": "Lake", "spgs": "Springs", "spg": "Springs", "hts": "Heights", "ft": "Fort", "mt": "Mount",
            "pt": "Point", "bch": "Beach", "vly": "Valley", "jct": "Junction", "twp": "Township",
            "n": "North", "s": "South", "e": "East", "w": "West", "st": "St.", "cty": "City"}
_CW_TRAIL_STATE_RX = re.compile(rf"(?:,\s*|\s+|-)({_STATE_TOKEN})$")
_CW_LINE_RX = re.compile(r"\bCWHH\s+([A-Za-z][A-Za-z.' -]{1,40}?),\s*([A-Z]{2})\s+branch\b", re.I)
_HOME_HEALTH_RX = re.compile(r"\bhome health\b", re.I)


def centerwell_place(raw_place: str, state: str = "") -> str:
    """The branch's place from the descriptor's tail ("Ballston Lk" ->
    "Ballston Lake", "LANCASTER-PA" -> "Lancaster", "Des-Moines" -> "Des
    Moines"), or ""."""
    p = _words(raw_place).replace("-", " ")
    p = " ".join(p.split())
    m = _CW_TRAIL_STATE_RX.search(p)
    st = (state or "").strip().upper()
    if m:
        st = m.group(1)
        p = p[:m.start()].strip(" ,")
    if not p:
        return ""
    words = p.split()
    expanded = " ".join(_CW_ABBR.get(w.lower().rstrip("."), w) if len(w) <= 4 else w for w in words)
    for cand in (expanded, p):
        kt = known_town(cand, st) if st else None
        if kt:
            return kt
    return _place_case(expanded) if _plausible_place(expanded) else ""


def centerwell_branch(descriptor, title: str = "", description: str = "", state: str = "") -> str:
    """"CenterWell Home Health - Ballston Lake" for a requisition location
    "CWHH Ballston Lk" (or "CW Marietta" on a home health posting); "" for
    the primary care centers' short names ("CON Kirby") and anything else."""
    m = _CW_PREFIX_RX.match(_words(descriptor))
    if not m:
        return ""
    prefix = m.group(1).upper()
    if prefix == "CW" and not (_HOME_HEALTH_RX.search(title or "")
                               or re.search(r"CenterWell Home Health", description or "", re.I)):
        return ""
    place = centerwell_place(m.group(2), state)
    return f"CenterWell Home Health - {place}" if place else ""


def stash_requisition(job, info: dict) -> None:
    """Keep tonight's detail jobRequisitionLocation on the Job (apply_facility
    reads it); a detail without one records "" (read, names nothing)."""
    loc = (info or {}).get("jobRequisitionLocation")
    desc = str(loc.get("descriptor") or "") if isinstance(loc, dict) else ""
    try:
        setattr(job, REQ_LOC_ATTR, desc.strip())
    except Exception:
        pass


# ── Body lines: BrightSpring's operating company, Compassus' joint venture ──
BRIGHTSPRING_BRANDS = {
    # spelling (as the "Our Company" line or a lead prints it) -> clean name
    "Adoration Health": "Adoration Health", "Adoration Home Health": "Adoration Health",
    "Adoration Hospice": "Adoration Health",
    "Rehab Without Walls Neuro Rehabilitation": "Rehab Without Walls Neuro Rehabilitation",
    "Rehab Without Walls": "Rehab Without Walls Neuro Rehabilitation",
    "PharMerica": "PharMerica",
    "SpringHealth Behavioral Health and Integrated Care": "SpringHealth Behavioral Health and Integrated Care",
    "Amerita": "Amerita",
    "Haven Hospice": "Haven Hospice",
    "OnePoint Patient Care": "OnePoint Patient Care",
    "Hospice of the Midwest": "Hospice of the Midwest",
    "Pate Rehabilitation": "Pate Rehabilitation",
    "Abode Care Partners": "Abode Care Partners",
    "StepStone Family & Youth Services": "StepStone Family & Youth Services",
    "Allay Hospice": "Allay Hospice",
    "Pharmacy Alternatives": "Pharmacy Alternatives",
    "Gateway Pediatric Therapy": "Gateway Pediatric Therapy",
    "Apreva Hospice": "Apreva Hospice",
    "Hospice of North Alabama": "Hospice of North Alabama",
    "At-Heart Home Care & Hospice": "At-Heart Home Care & Hospice",
    "Hospice Home Care": "Hospice Home Care",
    "Generations Hospice": "Generations Hospice",
    "Embrace Hospice": "Embrace Hospice",
    "CompassionCare": "CompassionCare",
    "Spring Valley Hospice": "Spring Valley Hospice",
    "All Saints Hospice": "All Saints Hospice",
    "Abode Hospice of Colorado": "Abode Hospice of Colorado",
    "At Home Care Hospice": "At Home Care Hospice",
    "Robin Hill Farm": "Robin Hill Farm",
    "Abilis Health Plan": "Abilis Health Plan",
    "All Ways Caring HomeCare": "All Ways Caring HomeCare",
    "ResCare Community Living": "ResCare Community Living",
}
COMPASSUS_VENTURES = {
    # spelling (the body's "Company:" line) -> clean name; plain "Compassus" is the system
    "Providence at Home with Compassus": "Providence at Home with Compassus",
    "Ascension at Home together with Compassus": "Ascension at Home together with Compassus",
    "OhioHealth at Home in partnership with Compassus": "OhioHealth at Home in partnership with Compassus",
    "Mercy Health Home Care and Hospice by Compassus": "Mercy Health Home Care and Hospice by Compassus",
    "Bon Secours Home Care and Hospice by Compassus": "Bon Secours Home Care and Hospice by Compassus",
}


def _bkey(s) -> str:
    s = clean_text(s).lower().replace("&", " and ").replace("'", "")
    return " ".join(re.sub(r"[^a-z0-9]+", " ", s).split())


_BS_KEYS = {_bkey(k): v for k, v in BRIGHTSPRING_BRANDS.items()}
_CP_KEYS = {_bkey(k): v for k, v in COMPASSUS_VENTURES.items()}
_BS_NAMES = frozenset(BRIGHTSPRING_BRANDS.values())
_CP_NAMES = frozenset(COMPASSUS_VENTURES.values())
# Longest spellings first, so "Abode Hospice of Colorado" wins over a shorter one.
_BS_LEAD_RX = re.compile(
    r"(?<![A-Za-z])(" + "|".join(re.escape(k) for k in sorted(BRIGHTSPRING_BRANDS, key=len, reverse=True))
    + r")(?![A-Za-z])")
_LEAD_CHARS = 400


def _lines(description) -> list:
    return [ln.strip() for ln in re.split(r"[\r\n]+", str(description or "")) if ln.strip()]


def _after_label(lines, label_rx) -> str | None:
    """The value of the first "<Label>" line: the text after "Label:" on the
    same line, else the next non-empty line. None when there is no such line."""
    for i, ln in enumerate(lines[:12]):
        m = label_rx.match(ln)
        if m:
            rest = ln[m.end():].strip(" :\t")
            if rest:
                return rest
            return lines[i + 1] if i + 1 < len(lines) else ""
    return None


_OUR_COMPANY_RX = re.compile(r"^Our Company\b", re.I)
_COMPANY_RX = re.compile(r"^Company\s*:", re.I)


def brightspring_brand(description) -> tuple[str, str]:
    """(clean company name, raw value) for a BrightSpring body: the "Our
    Company" line, else the one company a teaser's first 400 characters
    name ("Adoration Home Health is seeking ..."). ("", raw) when the line
    names the system itself, or a company the table does not list."""
    lines = _lines(description)
    val = _after_label(lines, _OUR_COMPANY_RX)
    if val is not None:
        v = _words(val)
        return _BS_KEYS.get(_bkey(v), ""), v
    head = clean_text(description)[:_LEAD_CHARS]
    found = {BRIGHTSPRING_BRANDS[m.group(1)] for m in _BS_LEAD_RX.finditer(head)}
    if len(found) == 1:
        return found.pop(), ""
    return "", ""


def compassus_venture(description) -> tuple[str, str]:
    """(clean joint-venture name, raw value) for a Compassus body's
    "Company:" line; ("", raw) for plain "Compassus" or an unlisted value."""
    val = _after_label(_lines(description), _COMPANY_RX)
    if val is None:
        return "", ""
    v = _words(val)
    return _CP_KEYS.get(_bkey(v), ""), v


def centerwell_text(description, state: str = "") -> tuple[str, str]:
    """The branch a CenterWell body names ("Position Type : On-site at CWHH
    Seattle, WA branch location"), only when it names one."""
    found = {}
    for m in _CW_LINE_RX.finditer(str(description or "")):
        place = centerwell_place(f"{m.group(1)}, {m.group(2).upper()}", state)
        if place:
            found[place] = m.group(0)
    if len(found) == 1:
        place, raw = next(iter(found.items()))
        return f"CenterWell Home Health - {place}", raw
    return "", ""


def text_branch(system, description, state: str = "") -> tuple[str, str]:
    """(name, raw value) the body names for a KEEP_SYSTEMS chain."""
    if system == BRIGHTSPRING:
        return brightspring_brand(description)
    if system == COMPASSUS:
        return compassus_venture(description)
    if system == CENTERWELL:
        return centerwell_text(description, state)
    return "", ""


def is_own_name(system, name) -> bool:
    """True when `name` is one this module writes for `system` (only those
    are kept on nights without a body)."""
    n = _words(name)
    if not n or n == system:
        return False
    if system == BRIGHTSPRING:
        return n in _BS_NAMES
    if system == COMPASSUS:
        return n in _CP_NAMES
    if system == CENTERWELL:
        return n.startswith("CenterWell Home Health - ") and len(n) > len("CenterWell Home Health - ")
    return False


# ── Elara Caring (a lost town in the Workday location text) ─────────────────
def elara_town(text, state: str = "") -> tuple[str, str] | None:
    """(town, ST) from an Elara location descriptor or its URL segment, the
    words before the first state code ("Brooklyn - NY Bay Ridge",
    "Valparaiso---IN-SHH", "New-Britain-CT-W-Main-St"), only when they are a
    known town of that state (and of the row's state, when it has one)."""
    toks = [t for t in re.split(r"[^A-Za-z']+", str(text or "")) if t]
    st_row = (state or "").strip().upper()
    for i, t in enumerate(toks):
        if i == 0 or t not in STATE_CODES:
            continue
        if st_row and t != st_row:
            return None
        town = known_town(" ".join(toks[:i]), t)
        return (town, t) if town else None
    return None


# ── Stored names (nights without a body or a detail) ────────────────────────
_KNOWN: dict = {}
_ON_LABEL: set = set()         # (system, job_id) of active CenterWell rows still on the label
KNOWN_PAGE = 1000
KNOWN_MAX_ROWS = 20000
NO_BODY_CHARS = 600            # under this the row carries no body tonight (blank or a teaser)
REREAD_SYSTEMS = frozenset({CENTERWELL})
REREAD_CYCLE = int(os.getenv("HH_FACILITY_REREAD_CYCLE", "3"))
REREAD_UNTIL = os.getenv("HH_FACILITY_REREAD_UNTIL", "2026-11-20")


def set_known_facilities(rows) -> int:
    """{(hospital_system, job_id): stored name} for rows whose stored name is
    one this module writes; CenterWell rows still on the label go to the
    re-read set. Returns the map's size."""
    global _KNOWN, _ON_LABEL
    out, label = {}, set()
    for r in rows or []:
        s, jid = r.get("hospital_system"), r.get("job_id")
        if s not in KEEP_SYSTEMS or jid in (None, ""):
            continue
        name = _words(r.get("hospital_name"))
        if is_own_name(s, name):
            out[(s, str(jid))] = name
        elif s in REREAD_SYSTEMS and (not name or name == s):
            label.add((s, str(jid)))
    _KNOWN, _ON_LABEL = out, label
    return len(out)


def known_facility(system, job_id) -> str:
    return _KNOWN.get(((system or "").strip(), str(job_id)), "")


def load_known_facilities() -> int:
    """Read-only PostgREST passes (the SUPABASE_URL / key the upsert uses)
    over the active rows of each KEEP_SYSTEMS chain, one system at a time,
    keyset-paged on id (idx_system; CenterWell 2 pages, BrightSpring 3,
    Compassus 1). Without credentials the map stays empty and nothing is kept
    or re-read. Never raises."""
    sb_url = os.environ.get("SUPABASE_URL", "")
    sb_key = sb_keys.supabase_key()
    if not sb_url or not sb_key:
        set_known_facilities([])
        return 0
    import urllib.parse as _urlparse
    import urllib.request as _urlreq
    rows = []
    for system in sorted(KEEP_SYSTEMS):
        flt = _urlparse.quote(f"eq.{system}", safe=".")
        last, got = 0, 0
        try:
            while len(rows) < KNOWN_MAX_ROWS:
                u = (f"{sb_url.rstrip('/')}/rest/v1/hospital_jobs?select=id,hospital_system,job_id,hospital_name"
                     f"&is_active=is.true&hospital_system={flt}&id=gt.{last}&order=id.asc&limit={KNOWN_PAGE}")
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
            logger.warning(f"Home-health branches: {system} load stopped after {got:,} rows ({e}); "
                           f"its rows past that keep a branch only on nights their body is read")
    try:
        n = set_known_facilities(rows)
    except Exception as e:          # never let this read end the run
        logger.warning(f"Home-health branches: stored names not installed ({e})")
        set_known_facilities([])
        return 0
    logger.info(f"Home-health branches: {n:,} stored rows of {', '.join(sorted(KEEP_SYSTEMS))} name a branch; "
                f"{len(_ON_LABEL):,} CenterWell rows on the label for the detail re-read")
    return n


def reread_slot(canon, job_id, run_day: int) -> bool:
    """True when this stored row of a REREAD_SYSTEMS chain is still on the
    label and tonight is its night (one in REREAD_CYCLE) to have its detail
    read again for the branch, until REREAD_UNTIL (new postings get it on
    their first read anyway)."""
    if canon not in REREAD_SYSTEMS or (canon, str(job_id)) not in _ON_LABEL:
        return False
    try:
        if run_day > datetime.strptime(REREAD_UNTIL, "%Y-%m-%d").toordinal():
            return False
    except (TypeError, ValueError):
        return False
    n = max(1, REREAD_CYCLE)
    return zlib.crc32(f"hhbranch|{canon}|{job_id}".encode("utf-8")) % n == run_day % n


# ── Applying it to a Job (finalize_jobs, before normalize_job) ──────────────
UNKNOWN: dict = {}       # system -> {raw value: rows} named by no table entry, this run


def apply_facility(job) -> str:
    """The branch on one Job of a KEEP_SYSTEMS chain. Returns "detail" (the
    requisition location read tonight), "text" (the body), "kept" (the
    stored name, no body or detail tonight), "none", "other" (a name the
    adapter set that this module does not write: left as it is), or "" for
    any other system. hospital_system is never changed; the town never
    changes."""
    system = job.hospital_system
    if system not in KEEP_SYSTEMS:
        return ""
    cur = _words(job.hospital_name)
    if cur and cur != system and not is_own_name(system, cur):
        return "other"
    stash = getattr(job, REQ_LOC_ATTR, None)
    if stash:
        name = centerwell_branch(stash, job.title, job.description, job.state)
        if name:
            job.hospital_name = name
            return "detail"
    name, raw = text_branch(system, job.description, job.state)
    if name:
        job.hospital_name = name
        return "text"
    body = len((job.description or "").strip())
    if stash is None and body < NO_BODY_CHARS:
        kept = known_facility(system, job.job_id)
        if kept:
            job.hospital_name = kept
            return "kept"
    if raw and _words(raw) != system:
        d = UNKNOWN.setdefault(system, {})
        d[raw[:80]] = d.get(raw[:80], 0) + 1
    if cur != system:
        job.hospital_name = system
    return "none"


def apply_facilities(jobs) -> dict:
    """apply_facility over a run's rows. Returns {system: {how: rows}}."""
    stats: dict = {}
    for job in jobs:
        how = apply_facility(job)
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
