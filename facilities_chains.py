"""The site a posting of a clinic, imaging, therapy, urgent care or nursing
home chain works at (2026-10-29, facility scale phase 4, the chains group).

The owner, 2026-10-29: every site inside a multi-site employer is named and
placed correctly on the site and in the dashboard ("i thought you were gonna
edit all the large multihospital systems"). His decisions: (1) cards and job
pages show the SITE ("Apply to <site>"), the system name and logo stay on the
job page and the system hub; (2) named sites (hospitals, named clinics,
centers, an agency's named branch, a named community) show their own name;
generic offices, home-based and remote postings show the system name with the
town; never guess a town or a site (a directory that maps a town to a branch
is not evidence unless the posting itself names that branch).

Where each chain's own feed or posting names the site (read 2026-10-29 off
each board, a few requests per host; every stored row read too):

  RadNet (Jibe /api/jobs)
      location_name, the imaging centre as the employer files it, an internal
      market code and the centre ("NJIN Clifton", "AIP Medical Arts 102",
      "MNHTN E 77th Ground Fl", "BK Kern Bahamas"); tags1 the practice brand
      ("New Jersey Imaging Network", "Advanced Radiology", "Lenox Hill
      Radiology"). Name: "<brand> - <centre>", the market code and the suite
      or floor dropped ("New Jersey Imaging Network - Clifton", "Advanced
      Radiology - Medical Arts", "Lenox Hill Radiology - Manhattan E 77th").
      The brand is the one most of that centre's postings carry
      (RADNET_SITES, facilities_chains_radnet.json, 278 centres of the
      2026-10-29 feed), else the posting's own; a RadNet regional label
      ("RadNet Orange County") is "RadNet", a practice in brackets ("RadNet
      Ventura (Rolling Oaks Radiology)") is that practice. Offices (market
      and regional admin, revenue cycle, IT, contact centre, credentialing,
      DeepHealth), remote and hybrid roles and mobile units keep "RadNet".
  US Physical Therapy (SmartRecruiters postings list)
      customField "Brands" (the clinic's brand, "Wright Physical Therapy")
      and "Clinic" ("01344-Wright PT - Central Boise"; the value is cut at 25
      characters after the code, "01306-Wright PT - Twin Falls Ma", so the
      "Sterling Bill Code" field's last part, "... - WPT - Twin Falls Main",
      completes it when it starts the same). Name: "<brand> - <clinic
      place>" ("Wright Physical Therapy - Twin Falls Main"); a clinic value
      that is the brand itself ("827-Xcelerate PT") is the brand alone.
      Central business offices, admin, corporate, home care, athletic
      training outreach, Briotix (industrial on-site at a client's plant),
      remote and hybrid roles keep "US Physical Therapy".
  American Family Care (CareerPlug board)
      Franchise clinics. The list card gives the town only; a posting names
      its centre in the title ("X-Ray Technologist AFC Urgent Care -
      Modesto") or the body ("Location: AFC Urgent Care Cicero", "AFC
      Urgent Care Stamford is seeking"). Named only when the posting names
      "AFC [Urgent Care] [of] <place>" and that place is the row's own town
      (or a part of it, "Southwest Austin"), or a named spot that is no town
      in any state ("AFC Urgent Care - Harper's Point" at Cincinnati, "AFC
      Urgent Care New Garden in Greensboro, NC"). A franchise group
      ("AFC Urgent Care Portland/Vancouver"), a centre in another town
      ("AFC Urgent Care San Diego" on a Santee row) or two centres name
      none. Name: "AFC Urgent Care <place>".
  CityMD (Workday summit_citymd)
      The board's location is the town only ("Florham Park, NJ"), and the
      text names the operating company at most (Summit Health, CityMD,
      Starling Physicians), never a centre or a practice site: every row
      keeps "CityMD" with its town (decision 2). Nothing to read.
  NHS Management (UKG Ready job-requisitions)
      location.address_line_1 is the nursing home's own name ("Huntsville
      Health and Rehab", "Park Manor Health and Rehab"); a street there
      ("3141 Old Columbiana Road", the Birmingham office) names none.
  GoHealth Urgent Care (Workday gohealthuc)
      locationsText, "<centre> - <town>, ST" ("Seminary Plaza - Alexandria,
      VA", "Kirkwood - Kirkwood, MO"): "GoHealth Urgent Care - <centre>" and
      the descriptor's own town. A "N Locations" posting (a provider covering
      several centres) names none and stays as before; "Remote" postings keep
      the system name.
  Cantex Continuing Care (Paycom previews)
      locations, "<site> - <town>, TX <zip>": "Skilled Nursing at The Carlyle
      at Stonebridge Park", "Pivot at The Madison on Marsh" (the community,
      its own name), "Elysian Hospice - Dallas", "TheraCare Home Health -
      Beaumont", "Pharmacare - San Antonio" (an agency's named branch);
      "Cantex Corporate" and "Cantex - Houston" keep the system name. The
      town stays the one the adapter reads from the same value.
  Nexion Health (ADP CX)
      The published title names the nursing home ("CNA: Natchez
      Rehabilitation and Healthcare Center", "LVN - Cross Timbers"), read
      against NEXION_FACILITIES (each home's own spellings; a bare town in a
      title names none); else the requisition location's legal entity
      ("Nexion Health at Sweetwater Inc", code AVQ = Sterling Hills
      Healthcare Center), when its town is the row's town. Regional and
      corporate roles keep "Nexion Health".

Every name a hook returns is "" / None for "keep the system name"; the
scraper calls each hook through scraper._fac_hook, so a hook that raises costs
that one row (it keeps the label), never the system or the night.

Nights without a body: only American Family Care is read from the body
(CareerPlug is a KNOWN_BODY platform, so most nights send no body); on those
nights the centre stored on the row is kept (load_known_facilities), as the
CVS rows do. Every other chain's field is in the list feed every night.

hospital_system is never touched (the upsert key, the hub, the filters, the
logo). Shared with the stored-row change list
(claud_outputinput/facility_scale_2026-10-29/phase4/chains_work/make_changes.py),
so the database holds what the scraper keeps.
"""
from __future__ import annotations

import html as _html
import json
import logging
import os
import re
import time
import unicodedata

logger = logging.getLogger("scraper.facility_chains")

RADNET = "RadNet"
USPH = "US Physical Therapy"
AFC = "American Family Care"
CITYMD = "CityMD"
NHS = "NHS Management"
GOHEALTH = "GoHealth Urgent Care"
CANTEX = "Cantex Continuing Care"
NEXION = "Nexion Health"
SYSTEMS = frozenset({RADNET, USPH, AFC, CITYMD, NHS, GOHEALTH, CANTEX, NEXION})
_HERE = os.path.dirname(os.path.abspath(__file__))

US_STATES = frozenset(
    "AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH NJ NM NY "
    "NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY PR".split())


def clean(s) -> str:
    """Entities decoded, a broken dash (U+FFFD, en / em dash) made a hyphen,
    curly quotes straightened, whitespace collapsed."""
    s = _html.unescape(str(s or ""))
    s = unicodedata.normalize("NFKC", s)
    s = s.replace("�", "-").replace("–", "-").replace("—", "-")
    s = s.replace("’", "'").replace("‘", "'")
    return " ".join(s.split())


def fold(s) -> str:
    """Comparison key: lower case, "&" = "and", "Saint" = "St", punctuation
    and apostrophes dropped."""
    s = clean(s).lower().replace("&", " and ").replace("'", "")
    s = re.sub(r"\bsaint\b", "st", s)
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    return " ".join(s.split())


def _title_case(s: str) -> str:
    small = {"of", "at", "and", "the", "for", "in", "on", "a"}
    return " ".join(w if i and w in small else w[:1].upper() + w[1:] for i, w in enumerate(s.lower().split()))


# ══ RadNet (Jibe) ═══════════════════════════════════════════════════════════
RADNET_FILE = os.path.join(_HERE, "facilities_chains_radnet.json")
try:
    with open(RADNET_FILE, encoding="utf-8") as _f:
        RADNET_SITES: dict = {fold(k): clean(v) for k, v in json.load(_f).items()}
except FileNotFoundError:          # the table builder imports this module before the file exists
    RADNET_SITES = {}

# The market codes RadNet puts before a centre ("NJIN Clifton", "SGV Arcadia",
# "BK Kern Bahamas", "HSTN HMI Pearland"); MNHTN is Manhattan.
_RADNET_CODES = ("AIP", "CIP", "ARS", "AZ", "PHNX", "IIP", "DIP", "IMI", "NJIN", "NCIP", "VIP", "TCIP", "LA", "LB",
                 "OC", "SFV", "SGV", "IE", "TM", "CV", "SD", "VC", "HD", "FS", "HV", "NWR", "BK", "HSTN", "Central")
_RADNET_CODE_RX = re.compile(r"^(?:(?:%s)\s+)+" % "|".join(_RADNET_CODES))
_RADNET_HMI_RX = re.compile(r"^HMI\s+")          # "HSTN HMI Pearland": Houston Medical Imaging's own code
# Not one centre (decision 2): offices, admin, contact centre, revenue cycle,
# IT, credentialing, DeepHealth's teams, remote work, mobile units.
_RADNET_OFFICE_RX = re.compile(
    r"\b(?:market admin|region admin|remote work|remote|RCO|IT|finance|credentialing|CNTCTR|mammo tracking|marketing|"
    r"scheduling|business development|commercial|quality and regulatory|corporate|admin|mobile)\b|\bDH$", re.I)
# A suite, floor or level after the centre ("Medical Arts 102", "Pleasanton
# (105)", "E 77th Ground Fl", "Pelham Bay (LL1)", "Corona S Main 801 Ste 101",
# "Waring Road B", "Simi Valley100", "PET CT MRI125").
_RADNET_TAIL_RX = re.compile(
    r"(?:(?<!Highway)(?<!Hwy)(?<!Route)(?<!\bUS)\s+\d{1,4}[A-Z]?"          # "Medical Arts 102", not "Highway 79"
    r"|\s+(?:\(\s*[A-Z]{0,2}\d+\s*\)|STE\s*\w+|Suite\s*\w+|Ground Fl|Lower Lvl|Cellar|Basement|Ninth Fl|"
    r"\d+(?:st|nd|rd|th) Fl|Fl \d+|LL\d|[A-Z])"
    r"|(?<=[A-Za-z])\d{2,4})$", re.I)
_RADNET_WORDS = ((re.compile(r"\bImg Ctr\b", re.I), "Imaging Center"), (re.compile(r"\bCtr\b\.?", re.I), "Center"),
                 (re.compile(r"\bHghts\b", re.I), "Heights"), (re.compile(r"\bIC$"), "Imaging Center"))


def radnet_brand(raw) -> str:
    """A tags1 value -> the practice brand: a practice in brackets wins
    ("RadNet Bakersfield (Kern-Radiology)" -> "Kern Radiology"), a RadNet
    regional label is "RadNet", a legal or branch tail is dropped ("Radiology
    Regional Center, P.A", "Stockton Diagnostic Imaging- March Lane ...")."""
    s = clean(raw)
    if not s:
        return RADNET
    m = re.search(r"\(([^()]+)\)\s*$", s)
    if m and s.lower().startswith("radnet"):
        return clean(m.group(1).replace("-", " "))
    if re.match(r"^radnet\b", s, re.I):
        return RADNET
    s = re.sub(r",\s*P\.?\s*A\.?$", "", s)
    s = re.sub(r"\s*-\s.*$|-\s+.*$", "", s)
    return s.strip(" ,-") or RADNET


def radnet_place(location_name) -> str:
    """The centre part of a location_name, or "" (an office, or nothing left)."""
    s = clean(location_name)
    if not s or _RADNET_OFFICE_RX.search(s):
        return ""
    s = re.sub(r"^MNHTN\b", "Manhattan", s)
    s = _RADNET_CODE_RX.sub("", s)
    s = _RADNET_HMI_RX.sub("", s)
    s = re.sub(r"^(?:-\s*|of\s+)", "", s).strip()
    for _ in range(3):
        t = _RADNET_TAIL_RX.sub("", s).strip()
        if t == s or not t:
            break
        s = t
    for rx, rep in _RADNET_WORDS:
        s = rx.sub(rep, s)
    return s.strip(" ,-")


def radnet_site(j: dict) -> tuple[str, str]:
    """(name, how) for one RadNet /api/jobs row: ("<brand> - <centre>",
    "centre"), or (RADNET, why: "office", "remote", "none")."""
    j = j or {}
    loc = clean(j.get("location_name"))
    kind = " ".join(str(v) for v in (j.get("tags2") or []) if v).lower()
    if "remote" in kind or "hybrid" in kind:
        return RADNET, "remote"
    if not loc:
        return RADNET, "none"
    place = radnet_place(loc)
    if not place:
        return RADNET, "office"
    raw_brand = RADNET_SITES.get(fold(loc))
    if raw_brand is None:
        tags = j.get("tags1") or []
        raw_brand = tags[0] if isinstance(tags, list) and tags else (tags if isinstance(tags, str) else "")
    if fold(raw_brand) == "deephealth":
        return RADNET, "office"
    brand = radnet_brand(raw_brand)
    words, bwords = place.split(), brand.split()
    if len(words) > 2 and words[-1].lower() == words[0].lower():
        words = words[:-1]                   # "Queens Elmhurst Queens"
    k = 0
    while k < min(len(words) - 1, len(bwords)) and words[k].lower() == bwords[k].lower():
        k += 1
    words = words[k:]                        # "Kern Bahamas" under Kern Radiology, "Rolling Oaks Simi Valley"
    if not words or fold(" ".join(words)) == fold(bwords[0] if bwords else ""):
        return brand, "centre"               # "Central Modesto" under Modesto Advanced Imaging
    if (len(words) > 1 and words[0].isupper() and len(words[0]) >= 2
            and words[0] == "".join(w[0] for w in bwords[:len(words[0])]).upper()
            and fold(" ".join(words[1:])) == fold(" ".join(bwords[len(words[0]):]))):
        return brand, "centre"               # "DRI Elk Grove" under Diagnostic Radiological Imaging Elk Grove
    return f"{brand} - {' '.join(words)}", "centre"


def jibe_site(system, j: dict):
    """The scraper's Jibe hook: hospital_name for a RadNet row, None for any
    other system."""
    if system != RADNET:
        return None
    return radnet_site(j)[0]


# ══ US Physical Therapy (SmartRecruiters) ═══════════════════════════════════
_USPH_CODE_RX = re.compile(r"^\s*\d{2,6}\s*-\s*")
# Not one clinic (decision 2): business offices, admin, corporate, home care,
# athletic training outreach, the industrial prevention teams.
_USPH_OFFICE_RX = re.compile(r"\b(?:CBO|admin|corporate|business office|homecare|home care|home health|athletic train\w*|"
                             r"prevention|equipment|sales|ignite|management)\b", re.I)
_USPH_NOT_SITE_BRANDS = frozenset({fold(USPH), fold("Briotix Health"), fold("Ignite Rehab")})
_USPH_CUT = 25                 # characters of the Clinic value after its code (longer values are cut there)


def _usph_fields(posting: dict) -> dict:
    return {str(f.get("fieldLabel") or ""): clean(f.get("valueLabel")) for f in (posting.get("customField") or [])
            if isinstance(f, dict)}


def _usph_bill_place(bill: str) -> list:
    """The parts of a Sterling Bill Code after its two numbers ("01305 -
    01306 - WPT - Twin Falls Main" -> ["WPT", "Twin Falls Main"])."""
    parts = [p.strip() for p in re.split(r"\s+-\s+", clean(bill)) if p.strip()]
    return [p for p in parts if not re.fullmatch(r"\d{2,6}", p)]


_USPH_GENERIC_PLACE = frozenset({"pt", "physical therapy", "clinic", "main office", "rehab", "therapy"})


def _usph_complete(place: str, text: str, bill: list, city: str) -> str:
    """The clinic place, completed from the bill code when the Clinic value
    was cut ("Twin Falls Ma" -> "Twin Falls Main", "Southern Rehab Colum" ->
    "Southern Rehab Columbus", "White Bridge Rd" -> "White Bridge Road"), or
    "" for a value that reached the cut and that nothing confirms."""
    cands = ([bill[-1]] if bill else []) + ([" ".join(bill[-2:])] if len(bill) >= 2 else [])
    pf = fold(place)
    for cand in cands:
        cf = fold(cand)
        if cf.startswith(pf) and len(cf) >= len(pf):
            return cand
    if len(text) < _USPH_CUT:
        return place
    for cand in cands[:1]:
        if (pf.split() and fold(cand).split() and fold(cand).split()[0] == pf.split()[0]
                and len(fold(cand)) > len(pf)):
            return cand                                         # a cut value: same first word, fuller spelling
    last = pf.split()[-1] if pf.split() else ""
    if last and cands and last in fold(cands[0]).split():
        return place                                            # "NE Columbia" (bill: Northeast Columbia)
    if last and fold(city) and last == fold(city).split()[-1]:
        return place                                            # "Central Boise" at Boise
    return ""


def usph_site(posting: dict) -> tuple[str, str]:
    """(name, how) for one US Physical Therapy postings-list row: ("<brand> -
    <place>" or "<brand>", "clinic"), or (USPH, why: "remote", "office",
    "none")."""
    p = posting or {}
    loc = p.get("location") or {}
    if loc.get("remote") or loc.get("hybrid"):
        return USPH, "remote"
    f = _usph_fields(p)
    brand = clean(f.get("Brands"))
    clinic = clean(f.get("Clinic"))
    if not brand or fold(brand) in _USPH_NOT_SITE_BRANDS:
        return USPH, "office" if brand else "none"
    text = _USPH_CODE_RX.sub("", clinic).strip()
    if not text:
        return USPH, "none"
    if _USPH_OFFICE_RX.search(text):
        return USPH, "office"
    m = re.match(r"^(.*?)\s*-\s*(.+)$", text)
    abbr, place = (clean(m.group(1)), clean(m.group(2))) if m else (text, "")
    if place and fold(place) == fold(abbr):
        place = ""                                               # "Elite S&S - Elite S&S"
    if place:
        place = _usph_complete(place, text, _usph_bill_place(f.get("Sterling Bill Code", "")), loc.get("city") or "")
    if place:
        place = re.sub(r"\s*\([^()]*\)\s*$", "", place)          # "Sports Medicine Oregon (SMO)", "East Nashville (055)"
        place = re.sub(r"\s+(?:Physical Therapy|PT)$", "", place)
        if len(place.split()) > 1:
            place = re.sub(r"\s+(?:%s)$" % "|".join(sorted(US_STATES)), "", place)   # "Clarksville TN"
        if place.isupper() and len(place) > 4:
            place = _title_case(place)                          # "CLARKSVILLE N"; "KCK" stays
        if _USPH_OFFICE_RX.search(place):
            return USPH, "office"
        words, bwords = place.split(), fold(brand).split()
        k = 0
        while k < min(len(words) - 1, len(bwords)) and fold(words[k]) == bwords[k]:
            k += 1
        words = words[k:]                                       # "Southern Rehab Columbus" under Southern Rehab ...
        rest = " ".join(words)
        if not words or fold(rest) in _USPH_GENERIC_PLACE or f" {fold(rest)} " in f" {' '.join(bwords)} ":
            rest = ""                                           # "Max Motion" under Max Motion Physical Therapy
        place = rest
    return (f"{brand} - {place}" if place else brand), "clinic"


def smartrecruiters_site(system, posting: dict):
    """The scraper's SmartRecruiters hook: hospital_name for a US Physical
    Therapy row, None for any other system."""
    if system != USPH:
        return None
    return usph_site(posting)[0]


# ══ NHS Management (UKG Ready) ══════════════════════════════════════════════
_STREET_END_RX = re.compile(r"\b(?:road|rd|street|st|avenue|ave|drive|dr|blvd|boulevard|highway|hwy|lane|ln|way|parkway|"
                            r"pkwy|circle|court|ct|place|pl|suite|ste)\.?$", re.I)
_NHS_OFFICE_RX = re.compile(r"\b(?:NHS|corporate|office|management|headquarters)\b", re.I)


def kronos_site(system, loc: dict):
    """The scraper's UKG Ready hook: the nursing home NHS Management's
    location.address_line_1 names ("Huntsville Health and Rehab"), "" when
    that line is a street or an office (the row keeps the system name), None
    for any other system."""
    if system != NHS:
        return None
    v = clean((loc or {}).get("address_line_1"))
    if not v or re.match(r"^\d", v) or not re.search(r"[A-Za-z]{3}", v) or "p.o. box" in v.lower():
        return ""
    if _STREET_END_RX.search(v) or _NHS_OFFICE_RX.search(v):
        return ""
    if v.isupper():
        v = _title_case(v)
    return v.strip(" ,.")


# ══ GoHealth Urgent Care (Workday) ══════════════════════════════════════════
_N_LOCATIONS_RX = re.compile(r"^\s*\d+\s+locations?\s*$", re.I)
_GH_TOWN_RX = re.compile(r"^(?P<town>[A-Za-z][A-Za-z .'-]*?),\s*(?P<st>[A-Z]{2})$")
# Descriptors in another shape: a market and a centre with no town; the
# centre's own descriptor elsewhere on the board gives its town.
GOHEALTH_DESCRIPTORS = {
    "mercy - west sunshine": ("West Sunshine", "Springfield", "MO"),     # = "West Sunshine - Springfield, MO"
    "upmc - shadyside": ("Shadyside", "Pittsburgh", "PA"),               # = "Shadyside - Pittsburgh, PA"
    "downtown hartford, ct": ("Downtown Hartford", "Hartford", "CT"),
}
_GH_NOT_SITE_RX = re.compile(r"\bremote\b|\bvirtual\b|\bcorporate\b", re.I)


def _gh_key(s) -> str:
    return " ".join(clean(s).lower().split())


def gohealth_centre(loc: str):
    """(centre, town, state) of one GoHealth descriptor, None when it names no
    centre (a "N Locations" posting, "Remote", a shape the board does not
    use)."""
    s = clean(loc)
    if not s or _N_LOCATIONS_RX.match(s) or _GH_NOT_SITE_RX.search(s):
        return None
    hit = GOHEALTH_DESCRIPTORS.get(_gh_key(s))
    if hit:
        return hit
    parts = [p.strip() for p in re.split(r"\s+-\s+", s) if p.strip()]
    if len(parts) < 2:
        return None
    m = _GH_TOWN_RX.match(parts[-1])
    if not m or m.group("st") not in US_STATES:
        return None
    head = parts[:-1]
    if len(head) > 1 and fold(head[1]).startswith(fold(head[0]) + " "):
        head = head[1:]                                            # "Erie - Erie West" -> "Erie West"
    centre = " ".join(head)                                        # "Erie - Summit Towne Centre" -> "Erie Summit Towne Centre"
    centre = re.sub(r"(?<=[a-z])-(?=[A-Z]\.)", " ", centre)        # "Harrisburg-S. Arlington" (a broken dash)
    return centre, m.group("town").strip(), m.group("st")


def gohealth_site(loc: str, external_path: str = ""):
    """The WD_SITE_FACILITY resolver for GoHealth: ("GoHealth Urgent Care -
    <centre>", town, state) from the list's locationsText, None otherwise
    (the row is placed as before). The path is not read: a "N Locations"
    posting's path names only its first centre."""
    got = gohealth_centre(loc)
    if not got:
        return None
    centre, town, st = got
    return f"{GOHEALTH} - {centre}", town, st


def gohealth_unknown(loc: str, external_path: str = "") -> str:
    """A descriptor that names no centre and is not "N Locations" or remote
    (for the run log), or ""."""
    s = clean(loc)
    if not s or _N_LOCATIONS_RX.match(s) or _GH_NOT_SITE_RX.search(s):
        return ""
    return "" if gohealth_centre(s) else s


# ══ Cantex Continuing Care (Paycom) ═════════════════════════════════════════
_PAYCOM_TOWN_RX = re.compile(r"^[A-Za-z][A-Za-z .'/()-]*?\s*,\s*[A-Z]{2}(?:\s+\d{5}(?:-\d{4})?)?\s*$")
_CANTEX_COMMUNITY_RX = re.compile(r"^(?:skilled nursing|pivot|assisted living|memory care|independent living|"
                                  r"rehab(?:ilitation)?|long term care)\s+at\s+(.+)$", re.I)
_CANTEX_OFFICE_RX = re.compile(r"^cantex\b|\bcorporate\b", re.I)


def cantex_site_name(site: str) -> str:
    """The name of one Paycom location name (the part before " - <town>,
    TX"): a community's own name ("Skilled Nursing at The Carlyle at
    Stonebridge Park" -> "The Carlyle at Stonebridge Park"), an agency branch
    as written ("Elysian Hospice - Dallas"), "" for the offices."""
    s = clean(site)
    if not s or _CANTEX_OFFICE_RX.search(s):
        return ""
    m = _CANTEX_COMMUNITY_RX.match(s)
    if m:
        s = m.group(1)
    s = re.sub(r"\s*\([^()]*\)\s*$", "", s)                        # "Sherman/Denison (Grayson County)"
    s = re.sub(r"\s+-\s+", " - ", s).strip(" -")
    s = s.replace("Health & Rehabilitation", "Health and Rehabilitation")
    return s


def paycom_site(system, loc_text: str):
    """The scraper's Paycom hook: the Cantex site its location value names
    ("" for an office or a value with no site), None for any other
    system."""
    if system != CANTEX:
        return None
    parts = [p.strip() for p in re.split(r"\s+-\s+", clean(loc_text)) if p.strip()]
    if len(parts) < 2 or not _PAYCOM_TOWN_RX.match(parts[-1]):
        return ""                                                  # no "<site> - <town>, TX": no site
    return cantex_site_name(" - ".join(parts[:-1]))


# Stored rows (the change list): the communities and branches the 2026-10-29
# board listed, with the town the board gives each; a stored title or body that
# names exactly one of them, at that town, names it.
CANTEX_SITES = {
    "Ashford Gardens": "Houston", "Beacon Hill": "Denison", "Bel Air at Teravista": "Round Rock",
    "Bonne Vie": "Port Arthur", "Carrara": "Plano", "Coronado at Stone Oak": "San Antonio",
    "Crestview Court": "Cedar Hill", "Edgemere Estates": "El Paso", "Fort Bend HealthCare Center": "Rosenberg",
    "Hollymead": "Flower Mound", "Larkspur": "Lufkin", "Laurel Court": "Alvin", "Magnolia Manor": "Groves",
    "Mill Creek": "Silsbee", "Oakwood Manor": "Vidor", "Palma Real": "Mathis", "Palomino Place": "Mesquite",
    "Park Valley Inn Health Center": "Round Rock", "Prairie Estates": "Frisco",
    "Renaissance Care Center": "Gainesville", "Riverside Oaks": "Victoria", "San Remo": "Richardson",
    "Solera at West Houston": "Houston", "Sorrento": "San Antonio", "Sundance Inn Health Center": "New Braunfels",
    "The Belmont at Twin Creeks": "Allen", "The Bradford at Brookside": "Livingston",
    "The Broadmoor at Creekside Park": "Spring", "The Carlyle at Stonebridge Park": "Southlake",
    "The Colonnades at Reflection Bay": "Pearland", "The Crescent": "Sugar Land",
    "The Harrison at Heritage": "Fort Worth", "The Madison on Marsh": "Carrollton",
    "The Manor at Seagoville": "Seagoville", "The Villa at Mountain View": "Dallas",
    "Windemere at Westover Hills": "San Antonio", "Windsor Gardens": "Lancaster",
    "Woodville Health and Rehabilitation Center": "Woodville",
    "Elysian Hospice - San Antonio": "San Antonio",
}
_CANTEX_KEYS = {fold(k): k for k in CANTEX_SITES}
# Generic words a community name could be confused with inside a sentence.
_CANTEX_WEAK = frozenset({fold("Carrara"), fold("Sorrento"), fold("Larkspur"), fold("Hollymead"), fold("Mill Creek"),
                          fold("Beacon Hill"), fold("The Crescent"), fold("Bonne Vie"), fold("San Remo")})


def cantex_stored_site(title, description, city="", state="") -> tuple[str, str]:
    """(site, how) a stored Cantex row's own text names, at the row's town:
    the title, else the first lines of the body ("The Belmont at Twin Creeks
    - Allen,TX", "Driver-Certified Nursing Assistant-The Madison on Marsh").
    A name made of one common word ("Carrara", "Sorrento") counts only in the
    title or right before the town. ("", why) otherwise."""
    town = fold(city)
    head = clean(description)[:400]
    found = set()
    for src, text in (("title", clean(title)), ("body", head)):
        t = f" {fold(text)} "
        for k, name in _CANTEX_KEYS.items():
            if f" {k} " not in t:
                continue
            if fold(CANTEX_SITES[name]) != town:
                continue
            if k in _CANTEX_WEAK and src == "body" and not re.search(
                    re.escape(k) + r"\s+" + re.escape(town), fold(text)):
                continue
            found.add(name)
        if found:
            break
    if len(found) == 1:
        return found.pop(), "text"
    return "", "two" if found else "none"


# ══ Nexion Health (ADP CX) ══════════════════════════════════════════════════
# code (the requisition location's legal entity), clean name (the fullest
# spelling its own postings use), town, state, the spellings a title may use.
# A bare town ("- Duncanville", "Admissions Coordinator-LPN Thibodaux") is not
# a spelling: the entity code names those rows.
_N = "Rehabilitation and Healthcare Center"
NEXION_FACILITIES = (
    ("W8E", "Gonzales Healthcare", "Gonzales", "LA", ("Gonzales Healthcare",)),
    ("BTZ", "Kaplan Healthcare", "Kaplan", "LA", ("Kaplan Healthcare",)),
    ("SZT", "Many Healthcare", "Many", "LA", ("Many Healthcare",)),
    ("SZS", "Marrero Healthcare", "Marrero", "LA", ("Marrero Healthcare",)),
    ("SZR", "Meadowview Healthcare", "Minden", "LA", ("Meadowview",)),
    ("SUE", "Maison Jardin", "Morgan City", "LA", ("Maison Jardin",)),
    ("JFC", "New Iberia Manor South", "New Iberia", "LA", ("New Iberia Manor South", "New Iberia South")),
    ("JZL", "New Iberia Manor North", "New Iberia", "LA", ("New Iberia Manor North", "New Iberia North")),
    ("6XT", "Patterson Healthcare", "Patterson", "LA", ("Patterson Healthcare",)),
    ("SEE", "Claiborne Rehabilitation", "Shreveport", "LA", ("Claiborne Rehabilitation",)),
    ("4VA", "Pierremont Healthcare", "Shreveport", "LA", ("Pierremont",)),
    ("SWU", "Thibodaux Healthcare", "Thibodaux", "LA", ("Thibodaux Healthcare",)),
    ("R26", "Maison Bienvenue", "Thibodaux", "LA", ("Maison Bienvenue",)),
    ("V84", "Vivian Healthcare", "Vivian", "LA", ("Vivian Healthcare",)),
    ("WJL", f"Great Oaks {_N}", "Byhalia", "MS", ("Great Oaks Rehabilitation",)),
    ("SZF", f"Delta {_N}", "Cleveland", "MS", ("Delta Rehabilitation", "Delt Rehabilitation")),
    ("S7K", f"Woodlands {_N}", "Clinton", "MS", ("Woodlands Rehabilitation",)),
    ("ST7", f"Columbia {_N}", "Columbia", "MS", ("Columbia Rehabilitation",)),
    ("SHC", f"Cornerstone {_N}", "Corinth", "MS", ("Cornerstone Rehabilitation",)),
    ("SHD", f"Crystal {_N}", "Greenwood", "MS", ("Crystal Rehabilitation",)),
    ("S3B", f"Grenada {_N}", "Grenada", "MS", ("Grenada Rehabilitation",)),
    ("WJM", f"Holly Springs {_N}", "Holly Springs", "MS", ("Holly Springs Rehabilitation",)),
    ("WJX", f"Indianola {_N}", "Indianola", "MS", ("Indianola Rehabilitation",)),
    ("Q8X", f"Natchez {_N}", "Natchez", "MS", ("Natchez Rehabilitation",)),
    ("SAA", f"Picayune {_N}", "Picayune", "MS", ("Picayune Rehabilitation",)),
    ("RDD", f"The Bluffs {_N}", "Vicksburg", "MS", ("The Bluffs Rehabilitation",)),
    ("RDB", f"Yazoo City {_N}", "Yazoo City", "MS", ("Yazoo City Rehabilitation", "Yazoo Rehabilitation")),
    ("C9P", "Barton Valley Rehabilitation Center", "Austin", "TX", ("Barton Valley",)),
    ("WKS", "North Star Ranch Healthcare Center", "Bonham", "TX", ("North Star Ranch", "North Star Rehabilitation")),
    ("TE6", "Lake Hills Healthcare Center", "Brownwood", "TX", ("Lake Hills",)),
    ("H3A", "Whispering Springs Rehabilitation", "Carrizo Springs", "TX", ("Whispering Springs",)),
    ("WKM", "Cass Valley Rehabilitation Center", "Centerville", "TX", ("Cass Valley",)),
    ("BFS", "Willow Park", "Clifton", "TX", ("Willow Park",)),
    ("EA7", "Coleman Healthcare Center", "Coleman", "TX", ("Coleman Healthcare",)),
    ("SED", "Duncanville Healthcare Center", "Duncanville", "TX", ("Duncanville Healthcare",)),
    ("APE", "Arbor Hills Rehabilitation", "Eagle Lake", "TX", ("Arbor Hills",)),
    ("3BV", "Gulf Shores Rehabilitation", "Falfurrias", "TX", ("Gulf Shores",)),
    ("WKP", "Flatonia Rehabilitation Center", "Flatonia", "TX", ("Flatonia Rehabilitation",)),
    ("XEH", "Prairie Meadows Rehabilitation Center", "Floresville", "TX", ("Prairie Meadows",)),
    ("ZPA", "Cross Timbers Healthcare Center", "Flower Mound", "TX", ("Cross Timbers",)),
    ("WXE", "Ridgecrest Healthcare Center", "Forney", "TX", ("Ridgecrest",)),
    ("SHG", "Truman Smith", "Gladewater", "TX", ("Truman Smith", "Truman Rehabilitation")),
    ("TE9", "Pecan Creek Rehabilitation Center", "Hamilton", "TX", ("Pecan Creek",)),
    ("3BU", "Renaissance Healthcare Center", "Italy", "TX", ("Renaissance",)),
    ("L6T", "Lone Star Ranch Rehabilitation", "Kingsville", "TX", ("Lone Star Ranch",)),
    ("WKR", "Bay Ridge Rehabilitation Center", "LaPorte", "TX", ("Bay Ridge",)),
    ("WKQ", "Brazos Rehabilitation Center", "Lake Jackson", "TX", ("Brazos Rehabilitation",)),
    ("FSZ", "Lily Springs Rehabilitation", "Lampasas", "TX", ("Lily Springs",)),
    ("AHE", "Village Creek Rehabilitation", "Lumberton", "TX", ("Village Creek",)),
    ("STR", "Golden Creek Healthcare and Rehabilitation", "Navasota", "TX", ("Golden Creek",)),
    ("F8F", "Green Valley Rehabilitation", "North Richland Hills", "TX", ("Green Valley",)),
    ("AVS", "Cedar Ridge Healthcare Center", "Pilot Point", "TX", ("Cedar Ridge",)),
    ("AVQ", "Sterling Hills Healthcare Center", "Sweetwater", "TX", ("Sterling Hills",)),
    ("SDM", "Terrell Healthcare Center", "Terrell", "TX", ("Terrell Healthcare",)),
    ("V82", "Midwestern Healthcare Center", "Wichita Falls", "TX", ("Midwestern",)),
    ("R28", "Lakeview Healthcare", "Winnsboro", "TX", ("Lakeview",)),
)
NEXION_BY_CODE = {c: (n, t, s) for c, n, t, s, _sp in NEXION_FACILITIES}
_NEXION_SPELLINGS = tuple((fold(sp), n, t, s) for _c, n, t, s, sps in NEXION_FACILITIES for sp in sps + (n,))
_NEXION_REGIONAL_RX = re.compile(r"\bregion(?:al)?\b|\barea director\b|\bcorporate\b|\bstaff accountant\b", re.I)


def _same_town(a, b) -> bool:
    return fold(a).replace(" ", "") == fold(b).replace(" ", "")


def nexion_title_site(title, city="", state="") -> tuple[str, str]:
    """(name, how) the title names, at the row's own town: ("<home>",
    "title"); ("", "regional") for a regional or corporate role; ("",
    "other town") when the home named sits elsewhere; ("", "none")."""
    t = clean(title)
    if _NEXION_REGIONAL_RX.search(t):
        return "", "regional"
    ft = f" {fold(t)} "
    hits = {(n, tw, st) for sp, n, tw, st in _NEXION_SPELLINGS if f" {sp} " in ft}
    if not hits:
        return "", "none"
    if len({h[0] for h in hits}) > 1:
        return "", "two"
    n, tw, st = hits.pop()
    if city and not _same_town(city, tw):
        return "", "other town"
    if state and str(state).strip().upper() not in ("", st):
        return "", "other town"
    return n, "title"


def nexion_site(title, city, state, loc0: dict | None = None) -> tuple[str, str]:
    """(name, how) for one Nexion posting: the title's home, else the
    requisition location's legal entity (NEXION_BY_CODE) when its town is
    the row's town; ("", why) keeps the system name."""
    name, how = nexion_title_site(title, city, state)
    if name or how in ("regional", "two", "other town"):
        return name, how
    code = str(((loc0 or {}).get("nameCode") or {}).get("codeValue") or "").strip().upper()
    hit = NEXION_BY_CODE.get(code)
    if not hit:
        return "", "none"
    n, tw, st = hit
    if not _same_town(city, tw) or str(state or "").strip().upper() != st:
        return "", "other town"
    return n, "entity"


def adpcx_site(system, title, city, state, loc0: dict | None = None):
    """The scraper's ADP CX hook: the Nexion home ("" keeps the system
    name), None for any other system."""
    if system != NEXION:
        return None
    return nexion_site(title, city, state, loc0)[0]


# ══ American Family Care (CareerPlug) ═══════════════════════════════════════
_AFC_RX = re.compile(r"\b(?:AFC(?![A-Za-z])|American Family Care)(?:[ \t]+Urgent[ \t]+Care)?(?:[ \t]+of)?[ \t]*"
                     r"[-:,]?[ \t]*(?P<place>(?:[A-Z][A-Za-z.'-]*|[A-Z]{2,})(?:[ ][A-Z][A-Za-z.'-]*){0,3})")
_AFC_NOT_PLACE = frozenset(fold(w) for w in (
    "Franchising", "Urgent", "Care", "Physicians", "Providers", "Provider", "Standards", "Team", "Teams", "X-ray",
    "Radiologic", "Location", "Locations", "Clinic", "Clinics", "Center", "Centers", "Company", "Medical", "Staff",
    "Brand", "Why", "Is", "Coming", "Overview", "The", "We", "NP", "PA", "Full", "Part", "PRN", "LLC", "Inc",
    "Advanced", "Practice", "Nurse", "Practitioner", "Physician", "Job", "Position", "Opening", "Future",
    "Corporate", "Franchise", "Franchisee", "Network", "Family", "Health", "Occupational", "AFC", "X", "Ray",
    "American", "Competitive", "Compensation", "Additional", "Flexible", "Supportive", "Greet", "Make", "Real",
    "Real-World", "Full-Time", "Part-Time", "Benefits", "About", "Join", "Our", "Your", "Apply", "Now"))
_AFC_TAIL_WORDS = frozenset(fold(w) for w in (
    "Full", "Part", "Time", "Per", "Diem", "PRN", "Is", "Location", "Address", "Has", "Openings", "Medical",
    "Director", "Will", "Be", "Team", "Teams", "Radiologic", "Technologist", "Job", "Position", "Seeking", "Clinic",
    "Center", "Urgent", "Care", "LLC", "Inc", "X", "Ray", "Tech", "New", "Jersey", "MA", "NP", "PA"))
_AFC_HEAD = 4000


def _afc_place(raw: str) -> str:
    words = clean(raw).split()
    # drop trailing words that are not part of a place ("Chino Part Time", "Torrance. Medical")
    while words and (fold(words[-1]) in _AFC_TAIL_WORDS or fold(words[-1]) in US_STATES_FOLD):
        words.pop()
    s = " ".join(words).strip(" .,'-")
    if s.isupper() and len(s) > 3:
        s = _title_case(s)
    return s


US_STATES_FOLD = frozenset(s.lower() for s in US_STATES)


def _afc_places(text: str) -> list[str]:
    """The places "AFC [Urgent Care] [of] <place>" names, line by line (a
    heading "AFC Urgent Care" followed by the next line is no place)."""
    out = []
    for line in str(text or "").splitlines():
        line = clean(line)
        for m in _AFC_RX.finditer(line):
            tail = line[m.end():m.end() + 2]
            if tail.startswith("/"):
                continue                                # "AFC Urgent Care Portland/Vancouver": a franchise group
            place = _afc_place(m.group("place"))
            if not place or fold(place.split()[0]) in _AFC_NOT_PLACE:
                continue
            out.append(place)
    return out


def afc_centre(title, description, city="", state="") -> tuple[str, str]:
    """(name, how) of the centre an AFC posting names (see the module
    notes): ("AFC Urgent Care <place>", "title" / "body"), or ("", why)."""
    town = fold(city).replace("-", " ")
    st = str(state or "").strip().upper()
    raw_body = str(description or "")[:_AFC_HEAD]
    seen: dict = {}
    for src, text in (("title", clean(title)), ("body", raw_body)):
        for place in _afc_places(text):
            pf = fold(place).replace("-", " ")
            if not pf:
                continue
            ok = False
            if pf == town or (town and (f" {town} " in f" {pf} ")):
                ok = True                                # the row's town, or a part of it ("Southwest Austin")
            elif town and not _is_town(place, st):
                ok = True                                # a named spot that is no town anywhere ("Harper's Point")
            if ok:
                seen.setdefault(pf, (place, src))
            else:
                seen.setdefault("x:" + pf, (place, "other"))
    good = {k: v for k, v in seen.items() if not k.startswith("x:")}
    if len(good) == 1:
        place, src = next(iter(good.values()))
        return f"AFC Urgent Care {place}", src
    if len(good) > 1:
        return "", "two"
    return "", "other town" if seen else "none"


def _is_town(place, st) -> bool:
    try:
        from city_utils import known_town
        return bool(known_town(place, st or None)) or bool(known_town(place, None))
    except Exception:
        return True                                      # unsure: treat as a town (names none)


# ── Stored names (nights without a body) ────────────────────────────────────
_KNOWN: dict = {}          # AFC job_id -> centre name stored on the active row
KNOWN_PAGE = 1000
NO_BODY_CHARS = 300


def set_known_facilities(rows) -> int:
    """{job_id: centre} for active American Family Care rows whose stored
    name is a centre this module writes ("AFC Urgent Care ...")."""
    global _KNOWN
    out = {}
    for r in rows or []:
        if (r.get("hospital_system") or "").strip() != AFC or r.get("job_id") in (None, ""):
            continue
        name = clean(r.get("hospital_name"))
        if name.startswith("AFC Urgent Care "):
            out[str(r["job_id"])] = name
    _KNOWN = out
    return len(out)


def load_known_facilities() -> int:
    """One read-only PostgREST pass over American Family Care's active rows
    (id, job_id, hospital_name), keyset-paged on id. Without credentials the
    map stays empty and nothing is kept. Never raises."""
    sb_url = os.environ.get("SUPABASE_URL", "")
    try:
        import sb_keys
        sb_key = sb_keys.supabase_key()
    except Exception:
        sb_key = ""
    if not sb_url or not sb_key:
        set_known_facilities([])
        return 0
    import urllib.parse as _urlparse
    import urllib.request as _urlreq
    rows, last = [], 0
    flt = _urlparse.quote(f"eq.{AFC}", safe=".")
    try:
        while True:
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
            last = chunk[-1]["id"]
            if len(chunk) < KNOWN_PAGE:
                break
    except Exception as e:
        logger.warning(f"Chains facilities: AFC load stopped after {len(rows):,} rows ({e}); "
                       f"the rows past that keep a centre only on nights their text names it")
    try:
        return set_known_facilities(rows)
    except Exception as e:          # never let this read end the run
        logger.warning(f"Chains facilities: stored names not installed ({e})")
        set_known_facilities([])
        return 0


def apply_facility(job) -> str:
    """The centre on one American Family Care Job, before normalize_job.
    Returns what happened ("title", "body", "kept", "none", "two", "other
    town"), or "" for any other system. hospital_system is never changed; a
    name another step already set is left as it is; the town is never
    changed."""
    if (job.hospital_system or "").strip() != AFC:
        return ""
    cur = clean(job.hospital_name)
    if cur and cur != AFC and not cur.startswith("AFC Urgent Care "):
        return "other"
    desc = job.description or ""
    name, how = afc_centre(job.title, desc, job.city, job.state)
    if name:
        job.hospital_name = name
        return how
    if len(desc.strip()) < NO_BODY_CHARS:
        stored = _KNOWN.get(str(job.job_id))
        if stored:
            job.hospital_name = stored
            return "kept"
    job.hospital_name = AFC
    return how


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
    """apply_facility over a run's rows (a row that raises keeps the name it
    came with and counts as "error"). Returns {system: {how: rows}}."""
    stats: dict = {}
    for job in jobs:
        how = _row_guard(apply_facility, job)
        if how:
            d = stats.setdefault(job.hospital_system, {})
            d[how] = d.get(how, 0) + 1
    return stats
