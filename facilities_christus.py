"""CHRISTUS Health's hospitals by the posting's street address (2026-10-30,
facility scale round 2, CHRISTUS group).

The owner, 2026-10-29: every hospital inside a multi-hospital system is named
and placed correctly on the site and in the dashboard. Decisions: (1) cards
and job pages show the HOSPITAL or site ("Apply to <site>"); the system name
and logo stay on the job page and the system hub (hospital_system is never
touched here); (2) named sites (hospitals, named clinics and centres) show
their own name; generic offices, home-based and remote jobs, multi-site pools
and regional roles show the system name with the town; a site or a town is
never guessed. 2026-10-30: "do christus now but push in 2nd round".

CHRISTUS's career site (careers.christushealth.org, the Playwright route in
run_playwright_scrapers) files every posting under a business unit ("bu"),
which the adapter stores as hospital_name. Most units are regional entities
that stand for several of the system's hospitals and clinics: "CHRISTUS Spohn
Health System" (Corpus Christi Shoreline and South, Alice, Beeville,
Kleberg), "CHRISTUS Trinity Mother Frances Health System" (Tyler, the heart
hospital, Jacksonville, Sulphur Springs, Winnsboro and 18 towns of clinics),
"CHRISTUS Santa Rosa Health System" (Westover Hills, New Braunfels, San
Marcos), "CHRISTUS Ochsner Health Southwestern Louisiana" (St. Patrick and
Lake Area, both in Lake Charles) ... About 2,200 active rows on 2026-10-30
(facility_scale_2026-10-29/phase7/variants_ranked.tsv: "fix-later").

The same feed row carries the street address of the posting's work location
("locationList": "600 Elizabeth Street, Corpus Christi, TX  78404"), which
the adapter never stored. The job page prints that location with its name
(<h4>Location</h4><p><strong>CHRISTUS Spohn Hospital Corpus Christi -
Shoreline</strong><br>600 Elizabeth Street<br>Corpus Christi, TX 78404).
SITES below is the reviewed table of the addresses that are one hospital's:

  page    the job page's own Location name for a posting at that address,
          read 2026-10-30 (one page per address; two at a few, which agreed);
          most also match the CMS hospital list's address for that hospital
  cms     the CMS hospital list's address (hospitals.address) for a hospital
          no page was read for; the name is the CMS / registry name
          (system_hospitals), the postings' own spelling where they write one

A row whose business unit is one of the regional entities, the system's own
label or a feed placeholder takes the hospital at its address, in that
address's town. Everything else keeps its name:
  - an address no entry lists: clinics, medical office buildings, outpatient
    and imaging centres, business offices; their pages name "CHRISTUS
    Health" or "CHRISTUS Trinity Clinic", nothing of their own (decision 2:
    the system with its town). A street that only neighbours a hospital
    ("2604 St Michael Drive" next to the Texarkana hospital at 2600) is not
    the hospital.
  - a CHRISTUS Trinity Clinic posting (the medical group), unless the
    entry's page was read for a Trinity Clinic posting and named the
    hospital (the heart hospital at 703 South Fleishel): two postings at one
    street can carry two location names ("CHRISTUS Trinity Clinic" and
    "CHRISTUS Health" at 5802 Saratoga), so the group's postings at a
    hospital street are not assumed to be the hospital's.
  - the Ministry System Office, CHRISTUS Children's, the ambulatory surgery
    centres and every other unit not in ENTITY_LABELS.
  - a title that says a regional or system-level role, a remote or
    multi-site posting, or a system office's function (keeps_label): the
    system with the town, whatever street the posting sits on.
    Review (2026-10-30): also a community programme (AmeriCorps community
    connectors, community health workers, community outreach: their
    postings serve "each of the communities we serve", the members are not
    the hospital's staff), a regional executive ("Vice President, Mission
    Integration": "a member of the regional senior leadership team"), a
    service run for several hospitals ("Director Telemetry Operations": the
    central monitoring unit "across multiple hospitals"; a laboratory
    "Technical Consultant" for the clinic and regional labs) and the system
    office's public relations and talent development roles.
  - a posting the feed lists at two or more streets (one job ID, one feed
    entry per location: "CRNA Louisiana and Texas", a CST posted at the
    Tyler hospital and at Highland in Shreveport; the url of the second copy
    ends "-1", "-2"): a multi-site posting keeps the system with its town,
    and finalize_jobs keeps one copy per job ID, so naming one copy's
    hospital would name whichever copy won (multi_location_ids).
A hospital's own float pool and its SkillBridge placement (the title names
the hospital) are the hospital's.

Every night: the feed carries the address on every row, so the decision is
the same on nights without a body (nothing is stored or re-read). Wired in
run_playwright_scrapers through scraper._fac_hook. Shared with the
stored-row change list (claud_outputinput/facility_scale_2026-10-29/round2/
christus_work/changes_christus.py), so the database holds what the nightly
writes. hospital_system, job_id, url and slug are never touched.
"""
from __future__ import annotations

import re
from typing import NamedTuple

import facilities_town as FT

SYSTEM = "CHRISTUS Health"

# The business units that stand for a group of the system's sites (or for the
# whole system): a posting filed under one takes the hospital at its street.
ENTITY_LABELS = frozenset({
    "CHRISTUS Health",                                   # the system label (and "N/A", phase 7)
    "CHRISTUS Spohn Health System",
    "CHRISTUS Trinity Mother Frances Health System",
    "CHRISTUS St. Vincent Health System",
    "CHRISTUS Santa Rosa Health System",
    "CHRISTUS St. Frances Cabrini Health System",
    "CHRISTUS Good Shepherd Health System",
    "CHRISTUS Shreveport-Bossier Health System",
    "CHRISTUS Southeast Texas Health System",
    "CHRISTUS St. Michael Health System",
    "CHRISTUS Health - Alamogordo",
    "CHRISTUS Ochsner Health Southwestern Louisiana",
})
# The medical group: only at an entry whose page was read for one of its
# postings and named the hospital (clinic_ok).
CLINIC_LABELS = frozenset({"CHRISTUS Trinity Clinic"})
# A feed value that names nothing (facilities_fac7_variants.PLACEHOLDER_RX
# turns it into the system label later in finalize_jobs).
_PLACEHOLDER_RX = re.compile(r"^\s*(?:n\s*/\s*a|n\.a\.?|na|none|null|unknown|tbd|-+)?\s*$", re.I)


def _label(bu) -> str:
    """The business unit as one clean string ("CHRISTUS Children\u2019s" keeps
    its apostrophe; spaces collapsed)."""
    return re.sub(r"\s+", " ", str(bu or "")).strip()


def eligible(bu, clinic_ok=False) -> bool:
    b = _label(bu)
    if b in ENTITY_LABELS or _PLACEHOLDER_RX.match(b):
        return True
    return clinic_ok and b in CLINIC_LABELS


# ── Street addresses ────────────────────────────────────────────────────────
_ABBR = {
    "e": "east", "w": "west", "n": "north", "s": "south", "ne": "northeast", "nw": "northwest",
    "se": "southeast", "sw": "southwest", "st": "street", "str": "street", "dr": "drive", "ave": "avenue",
    "av": "avenue", "blvd": "boulevard", "rd": "road", "hwy": "highway", "ln": "lane", "pkwy": "parkway",
    "ct": "court", "pl": "place", "cir": "circle", "fwy": "freeway", "expy": "expressway",
}
_ADDR_RX = re.compile(r"^\s*(.+?)\s*,\s*([^,]+?)\s*,\s*([A-Za-z]{2})\b\s*(\d{5})?(?:-\d{4})?\s*$")


def street_key(street) -> str:
    """"455 St Michael's Drive" and "455 St Michaels Dr." read alike: lower
    case, no apostrophes or punctuation, the common abbreviations written
    out (applied to both the table and the feed, so "St" may be Saint or
    Street alike)."""
    s = FT.clean_text(street).lower().replace("'", "").replace("\u2019", "")
    return " ".join(_ABBR.get(w, w) for w in re.sub(r"[^a-z0-9]+", " ", s).split())


def parse_address(text):
    """(street, city, ST) from the feed's locationList ("600 Elizabeth Street,
    Corpus Christi, TX  78404"), or None ("N/A", blank, no state)."""
    t = FT.clean_text(str(text or "").replace("\u00a0", " "))
    m = _ADDR_RX.match(t)
    if not m or not re.search(r"\d", m.group(1)):
        return None
    return m.group(1).strip(), m.group(2).strip(), m.group(3).upper()


def address_key(street, city, state) -> tuple:
    return street_key(street), FT.town_key(city), str(state or "").strip().upper()


class Site(NamedTuple):
    name: str
    city: str
    state: str
    streets: tuple          # the street spellings the feed uses for it
    source: str
    clinic_ok: bool = False


def _s(name, city, state, source, *streets, clinic_ok=False):
    return Site(name, city, state, tuple(streets), source, clinic_ok)


SITES = (
    # page: the job page's Location name (2026-10-30); "cms": hospitals.address
    _s("CHRISTUS St. Vincent Regional Medical Center", "Santa Fe", "NM", "page; cms 320002",
       "455 St Michaels Drive"),
    _s("CHRISTUS Mother Frances Hospital - Tyler", "Tyler", "TX", "page; cms 450102",
       "800 East Dawson Street", "800 East Dawson"),
    _s("CHRISTUS Louis and Peaches Owen Heart Hospital", "Tyler", "TX",
       "page (two postings, one of them CHRISTUS Trinity Clinic's)",
       "703 South Fleishel Avenue", "703 South Fleishel", clinic_ok=True),
    _s("CHRISTUS Mother Frances Hospital - Jacksonville", "Jacksonville", "TX", "page; cms 451319",
       "2026 South Jackson", "2026 South Jackson Street"),
    _s("CHRISTUS Mother Frances Hospital - Sulphur Springs", "Sulphur Springs", "TX", "page; cms 450236",
       "115 Airport Road"),
    _s("CHRISTUS Mother Frances Hospital - Winnsboro", "Winnsboro", "TX", "page; cms 451381",
       "719 West Coke Road"),
    _s("CHRISTUS St. Frances Cabrini Hospital", "Alexandria", "LA", "page; cms 190019",
       "3330 Masonic Drive"),
    _s("CHRISTUS Spohn Hospital Corpus Christi - Shoreline", "Corpus Christi", "TX", "page; cms 450046",
       "600 Elizabeth Street"),
    _s("CHRISTUS Spohn Hospital Corpus Christi - South", "Corpus Christi", "TX", "page",
       "5950 Saratoga Boulevard"),
    _s("CHRISTUS Spohn Hospital - Alice", "Alice", "TX", "page; cms 450828",
       "2500 East Main Street"),
    _s("CHRISTUS Spohn Hospital - Beeville", "Beeville", "TX", "page; cms 450082",
       "1500 East Houston Street", "1500 East Houston Highway"),
    # CMS 450163 lists "1311 East General Cavazos Blvd"; the postings at that
    # street write "CHRISTUS Spohn Hospital Kleberg" in their own text
    _s("CHRISTUS Spohn Hospital Kleberg", "Kingsville", "TX", "cms 450163 (address; posting text names it)",
       "1311 General Cavazos Boulevard", "1311 East General Cavazos Boulevard"),
    _s("CHRISTUS Santa Rosa Hospital - Westover Hills", "San Antonio", "TX", "page",
       "11212 State Highway 151"),
    _s("CHRISTUS Santa Rosa Hospital - New Braunfels", "New Braunfels", "TX", "page",
       "600 North Union Avenue"),
    _s("CHRISTUS Santa Rosa Hospital - San Marcos", "San Marcos", "TX", "page; cms 450272",
       "1301 Wonder World Drive"),
    _s("CHRISTUS Highland Medical Center", "Shreveport", "LA", "page; cms 190041",
       "1453 East Bert Kouns Industrial Loop"),
    _s("CHRISTUS Coushatta Health Care Center", "Coushatta", "LA", "cms 191312",
       "1635 Marvel Street"),
    _s("CHRISTUS Southeast Texas St. Elizabeth", "Beaumont", "TX", "page; cms 450034",
       "2830 Calder Avenue"),
    # the postings write "CHRISTUS Southeast Texas - Jasper Memorial"; the CMS
    # and registry name says what it is
    _s("CHRISTUS Jasper Memorial Hospital", "Jasper", "TX", "cms 450573",
       "1275 Marvin Hancock Drive"),
    # the page prints the entity name ("CHRISTUS St. Michael Health System",
    # also CMS 450801's); the postings name the hospital "CHRISTUS St. Michael
    # Hospital--Texarkana" / "CHRISTUS St. Michael Hospital in Texarkana"
    _s("CHRISTUS St. Michael Hospital - Texarkana", "Texarkana", "TX",
       "page names the entity; cms 450801; posting text names the hospital",
       "2600 St Michael Drive"),
    _s("Wadley Regional Medical Center", "Texarkana", "TX", "cms 450200",
       "1000 Pine Street"),
    _s("CHRISTUS Good Shepherd Medical Center - Longview", "Longview", "TX", "page; cms 450032",
       "700 East Marshall Avenue"),
    _s("CHRISTUS Good Shepherd Medical Center - Marshall", "Marshall", "TX", "page",
       "811 South Washington", "811 South Washington Avenue"),
    _s("CHRISTUS Health Mount Pleasant Hospital", "Mount Pleasant", "TX", "page; cms 450080",
       "2001 North Jefferson Avenue", "2001 North Jefferson"),
    _s("CHRISTUS Ochsner St. Patrick Hospital", "Lake Charles", "LA", "page; cms 190027",
       "524 Dr Michael DeBakey Drive"),
    _s("CHRISTUS Ochsner Lake Area Hospital", "Lake Charles", "LA", "page; cms 190201",
       "4200 Nelson Road"),
    _s("Savoy Medical Center", "Mamou", "LA", "cms 190025",
       "801 Poinciana Avenue"),
    # the page prints "CHRISTUS Health"; CMS 320004 lists the hospital at the
    # street, and the postings name it "CHRISTUS Southern New Mexico in
    # Alamogordo is a 100-bed ..."
    _s("CHRISTUS Southern New Mexico", "Alamogordo", "NM", "cms 320004; posting text names it",
       "2669 Scenic Drive"),
)

BY_ADDRESS: dict = {}
for _site in SITES:
    for _st in _site.streets:
        BY_ADDRESS[address_key(_st, _site.city, _site.state)] = _site
NAMES = frozenset(s.name for s in SITES)


def site_at(location_list):
    """The SITES entry at the feed's street address, or None."""
    a = parse_address(location_list)
    if a is None:
        return None
    return BY_ADDRESS.get(address_key(*a))


# ── What keeps the label, by the title (decision 2) ─────────────────────────
# A regional or system-level role ("Regional Director Supply Chain",
# "Radiology Technologist - Regional Outpt Tyler", "System Director, Managed
# Care") is no one hospital's, whatever street it sits on.
REGIONAL_RX = re.compile(
    r"\bregional\b|\bregions?\b|\bministry\b|\bcorporate\b|\benterprise\b|\bhealth\s+plans?\b|"
    r"\bmanaged\s+care\b|\bsystem\s+(?:director|manager|vice\s+president|vp|executive|administrator|"
    r"lead|coordinator|specialist|chief)\b|"
    # review 2026-10-30: the regional entity's vice presidents, the central
    # telemetry monitoring unit, the laboratory technical consultant
    r"\bvice\s+president\b|\btelemetry\s+operations\b|\bcentral(?:ized)?\s+(?:telemetry|monitoring)\b|"
    r"\btechnical\s+consultant\b", re.I)
# A system office's function (IT, coding, billing, revenue cycle, finance, HR,
# legal, compliance, marketing, public relations, the foundation, a transfer
# or call centre).
OFFICE_RX = re.compile(
    r"\b(?:application\s+system\s+analyst|business\s+system\s+analyst|systems?\s+analyst|coders?|coding|"
    r"billing|revenue\s+cycle|information\s+(?:services|technology|systems)|payroll|"
    r"accounts\s+(?:payable|receivable)|accounting|accountant|finance|financial\s+analyst|"
    r"human\s+resources|recruiter|talent\s+acquisition|talent\s+development|legal|counsel|compliance|privacy|"
    r"marketing|public\s+relations|"
    r"foundation|philanthrop\w*|transfer\s+center|call\s+cent(?:er|re)|contact\s+cent(?:er|re))\b", re.I)
# A community programme (review 2026-10-30): AmeriCorps community connectors
# ("Members ... will not be considered employees of CHRISTUS Health"),
# community health workers, community outreach and community benefit.
COMMUNITY_RX = re.compile(
    r"\bamericorps\b|\bcommunity\s+(?:health\s+(?:workers?|development)|outreach|connectors?|benefit)\b", re.I)


def keeps_label(title) -> str:
    """Why a posting's title makes it no one hospital's, or ""."""
    t = FT.clean_text(title)
    if FT.REMOTE_RX.search(t):
        return "remote"
    if FT.MULTI_SITE_RX.search(t):
        return "multi-site"
    if FT.SYSTEM_ROLE_RX.search(t) or REGIONAL_RX.search(t):
        return "regional or system role"
    if OFFICE_RX.search(t):
        return "system office function"
    if COMMUNITY_RX.search(t):
        return "community program"
    return ""


def _entry_key(entry):
    a = parse_address((entry or {}).get("locationList"))
    return address_key(*a) if a else ("?", FT.clean_text((entry or {}).get("locationList")).lower())


def multi_location_ids(entries) -> frozenset:
    """The job IDs the feed lists at two or more streets tonight (one entry
    per location, one job ID). Never raises; a malformed entry is skipped."""
    seen: dict = {}
    for e in entries or []:
        try:
            if not isinstance(e, dict):
                continue
            jid = str(e.get("jobId") or e.get("id") or "").strip()
            if jid:
                seen.setdefault(jid, set()).add(_entry_key(e))
        except Exception:
            continue
    return frozenset(j for j, keys in seen.items() if len(keys) > 1)


def decide(bu, location_list, title, multi=False):
    """(site, how) for one CHRISTUS posting: site is the SITES entry its
    business unit, street and title give it, else None; how says why
    ("street", "no-address", "other-street", "clinic-group", "other-unit",
    "multi-location", "title: <reason>"). multi: the feed lists the posting's
    job ID at two or more streets tonight."""
    b = _label(bu)
    if not (b in ENTITY_LABELS or b in CLINIC_LABELS or _PLACEHOLDER_RX.match(b)):
        return None, "other-unit"
    a = parse_address(location_list)
    if a is None:
        return None, "no-address"
    site = BY_ADDRESS.get(address_key(*a))
    if site is None:
        return None, "other-street"
    if not eligible(b, site.clinic_ok):
        return None, "clinic-group"
    if multi:
        return None, "multi-location"
    why = keeps_label(title)
    if why:
        return None, "title: " + why
    return site, "street"


def feed_site(bu, location_list, title, multi=False):
    """The adapter hook: (hospital_name, city, state) for a feed row whose
    street is one hospital's, else None (the row keeps its business unit and
    town). Counts each decision in STATS."""
    site, how = decide(bu, location_list, title, multi)
    note(how)
    if site is None:
        return None
    return site.name, site.city, site.state


STATS: dict = {}


def note(how) -> None:
    key = how.split(":")[0]
    STATS[key] = STATS.get(key, 0) + 1


def stats_summary() -> str:
    return ", ".join(f"{k} {v:,}" for k, v in sorted(STATS.items(), key=lambda kv: -kv[1]))


# ═══════════════════════ the stored-row change list ═════════════════════════
def stored_change(row, location_list, multi=False):
    """(new_hospital_name, new_city, new_state, source) for one stored
    CHRISTUS row and the street its feed row carries tonight, or None. The
    same decision the nightly makes (decide)."""
    if (row.get("hospital_system") or "").strip() != SYSTEM:
        return None
    site, how = decide(row.get("hospital_name"), location_list, row.get("title") or "", multi)
    if site is None:
        return None
    if site.name == FT.clean_text(row.get("hospital_name")):
        return None
    a = parse_address(location_list)
    return site.name, site.city, site.state, f"feed street '{a[0]}, {a[1]}, {a[2]}' -> {site.name} ({site.source})"
