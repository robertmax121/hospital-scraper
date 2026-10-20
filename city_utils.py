# city_utils.py
# Server-side city sanitization for the scraper write path.
#
# ATS feeds routinely dump non-city junk into the location/city field:
#   - "2 Locations" / "Multiple Locations"  (multi-site Workday reqs)
#   - "Remote" / "Work From Home" / "Nationwide"
#   - street addresses            ("Atrium Health Mercy - 2001 Vail Ave")
#   - pipe-delimited facility rows ("Saint Luke's Hospital | 4401 Wornall Rd | Kansas City | MO")
#   - newline facility blocks      ("Location\nMedical Center Alpena\nAlpena")
#   - bare facility names          ("Akron General Medical Center")
#
# clean_city() first tries to EXTRACT the real city out of the known embedded
# patterns (pipe, newline, "Facility - City"), then validates the result. If it
# still isn't a plausible city name, it returns "" so normalize_job()'s
# FACILITY_LOCATION_MAP / SYSTEM_LOCATION_DEFAULTS fallback can fill it from the
# facility/system name instead.
#
# Mirrors the front-end guard in waypoint-jobs/lib/clean-city.js, but adds
# extraction (the front-end only needs to hide junk from dropdowns; here we want
# to recover the real value before it's stored). Validated against all distinct
# live city values (see _validate_city_clean.py).

import re

# 2-char USPS codes (+ territories) — used to locate the state segment when
# extracting a city from pipe/comma-delimited facility strings.
STATE_CODES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID",
    "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
    "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK",
    "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
    "WI", "WY", "DC", "PR", "GU", "VI",
}

# Facility / non-place phrases that never occur in a real US city name. Matched
# as whole words/phrases so real cities survive: "Center" -> Centerville,
# "Health" -> Healdsburg, "Mansfield Center" (a real CT town) are all kept
# because none of them contain one of these multi-word phrases (or the
# standalone "hospital"/"clinic" tokens, which no US city name contains).
_FACILITY_RE = re.compile(
    r"\b("
    r"medical center|medical ctr|regional medical|memorial hospital|"
    r"care center|care ctr|health center|health system|health services|"
    r"hospital|clinic|outpatient|surgery center|cancer center|"
    r"main campus|campus|all locations|flexible work|corporate"
    r")\b",
    re.IGNORECASE,
)
_LOCATIONS_RE = re.compile(r"\b(multiple|various|two|three|four|several)\s+locations?\b", re.IGNORECASE)
_LOCATION_PREFIX_RE = re.compile(r"^locations?\b", re.IGNORECASE)
_REMOTE_RE = re.compile(r"\b(remote|work\s*from\s*home|virtual|nationwide|statewide|telework|telecommute)\b", re.IGNORECASE)

# ── Not a place (2026-10-20, owner: "fix the messy locations") ─────────────
# What the city column held on 2026-10-20 that names no town, by class
# (active rows):
#   a worker type        "Regular" 888 and "Casual" 140 (OhioHealth: the first
#                        bulletField), "Provisional (Fixed Term)"
#   a building / office  "Memorial Executive Office Building", "Olathe Medical
#                        Pavilion A", "Digital Innovation Office", "Tower Bldg",
#                        "Offices", "Suite A", "Unit B" (722 rows by the
#                        site's realCity rule, lib/job-geo.js)
#   a facility / service "Peak View Behavioral Health", "Twin Lakes Recovery
#                        Center" (Summit BHC), "Brown Health Medical Group",
#                        "Business Operations Center", "Wellstar Pediatrics"
#   a corporate suffix   "Inc." (Wellstar's "AU Medical Center, Inc.", 72)
#   parse debris         "NY)" (151), "NYP/Hudson Valley", "Place" (the tail
#                        of "1800 Parkway Place")
# Real towns that share these words stay: a building word counts only in a
# value of two or more words ("Pavilion, NY"; "Campus, IL" is refused by the
# older "campus" rule as before), "center" / "centre" only when the value is
# no known town ("Rockville Centre", "Sun City Center", "Sioux Center",
# "Brooklyn Center" are towns; "Business Center" is not), and "medical" only
# in a facility phrase ("Medical Lake", WA).
_WORKER_TYPE_RE = re.compile(
    r"^(?:regular|casual|temporary|temp|provisional|seasonal|contingent|contractor|contract|"
    r"per\s*diem|prn|on\s*call|full[\s-]*time|part[\s-]*time|fixed\s*term|flex|float|pool|"
    r"employee|staff|intern|student)"
    r"(?:\s*\((?:fixed\s*term|seasonal|temporary)\)(?:\s*\([^)]*\))?)?$", re.I)
_CORP_SUFFIX_RE = re.compile(r"^(?:inc|incorporated|llc|l\.l\.c|corp|corporation|co|company|ltd|pc|p\.c|pllc|lp|llp|pa|plc)\.?$", re.I)
_NON_PLACE_WHOLE_RE = re.compile(
    r"^(?:place|office|offices|system\s+office|corporate\s+office|home\s+office|main\s+office|"
    r"tbd|tba|n/?a|none|null|unknown|other|various|varies|any|anywhere|home|field|travel|traveling|"
    r"united\s+states(?:\s+of\s+america)?|usa|us|headquarters|hq|onsite|on\s*site|hybrid|"
    r"work\s*at\s*home|varied\s+locations?|default\s+locations?)$", re.I)
# Two or more words with one of these: a building, an office, a unit.
_BUILDING_WORD_RE = re.compile(
    r"\b(?:offices?|building|bldg|headquarters|administrative|administration|pavilion|suite|"
    r"department|dept|unit|location|recovery|region|institute)\b", re.I)
# Facility, service and department phrases no US town name contains
# ("medicine" only at the end or before a practice word: Medicine Lodge, KS
# and Medicine Park, OK are towns).
_NON_PLACE_RE = re.compile(
    r"\b(?:health|healthcare|behavioral|psychiatric|medicine$|"
    r"medicine\s+(?:associates|clinic|clinics|group|specialists|center|department)|medical\s+(?:office|offices|arts|mall|plaza|"
    r"building|bldg|park|group|associates|specialists|clinic|pavilion|center|centre|campus|tower|"
    r"research|education|staff)|"
    r"hospice|homecare|home\s+care|home\s+health|ambulance|urgent\s+care|primary\s+care|"
    r"family\s+(?:medicine|practice|physicians|health)|internal\s+medicine|pediatrics?|pediatric|"
    r"cardiology|cardiovascular|neurology|neurosurgery|gastroenterology|oncology|orthopa?edics?|"
    r"ob\s*/?\s*gyn|obgyn|obstetrics|gynecology|urology|dermatology|radiology|imaging|laboratory|"
    r"rehab|rehabilitation|physical\s+therapy|therapy|therapies|pharmacy|dialysis|infusion|"
    r"physicians?|associates|specialists|surgical|surgery|institute\s+(?:of|for)|"
    r"wellness|treatment|crisis|triage|simulation|supply\s+chain|support\s+services|"
    r"patient\s+care|care\s+services|services\s+center|service\s+center|data\s+center|"
    r"business\s+(?:center|centre|operations)|operations\s+center|technology\s+center|"
    r"facilities|learning\s+center|athletic\s+center|resource\s+center|quality\s+alliance|"
    r"health\s+park|office\s+park|research\s+park|business\s+park|mgmt|management|"
    r"mobile\s+unit|medical\s+group|ambulatory|outpatient|inpatient|emergency|"
    r"college\s+of|school\s+of|university\s+of)\b", re.I)
# "<Words> Center" / "<Words> Centre" / "... Ctr": a town only when known.
_CENTER_END_RE = re.compile(r"\b(?:center|centre|ctr)\.?$", re.I)
# Words a town name never carries beside other words: " at " ("The Ranch at
# Dove Tree", "MUSC Health at Tidelands") and a leading "The" before a
# facility word.
_AT_RE = re.compile(r"\s+at\s+", re.I)


def town_key(city: str) -> str:
    """Spelling-blind key for a town (scraper._wd_city_key's rule): lower
    case, accents dropped, Saint/Mount/Fort folded to St/Mt/Ft, no
    punctuation or spaces ("St. Louis" == "Saint Louis" == "ST LOUIS")."""
    import unicodedata
    s = "".join(ch for ch in unicodedata.normalize("NFKD", (city or "").lower())
                if not unicodedata.combining(ch))
    w = re.sub(r"[^a-z0-9 ]+", " ", s.replace("'", "").replace("’", "")).split()
    fold = {"saint": "st", "mount": "mt", "fort": "ft"}
    return "".join(fold.get(x, x) for x in w)


# ── Known towns (2026-10-20) ────────────────────────────────────────────────
# known_towns.json: {"ST": {town_key: spelling}} for every town a CMS hospital
# sits in, every town two or more health systems post jobs in, and every town
# two or more travel agencies list (built by tools/build_known_towns.py from
# the 2026-10-20 snapshot; rebuild with the CMS refresh). It decides the two
# questions a word list cannot: is "Sioux Center" a town (yes) and is
# "Business Center" one (no), and which town a building string names
# ("Bridgeport Medical Office Building", WV -> Bridgeport). The run's own
# CMS read (scraper.set_cms_lookup) adds its towns with set_runtime_towns,
# which replaces the previous run-time set (a test's CMS sample never leaks).
_KNOWN_TOWNS: dict | None = None
_KNOWN_ANY: set | None = None
_RUNTIME_TOWNS: dict = {}


def _load_known_towns() -> dict:
    global _KNOWN_TOWNS, _KNOWN_ANY
    if _KNOWN_TOWNS is None:
        import json
        import os
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "known_towns.json")
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            _KNOWN_TOWNS = {st: dict(m) for st, m in (data.get("towns") or {}).items()}
        except (OSError, ValueError):
            _KNOWN_TOWNS = {}
        _KNOWN_ANY = {k for m in _KNOWN_TOWNS.values() for k in m}
    return _KNOWN_TOWNS


def set_runtime_towns(pairs) -> int:
    """Replace the run-time towns with these (city, state) pairs (the run's
    CMS hospitals). Plausible names only; returns how many are held."""
    global _RUNTIME_TOWNS
    out: dict = {}
    for city, st in pairs or ():
        st = (st or "").strip().upper()
        city = re.sub(r"\s+", " ", (city or "")).strip()
        if st not in STATE_CODES or not city:
            continue
        k = town_key(city)
        if not k or not is_plausible_city(city, known=False):
            continue
        out.setdefault(st, {}).setdefault(k, city.title() if city.isupper() else city)
    _RUNTIME_TOWNS = out
    return sum(len(m) for m in out.values())


def runtime_towns() -> list:
    """The run-time (city, state) pairs, for a worker process to repeat."""
    return [(c, st) for st, m in _RUNTIME_TOWNS.items() for c in m.values()]


def known_town(city: str, state: str | None = None) -> str | None:
    """The known spelling of `city` in `state`, or None; with no state, any
    state's spelling (used only to tell a town from a facility name)."""
    towns = _load_known_towns()
    k = town_key(city)
    if not k:
        return None
    if state:
        st = state.strip().upper()
        return towns.get(st, {}).get(k) or _RUNTIME_TOWNS.get(st, {}).get(k)
    if k in _KNOWN_ANY:
        for m in towns.values():
            if k in m:
                return m[k]
    for m in _RUNTIME_TOWNS.values():
        if k in m:
            return m[k]
    return None


def not_a_place(c: str, known: bool = True) -> str | None:
    """Why `c` names no town, or None. The 2026-10-20 classes above.
    known=False skips the known-town test of "... Center" names (the town
    list's own build, tools/build_known_towns.py, settles those)."""
    c = re.sub(r"\s+", " ", (c or "")).strip()
    if not c:
        return "blank"
    words = c.split(" ")
    if _WORKER_TYPE_RE.match(c):
        return "worker type"
    if _CORP_SUFFIX_RE.match(c):
        return "corporate suffix"
    if _NON_PLACE_WHOLE_RE.match(c):
        return "not a place"
    # "Raleigh/Durham", "Orlando/Jacksonville": two known towns stay as
    # written; any other slash ("NYP/Hudson Valley") is a site code
    if "/" in c and not all(p.strip() and known_town(p.strip()) for p in c.split("/")):
        return "parse debris"
    if c.count("(") != c.count(")") or ";" in c or ":" in c or "@" in c or "#" in c:
        return "parse debris"
    if len(words) >= 2 and _BUILDING_WORD_RE.search(c):
        return "building"
    if _NON_PLACE_RE.search(c):
        return "facility"
    if len(words) >= 3 and _AT_RE.search(c):
        return "facility"
    if known and len(words) >= 2 and _CENTER_END_RE.search(c) and not known_town(c):
        return "facility"
    return None


def is_plausible_city(c: str, known: bool = True, legacy: bool = False) -> bool:
    """True only if `c` looks like a real US city name (no extraction).
    legacy=True applies only the rules before 2026-10-20 (normalize_job tells
    a value the new rules refuse from one that was always refused)."""
    if not c:
        return False
    c = c.strip()
    if len(c) < 2 or len(c) > 40:
        return False
    if c.upper() in STATE_CODES:                # bare "MO"/"KY" is a state, not a city
        return False
    if any(ch.isdigit() for ch in c):          # street addresses, "N Locations"
        return False
    if "|" in c or "\n" in c or "\r" in c or "\t" in c:
        return False
    if _LOCATION_PREFIX_RE.match(c):            # "Location...", "Locations..."
        return False
    if _LOCATIONS_RE.search(c):                 # "Multiple Locations"
        return False
    if _REMOTE_RE.search(c):                    # "Remote", "Work From Home"
        return False
    if _FACILITY_RE.search(c):                  # "... Medical Center", "... Hospital"
        return False
    if not legacy and not_a_place(c, known):    # 2026-10-20: buildings, worker types, facilities
        return False
    return True


# A town inside a building or facility string, when the string names one
# known town in the row's state at its start or its end and the rest of it
# is the building or facility ("Bridgeport Medical Office Building", WV ->
# Bridgeport; "Sentara Office Building Williamsburg", VA -> Williamsburg;
# "Olathe Medical Pavilion A", KS -> Olathe; "Knoxville (Middlebrook Pike)",
# TN -> Knoxville). Nothing else is guessed: "Sentara Independence" names no
# building beside "Independence" (a VA town Sentara has no site in), so it
# gives nothing; the rest of the string must itself be no place.
# One-word towns that are also the facility words around them (Center, TX;
# Pavilion, NY; Campus, IL): never read out of a site name.
_GENERIC_TOWN_WORDS = frozenset({
    "center", "centre", "ctr", "pavilion", "campus", "plaza", "tower", "institute", "office", "offices",
    "building", "bldg", "park", "place", "point", "square", "commons", "village", "crossing", "crossings",
    "hall", "court", "station", "mall", "health", "medical", "clinic", "hospital", "home", "west", "east",
    "north", "south", "main", "central"})
_TRAILING_TAG_RE = re.compile(r"\s+(?:[A-Z]{1,2}|I{1,3}|IV|V|VI{0,3}|\d{1,3})$")


# Words a site, building or service name is made of (2026-10-20). A refused
# site string whose every word is one of these, a word of the system's own
# name, or a short code names no other place, so the system's home town is
# safe for it; a string with any other word ("Stow Health and Wellness",
# "Sentara Therapy Center Quinton") may name a town the town list lacks, and
# keeps no guessed town (site_leftover).
SITE_WORDS = frozenset("""
executive support services service digital innovation administrative administration operations business
technology research science sciences basic main central east west north south upper lower primary specialty
spec professional physicians physician doctors doctor medical med health healthcare care center centre ctr
office offices building bldg pavilion plaza park institute clinic clinics group associates partners home
community family women womens children childrens pediatric pediatrics adult senior learning training education
wellness fitness therapy rehab rehabilitation sports spine pain heart vascular cancer imaging laboratory lab
pharmacy supply chain distribution logistics data network hub courier patient accounts financial accounting
talent acquisition human resources simulation consolidated system systems corporate headquarters ambulance ems
emergency department urgent express campus tower suite floor unit wing annex mob fsed the of and at for in on
mental behavioral psychiatric psychiatry addiction recovery hospice palliative surgery surgical orthopedic
orthopedics orthopaedic orthopaedics neurology neurosurgery cardiology cardiovascular gastroenterology oncology
urology obgyn obstetrics gynecology dermatology radiology internal medicine practice sleep endoscopy infusion
dialysis respiratory occupational occ physical speech nursing living assisted residence location locations
facilities facility management mgmt quality alliance employee child development rec athletic
regular casual temporary provisional fixed term inc llc place region regional general specialists consultants
pulmonology pulmonary allergy infectious diseases endocrinology diabetes nephrology kidney rheumatology
hematology breast wound bariatric gme residency program provider providers float pool resource detention
county ob gyn peds hospitalist hospitalists anesthesia anesthesiology commons square hall court
academic advanced digestive ear nose throat ent essentials lung southwest northwest northeast southeast
""".split())


# A value that says the job has no one place ("Varied Location", "USA",
# "Hybrid"): never a home-office town either.
_NO_ONE_PLACE_RE = re.compile(
    r"^(?:various|varies|varied(?:\s+locations?)?|multiple(?:\s+locations?)?|default\s+location|any|anywhere|"
    r"us|usa|u\.s\.a?\.?|united\s+states(?:\s+of\s+america)?|nationwide|statewide|tbd|tba|n/?a|none|null|"
    r"unknown|other|remote|virtual|hybrid|telehealth|work\s*(?:from|at)\s*home)$", re.I)


def site_leftover(raw: str, system: str | None = None) -> list:
    """The words of a refused site string that are no site word, no word of
    the system's name and no short code: a town the town list may lack. A
    value that names no one place ("Varied Location", "USA") returns itself."""
    if _NO_ONE_PLACE_RE.match(re.sub(r"\s+", " ", (raw or "")).strip()):
        return [raw.strip()]
    sysw = {w.lower() for w in re.findall(r"[A-Za-z]+", system or "")}
    out = []
    for w in re.findall(r"[A-Za-z][A-Za-z'.]*", raw or ""):
        lw = re.sub(r"'s$|[.']", "", w.lower())
        if lw in SITE_WORDS or lw in sysw or len(lw) <= 2 or (w.isupper() and len(w) <= 5):
            continue
        out.append(w)
    return out


def town_window(raw: str, state: str, system: str | None = None) -> str:
    """A known town of `state` anywhere in a site string ("MUSC Health
    Summerville FSED" -> Summerville) when every other word is a site word,
    a word of the system's name or a short code; the longest such window,
    '' when there is none or two different ones."""
    st = (state or "").strip().upper()
    text = re.sub(r"[()\[\],;/]|\s-\s|-\s|\s-", " ", raw or "")
    words = re.sub(r"\s+", " ", text).strip().split(" ")
    if st not in STATE_CODES or not words or not words[0]:
        return ""
    found = set()
    for n in (3, 2, 1):
        for i in range(0, len(words) - n + 1):
            win = words[i:i + n]
            if n == 1 and win[0].lower().strip(".,") in _GENERIC_TOWN_WORDS:
                continue
            town = known_town(" ".join(win), st)
            if not town:
                continue
            rest = " ".join(words[:i] + words[i + n:])
            if not site_leftover(rest, system):
                found.add(town)
        if found:
            break
    return next(iter(found)) if len(found) == 1 else ""


def embedded_town(raw: str, state: str) -> str:
    st = (state or "").strip().upper()
    s = re.sub(r"\s+", " ", (raw or "")).strip().strip(",").strip()
    if not s or st not in STATE_CODES:
        return ""
    # "Knoxville (Middlebrook Pike)": the head before a bracket
    m = re.match(r"^([^()]+?)\s*\(([^()]*)\)?\s*$", s)
    if m:
        head = m.group(1).strip()
        town = known_town(head, st)
        if town:
            return town
        s = head
    s = _TRAILING_TAG_RE.sub("", s).strip()     # "Olathe Medical Pavilion A" -> "... Pavilion"
    words = s.split(" ")
    if len(words) < 2:
        return ""
    for n in (3, 2, 1):
        if len(words) <= n:
            continue
        for town_words, rest_words in ((words[:n], words[n:]), (words[-n:], words[:-n])):
            if n == 1 and town_words[0].lower().strip(".,") in _GENERIC_TOWN_WORDS:
                continue    # "Great Oaks Recovery Center" names no town "Center", TX
            town = known_town(" ".join(town_words), st)
            if not town:
                continue
            rest = " ".join(rest_words)
            if not_a_place(rest) or _FACILITY_RE.search(rest) or _CENTER_END_RE.search(rest):
                return town
    return ""


def _pick_from_segments(segs, legacy: bool = False) -> str:
    """From delimited facility segments, return the real city or ''.
    Facility strings are ordered Facility [| Addr] | City [| ST], so the city
    is either the segment right before a trailing state code, or the FINAL
    segment. We never fall back to an earlier segment: those hold the facility
    name, and returning one just writes fresh junk ("Atrium Health Cabarrus -
    920 Church St N" must NOT yield "Atrium Health Cabarrus")."""
    segs = [s.strip() for s in segs if s and s.strip()]
    if not segs:
        return ""
    # Segment immediately before a trailing 2-char state code (most reliable).
    for i, s in enumerate(segs):
        if s.upper() in STATE_CODES and i > 0 and is_plausible_city(segs[i - 1], legacy=legacy):
            return segs[i - 1]
    # Otherwise only the final segment can be the city.
    return segs[-1] if is_plausible_city(segs[-1], legacy=legacy) else ""


def clean_city(raw: str, legacy: bool = False) -> str:
    """Return a plausible city name, extracting it from embedded facility/address
    strings where possible; '' when no usable city can be recovered.
    legacy=True: the rules before 2026-10-20 (see is_plausible_city)."""
    if not raw:
        return ""
    c = str(raw).strip()

    # Pipe-delimited: "Facility | Addr | City | ST"
    if "|" in c:
        return _pick_from_segments(c.split("|"))

    # Newline facility blocks: "Location\nMedical Center Alpena\nAlpena"
    # (the real city is typically the last line).
    if "\n" in c or "\r" in c:
        return _pick_from_segments(re.split(r"[\n\r]+", c))

    # 2026-10-20: "Knoxville (Middlebrook Pike)", "Huntsville (Cochran
    # Center)": a known town before a bracketed site is the town.
    m = re.match(r"^([^()]+?)\s*\([^()]*\)\s*$", c)
    if m and not legacy and known_town(m.group(1).strip()):
        return m.group(1).strip()

    # Already clean — the common case, checked before the dash split so cities
    # that legitimately contain " - " (none known, but be safe) aren't mangled.
    if is_plausible_city(c, legacy=legacy):
        return c

    # "Facility - City" / "City - Facility": recover the plausible side.
    if " - " in c:
        return _pick_from_segments(c.split(" - "), legacy)

    return ""
