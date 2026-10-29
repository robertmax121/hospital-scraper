"""The site a posting of an ambulatory, retail or surgery chain works at
(2026-10-29, facility scale phase 3, the ambulatory group).

The owner, 2026-10-29: every site inside a multi-site employer is named and
placed correctly ("i thought you were gonna edit all the large
multihospital systems"). His decisions: (1) cards and job pages show the
SITE ("Apply to <site>"), the system name and logo stay on the job page and
the system hub; (2) named sites (hospitals, named clinics and centers, an
agency's named branch) show their own name, generic offices, home-based and
remote jobs show the system name with the town; never guess a town.

Each chain's feed stamps every posting with the system label. Where each one
names the site:

  United Surgical Partners International (USPI, TalentBrew company 35934)
      The job page's "Facility" field: <span class="job-companylocation">
      "Winter Haven Ambulatory Surgical Center (10622)", and on the other page
      template the same value in the <html data-facet-id> attribute
      ("custom_fields.CompanyLocation-Arizona-Specialty-Hospital-(11101)").
      The list cards carry no facility. The trailing code is the facility's
      own number: USPI_TABLE (facilities_ambulatory_uspi.json, the board's
      Facility facet on 2026-10-29, 428 values) gives its clean name (ten
      internal shorthand values, "Mills Park", "Kirkland Endo", carry the
      public name the postings use: "AdventHealth Surgery Center Mills
      Park"); a code the table does not know is cleaned the same way (code
      and ", LLC" dropped, "Ctr" and a closing "SC" spelled out, an
      all-capitals value in title case). Corporate and regional offices, float pools, "Remote -
      Countrywide" and MSO entities keep the system name (decision 2).
      Read by the TalentBrew detail pass (scraper._jsonld_detail); stored
      rows still on the label are read once a cycle for it, and a stored
      name is carried on nights the page is not read (the scraper's stored
      company map, PAGE_FIELD_FACILITY).
  Concentra (Sitecore SXA job search)
      Every list result carries the job's map point (Geospatial latitude,
      longitude); the board names no centre. CONCENTRA_CENTERS
      (facilities_ambulatory_concentra.json: the 639 centres of
      concentra.com/urgent-care-centers on 2026-10-29, name, town, state,
      map point) gives the centre at that point: "Concentra <centre name>"
      when the nearest centre is in the posting's state, within 250 m (a
      centre whose town is spelled like the posting's own) or 150 m (a
      neighbouring town's spelling: Tanasbourne in Beaverton for a Hillsboro
      posting), and no second centre is about as near. Onsite postings (a
      client's site: athletic trainers, EMTs, paramedics, "Onsite" titles),
      corporate and remote postings are not at a centre point and keep
      "Concentra". The posting's town is never changed.
  CVS Health (Workday)
      The posting's own text names the business: "Company: Oak Street
      Health" (primary care centres), "MinuteClinic" (retail clinics),
      "Omnicare" (long-term-care pharmacies), read from the opening of the
      body after CVS's shared opening paragraph. Signify Health in-home
      visits, Aetna case management, virtual care, remote and field roles,
      and Coram home infusion nurses keep "CVS Health" (decision 2). A row
      whose Workday location gave no state (an Oak Street centre's name,
      "Roosevelt", "Southeast Raleigh") takes the town of the posting's own
      address line ("Location: 2311 Cottman Ave #71, Philadelphia, PA
      19149"); a row with a town keeps it. On nights a row's body is not
      read again (scraper._keep_stored_body), the site and town stored on
      the row are kept (load_known_facilities).

Select Medical stays on its own iCIMS Company-field re-read
(scraper.ICIMS_COMPANY_FACILITY, live since 10-27).

hospital_system is never touched (the upsert key, the hub, the filters, the
logo). Shared with the stored-row change list
(claud_outputinput/facility_scale_2026-10-29/phase3/ambulatory/make_changes.py),
so the database holds what the scraper keeps.
"""
from __future__ import annotations

import html as _html
import json
import logging
import math
import os
import re
import time

logger = logging.getLogger("scraper.facility_ambulatory")

SYSTEM_CVS = "CVS Health"
SYSTEM_USPI = "United Surgical Partners International"
SYSTEM_CONCENTRA = "Concentra"
SYSTEMS = frozenset({SYSTEM_CVS, SYSTEM_USPI, SYSTEM_CONCENTRA})
_HERE = os.path.dirname(os.path.abspath(__file__))

US_STATES = frozenset(
    "AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH NJ NM NY "
    "NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY PR".split())


def _clean(s) -> str:
    return re.sub(r"\s+", " ", _html.unescape(str(s or ""))).strip()


def _fold(s) -> str:
    """Comparison key: lower case, "&" = "and", "Saint" = "St", punctuation
    dropped, "Surgery"/"Surgical" = "surg", "Ctr"/"Centre" = "center", and
    "the", "of", "at", "llc" left out."""
    s = _clean(s).lower().replace("&", " and ")
    s = re.sub(r"\bsaint\b", "st", s)
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    s = re.sub(r"\bsurg(?:ery|ical)\b", "surg", s)
    s = re.sub(r"\b(?:ctr|cent(?:er|re)s?)\b", "center", s)
    s = re.sub(r"\b(?:the|of|at|llc|inc|lp)\b", " ", s)
    return " ".join(s.split())


# ══ USPI ════════════════════════════════════════════════════════════════════
TB_FACILITY_SYSTEMS = frozenset({SYSTEM_USPI})
USPI_FILE = os.path.join(_HERE, "facilities_ambulatory_uspi.json")
try:
    with open(USPI_FILE, encoding="utf-8") as _f:
        USPI_TABLE: dict = {str(k).upper(): v for k, v in json.load(_f).items()}
except FileNotFoundError:          # the table builder imports this module before the file exists
    USPI_TABLE = {}

_CODE_RX = re.compile(r"\(\s*([A-Za-z0-9-]+)\s*\)\s*$")
_LEGAL_RX = re.compile(r"(?:,\s*|\s+)(?:LLC|L\.L\.C\.?|LP|L\.P\.?|Inc\.?|PLLC|Ltd\.?)\s*$", re.I)
# Not a site (decision 2): a corporate or regional office, a float pool, a
# remote posting, a management services organisation, a central support
# office ("Dallas CSO - Hospital").
_OFFICE_RX = re.compile(r"\b(?:corporate|regional office|float pool|remote|countrywide|MSO|CSO)\b", re.I)
_SPELLING = ((re.compile(r"\bCtr\b\.?"), "Center"), (re.compile(r"\bSurger\b"), "Surgery"),
             (re.compile(r"\bSC$"), "Surgery Center"))      # "Briarcliff Ambulatory SC"
_SMALL = frozenset({"of", "at", "and", "the", "for", "in", "on", "a"})


def uspi_code(raw) -> str:
    """The facility number at the end of a Facility value ("(10622)",
    "(13033-01)", "(RMT)"), upper case; "" when there is none."""
    m = _CODE_RX.search(_clean(raw))
    return m.group(1).upper() if m else ""


def _title_case(s: str) -> str:
    out = []
    for i, w in enumerate(s.lower().split()):
        out.append(w if i and w in _SMALL else w[:1].upper() + w[1:])
    return " ".join(out)


def uspi_clean(raw, use_table: bool = True) -> str:
    """A USPI Facility value -> the site's clean name, or "" (an office, a
    float pool, a remote posting, or no value: the row keeps the system
    name). A code USPI_TABLE knows gives the table's name."""
    s = _clean(raw)
    if not s:
        return ""
    code = uspi_code(s)
    if use_table and code and code in USPI_TABLE:
        return USPI_TABLE[code] or ""
    s = _CODE_RX.sub("", s).strip()
    if _OFFICE_RX.search(s):
        return ""
    s = re.sub(r"\s*\([^()]*\)\s*$", "", s)                  # an acronym after the name: "(TCCFS)"
    s = re.sub(r",\s*an affiliate of\b.*$", "", s, flags=re.I)  # "..., an affiliate of Midland Memorial Hospital"
    for _ in range(2):
        s = _LEGAL_RX.sub("", s).strip(" ,")
    for rx, rep in _SPELLING:
        s = rx.sub(rep, s)
    if not re.search(r"[a-z]", s):
        s = _title_case(s)
    s = re.sub(r"^the\s+", "", s, flags=re.I)
    return re.sub(r"\s+", " ", s).strip(" ,-")


_SPAN_RX = re.compile(r'class="[^"]*\bjob-companylocation\b[^"]*"[^>]*>\s*(?:<b>\s*Facility\s*</b>)?\s*([^<]+)<', re.I)
_FACET_ATTR_RX = re.compile(r'data-facet-id="([^"]*)"', re.I)
_FACET_SLUG_RX = re.compile(r"custom_fields\.CompanyLocation-(.+?\))(?=\s|$)")


def _deslug(slug: str) -> str:
    """"Urology-ASC-Phoenix---Downtown-(10526)" -> "Urology ASC Phoenix - Downtown (10526)"."""
    return re.sub(r"\s+", " ", slug.replace("---", " \x00 ").replace("-", " ").replace("\x00", "-")).strip()


def tb_page_facility(system, page) -> tuple[str, bool]:
    """(clean site name, the page names a Facility) from a TalentBrew job
    page of a TB_FACILITY_SYSTEMS system: the job-companylocation field,
    else the CompanyLocation value in the page's data-facet-id. A page that
    names an office gives ("", True): the row keeps the system name.
    ("", False) when the page names no facility (a search page, an error)."""
    if system not in TB_FACILITY_SYSTEMS or not page:
        return "", False
    m = _SPAN_RX.search(page)
    raw = _clean(m.group(1)) if m else ""
    if not raw:
        a = _FACET_ATTR_RX.search(page)
        s = _FACET_SLUG_RX.search(_html.unescape(a.group(1))) if a else None
        raw = _deslug(s.group(1)) if s else ""
    if not raw:
        return "", False
    return uspi_clean(raw), True


# Stored-row change list (USPI): the first lines of a stored body that name
# exactly one table site ("Welcome to Winter Haven Ambulatory Surgery
# Center", "Arizona Digestive Institute is hiring a PREOP/PACU RN"). A name
# made only of generic words ("Advanced Surgical Care", "Regional
# SurgiCenter") could be a phrase of the text and is never matched.
_GENERIC = frozenset("advanced ambulatory and care center day diagnostic endoscopy hospital imaging institute "
                     "medical outpatient regional same services specialty surg surgicenter surgicare suites "
                     "therapy treatment".split())
_BODY_LINES = 8


def _body_keys() -> dict:
    keys: dict = {}
    for name in USPI_TABLE.values():
        if not name:
            continue
        k = _fold(name)
        if len(k) < 8 or set(k.split()) <= _GENERIC:
            continue
        keys.setdefault(k, set()).add(name)
    return keys


_USPI_BODY_KEYS = _body_keys()


def uspi_body_facility(description) -> str:
    """The one table site the first lines of a stored USPI body name, else ""
    (none, or two different sites)."""
    lines = [ln.strip() for ln in str(description or "").splitlines() if ln.strip()][:_BODY_LINES]
    if not lines:
        return ""
    text = f" {_fold(' | '.join(lines))} "
    hits = {k for k in _USPI_BODY_KEYS if f" {k} " in text}
    hits = {h for h in hits if not any(h != o and f" {h} " in f" {o} " for o in hits)}
    names = set()
    for h in hits:
        names |= _USPI_BODY_KEYS[h]
    return names.pop() if len(names) == 1 else ""


# ══ Concentra ═══════════════════════════════════════════════════════════════
CONCENTRA_FILE = os.path.join(_HERE, "facilities_ambulatory_concentra.json")
try:
    with open(CONCENTRA_FILE, encoding="utf-8") as _f:
        CONCENTRA_CENTERS: list = [tuple(r) for r in json.load(_f)]   # (name, city, state, lat, lon)
except FileNotFoundError:
    CONCENTRA_CENTERS = []
CENTER_M_SAME_TOWN = 250      # metres, a centre whose town is spelled like the posting's
CENTER_M_OTHER_TOWN = 150     # metres, a neighbouring town's spelling
CENTER_M_APART = 100          # the second centre must be this much farther (and twice as far)
# Not at one centre (decision 2): onsite programmes at a client's site, float,
# telecommute, hybrid and field sales roles, and the corporate departments
# (by the board's own category).
_ONSITE_TITLE_RX = re.compile(r"\bon-?site\b|athletic\s+trainer|\bATC?\b|\bEMT\b|emergency\s+medical\s+tech|"
                              r"paramedic|\bmedic\b|\bfloat\b|telecommute|telemedicine|telehealth|\bremote\b|"
                              r"\bhybrid\b|account\s+executive|\bsales\b", re.I)
_NOT_CENTER_CATEGORIES = frozenset({"certified athletic trainer", "emergency medical technician", "medical review ur",
                                    "central billing office", "information systems", "accounting finance",
                                    "customer service"})
_TOWN_ABBR = ((r"\bft\b", "fort"), (r"\bst\b", "saint"), (r"\bmt\b", "mount"), (r"\bn\b", "north"),
              (r"\bs\b", "south"), (r"\be\b", "east"), (r"\bw\b", "west"))


def _town_key(s) -> str:
    s = _clean(s).lower().replace(".", " ")
    for a, b in _TOWN_ABBR:
        s = re.sub(a, b, s)
    return re.sub(r"[^a-z]", "", s)


def _metres(lat1, lon1, lat2, lon2) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    x = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * 6371000 * math.asin(min(1.0, math.sqrt(x)))


def concentra_center(lat, lon, city="", state="", title="", category="") -> str:
    """"Concentra <centre>" for a Concentra posting at a centre's map point
    (see the module notes), else "" (the row keeps "Concentra"). `category`
    is the board's own job category ("Physical Therapist", "Central
    Billing Office"; hyphens or spaces)."""
    try:
        la, lo = float(lat), float(lon)
    except (TypeError, ValueError):
        return ""
    if not (-90 <= la <= 90 and -180 <= lo <= 180) or (la == 0 and lo == 0):
        return ""
    if _ONSITE_TITLE_RX.search(title or ""):
        return ""
    if " ".join(re.split(r"[-\s]+", (category or "").lower())).strip() in _NOT_CENTER_CATEGORIES:
        return ""
    near = []
    for c in CONCENTRA_CENTERS:
        if abs(c[3] - la) < 0.05 and abs(c[4] - lo) < 0.07:
            near.append((_metres(la, lo, c[3], c[4]), c))
    if not near:
        return ""
    near.sort(key=lambda t: t[0])
    d1, c = near[0]
    d2 = near[1][0] if len(near) > 1 else float("inf")
    st = (state or "").strip().upper()
    if c[2] != st:
        return ""
    limit = CENTER_M_SAME_TOWN if _town_key(c[1]) == _town_key(city) else CENTER_M_OTHER_TOWN
    if d1 > limit or d2 < max(2 * d1, d1 + CENTER_M_APART):
        return ""
    return f"Concentra {c[0]}"


# ══ CVS Health ══════════════════════════════════════════════════════════════
CVS_SITES = ("Oak Street Health", "MinuteClinic", "Omnicare")
_CVS_OPENING_END = "one community at a time"
_CVS_HEAD = 700
_COMPANY_OAK_RX = re.compile(r"Company:\s*Oak Street Health", re.I)
_OAK_RX = re.compile(r"\bOak Street Health\b", re.I)
_MINUTECLINIC_RX = re.compile(r"\bMinute\s?Clinic\b", re.I)
_OMNICARE_RX = re.compile(r"\bOmnicare\b", re.I)
_SIGNIFY_RX = re.compile(r"\bSignify\b", re.I)
# decision 2: home-based, remote, virtual and office roles keep "CVS Health"
_CVS_NOT_SITE_TITLE_RX = re.compile(r"\b(?:virtual|remote|work from home|telework|hybrid|utilization management|"
                                    r"regional director|senior medical director|in-?home)\b", re.I)
_CVS_REMOTE_LINE_RX = re.compile(r"Location:\s*(?:fully\s+)?(?:remote|virtual|work from home|telework)", re.I)
# A remote role says so ("This is a 40 hour/week, fully remote position",
# "100% remote position providing telehealth therapy"); a clinic-based role
# may mention virtual visits ("Virtual care visits may be conducted from the
# clinic") and stays a clinic role.
_CVS_VIRTUAL_HEAD_RX = re.compile(r"(?:\bfully\s+remote\b|\b100%\s+remote\b|\bremote\s+(?:position|role)\b|"
                                  r"\bremote\s+technology\s+platform\b)", re.I)
_ADDR_LINE_RX = re.compile(r"Location:\s*([^\n]{3,160})")
_ADDR_TOWN_RX = re.compile(r"(?:^|,)\s*([A-Za-z][A-Za-z .'-]{1,40}?),\s*([A-Z]{2})(?:\s+\d{5}(?:-\d{4})?)?\s*"
                           r"(?=$|\s+(?:Hours|Salary|Role|Schedule|Title|Company|Fellowship|Pay)\b)")


def cvs_opening(description) -> str:
    """The body after CVS's shared opening paragraph (it ends "... one
    community at a time."), or the whole body when it has none."""
    d = str(description or "")
    i = d.find(_CVS_OPENING_END, 0, 2500)
    return d[i + len(_CVS_OPENING_END):].lstrip(" .\n\t") if i >= 0 else d


def cvs_site(title, description) -> tuple[str, str]:
    """(site name, how) for a CVS Health posting: ("Oak Street Health",
    "company line" / "text"), ("MinuteClinic", "text"), ("Omnicare",
    "text"), or ("", why it keeps the system name: "home-based", "remote",
    "none")."""
    body = cvs_opening(description)
    head = body[:_CVS_HEAD]
    title = str(title or "")
    if _SIGNIFY_RX.search(head):
        return "", "home-based"
    if (_CVS_NOT_SITE_TITLE_RX.search(title) or _CVS_REMOTE_LINE_RX.search(body[:1500])
            or _CVS_VIRTUAL_HEAD_RX.search(head)):
        return "", "remote"
    oak = bool(_COMPANY_OAK_RX.search(body)) or bool(_OAK_RX.search(title)) or bool(_OAK_RX.search(head))
    mc = bool(_MINUTECLINIC_RX.search(head)) or bool(_MINUTECLINIC_RX.search(title))
    omni = bool(_OMNICARE_RX.search(head))
    if oak + mc + omni != 1:
        return "", "none"
    if oak:
        return "Oak Street Health", "company line" if _COMPANY_OAK_RX.search(body) else "text"
    return ("MinuteClinic" if mc else "Omnicare"), "text"


def cvs_address_town(description) -> tuple[str, str] | None:
    """(town, state) from the posting's own "Location:" address line, when it
    ends "<street>, <Town>, ST 12345" or is "<Town>, ST"; else None."""
    m = _ADDR_LINE_RX.search(cvs_opening(description)[:2500])
    if not m:
        return None
    line = re.split(r"\s+(?:Hours|Salary|Role Description|Schedule|Title|Company|Fellowship|Pay)\s*:", m.group(1))[0]
    line = line.strip().rstrip(".")
    t = _ADDR_TOWN_RX.search(line)
    if not t:
        return None
    town, st = t.group(1).strip(), t.group(2)
    if st not in US_STATES or re.search(r"\d", town) or len(town) < 3:
        return None
    # "<Town>, ST" alone, or the town is its own comma segment after a street
    if not (line.startswith(town) or re.search(r",\s*" + re.escape(town) + r",\s*" + st, line)):
        return None
    return town, st


def _has_town(job) -> bool:
    return bool((job.city or "").strip()) and (job.state or "").strip().upper() in US_STATES


# ── Stored names and towns (nights without a body) ──────────────────────────
_KNOWN: dict = {}          # CVS job_id -> (hospital_name, city, state) stored on the active row
KNOWN_PAGE = 1000
NO_BODY_CHARS = 600


def set_known_facilities(rows) -> int:
    """{job_id: (name, city, state)} for active CVS rows that store a site
    name or a town with a state."""
    global _KNOWN
    out = {}
    for r in rows or []:
        if (r.get("hospital_system") or "").strip() != SYSTEM_CVS or r.get("job_id") in (None, ""):
            continue
        name = _clean(r.get("hospital_name"))
        city, st = _clean(r.get("city")), _clean(r.get("state")).upper()
        if name in CVS_SITES or (city and st in US_STATES):
            out[str(r["job_id"])] = (name if name in CVS_SITES else "", city, st if st in US_STATES else "")
    _KNOWN = out
    return len(out)


def load_known_facilities() -> int:
    """One read-only PostgREST pass over CVS Health's active rows (job_id,
    hospital_name, city, state), keyset-paged on id. Without credentials the
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
    flt = _urlparse.quote(f"eq.{SYSTEM_CVS}", safe=".")
    try:
        while True:
            u = (f"{sb_url.rstrip('/')}/rest/v1/hospital_jobs?select=id,hospital_system,job_id,hospital_name,city,state"
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
        logger.warning(f"Ambulatory facilities: CVS load stopped after {len(rows):,} rows ({e}); "
                       f"the rows past that keep a site only on nights their body is read")
    try:
        return set_known_facilities(rows)
    except Exception as e:          # never let this read end the run
        logger.warning(f"Ambulatory facilities: stored names not installed ({e})")
        set_known_facilities([])
        return 0


def _set_town(job, city: str, state: str) -> None:
    job.city, job.state, job.location = city, state, f"{city}, {state}"


def apply_facility(job) -> str:
    """The site (and a missing town) on one CVS Health Job, before
    normalize_job. Returns what happened ("text", "company line",
    "kept", "home-based", "remote", "none", plus "+town" when the town was
    filled), or "" for any other system. hospital_system is never changed;
    a name another step already set is left as it is."""
    if (job.hospital_system or "").strip() != SYSTEM_CVS:
        return ""
    cur = _clean(job.hospital_name)
    if cur and cur != SYSTEM_CVS and cur not in CVS_SITES:
        return "other"
    desc = job.description or ""
    if len(desc.strip()) >= NO_BODY_CHARS:
        name, how = cvs_site(job.title, desc)
        job.hospital_name = name or SYSTEM_CVS
        if not _has_town(job):
            town = cvs_address_town(desc)
            if town:
                _set_town(job, *town)
                how += "+town"
        return how
    stored = _KNOWN.get(str(job.job_id))
    how = "none"
    if stored:
        name, city, st = stored
        if name:
            job.hospital_name = name
            how = "kept"
        if not _has_town(job) and city and st:
            _set_town(job, city, st)
            how += "+town"
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
