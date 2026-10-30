"""Facility round 2, the "others" group (2026-10-30): AnMed Health,
Spartanburg Regional, CentraCare, CommonSpirit, MultiCare, Erlanger and
Northwell's CX sites.

The owner, 2026-10-29: every hospital inside a multi-hospital system is named
and placed correctly on the site and in the dashboard. Decisions: (1) cards
and job pages show the HOSPITAL or site; the system name and logo stay on the
job page and the hub (hospital_system is never touched); (2) named sites
(hospitals, named clinics and centres) show their own name; generic offices,
home-based and remote jobs, multi-site pools and regional roles show the
system name with the town; a town or a site is never guessed. 2026-10-30:
"do christus now but push in 2nd round": the phase 7 sweep's "fix-later"
variants are built now and pushed with round 2.

Each system was read on 2026-10-30 from its own feed (claud_outputinput/
facility_scale_2026-10-29/round2/OTHERS-R2.txt has the evidence):

  AnMed Health (HealthcareSource, 256 rows on "AnMed"). The hit's
      hiringOrganization.subOrganization is "AnMed" for the whole Anderson
      entity, but jobLocation.address.streetAddress is the work site and the
      next subOrganization is the department. 800 N Fant St is AnMed Medical
      Center (cms 420027); 2000 E Greenville St is AnMed North Campus (its
      departments are "NC - Surgery", "NC - PACU" ...). The address alone is
      not enough: the urgent cares in Seneca and Clemson, the practices and
      the system offices carry the main campus's address too, so a department
      that is a practice, an urgent care, a cost-centre code or an office
      keeps the label (hcs_site).
  Spartanburg Regional (Phenom, 204 rows). The FacilityName entity of the
      label rows is "Medical Group of the Carolinas", "Beaumont Office",
      "Post-Acute Services" (home health, hospice), "Grants Administration":
      the system with the town (decision 2). Named here: "Restorative Care"
      (Spartanburg Hospital for Restorative Care, cms 422004, 20 rows) and
      "Surgery Center at Pelham" in their own towns; a row the feed placed in
      "South Carolina" (the state, no town) takes the town of the posting's own
      street address when it gives exactly one, and an entity that is one
      hospital (Spartanburg Medical Center, Cherokee Medical Center) its
      hospital and town (phenom_site).
  CentraCare (Phenom, 124 rows). The Willmar and Redwood Falls postings of
      the former Carris entities carry no workLocation; their department names
      the unit. A hospital department in Willmar is CentraCare - Rice Memorial
      Hospital's, in Redwood Falls CentraCare - Redwood's (each the system's
      one hospital in its town). Clinics, dental, eye, dermatology, urgent
      care, EMS, home health and physician recruiting keep the label.
  CommonSpirit (TalentBrew, 136 rows on "CommonSpirit Health"). National
      roles: revenue cycle, IT, clinical engineering, the internal travel
      programme, system executives. They keep the label. A patient access
      title that names one hospital in the row's own town names it.
  MultiCare (Workday, 511 rows on "MultiCare Health"). The hiring-entity
      bullet of these rows is "MultiCare Health System", "MultiCare Clinics"
      or "Pulse Heart Institute" (the hospital rows say "Auburn Medical
      Center" and are named by facility_names): system staff, the Summit Team
      and regional float pools, clinics. A town table would guess, so none is
      built; a title that names one MultiCare hospital in the row's own town
      names it ("Polysomnographic Technologist/Scorer - Auburn Medical Center").
  Erlanger (Taleo, 100 rows on "Erlanger Health System"). The board's own
      location for the medical group, health centres, practices and contact
      centres: the label is right. The town is not always: the board stamps
      Chattanooga, and a title that names another town with its state
      ("NP/PA- Primary Care (Pikeville, TN)", "LPN - Family Medicine Ringold,
      GA") gives that town.
  Northwell Health CX_1 / CX_3 (Oracle, 165 rows). CX_1 is FlexStaff, the
      staffing business (temp, contract and direct-hire postings for clients);
      CX_3 rows are ambulatory, EMS, laboratory and office postings whose work
      location facilities_oracle_other does not list. Both print as
      "Northwell Health" with the town already; nothing is renamed here.

hospital_system, job_id, url and slug are never touched. Shared with the
stored-row change list (claud_outputinput/facility_scale_2026-10-29/round2/
others_work/changes_others.py), so the database holds what the nightly writes.
"""
from __future__ import annotations

import re

import facilities_town as FT
import facility_names as FN

fkey = FN.fkey
clean_text = FN.clean_text

# The adapters' emit names (HOSPITAL_SYSTEM_ALIASES keys) and the stored systems.
ANMED = "AnMed Health"
SRHS = "Spartanburg Regional"
SRHS_DB = "Spartanburg Regional Healthcare"
CENTRACARE = "CentraCare"
CENTRACARE_DB = "CentraCare Health"
COMMONSPIRIT = "CommonSpirit Health"
COMMONSPIRIT_DB = "CommonSpirit"
MULTICARE = "MultiCare Health"
MULTICARE_DB = "MultiCare"
ERLANGER = "Erlanger Health System"
ERLANGER_DB = "Erlanger"

_DB = {ANMED: ANMED, SRHS: SRHS_DB, CENTRACARE: CENTRACARE_DB, COMMONSPIRIT: COMMONSPIRIT_DB,
       MULTICARE: MULTICARE_DB, ERLANGER: ERLANGER_DB}
_EMIT = {v: k for k, v in _DB.items()}

# The label each system's rows carry when no site is named (the adapter's
# emit name, the system name, and AnMed's entity value).
LABELS = {
    ANMED: frozenset({"AnMed", "AnMed Health"}),
    SRHS_DB: frozenset({SRHS, SRHS_DB}),
    CENTRACARE_DB: frozenset({CENTRACARE, CENTRACARE_DB}),
    COMMONSPIRIT_DB: frozenset({COMMONSPIRIT, COMMONSPIRIT_DB}),
    MULTICARE_DB: frozenset({MULTICARE, MULTICARE_DB}),
    ERLANGER_DB: frozenset({ERLANGER, ERLANGER_DB}),
}


def db_system(system) -> str:
    """The stored hospital_system for an emit name or a stored name."""
    s = (system or "").strip()
    return _DB.get(s, s)


def on_label(system, name) -> bool:
    labels = LABELS.get(db_system(system))
    return bool(labels) and clean_text(name) in labels


def _same_town(a, b) -> bool:
    return bool(clean_text(a)) and FT.town_key(a) == FT.town_key(b)


def _keeps_label(title, description="") -> str:
    """Why a title is no single site's (facilities_town's rules: remote, a
    named site that is no hospital, several sites, an office or a clinic by
    its title or location lines, a system role), or ""."""
    t = clean_text(title)
    if FT.is_remote(t, description):
        return "remote"
    if FT.CARE_SITE_RX.search(t):
        return "care-site"
    if FT.is_multi_site(t, description):
        return "multi-site"
    if FT.SYSTEM_ROLE_RX.search(t):
        return "system-role"
    if FT._offsite_by_title(t, description):
        return "offsite"
    return ""


# ═══════════════════════════ AnMed (HealthcareSource) ══════════════════════
_DIRECTIONS = {"n", "s", "e", "w", "north", "south", "east", "west"}


def street_key(address) -> str:
    """"<number> <street word>" of a street address ("800 N. Fant Street" ->
    "800 fant"; "2000 East Greenville St." -> "2000 greenville"), "" when it
    does not start with a number."""
    toks = fkey(address).split()
    if len(toks) < 2 or not toks[0].isdigit():
        return ""
    words = [t for t in toks[1:] if t not in _DIRECTIONS]
    return f"{toks[0]} {words[0]}" if words else ""


ANMED_MC = ("AnMed Medical Center", "Anderson", "SC", "cms 420027, 800 N Fant St")
ANMED_NC = ("AnMed North Campus", "Anderson", "SC", "address 2000 E Greenville St (AnMed's own name, cms 420027)")
_ANMED_ADDRESSES = {"800 fant": ANMED_MC, "2000 greenville": ANMED_NC}
# A department that is no unit of the hospital: a named AnMed practice or
# service ("AnMed Urgent Care - Seneca (5720)", "AnMed Primary Care ...",
# "AnMed Therapy - Acute Care"), a cost-centre code, the medical group, a
# provider group, a system office, the resource (float) team.
_ANMED_NOT_UNIT_RX = re.compile(
    r"^\s*AnMed\b|\(\d{3,5}\)|\bmedical\s+group\b|\bphysicians?\b|\bproviders?\b|\bpractice\b|\bclinic\b|"
    r"\burgent\s+care\b|\bprimary\s+care\b|\bhome\s+health\b|\bpartners\b|\baccess\s+health\b|"
    r"\binformation\s+services\b|\bmarketing\b|\bbusiness\s+services\b|\bcustomer\s+service\b|\bepic\b|"
    r"\bhealth\s+information\b|\bresource\s+team\b|\blanguage\s+services\b|\bstaff\s+development\b|"
    r"\bappearance\s+shoppe\b", re.I)
# A named AnMed service that is a unit of the hospital (acute care therapy
# works on the floors).
_ANMED_UNIT_SERVICE_RX = re.compile(r"^\s*AnMed\s+Therapy\s*-\s*Acute\s+Care\s*$", re.I)
# The North Campus by its own departments ("NC - Surgery"), the two campus
# services without the prefix (its inpatient behavioural health unit and its
# infusion pharmacy), or a title that says so ("Pharmacy Manager - North
# Campus"). The practices in its office buildings keep the label.
_ANMED_NC_DEPT_RX = re.compile(r"^\s*NC\s*-\s*\S|^\s*(?:Behavioral\s+Health\s+Services|Infusion\s+Pharmacy)\s*$", re.I)
_ANMED_NC_TITLE_RX = re.compile(r"\bNorth\s+Campus\b", re.I)
# A posting for both campuses ("Travel between AnMed's Main Medical Center
# and North Campus is required", "spans both the Main Medical Center and
# North Campus"): no one site's.
_ANMED_BOTH_RX = re.compile(
    r"\b(?:between|both)\b[^.]{0,80}\bMain\s+(?:Medical\s+Center|Campus)\b[^.]{0,40}\bNorth\s+Campus\b|"
    r"\b(?:between|both)\b[^.]{0,80}\bNorth\s+Campus\b[^.]{0,40}\bMain\s+(?:Medical\s+Center|Campus)\b", re.I)


def anmed_site(entity, dept, street, city, title, body=""):
    """(site, why) for one AnMed posting: site is (name, town, state, source)
    or None, why says what decided. Named only for the "AnMed" entity, in
    Anderson, at one of the two campus addresses, with a department that is a
    unit of that campus and a title that is no office's, clinic's, pool's or
    remote role's."""
    if clean_text(entity) != "AnMed":
        return None, "entity"
    site = _ANMED_ADDRESSES.get(street_key(street))
    if site is None:
        return None, "address"
    if not _same_town(city, site[1]):
        return None, "elsewhere"
    d = clean_text(dept)
    if _ANMED_NOT_UNIT_RX.search(d) and not _ANMED_UNIT_SERVICE_RX.search(d):
        return None, "department"
    if site is ANMED_NC and not (_ANMED_NC_DEPT_RX.search(d) or _ANMED_NC_TITLE_RX.search(clean_text(title))):
        return None, "department"
    if _ANMED_BOTH_RX.search(clean_text(body)):
        return None, "both-campuses"
    why = _keeps_label(title)
    if why:
        return None, why
    return site, "address"


def _dig(d, *path):
    cur = d
    for k in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def hcs_parts(src) -> tuple:
    """(entity, department, street, town, title) of one HealthcareSource hit."""
    src = src.get("_source") if isinstance(src.get("_source"), dict) else src
    org = _dig(src, "hiringOrganization", "subOrganization") or {}
    dept = _dig(org, "subOrganization", "name") or ""
    return (str(org.get("name") or ""), str(dept), str(_dig(src, "jobLocation", "address", "streetAddress") or ""),
            str(_dig(src, "jobLocation", "address", "addressLocality") or ""),
            str(src.get("title") or src.get("jobTitle") or ""))


def hcs_site(system, src, facility, body=""):
    """For _hcs_job: the site name an AnMed hit's street address and department
    give, or None (the adapter keeps its own value)."""
    if (system or "").strip() != ANMED or clean_text(facility) != "AnMed" or not isinstance(src, dict):
        return None
    entity, dept, street, town, title = hcs_parts(src)
    site, _ = anmed_site(entity, dept, street, town, title, body)
    return site[0] if site else None


# ═══════════════════════════ Phenom: Spartanburg and CentraCare ════════════
_STATE_NAMES = {v: k for k, v in FT._STATE_NAMES.items()}

_SRHS_SITES = {
    # FacilityName -> its one site (the table facilities_phenom keeps for the
    # hospitals, plus the two values it does not list)
    "spartanburg medical center": ("Spartanburg Medical Center", "Spartanburg", "SC", "cms 420007"),
    "pelham medical center": ("Pelham Medical Center", "Greer", "SC", "cms 420103"),
    "cherokee medical center": ("Cherokee Medical Center", "Gaffney", "SC", "cms 420043"),
    "union medical center": ("Union Medical Center", "Union", "SC", "cms 420108"),
    "restorative care": ("Spartanburg Hospital for Restorative Care", "Spartanburg", "SC",
                         "cms 422004 (389 Serpentine Dr); the board's FacilityName \"Restorative Care\""),
    "surgery center at pelham": ("Surgery Center at Pelham", "Greer", "SC", "board (2755 SC-14, Greer)"),
}
# the two the phase 2 table (facilities_phenom._SRHS_UNITS) does not list
_SRHS_NEW = frozenset({"restorative care", "surgery center at pelham"})
_ADDR_TOWN_RX = re.compile(r"(?:^|,)\s*([A-Za-z][A-Za-z .'-]{1,40}?)\s*,\s*SC\b")


def is_state_name(city) -> bool:
    """A town field that holds a state's name ("South Carolina")."""
    return clean_text(city).lower() in _STATE_NAMES


def posting_towns(doc) -> list:
    """The towns of a Phenom doc's own locations (multi_location_array,
    multi_location, location), in South Carolina, a state alone skipped."""
    vals = []
    for x in (doc or {}).get("multi_location_array") or []:
        vals.append(x.get("location") if isinstance(x, dict) else x)
    for x in (doc or {}).get("multi_location") or []:
        vals.append(x)
    vals.append((doc or {}).get("location"))
    towns = []
    for v in vals:
        for m in _ADDR_TOWN_RX.finditer(clean_text(v)):
            t = clean_text(m.group(1))
            if t and not is_state_name(t) and t not in towns:
                towns.append(t)
    return towns


def srhs_site(doc, city, state):
    """(name, city, state) for a Spartanburg Regional list doc, "" name = the
    system label; None when this module has nothing to say (facilities_phenom
    and the adapter decide)."""
    doc = doc or {}
    raw = fkey(doc.get("FacilityName"))
    site = _SRHS_SITES.get(raw)
    st = (state or "").strip().upper() or "SC"
    if is_state_name(city) or not clean_text(city):
        towns = posting_towns(doc)
        town = towns[0] if len(towns) == 1 else ""
        if site and (not town or _same_town(town, site[1])):
            return site[0], site[1], site[2]
        if town:
            return "", town, "SC"
        return None
    if raw in _SRHS_NEW and site and _same_town(city, site[1]) and st == site[2]:
        return site[0], clean_text(city), st
    return None


CC_RICE = ("CentraCare - Rice Memorial Hospital", "Willmar", "MN", "cms 240088")
CC_REDWOOD = ("CentraCare - Redwood", "Redwood Falls", "MN", "cms 241351")
_CC_TOWNS = {FT.town_key(s[1]): s for s in (CC_RICE, CC_REDWOOD)}
# The departments of the list docs in those towns, read one by one on
# 2026-10-30, that are units of the town's hospital. A clinic, dental, eye,
# dermatology, urgent care, lab clinic, family practice, outpatient
# behavioural health, EMS (the ambulance service) and physical therapy
# (athletic trainers at the schools) are not.
_CC_UNITS = frozenset(fkey(d) for d in (
    "Intensive Care Unit", "Respiratory Therapy", "Adult Health Care", "Emergency Trauma Center",
    "Women And Newborn Health", "Hospital Med Hospitalists", "Care Management", "Imaging C T Scanning",
    "Imaging Diagnostic", "Lab Pathology", "Security And Safety", "Anesthesiology", "Surgery", "Nutrition"))
_CC_INPATIENT_TITLE_RX = re.compile(r"\bmed[- ]?surg\w*\b|\bOB\s*(?:&|and)\s*(?:ER|ED)\b", re.I)
_SCHOOL_RX = re.compile(r"\bschool\b", re.I)


def centracare_site(doc, city, state, title):
    """(name, city, state) for a CentraCare list doc with no workLocation in
    Willmar or Redwood Falls whose department is a unit of the town's one
    hospital (or, with no department, whose title says an inpatient unit);
    None otherwise."""
    doc = doc or {}
    if clean_text(doc.get("workLocation")):
        return None
    site = _CC_TOWNS.get(FT.town_key(city))
    if site is None or (state or "").strip().upper() not in ("", site[2]):
        return None
    t = clean_text(title)
    dept = fkey(doc.get("department"))
    if dept:
        if dept not in _CC_UNITS:
            return None
    elif not (FT.INPATIENT_RX.search(t) or _CC_INPATIENT_TITLE_RX.search(t)):
        return None
    if _SCHOOL_RX.search(t) or _keeps_label(t):
        return None
    return site[0], clean_text(city), site[2]


def phenom_site(system, doc, city="", state="", title=""):
    """For scrape_phenom, before facilities_phenom: (hospital_name, city,
    state) with "" for the system label, or None (the adapter's own chain
    runs)."""
    s = (system or "").strip()
    if s == SRHS:
        return srhs_site(doc, city, state)
    if s == CENTRACARE:
        return centracare_site(doc, city, state, title)
    return None


# ═══════════════════════════ Title rules (finalize_jobs) ═══════════════════
def _t(name, town, state, source, pattern):
    return (name, town, state, source, re.compile(pattern, re.I))


# A title that names one hospital, with that hospital's town: the row is
# named only in that town (a posting elsewhere names no site).
TITLE_SITES = {
    COMMONSPIRIT_DB: (
        _t("Saint Joseph Hospital - Mount Sterling", "Mount Sterling", "KY", "stored name",
           r"\bSt\.?\s*Joseph\s+(?:Mt\.?|Mount)\s+Sterling\b"),
        _t("CHI Mercy Health of Roseburg", "Roseburg", "OR", "stored name",
           r"\bCHI\s+Mercy\s+Health\s+(?:of\s+)?Roseburg\b"),
        _t("St Anne Hospital", "Burien", "WA", "stored name", r"\b(?:VMFH\s+)?St\.?\s+Anne\s+(?:Hospital\s+)?Burien\b"),
    ),
    MULTICARE_DB: tuple(
        _t(name, town, "WA", "facility_names", rx) for name, town, rx in (
            ("MultiCare Auburn Medical Center", "Auburn", r"\bAuburn\s+Medical\s+Center\b"),
            ("MultiCare Covington Medical Center", "Covington", r"\bCovington\s+Medical\s+Center\b"),
            ("MultiCare Capital Medical Center", "Olympia", r"\bCapital\s+Medical\s+Center\b"),
            ("MultiCare Good Samaritan Hospital", "Puyallup", r"\bGood\s+Samaritan\s+Hospital\b"),
            ("MultiCare Tacoma General Hospital", "Tacoma", r"\bTacoma\s+General\b"),
            ("MultiCare Allenmore Hospital", "Tacoma", r"\bAllenmore\b"),
            ("MultiCare Mary Bridge Children's Hospital", "Tacoma", r"\bMary\s+Bridge\s+Children'?s\s+Hospital\b"),
            ("MultiCare Deaconess Hospital", "Spokane", r"\bDeaconess\s+Hospital\b"),
            ("MultiCare Valley Hospital", "Spokane Valley", r"\bValley\s+Hospital\b"),
            ("MultiCare Yakima Memorial Hospital", "Yakima", r"\bYakima\s+Memorial\s+Hospital\b"),
        )),
}

# Erlanger: a title that names a town with its state. The board stamps
# Chattanooga on the system entity's postings; only these towns (Erlanger
# practices, read off the stored titles) are read, spelled as the titles do.
_ERL_TOWNS = {("pikeville", "TN"): "Pikeville", ("ringgold", "GA"): "Ringgold", ("ringold", "GA"): "Ringgold"}
_ERL_STATE = {"Pikeville": "TN", "Ringgold": "GA"}
_ERL_TOWN_RX = re.compile(r"\b(Pikeville|Ringgold|Ringold)\s*,\s*(TN|GA)\b", re.I)
ERLANGER_STAMP = ("Chattanooga", "TN")


def title_site(system, name, city, state, title):
    """(name, city, state, source) a label row's title gives, or None."""
    db = db_system(system)
    if not on_label(db, name):
        return None
    t = clean_text(title)
    if db == ERLANGER_DB:
        towns = {_ERL_TOWNS.get((m.group(1).lower(), m.group(2).upper())) for m in _ERL_TOWN_RX.finditer(t)} - {None}
        if len(towns) != 1 or not (_same_town(city, ERLANGER_STAMP[0])
                                   and (state or "").strip().upper() == ERLANGER_STAMP[1]):
            return None
        town = next(iter(towns))
        st = _ERL_STATE[town]
        return clean_text(name), town, st, f"title names the town: {t[:80]}"
    hits = [s for s in TITLE_SITES.get(db, ()) if s[4].search(t)]
    if len({h[0] for h in hits}) != 1:
        return None
    site = hits[0]
    if not _same_town(city, site[1]) or (state or "").strip().upper() not in ("", site[2]):
        return None
    rest = site[4].sub(" ", t)
    if _keeps_label(rest):
        return None
    return site[0], clean_text(city), site[2], f"title names the hospital: {t[:80]}"


def apply_facility(job, canon=None) -> str:
    """One Job: "" for another system, else how ("title", "town", "none")."""
    system = (job.hospital_system or "").strip()
    db = db_system(canon(system) if canon else system)
    if db not in TITLE_SITES and db != ERLANGER_DB:
        return ""
    if not on_label(db, job.hospital_name):
        return "named-already"
    got = title_site(db, job.hospital_name, job.city, job.state, job.title)
    if not got:
        return "none"
    name, city, state, _ = got
    how = "town" if db == ERLANGER_DB else "title"
    job.hospital_name = name
    if (city, state) != (clean_text(job.city), (job.state or "").strip().upper()):
        job.city, job.state, job.location = city, state, f"{city}, {state}"
    return how


_ROW_FIELDS = ("hospital_name", "city", "state", "location")


def apply_facilities(jobs, canon=None) -> dict:
    """apply_facility over a run's rows (finalize_jobs, after the phase 7
    variants pass and before the town tables). A row it raises on keeps what
    it had. Returns {system: {how: rows}}."""
    stats: dict = {}
    for job in jobs:
        keep = [(a, getattr(job, a)) for a in _ROW_FIELDS if hasattr(job, a)]
        try:
            how = apply_facility(job, canon)
        except Exception:
            for a, v in keep:
                try:
                    setattr(job, a, v)
                except Exception:
                    pass
            how = "error"
        if how and how != "named-already":
            s = db_system(canon(job.hospital_system) if canon else job.hospital_system)
            d = stats.setdefault(s, {})
            d[how] = d.get(how, 0) + 1
    return stats


# ═══════════════════════ the stored-row change list ═════════════════════════
def stored_change(row):
    """(new_hospital_name, new_city, new_state, source) for one stored label
    row of the title rules (CommonSpirit, MultiCare, Erlanger), or None. The
    feed-read systems (AnMed, Spartanburg, CentraCare) need tonight's list
    payload: changes_others.py reads it and calls anmed_site / phenom_site."""
    got = title_site(row.get("hospital_system"), row.get("hospital_name"), row.get("city") or "",
                     row.get("state") or "", row.get("title") or "")
    return got
