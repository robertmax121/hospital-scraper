"""The hospital or named site on the rows twelve more systems still filed under the system name (2026-10-29, phase 4, group B).

The owner, 2026-10-29: "i thought you were gonna edit all the large
multihospital systems" and "unitedhealth group hospital list is incomplete".
His two rules stand: (1) cards and job pages show the HOSPITAL or named site
("Apply to <site>"); the system name and logo stay on the job page and the
system hub (hospital_system is never touched here: the upsert key, the hub,
the filters, the logo); (2) named sites (hospitals, named clinics, centers,
named communities) show their own name; generic offices, home-based and
remote postings show the system name with the town; a town or a site is
never guessed.

What each system's postings carry, and who names it (stored label rows read
2026-10-29, after the phase 2 and phase 3 SQL):

  Vandalia Health (Infor)  the title ends "-- <site>" and the body carries
                        the same "Location: <site>" line ("Memorial
                        Hospital", "General Hospital", "Women & Children's
                        Hospital", "Heart & Vascular Center", "Memorial-MSOB",
                        "Northgate-400 Association Dr."). The title segment
                        decides when there is one (it is on every night's
                        list, body or not); the body line otherwise. Office
                        buildings (MSOB, Northgate, Chesterfield ...) keep the
                        label (decision 2).
  Innovative Renal Care (Phenom) 17 bodies carry "Location: <clinic> - <street>
                        <town> <ST> <zip>": the clinic by its own name.
  UK HealthCare (PeopleAdmin) the "Working Title" line or the title names the
                        hospital ("LPN/Good Sam 4m/7a", "RN/Neonatal ICU/
                        Kentucky Children's Hospital"); the "Work Location"
                        line is the posting's own town (the adapter stamps
                        Lexington on every row: a row whose Work Location is
                        another Kentucky town gets that town, with the label).
  Mount Sinai Health System (Oracle) the list-time title table names most rows
                        (scraper.MOUNT_SINAI_SITES); the body's closing cost
                        centre line names the employing hospital ("Non-
                        Bargaining Unit, 1234 - Radiology - MSH, Mount Sinai
                        Hospital"; "BI", "WST", "STL", "USD"); "OFFS" (off-site),
                        "ISM" (the school), "QHC" / "SSQ" (NYC Health +
                        Hospitals, shared services) keep the label. Titles
                        spelling "Mt Sinai Hospital" / "Mt Sinai West" are
                        read too.
  OHSU (iCIMS)          the title names Doernbecher, Adventist Health Portland
                        or Hillsboro Medical Center, or the body carries the
                        "About OHSU Doernbecher Children's Hospital" section.
  SolutionHealth (Workday) a town table (facilities_town): Manchester = Elliot
                        Hospital, Nashua = Southern New Hampshire Medical
                        Center; offices, clinics, remote keep the label.
  Dartmouth Health (iCIMS) a town table: Keene, New London, Windsor VT,
                        Bennington VT hold one hospital each; Lebanon holds
                        two (DHMC and Alice Peck Day): only a title naming one.
  Duke Health (Phenom)  a town table: Raleigh = Duke Raleigh Hospital,
                        Mooresville = Duke Health Lake Norman Hospital; Durham
                        holds two (only a title naming one).
  Cedars-Sinai, Franciscan Missionaries of Our Lady (Oracle) phase 2 left
                        them to the nightly's work location, which their live
                        payload does not carry: town tables (Marina del Rey,
                        Los Angeles; Jackson MS, Monroe, Bogalusa, Gonzales;
                        Baton Rouge and Lafayette only by a title naming one).
  Northwell Health, Parkview Health (Oracle) work-location spellings their
                        phase 2 tables lacked (Lenox Hill, Phelps, Peconic Bay;
                        Parkview Wabash, Whitley, Noble): nightly only.
  Rochester Regional Health, Genesis HealthCare System (Workday) the site is
                        the facility (facilities_workday tables): the named
                        living centers, cancer institutes, health centers
                        and primary care sites the phase 2 table left; office
                        buildings, campuses and garages stay.

Hooks (scraper.py): this module registers its Workday and town tables in the
family modules at import (before scraper.WD_SITE_FACILITY is built), the run
start loads the names stored on its body-named rows (load_known_facilities:
a night without a body keeps them), and finalize_jobs calls apply_facilities
after the phase 3 A pass, before the town tables. Shared with the stored-row
change list (claud_outputinput/facility_scale_2026-10-29/phase4/sb_work/
changes.py).
"""
from __future__ import annotations

import json
import logging
import os
import re
import time

import facilities_text as T
import facilities_town
import facilities_workday
import sb_keys  # the DATA project key, one reader for the repo
from city_utils import known_town

logger = logging.getLogger("scraper.facility_sysb")

S = T.S
Read = T.Read
Spec = T.Spec
key = T.key
clean_text = T.clean_text

VANDALIA = "Vandalia Health"
IRC = "Innovative Renal Care"
UK = "UK HealthCare"
MOUNT_SINAI = "Mount Sinai Health System"
OHSU = "OHSU"
SOLUTION = "SolutionHealth"
DARTMOUTH = "Dartmouth Health"
DUKE = "Duke Health"
ROCHESTER = "Rochester Regional Health"
GENESIS = "Genesis HealthCare System"


# ══════════════════════════════════════════════════════════════════════════
#  1. BODY / TITLE SITES (this module's own pass, the facilities_text tables)
# ══════════════════════════════════════════════════════════════════════════
# Vandalia Health: the "-- <site>" title segment / body "Location:" line.
# The three CAMC hospitals share CMS 510022 (one licence, three campuses in
# Charleston); the Heart & Vascular Center is a named center of the Memorial
# campus. Office buildings, a garage, a document center, the IT building and
# street-address sites are offices (decision 2).
_VANDALIA_SITES = (
    S("CAMC Memorial Hospital", "Charleston", "WV", "cms 510022 (Memorial campus)", "Memorial Hospital"),
    S("CAMC General Hospital", "Charleston", "WV", "cms 510022 (General campus)", "General Hospital"),
    S("CAMC Women and Children's Hospital", "Charleston", "WV", "cms 510022 (Women and Children's campus)",
      "Women & Children's Hospital", "Women and Children's Hospital", "Womens & Childrens Hospital", "Women & Childrens Hospital", "Women and Childrens Hospital"),
    S("CAMC Heart and Vascular Center", "Charleston", "WV", "board", "Heart & Vascular Center",
      "Heart and Vascular Center"),
)
_VANDALIA_OFFICES = (
    "Memorial-MSOB", "Memorial MSOB", "General-MSOB", "GENERAL-MSOB", "WCH-MSOB", "Memorial-Parking Garage",
    "Northgate-400 Association Dr.", "Northgate-100 Tracy Way", "Northgate-600 Tracy Way", "Chesterfield Bldg- 2",
    "Chesterfield Bldg 4-Center for Learning & Research", "Document Center Building", "Cancer Center Building",
    "Healthcare Financial Services Building", "Cornerstone Building (M)", "Dover Building", "Solutions",
    "All CAMC Locations", "All locations", "ALL CAMC, PMC, GVMC Locations", "(REMOTE)", "Sedgely Park",
)
VANDALIA_SPEC = Spec(VANDALIA, None, (Read("label", "Location", "site"),), _VANDALIA_SITES,
                     offices=_VANDALIA_OFFICES)
_VANDALIA_TITLE_RX = re.compile(r"\s+-{2,}\s*|\s*-{2,}\s+")

# Innovative Renal Care: the clinic of a body "Location:" line, at the town
# the line itself writes ("board"). An LLC name that is no clinic's
# ("Harriman Partners LLC") and an office line ("Franklin, TN (100% onsite)")
# name none.
_IRC_SITES = (
    S("Dialysis Center of East Orange", "East Orange", "NJ", "board"),
    S("Kidney Center of Beaumont", "Beaumont", "TX", "board"),
    S("NCG Piedmont Dialysis Center", "Covington", "GA", "board"),
    S("Dialysis Center of Milledgeville", "Milledgeville", "GA", "board", "Dialysis Center Of Milledgeville LLC"),
    S("Keowee Dialysis Center", "Seneca", "SC", "board", "Keowee Dialysis Center LLC"),
    S("Macon Southside Dialysis Center", "Macon", "GA", "board", "Macon Southside Dialysis Center LLC"),
    S("Dialysis Center of Macon", "Macon", "GA", "board", "Dialysis Center Of Macon LLC"),
    S("Sandersville Dialysis Clinic", "Sandersville", "GA", "board", "Sandersville Dialysis Clinic LLC"),
    S("Waynesboro Dialysis Clinic", "Waynesboro", "GA", "board"),
    S("Hephzibah Dialysis Clinic", "Hephzibah", "GA", "board"),
    S("South Augusta Dialysis Clinic", "Augusta", "GA", "board"),
    S("Lexington Kidney Center", "West Columbia", "SC", "board", "Lexington Kidney Center LLC"),
    S("Northeast Philadelphia Dialysis Center", "Philadelphia", "PA", "board",
      "Northeast Philadelphia Dialysis Center LLC"),
    S("Bristol Dialysis", "Bristol", "PA", "board", "Bristol Dialysis LLC"),
)
IRC_SPEC = Spec(IRC, None, (Read("label", "Location", "site"),), _IRC_SITES)

# UK HealthCare: the hospital a Working Title or title names. Chandler is the
# University of Kentucky Hospital's main campus (CMS 180067); Good Samaritan
# its second campus; Kentucky Children's Hospital sits inside Chandler.
_UK_SITES = (
    S("UK Albert B. Chandler Hospital", "Lexington", "KY", "cms 180067"),
    S("UK Good Samaritan Hospital", "Lexington", "KY", "cms 180067 (Good Samaritan campus)"),
    S("Kentucky Children's Hospital", "Lexington", "KY", "address 800 Rose St, Lexington"),
    S("Eastern State Hospital", "Lexington", "KY", "cms 184004"),
    S("UK Markey Cancer Center", "Lexington", "KY", "address 800 Rose St, Lexington (UK HealthCare campus)"),
    S("UK St. Claire", "Morehead", "KY", "cms 180018"),
)
UK_SPEC = Spec(UK, ("Lexington", "KY"), (), _UK_SITES)
UK_CUES = (
    (re.compile(r"\bchandler\b", re.I), "UK Albert B. Chandler Hospital"),
    (re.compile(r"\bgood\s*sam(?:aritan)?\b", re.I), "UK Good Samaritan Hospital"),
    (re.compile(r"\bkentucky\s+children'?s\b|\bKCH\b"), "Kentucky Children's Hospital"),
    (re.compile(r"\beastern\s+state\b|\bESH\b"), "Eastern State Hospital"),
    (re.compile(r"\bmarkey\b", re.I), "UK Markey Cancer Center"),
    (re.compile(r"\bst\.?\s*claire\b", re.I), "UK St. Claire"),
)
_UK_WT_RX = re.compile(r"(?m)^[ \t]*Working Title[ \t]*:?[ \t]*\n+[ \t]*([^\n]{2,200})")
_UK_WL_RX = re.compile(r"(?m)^[ \t]*Work Location[ \t]*:?[ \t]*\n+[ \t]*([^\n]{2,80})")

# Mount Sinai: the employing hospital of the body's closing cost-centre line
# ("<union>, <cost centre> - <department> - <code>, <entity>"). The names are
# the ones the list-time title table writes (scraper.MOUNT_SINAI_SITES).
_MS_SITES = (
    S("The Mount Sinai Hospital", "New York", "NY", "cms 330024"),
    S("Mount Sinai Beth Israel", "New York", "NY", "cms 330169"),
    S("Mount Sinai West", "New York", "NY", "cms 330046"),
    S("Mount Sinai Morningside", "New York", "NY", "cms 330046 (Morningside campus, formerly St. Luke's)"),
    S("Mount Sinai Union Square", "New York", "NY", "address 10 Union Sq E, New York"),
    S("Mount Sinai Queens", "Queens", "NY", "address 25-10 30th Ave, Astoria (Queens)"),
    S("Mount Sinai Brooklyn", "Brooklyn", "NY", "address 3201 Kings Hwy, Brooklyn"),
)
MS_SPEC = Spec(MOUNT_SINAI, None, (), _MS_SITES)
MS_ENTITY = {"MSH": "The Mount Sinai Hospital", "BI": "Mount Sinai Beth Israel", "WST": "Mount Sinai West",
             "STL": "Mount Sinai Morningside", "USD": "Mount Sinai Union Square"}
# codes that name no hospital: the off-site practices of the hospital, the
# school, NYC Health + Hospitals sites Mount Sinai staffs, shared services
MS_NOT_SITE = frozenset({"OFFS", "ISM", "QHC", "SSQ", "ELM", "MSHS"})
_MS_ENTITY_RX = re.compile(r"(?m)^[^\n]{0,120}?Bargaining Unit[^\n]*?\s-\s*([A-Z]{2,5}),\s*([^\n]{3,120}?)\s*$")
MS_TITLE = (
    (re.compile(r"\bmt\.?\s*sinai\s+hospital\b", re.I), "The Mount Sinai Hospital"),
    (re.compile(r"\bmt\.?\s*sinai\s+west\b", re.I), "Mount Sinai West"),
    (re.compile(r"\bmt\.?\s*sinai\s+morningside\b", re.I), "Mount Sinai Morningside"),
    (re.compile(r"\bmt\.?\s*sinai\s+beth\s+israel\b", re.I), "Mount Sinai Beth Israel"),
    (re.compile(r"\bmt\.?\s*sinai\s+queens\b", re.I), "Mount Sinai Queens"),
    (re.compile(r"\bmt\.?\s*sinai\s+brooklyn\b", re.I), "Mount Sinai Brooklyn"),
)

# OHSU: the sites a title names, or the Doernbecher section of the body.
_OHSU_SITES = (
    S("OHSU Doernbecher Children's Hospital", "Portland", "OR", "cms 380009 (Doernbecher)"),
    S("Adventist Health Portland", "Portland", "OR", "cms 380060"),
    S("Hillsboro Medical Center", "Hillsboro", "OR", "cms 380021"),
)
OHSU_SPEC = Spec(OHSU, None, (), _OHSU_SITES)
OHSU_TITLE = (
    (re.compile(r"\bdoernbecher\b", re.I), "OHSU Doernbecher Children's Hospital"),
    (re.compile(r"\badventist\s+health\s+portland\b|\bAHP\b"), "Adventist Health Portland"),
    (re.compile(r"\bhillsboro\s+medical\s+center\b|\bHMC\b"), "Hillsboro Medical Center"),
)
_OHSU_ABOUT_RX = re.compile(r"(?mi)^[ \t]*About\s+(?:OHSU\s+)?Doernbecher\s+Children(?:'|’)?s\s+Hospital\b")

SPECS = {s.system: s for s in (VANDALIA_SPEC, IRC_SPEC, UK_SPEC, MS_SPEC, OHSU_SPEC)}
SYSTEMS = frozenset(SPECS)

_REMOTE_RX = re.compile(r"\b(?:remote|work\s+from\s+home|telecommute|virtual|hybrid)\b", re.I)


def _site(spec, name):
    return spec.names.get(name)


def _one(hits):
    names = {h for h in hits if h}
    return next(iter(names)) if len(names) == 1 else None


def vandalia_read(title, description):
    """(Site or None, kind, what, raw): the title's "-- <site>" segment when
    the title has one (it decides: an office there is an office even when the
    body line names a hospital), else the body's "Location:" line."""
    parts = _VANDALIA_TITLE_RX.split(clean_text(title))
    if len(parts) > 1 and parts[-1].strip():
        seg = parts[-1].strip()
        site = T.match_value(VANDALIA_SPEC, seg)
        if site is not None:
            return site, "site", "site", seg
        what = "office" if any(key(h) in VANDALIA_SPEC.offices for h in T._candidates(seg)[0]) else "unknown"
        return None, "", what, seg
    return T.read_text(VANDALIA_SPEC, description)


def uk_read(title, description):
    """(Site or None, kind, what, raw): the one hospital the Working Title
    line and the title name together; two different ones name none."""
    m = _UK_WT_RX.search(description or "")
    text = " | ".join(x for x in (clean_text(title), clean_text(m.group(1)) if m else "") if x)
    name = _one(n for rx, n in UK_CUES if rx.search(text))
    if name:
        return UK_SPEC.names[name], "site", "site", text
    return None, "", ("none" if not m else "unknown"), (m.group(1) if m else "")


def uk_work_town(description):
    """(town, "KY") of the body's Work Location line when it is a known
    Kentucky town, else None."""
    m = _UK_WL_RX.search(description or "")
    if not m:
        return None
    v = re.sub(r"\s+", " ", m.group(1)).strip()
    v = re.sub(r"[,\s]+(?:KY|Ky|Kentucky)\.?$", "", v).strip(" ,")
    t = known_town(v, "KY")
    return (t, "KY") if t else None


def mount_sinai_read(title, description):
    """(Site or None, kind, what, raw): a title spelling "Mt Sinai <site>",
    else the closing cost-centre line's hospital code."""
    t = clean_text(title)
    name = _one(n for rx, n in MS_TITLE if rx.search(t))
    if name:
        return MS_SPEC.names[name], "site", "site", t
    ms = _MS_ENTITY_RX.findall(description or "")
    if not ms:
        return None, "", "none", ""
    code, ent = ms[-1]
    if code in MS_ENTITY:
        return MS_SPEC.names[MS_ENTITY[code]], "entity", "site", f"{code}, {ent}"
    return None, "", ("office" if code in MS_NOT_SITE else "unknown"), f"{code}, {ent}"


def ohsu_read(title, description):
    t = clean_text(title)
    name = _one(n for rx, n in OHSU_TITLE if rx.search(t))
    if name:
        return OHSU_SPEC.names[name], "site", "site", t
    if _OHSU_ABOUT_RX.search(description or ""):
        return OHSU_SPEC.names["OHSU Doernbecher Children's Hospital"], "entity", "site", "About Doernbecher"
    return None, "", "none", ""


READERS = {
    VANDALIA: vandalia_read,
    IRC: lambda title, description: T.read_text(IRC_SPEC, description),
    UK: uk_read,
    MOUNT_SINAI: mount_sinai_read,
    OHSU: ohsu_read,
}


def read(system, title, description):
    """(Site or None, kind, what, raw) for one posting of a SYSTEMS system."""
    fn = READERS.get(system)
    return fn(title, description) if fn else (None, "", "none", "")


def decide(system, hospital_name, city, state, title="", description=""):
    """(hospital_name, city, state, how) for one stored or scraped row of a
    SYSTEMS system on the label, or (None, None, None, how) when it keeps the
    label. Pure: no stored name. how: "text" (named), "town" (the label with
    the posting's own town, UK's Work Location), "remote", "elsewhere" (the
    site is in another town than the posting), "office", "unknown", "none",
    "other" (already a site)."""
    spec = SPECS.get((system or "").strip())
    if spec is None:
        return None, None, None, ""
    if clean_text(hospital_name) not in ("", spec.system):
        return None, None, None, "other"
    site, kind, what, raw = read(spec.system, title, description)
    jc, js = clean_text(city), clean_text(state).upper()
    if spec.system == UK:
        wt = uk_work_town(description)
        if wt and (not jc or T._at_default(spec, jc, js)) and key(wt[0]) != key(jc):
            jc, js = wt          # the posting's own town, not the Lexington stamp
            if site is None:
                return spec.system, jc, js, "town"
    moved = (jc, js) != (clean_text(city), clean_text(state).upper())
    if site is not None:
        if kind == "entity" and _REMOTE_RX.search(clean_text(title)):
            return (spec.system, jc, js, "town") if moved else (None, None, None, "remote")
        placed = T.place(spec, site, kind, jc, js, raw)
        if placed is None:
            return (spec.system, jc, js, "town") if moved else (None, None, None, "elsewhere")
        return placed[0], placed[1], placed[2], "text"
    return None, None, None, what


# ── Stored names (nights without a body) ────────────────────────────────────
_KNOWN: dict = {}
KNOWN_PAGE = 1000
KNOWN_MAX_ROWS = 20000


def set_known_facilities(rows) -> int:
    """{(hospital_system, job_id): (stored hospital_name, city, state)} for
    rows whose stored name is one of the system's site names here, and, for
    a system with a stamped default town (UK HealthCare), label rows stored
    at another town (the Work Location gave it)."""
    global _KNOWN
    out = {}
    for r in rows or []:
        spec = SPECS.get((r.get("hospital_system") or "").strip())
        name, jid = clean_text(r.get("hospital_name")), r.get("job_id")
        if spec is None or jid in (None, ""):
            continue
        if name not in spec.names:
            city = clean_text(r.get("city"))
            if not (spec.default and name == spec.system and city
                    and not T._at_default(spec, city, clean_text(r.get("state")).upper())):
                continue
        out[(spec.system, str(jid))] = (name, clean_text(r.get("city")), clean_text(r.get("state")).upper())
    _KNOWN = out
    return len(out)


def known_facility(system, job_id):
    return _KNOWN.get(((system or "").strip(), str(job_id)))


def load_known_facilities() -> int:
    """Read-only PostgREST passes (the SUPABASE_URL / key the upsert uses)
    over the active rows of each system here whose hospital_name is not the
    system label, keyset-paged on id (idx_system). Without credentials the
    map stays empty and nothing is kept. Never raises."""
    sb_url = os.environ.get("SUPABASE_URL", "")
    sb_key = sb_keys.supabase_key()
    if not sb_url or not sb_key:
        set_known_facilities([])
        return 0
    import urllib.parse as _urlparse
    import urllib.request as _urlreq
    rows = []
    for system in sorted(SPECS):
        flt = _urlparse.quote(f"eq.{system}", safe=".")
        nflt = _urlparse.quote(f"neq.{system}", safe=".")
        # a system with a stamped default town reads its label rows too (their stored town)
        name_flt = "" if SPECS[system].default else f"&hospital_name={nflt}"
        last, got = 0, 0
        try:
            while len(rows) < KNOWN_MAX_ROWS:
                u = (f"{sb_url.rstrip('/')}/rest/v1/hospital_jobs?select=id,hospital_system,job_id,hospital_name,"
                     f"city,state&is_active=is.true&hospital_system={flt}{name_flt}"
                     f"&id=gt.{last}&order=id.asc&limit={KNOWN_PAGE}")
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
            logger.warning(f"Systems B: {system} load stopped after {got:,} rows ({e}); "
                           f"its rows past that keep a site only on nights their body is read")
    try:
        return set_known_facilities(rows)
    except Exception as e:          # never let this read end the run
        logger.warning(f"Systems B: stored names not installed ({e})")
        set_known_facilities([])
        return 0


# ── Applying it to a Job (finalize_jobs, before the town tables) ────────────
UNKNOWN: dict = {}       # system -> {raw value: rows} named by no table entry, this run


def _set(job, name, city, state) -> None:
    job.hospital_name = name
    if (city, state) != (clean_text(job.city), clean_text(job.state).upper()):
        job.city, job.state, job.location = city, state, f"{city}, {state}"


def apply_facility(job) -> str:
    """Store the site on one Job of a SYSTEMS system still on the label.
    Returns decide()'s how, or "kept" (no body tonight: the stored site
    stands), or "" for any other system. hospital_system is never changed."""
    spec = SPECS.get((job.hospital_system or "").strip())
    if spec is None:
        return ""
    name, city, state, how = decide(spec.system, job.hospital_name, job.city, job.state, job.title,
                                    job.description)
    if name:
        _set(job, name, city, state)
        return how
    if how in ("none", "unknown") and len((job.description or "").strip()) < T.NO_BODY_CHARS:
        kept = known_facility(spec.system, job.job_id)
        if kept and kept[0] == spec.system:
            # the label at the town a past body gave (UK's Work Location)
            jc, js = clean_text(job.city), clean_text(job.state).upper()
            if (not jc or T._at_default(spec, jc, js)) and kept[1]:
                _set(job, spec.system, kept[1], kept[2] or js)
                return "kept"
        elif kept:
            site = spec.names[kept[0]]
            jc, js = clean_text(job.city), clean_text(job.state).upper()
            if kept[1] and T.place(spec, site, "entity", kept[1], kept[2]) is not None:
                jc, js = kept[1], kept[2]          # the stored decision stands, town included
            placed = T.place(spec, site, "entity", jc, js)
            if placed is not None:
                _set(job, kept[0], placed[1], placed[2])
                return "kept"
    if how == "unknown":
        _s, _k, _w, raw = read(spec.system, job.title, job.description)
        if raw:
            d = UNKNOWN.setdefault(spec.system, {})
            d[raw[:80]] = d.get(raw[:80], 0) + 1
    return how


ROW_ERRORS: dict = {}     # system -> rows apply_facility raised on this run (each kept its name)


def apply_facilities(jobs) -> dict:
    """apply_facility over a run's rows. Returns {system: {how: rows}}. A row
    apply_facility raises on keeps the name and town it had (the label) and
    is counted ("error"); the pass goes on to the next row."""
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
                logger.warning(f"Systems B: one {job.hospital_system} row kept its name ({type(e).__name__}: {e})")
            how = "error"
        if how and how != "other":
            d = stats.setdefault(job.hospital_system, {})
            d[how] = d.get(how, 0) + 1
    return stats


def unknown_summary(top=4) -> str:
    parts = []
    for system, d in sorted(UNKNOWN.items()):
        vals = sorted(d.items(), key=lambda kv: -kv[1])[:top]
        parts.append(f"{system}: " + ", ".join(f"{v!r} {n}" for v, n in vals))
    return "; ".join(parts)


# ══════════════════════════════════════════════════════════════════════════
#  2. TOWN TABLES (facilities_town: the one hospital of the system in a town)
# ══════════════════════════════════════════════════════════════════════════
_h = facilities_town._h

# SolutionHealth: Elliot Health System (Manchester) and Southern New
# Hampshire Health (Nashua). Catholic Medical Center (Manchester) and St.
# Joseph Hospital (Nashua) are other systems' hospitals. The Workday site
# "Manchester-NH" names the town, so a row at the Manchester stamp is placed
# only when its site segment (or its text) says Manchester.
SOLUTION_TOWNS = facilities_town._spec(SOLUTION, [
    _h("Elliot Hospital", "Manchester", "NH", "cms 300012"),
    _h("Southern New Hampshire Medical Center", "Nashua", "NH", "cms 300020"),
], default=("Manchester", "NH"))

# Dartmouth Health: the member hospitals its own board posts for. Lebanon has
# two (Dartmouth Hitchcock Medical Center, CMS "Mary Hitchcock Memorial
# Hospital", and Alice Peck Day): a Lebanon row is named only when its title
# names one. Lebanon is also the stamp a row with no town takes. The names
# are the ones the database already uses for each hospital's own board.
DARTMOUTH_TOWNS = facilities_town._spec(DARTMOUTH, [
    _h("Dartmouth Hitchcock Medical Center", "Lebanon", "NH", "cms 300003"),
    _h("Alice Peck Day Memorial Hospital", "Lebanon", "NH", "cms 301305"),
    _h("New London Hospital", "New London", "NH", "cms 301304"),
    _h("Cheshire Medical Center", "Keene", "NH", "cms 300019"),
    _h("Mt. Ascutney Hospital", "Windsor", "VT", "cms 471302"),
    _h("Southwestern Vermont Medical Center", "Bennington", "VT", "cms 470012"),
], default=("Lebanon", "NH"))

# Duke Health: Durham holds Duke University Hospital and Duke Regional (named
# only when the title names one); Raleigh and Mooresville one hospital each.
# Cary is not listed: its CMS hospital is WakeMed's.
DUKE_TOWNS = facilities_town._spec(DUKE, [
    _h("Duke University Hospital", "Durham", "NC", "cms 340030"),
    _h("Duke Regional Hospital", "Durham", "NC", "cms 340155"),
    _h("Duke Raleigh Hospital", "Raleigh", "NC", "site (3400 Wake Forest Rd, Raleigh)"),
    _h("Duke Health Lake Norman Hospital", "Mooresville", "NC", "cms 340129"),
])


# ══════════════════════════════════════════════════════════════════════════
#  3. WORKDAY SITE TABLES (facilities_workday: the adapter's WD_SITE_FACILITY)
# ══════════════════════════════════════════════════════════════════════════
# Rochester Regional Health: named sites the phase 2 table left. The living
# centers are the system's own nursing homes (named communities, decision 2);
# the Sands-Constellation Center is Rochester General Hospital's critical care
# tower; the Lipson Cancer Institute sites by their own names. Each in the
# town of its own street. "SLH - GH Main" is Gouverneur Hospital's main
# building (as "SLH - GH Main 40"). Campuses, office buildings, home care
# offices and the dialysis and ElderONE sites (towns not on the posting) stay.
def _name_label_site(system, site, name, source) -> None:
    """A site the phase 2 table placed on the label ("-") that is a named
    community (decision 2 as the owner widened it in phase 3: named
    communities show their own name): the same town, now its own name."""
    tbl = facilities_workday.TABLES[system]
    k = facilities_workday.key(site)
    old = tbl.get(k)
    if old is None or old[0] not in (None, name):
        raise ValueError(f"{system}: {site!r} is not a label entry of the table ({old!r})")
    tbl[k] = (name, old[1], old[2], source)


# The two living centers on the Unity campus were "-" in phase 2 (DeMay
# Living Center already had its name): named communities, their own name.
_name_label_site(ROCHESTER, "Unity-Living-Center", "Unity Living Center",
                 "address 1565 Long Pond Rd, Rochester (the Unity campus)")
_name_label_site(ROCHESTER, "Park-Ridge-Living-Center", "Park Ridge Living Center",
                 "address 1555 Long Pond Rd, Rochester (the Unity campus)")
facilities_workday._t(ROCHESTER, """
Edna-Tina-Wilson-Living-Center                       | Edna Tina Wilson Living Center | Rochester | NY | address 900 Westfall Rd, Rochester
Lipson-Cancer-Institute---Rochester-General          | Lipson Cancer Institute - Rochester General | Rochester | NY | address 1425 Portland Ave, Rochester (RGH)
Sands-Constellation-Center-for-Critical-Care         | Rochester General Hospital | Rochester | NY | cms 330125 (the hospital's critical care tower)
SLH---GH-Main                                        | Gouverneur Hospital | Gouverneur | NY | cms 331315
Canandaigua-Ambulatory-Center                        | - | Canandaigua | NY | board (the site's name says the town)
""")

# Genesis HealthCare System (Zanesville OH): named sites. The town is the one
# every posting naming the site sits in ("rows"), or the one the name itself
# says ("board"). Office buildings (Medical Arts, Doctors Park, Northpointe),
# street addresses, the ambulance garage, the university and sites whose
# postings carry no town (Healthplex, Morrison House) stay.
facilities_workday._t(GENESIS, """
Genesis-Behavioral-Health-Center                     | Genesis Behavioral Health Center | Zanesville | OH | rows
Genesis-HealthCenter---Somerset                      | Genesis HealthCenter Somerset | Somerset | OH | rows; board
Genesis-SBHC---Caldwell                              | Genesis School-Based Health Center Caldwell | Caldwell | OH | rows; board
Genesis-Orthopedic-Center                            | Genesis Orthopedic Center | Zanesville | OH | rows
Genesis-Surgery-Center                               | Genesis Surgery Center | Zanesville | OH | rows
Genesis-East-Pointe-Health-Center                    | Genesis East Pointe Health Center | Zanesville | OH | rows
Genesis-Rheumatology-Care-Center                     | Genesis Rheumatology Care Center | Zanesville | OH | rows
Genesis-Primary-Care-Dresden                         | Genesis Primary Care Dresden | Dresden | OH | board
Genesis-Primary-Care-New-Lexington                   | Genesis Primary Care New Lexington | New Lexington | OH | board
Genesis-Primary-Care-Shawnee                         | Genesis Primary Care Shawnee | Shawnee | OH | board
Genesis-New-Concord-Pharmacy                         | Genesis New Concord Pharmacy | New Concord | OH | board
""")

# Titles naming an off-site service of the town's hospital ("Radiology
# Technologist - Off Site Radiology") or a practice building: no hospital.
_OFFSITE_TITLE = re.compile(r"\boff[\s-]?site\b|\boutpatient\s+(?:center|centre|clinic)\b|\bMOB\b", re.I)
for _sys in (SOLUTION, DARTMOUTH, DUKE):
    if _sys in facilities_town.CARE_SITE_BY_SYSTEM:
        raise ValueError(f"facilities_town already has care sites for {_sys}")
    facilities_town.CARE_SITE_BY_SYSTEM[_sys] = _OFFSITE_TITLE


# ══════════════════════════════════════════════════════════════════════════
#  4. TWO ORACLE SYSTEMS WHOSE LIVE WORK LOCATION IS BLANK (town tables)
# ══════════════════════════════════════════════════════════════════════════
# Cedars-Sinai and Franciscan Missionaries of Our Lady: phase 2 listed their
# label rows for the nightly's Oracle work location, but a live page of 100
# requisitions each (2026-10-29) carries no work location at all, so the
# nightly names none. Their hospitals each sit in a town of their own except
# Baton Rouge and Lafayette (two and three campuses: named only when the title
# names one). The names and towns are facilities_oracle_other's.
CEDARS = "Cedars-Sinai"
FMOL = "Franciscan Missionaries of Our Lady Health System"
CEDARS_TOWNS = facilities_town._spec(CEDARS, [
    _h("Cedars-Sinai Medical Center", "Los Angeles", "CA", "cms 050625"),
    _h("Cedars-Sinai Marina del Rey Hospital", "Marina Del Rey", "CA", "cms 050740"),
])
FMOL_TOWNS = facilities_town._spec(FMOL, [
    _h("St. Dominic Hospital", "Jackson", "MS", "cms 250048"),
    _h("Our Lady of the Lake Regional Medical Center", "Baton Rouge", "LA", "cms 190064"),
    _h("Our Lady of the Lake Children's Hospital", "Baton Rouge", "LA", "site (8300 Constantin Blvd, Baton Rouge)"),
    _h("Our Lady of Lourdes Regional Medical Center", "Lafayette", "LA", "cms 190102"),
    _h("Our Lady of Lourdes Women's & Children's Hospital", "Lafayette", "LA",
       "site (4600 Ambassador Caffery Pkwy, Lafayette)"),
    _h("Our Lady of Lourdes Heart Hospital", "Lafayette", "LA", "site (1105 Kaliste Saloom Rd, Lafayette)"),
    _h("St. Francis Medical Center", "Monroe", "LA", "cms 190125"),
    _h("Our Lady of the Angels Hospital", "Bogalusa", "LA", "cms 190312"),
    _h("St. Elizabeth Hospital", "Gonzales", "LA", "site (1125 W Hwy 30, Gonzales)"),
], default=("Baton Rouge", "LA"))
for _sys in (CEDARS, FMOL):
    facilities_town.CARE_SITE_BY_SYSTEM.setdefault(_sys, _OFFSITE_TITLE)


# ══════════════════════════════════════════════════════════════════════════
#  5. ORACLE WORK-LOCATION SPELLINGS THE PHASE 2 TABLES LACK (nightly only)
# ══════════════════════════════════════════════════════════════════════════
# A live page of 100 requisitions each (2026-10-29): Northwell's and
# Parkview's hospitals below came back on the label because their work
# location is not in facilities_oracle_other's table. Added before its Specs
# are built (facilities_oracle_other calls extend_oracle), so scrape_oracle
# names them from the list payload every night; the database does not store
# the work location, so these rows are named by the nightly, not by SQL.
ORACLE_EXTRA = {
    "Northwell Health": {"hospitals": {
        "Lenox Hill Hospital":        ("New York", "NY", "cms 330119"),
        "Phelps Hospital":            ("Sleepy Hollow", "NY", "cms 330261"),
        "Peconic Bay Medical Center": ("Riverhead", "NY", "cms 330107"),
    }, "sites": {
        "Lenox Hill Hosp": "Lenox Hill Hospital",
        "Lenox Hill Hospital": "Lenox Hill Hospital",
        "Phelps Memorial": "Phelps Hospital",
        "Phelps Hospital": "Phelps Hospital",
        "Peconic Bay Medical Center": "Peconic Bay Medical Center",
    }},
    "Parkview Health": {"hospitals": {
        "Parkview Wabash Hospital":   ("Wabash", "IN", "cms 151310"),
        "Parkview Whitley Hospital":  ("Columbia City", "IN", "cms 150101"),
        "Parkview Noble Hospital":    ("Kendallville", "IN", "cms 150146"),
    }, "sites": {
        "Parkview Wabash Hospital": "Parkview Wabash Hospital",
        "Parkview Whitley Hospital": "Parkview Whitley Hospital",
        "Parkview Noble Hospital": "Parkview Noble Hospital",
    }},
}


def extend_oracle(systems: dict) -> dict:
    """Add ORACLE_EXTRA to facilities_oracle_other's SYSTEMS (before its
    _build). All or nothing: a clean name already there must sit in the same
    town, a spelling already there must name the same hospital (ValueError
    otherwise, and `systems` is left as it was). Returns {system: added}."""
    staged, out = {}, {}
    for system, extra in ORACLE_EXTRA.items():
        src = systems.get(system)
        if src is None:
            raise ValueError(f"extend_oracle: {system} is not in facilities_oracle_other")
        hosp, sites = dict(src["hospitals"]), dict(src["sites"])
        added = 0
        for name, place in extra["hospitals"].items():
            old = hosp.get(name)
            if old is not None and tuple(old[:2]) != tuple(place[:2]):
                raise ValueError(f"extend_oracle: {system} {name!r} is in {old[0]}, not {place[0]}")
            if old is None:
                hosp[name] = place
                added += 1
        folded = {key(k): v for k, v in sites.items()}
        for raw, target in extra["sites"].items():
            if target not in hosp:
                raise ValueError(f"extend_oracle: {system} {raw!r} names unknown {target!r}")
            prev = folded.get(key(raw))
            if prev is not None and prev != target:
                raise ValueError(f"extend_oracle: {system} {raw!r} already names {prev!r}")
            sites[raw] = target
            folded[key(raw)] = target
        staged[system] = (hosp, sites)
        out[system] = added
    for system, (hosp, sites) in staged.items():
        systems[system].update(hospitals=hosp, sites=sites)
    return out
