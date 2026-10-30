"""Label variants: rows whose name stands for the whole system but is not the
system's own label (2026-10-30, facility scale phase 7, variants group).

The owner, 2026-10-29: every hospital inside a multi-hospital system is named
and placed correctly on the site and in the dashboard. Decisions: (1) cards
and job pages show the HOSPITAL or site ("Apply to <site>"); the system name
and logo stay on the job page and the system hub (hospital_system is never
touched); (2) named sites (hospitals, named clinics and centres) show their
own name; generic offices, home-based and remote jobs, multi-site pools and
regional roles show the system name with the town; a town or a site is never
guessed.

Phases 2 to 6 measured what was left by rows whose hospital_name equals
hospital_system. A row filed under a VARIANT of the system's name never
counted: the adapter's emit name ("Ascension Health" under Ascension, "HSHS
Hospitals" under HSHS, "Intermountain Health (IMH)" under Intermountain
Healthcare), an entity that stands for a group of the system's hospitals
("UVA Community Health" under UVA Health) or a raw feed value ("N/A", 232
CHRISTUS rows). The sweep of 2026-10-30 (claud_outputinput/
facility_scale_2026-10-29/phase7/variants_ranked.tsv) ranks every such name
by active rows; this module builds the fixes for the top ones the other
modules did not already read (Intermountain is the imh builder's):

  Placeholders (every system). A feed value that names nothing ("N/A",
      "Unknown", "None", "TBD", "-") is no site: the row takes the system's
      own label, as every other label row of its system (normalize_jobs; the
      CHRISTUS feed's business unit is "N/A" on 232 rows).
  HSHS (Workday hshs; 416 of 433 active rows on "HSHS Hospitals"). The
      Workday site is a town ("Springfield-IL", "Green-Bay-WI"): the TOWN
      family's rules give the one HSHS hospital of the town (clinics,
      offices, remote and multi-site postings keep the label; Green Bay's two
      hospitals only when the posting names one, as its "Facility:" line
      often does). Registered in facilities_town.
  UVA Health (Phenom; 257 rows on "UVA Community Health", the entity of its
      three community hospitals and their clinics). Manassas, Culpeper and
      Haymarket each hold one UVA hospital; Gainesville, Warrenton and Bristow
      hold none (the rows keep the label). Registered in facilities_town.
  Ascension (Phenom over Oracle; 1,414 rows on "Ascension Health").
      facility_names reads the body's "Facility:" line for a hospital its
      table lists; the rows left on the label carry a line that names a
      hospital it does not list or spells otherwise ("Seton Medical Ctr
      Austin", "Ascension Williamson", "Ascension Seton Cedar Park"), a
      "Hospital:" line it does not read ("Hospital: Ascension Sacred Heart
      Hospital Pensacola"), or a named clinic, centre or community (decision
      2: "Ascension Via Christi West ER", "Alexian Village of Tennessee",
      "Ascension St. Vincent's Primary Care - Nocatee"). Read here: the
      "Facility Name:", "Facility:" or "Hospital:" line, its value matched
      whole against facility_names' hospitals and the table below (exact
      spelling after case and punctuation; facilities_text's reader, so a
      value naming two sites names none). The table lists the values two or
      more stored postings used (reviewed one by one); a medical group, a
      practice group, an office or medical office building, an imaging or
      outpatient service without a name of its own, a list of sites, a
      programme and a value whose postings sit in several towns name nothing.
      The row keeps its own town; a posting in another town than the site's
      stays on the label (facilities_text.place: no town is guessed).

Nights without a body: the town tables keep their stored decision
(facilities_town.STORED); an Ascension row whose stored name is one of this
table's sites keeps it (load_known_facilities); facility_names keeps the
hospitals it lists (its own _KNOWN reads any stored hospital of its table).

hospital_system, job_id, url and slug are never touched. Shared with the
stored-row change list (claud_outputinput/facility_scale_2026-10-29/phase7/
variants_work/changes_variants.py), so the database holds what the nightly
writes.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time

import facilities_text as T
import facilities_town as FT
import facility_names as FN
import sb_keys  # the DATA project key, one reader for the repo

logger = logging.getLogger("scraper.facility_fac7_variants")

S = T.S
_h = FT._h

# ── Placeholders ────────────────────────────────────────────────────────────
PLACEHOLDER_RX = re.compile(
    r"^\s*(?:n\s*/\s*a|n\.a\.?|na|none|null|nil|unknown|undefined|tbd|tba|not\s+(?:specified|applicable|available)|"
    r"-+|\.+|\?+)\s*$", re.I)


def is_placeholder(value) -> bool:
    """True for a feed value that names nothing ("N/A", "Unknown", "-")."""
    return bool(PLACEHOLDER_RX.match(str(value or ""))) and str(value or "").strip() != ""


def normalize_jobs(jobs) -> dict:
    """A row whose hospital_name is a placeholder takes its system's own label
    (hospital_system as the adapter stamps it). Returns {system: rows}."""
    out: dict = {}
    for job in jobs:
        try:
            if is_placeholder(job.hospital_name) and (job.hospital_system or "").strip():
                job.hospital_name = job.hospital_system
                out[job.hospital_system] = out.get(job.hospital_system, 0) + 1
        except Exception:
            continue
    return out


# ── HSHS and UVA Health: town tables (facilities_town) ──────────────────────
HSHS = "HSHS"
UVA = "UVA Health"

HSHS_TOWNS = FT._spec(HSHS, [
    # hshs.org/locations (read 2026-10-30): the 13 hospitals; CMS ids from the
    # database's hospital list. Sacred Heart (Eau Claire) and St. Joseph's
    # (Chippewa Falls) closed in 2024: their towns map to none. HSHS
    # Rehabilitation Hospital (Green Bay) is Lifepoint's joint venture and is
    # named by facilities_hospital_systems_a.
    _h("HSHS St. John's Hospital", "Springfield", "IL", "cms 140053"),
    _h("HSHS St. Mary's Hospital", "Decatur", "IL", "cms 140166"),
    _h("HSHS Good Shepherd Hospital", "Shelbyville", "IL", "cms 141354"),
    _h("HSHS St. Francis Hospital", "Litchfield", "IL", "cms 141350"),
    _h("HSHS Holy Family Hospital", "Greenville", "IL", "cms 140137"),
    _h("HSHS St. Joseph's Hospital Breese", "Breese", "IL", "cms 140145"),
    _h("HSHS St. Joseph's Hospital Highland", "Highland", "IL", "cms 141336"),
    _h("HSHS St. Anthony's Memorial Hospital", "Effingham", "IL", "cms 140032"),
    _h("HSHS St. Elizabeth's Hospital", "O'Fallon", "IL", "cms 140187"),
    _h("HSHS St. Vincent Hospital", "Green Bay", "WI", "cms 520075"),
    _h("HSHS St. Mary's Hospital Medical Center", "Green Bay", "WI", "cms 520097"),
    _h("HSHS St. Nicholas Hospital", "Sheboygan", "WI", "cms 520044"),
    _h("HSHS St. Clare Memorial Hospital", "Oconto Falls", "WI", "cms 521310"),
], emit=("HSHS Hospitals",), default=("Springfield", "IL"), sites=[
    # the children's hospitals inside St. John's and St. Vincent: named only
    # when the posting names them
    ("HSHS St. John's Children's Hospital", "Springfield", "IL",
     ("St. John's Children's Hospital", "St. Johns Childrens Hospital")),
    ("HSHS St. Vincent Children's Hospital", "Green Bay", "WI",
     ("St. Vincent Children's Hospital", "St. Vincent Childrens Hospital")),
])

UVA_TOWNS = FT._spec(UVA, [
    # CMS ids from the database's hospital list; the community hospitals'
    # names as the postings print them ("UVA Health Prince William Medical
    # Center": CMS still lists 490045 as Novant Prince William Medical Center,
    # UVA's since 2024). Charlottesville holds two: it maps to none.
    _h("UVA Medical Center", "Charlottesville", "VA", "cms 490009"),
    _h("UVA Transitional Care Hospital", "Charlottesville", "VA", "cms 492011"),
    _h("UVA Health Culpeper Medical Center", "Culpeper", "VA", "cms 490019"),
    _h("UVA Health Haymarket Medical Center", "Haymarket", "VA", "cms 490144"),
    _h("UVA Health Prince William Medical Center", "Manassas", "VA", "cms 490045"),
], labels=("UVA Community Health",))

TOWN_SYSTEMS = frozenset({HSHS, UVA})
# facilities_town.EMIT is built when that module loads; a table registered
# later adds its emit names itself (the tests check EMIT[emit] is the table)
for _spec_ in (HSHS_TOWNS, UVA_TOWNS):
    for _e in _spec_.emit:
        FT.EMIT[_e] = _spec_

# ── Ascension: the Facility / Facility Name / Hospital line ─────────────────
ASCENSION = "Ascension"
ASCENSION_LABELS = frozenset({"Ascension", "Ascension Health"})


def _fn(name, *spellings):
    """A hospital facility_names lists, under a spelling its resolver misses:
    its proper name and town, from facility_names' own table."""
    town, st = FN.spec_for(ASCENSION).hospitals[name]
    return S(name, town, st, "facility_names", *spellings)


_ASC_SITES = (
    # hospitals facility_names lists, spelled otherwise on the postings
    _fn("Ascension Seton Medical Center Austin", "Seton Medical Ctr Austin"),
    _fn("Ascension Seton Williamson", "Ascension Williamson"),
    _fn("Ascension St. John Jane Phillips", "Ascension Jane Phillips",
        "Ascension St. John Jane Phillips Medical Center"),
    _fn("Ascension Seton Smithville", "Seton Smithville Regional Hospital"),
    _fn("Ascension Alexian Brothers Rehabilitation Hospital", "Ascension Alexian Brothers Rehabilitation"),
    _fn("Ascension St. Vincent Women's Hospital",
        "Ascension St. Vincent Indianapolis (86th Street) - Women & Infants Hospital"),
    # hospitals facility_names does not list
    S("Ascension Seton Cedar Park", "Cedar Park", "TX", "cms 670043 (Cedar Park Regional Medical Center)",
      "Ascension Seton Cedar Park Medical Center"),
    S("Dell Children's Medical Center North", "Austin", "TX", "board"),
    S("Ascension Via Christi St. Peter", "Derby", "KS", "board",
      "Via Christi, St Peter", "Via Christi, St Peters", "Via Christi St. Peter"),
    S("Northwest Hills Surgical Hospital", "Austin", "TX", "cms 450808"),
    S("Wamego Health Center", "Wamego", "KS", "cms 171337"),
    S("Ascension St. John Jenks", "Jenks", "OK", "board"),
    # named centres, clinics and communities (decision 2): the posting's own
    # spelling, in the town every posting naming it sits in
    S("Ascension Via Christi West ER", "Wichita", "KS", "board"),
    S("Wauwatosa Ambulatory Center", "Wauwatosa", "WI", "board"),
    S("Sacred Heart Village", "Avilla", "IN", "board"),
    S("Franciscan Place", "Brookfield", "WI", "board"),
    S("Via Christi Village Manhattan", "Manhattan", "KS", "board"),
    S("Via Christi Village McLean", "Wichita", "KS", "board"),
    S("Via Christi Village Ridge", "Wichita", "KS", "board"),
    S("Alexian Village of Tennessee", "Signal Mountain", "TN", "board"),
    S("Alexian PACE", "Chattanooga", "TN", "board"),
    S("St. Vincent PACE", "Indianapolis", "IN", "board"),
    S("Ascension Living HOPE", "Wichita", "KS", "board"),
    S("Providence Village", "Waco", "TX", "board"),
    S("Carroll Manor", "Washington", "DC", "board"),
    S("Nazarethville Place", "Des Plaines", "IL", "board"),
    S("Bethlehem Woods", "La Grange Park", "IL", "board"),
    S("Resurrection Village Life Center", "Chicago", "IL", "board"),
    S("Resurrection Village", "Chicago", "IL", "board"),
    S("Ascension Living St. Francis Place", "Milwaukee", "WI", "board", "St. Francis Place", "Saint Francis Place"),
    S("Ascension St. Vincent Lucas Family Brain and Spine", "Indianapolis", "IN", "board"),
    S("Center for Advanced Medicine", "Evansville", "IN", "board"),
    S("Ascension St. Vincent Evansville Cancer Center", "Newburgh", "IN", "board"),
    S("Harcourt Rd Primary Care", "Indianapolis", "IN", "board"),
    S("AMG Providence Women's Health Clinic", "Waco", "TX", "board"),
    S("Providence Ortho & Sports", "Waco", "TX", "board"),
    S("St. Elizabeth Ann Seton Dental Clinic", "Milwaukee", "WI", "board"),
    S("North Ave Clinic", "Milwaukee", "WI", "board"),
    S("AMG Reiman Cancer Center", "Franklin", "WI", "board"),
    S("AMG Waukesha Primary Care", "Waukesha", "WI", "board"),
    S("Ascension Little Chute Clinic", "Little Chute", "WI", "board"),
    S("Ascension Calumet Clinic", "Chilton", "WI", "board", "Ascension Medical Group Calumet Clinic"),
    S("Greenfield Clinic", "Greenfield", "WI", "board"),
    S("River Woods Outpatient Center", "Glendale", "WI", "board"),
    S("117th Street Clinic", "Wauwatosa", "WI", "board"),
    S("Ascension Center for Mental Health", "Arlington Heights", "IL", "board"),
    S("Ascension Saint Agnes Cancer Institute", "Baltimore", "MD", "board"),
    S("Via Christi Cancer Center", "Wichita", "KS", "board"),
    S("Ascension Via Christi Occupational Health Center", "Manhattan", "KS", "board"),
    S("Ascension Seton Health Center - Lamar", "Austin", "TX", "board"),
    S("Seton Lockhart Health Center", "Lockhart", "TX", "board"),
    S("Park Valley Ortho", "Round Rock", "TX", "board"),
    S("Ascension DePaul Services", "New Orleans", "LA", "board"),
    S("Ascension Sacred Heart Rehab Grande Drive", "Pensacola", "FL", "board"),
    S("Ascension Sacred Heart Primary Care - South Walton", "Destin", "FL", "board"),
    S("Ascension St. Vincent's Aberdeen Primary Care", "Fruit Cove", "FL", "board"),
    S("Ascension St. Vincent's Primary Care - Nocatee", "Jacksonville", "FL", "board"),
    S("Ascension St. Vincent's Primary Care - Plantation Plaza", "Jacksonville", "FL", "board"),
    S("Ascension St. Vincent's Primary Care Fleming Island", "Fleming Island", "FL", "board"),
    S("Ascension St. Vincent's St. John's Cardiology", "Saint Johns", "FL", "board"),
    S("Howell Allen Clinic", "Nashville", "TN", "board"),
    S("Holy Family Health Center", "Nashville", "TN", "board"),
    S("St. Louise Family Medical Center", "Murfreesboro", "TN", "board"),
)

ASCENSION_SPEC = T.Spec(ASCENSION, ("St. Louis", "MO"),
                        (T.Read("label", r"Facility\s+Name|Facility|Hospital", "site"),), _ASC_SITES)
NO_BODY_CHARS = T.NO_BODY_CHARS
UNKNOWN: dict = {}       # system -> {value: rows} no entry names, this run


def ascension_value(description) -> str:
    """The first Facility Name / Facility / Hospital line's value, or ""."""
    lines = T._lines(description)
    v = T._label_value(ASCENSION_SPEC.rx[ASCENSION_SPEC.reads[0].arg], lines)
    return (v or "").strip()


def ascension_site(description, city="", state=""):
    """(name, city, state, value) for one Ascension body whose site line names
    a hospital facility_names lists or a site of the table here, placed in the
    row's town (facilities_text.place), or (None, None, None, value) when the
    line names none, the posting sits elsewhere, or there is no line."""
    v = ascension_value(description)
    if not v:
        return None, None, None, ""
    hit = FN.resolve(ASCENSION, v, city, state)
    if hit:
        name, town, st = hit
        site = T.Site(name, town, st, "facility_names", ())
    else:
        site = T.match_value(ASCENSION_SPEC, v)
        if site is None:
            return None, None, None, v
    placed = T.place(ASCENSION_SPEC, site, "site", city, state, v)
    if placed is None:
        return None, None, None, v
    return placed[0], placed[1], placed[2], v


# ── Stored names, for nights without a body ─────────────────────────────────
_KNOWN: dict = {}
KNOWN_PAGE = 1000
KNOWN_MAX_ROWS = 20000


def set_known_facilities(rows) -> int:
    """{job_id: (stored hospital_name, city, state)} for Ascension rows whose
    stored name is one of the table's sites."""
    global _KNOWN
    out = {}
    for r in rows or []:
        if (r.get("hospital_system") or "").strip() != ASCENSION:
            continue
        name, jid = T.clean_text(r.get("hospital_name")), r.get("job_id")
        if jid in (None, "") or name not in ASCENSION_SPEC.names:
            continue
        out[str(jid)] = (name, T.clean_text(r.get("city")), T.clean_text(r.get("state")).upper())
    _KNOWN = out
    return len(out)


def known_facility(job_id) -> str:
    return (_KNOWN.get(str(job_id)) or ("",))[0]


def load_known_facilities() -> int:
    """Read-only PostgREST pass over the active Ascension rows whose
    hospital_name is not a label, keyset-paged on id (the facilities_fac6a
    pattern). Without credentials the map stays empty. Never raises."""
    sb_url = os.environ.get("SUPABASE_URL", "")
    sb_key = sb_keys.supabase_key()
    if not sb_url or not sb_key:
        set_known_facilities([])
        return 0
    import urllib.parse as _urlparse
    import urllib.request as _urlreq
    rows = []
    flt = _urlparse.quote(f"eq.{ASCENSION}", safe=".")
    nflt = _urlparse.quote("not.in.(\"Ascension\",\"Ascension Health\")", safe=".()\",")
    last, got = 0, 0
    try:
        while len(rows) < KNOWN_MAX_ROWS:
            u = (f"{sb_url.rstrip('/')}/rest/v1/hospital_jobs?select=id,hospital_system,job_id,hospital_name,city,state"
                 f"&is_active=is.true&hospital_system={flt}&hospital_name={nflt}"
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
        logger.warning(f"Phase 7 variants: Ascension stored names stopped after {got:,} rows ({e}); "
                       f"its rows past that keep a site only on nights their body is read")
    try:
        return set_known_facilities(rows)
    except Exception as e:          # never let this read end the run
        logger.warning(f"Phase 7 variants: stored names not installed ({e})")
        set_known_facilities([])
        return 0


# ── Run time ────────────────────────────────────────────────────────────────
def _set(job, name, city, state) -> None:
    job.hospital_name = name
    if (city, state) != ((job.city or "").strip(), (job.state or "").strip().upper()):
        job.city, job.state, job.location = city, state, f"{city}, {state}"


def _note(value) -> None:
    if value:
        d = UNKNOWN.setdefault(ASCENSION, {})
        d[value[:80]] = d.get(value[:80], 0) + 1


def _kept_place(job):
    kept = _KNOWN.get(str(job.job_id))
    if not kept:
        return None
    name, kc, ks = kept
    site = ASCENSION_SPEC.names[name]
    placed = T.place(ASCENSION_SPEC, site, "site", job.city, job.state)
    if placed is None and kc and ks == site.state and T.key(kc) == T.key(site.city):
        placed = (site.name, kc, ks)
    return (name, placed[1], placed[2]) if placed else None


def apply_facility(job, canon=None) -> str:
    """One Job: "" for another system, else how ("text", "kept", "elsewhere",
    "unknown", "none", "no-body", "other")."""
    system = (job.hospital_system or "").strip()
    if canon:
        system = canon(system)
    if system != ASCENSION:
        return ""
    if T.clean_text(job.hospital_name) not in ASCENSION_LABELS:
        return "other"
    body = job.description or ""
    if len(body.strip()) < NO_BODY_CHARS:
        kept = _kept_place(job)
        if kept:
            _set(job, *kept)
            return "kept"
        return "no-body"
    name, city, state, v = ascension_site(body, job.city, job.state)
    if name:
        _set(job, name, city, state)
        return "text"
    if not v:
        return "none"
    if T.match_value(ASCENSION_SPEC, v) is not None or FN.resolve(ASCENSION, v, "", ""):
        return "elsewhere"
    _note(v)
    return "unknown"


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


def apply_facilities(jobs, canon=None) -> dict:
    """apply_facility over a run's rows (finalize_jobs, after facility_names
    and before the town tables). Returns {system: {how: rows}}."""
    stats: dict = {}
    for job in jobs:
        how = _row_guard(apply_facility, job, canon)
        if how:
            d = stats.setdefault(ASCENSION, {})
            d[how] = d.get(how, 0) + 1
    return stats


def unknown_summary(top=6) -> str:
    parts = []
    for system, d in sorted(UNKNOWN.items()):
        vals = sorted(d.items(), key=lambda kv: -kv[1])[:top]
        parts.append(f"{system}: " + "; ".join(f"{v} ({n})" for v, n in vals))
    return " | ".join(parts)


# ═══════════════════════ the stored-row change list ═════════════════════════
def stored_change(row):
    """(new_hospital_name, new_city, new_state, source) for one stored row of
    this module's variants, or None. Read only from what hospital_jobs stores,
    through the same readers the nightly runs."""
    system = (row.get("hospital_system") or "").strip()
    name = T.clean_text(row.get("hospital_name"))
    city, state = row.get("city") or "", row.get("state") or ""
    if is_placeholder(row.get("hospital_name")) and system:
        return system, T.clean_text(city), T.clean_text(state).upper(), \
            f"feed placeholder {row.get('hospital_name')!r}: the system's own label"
    if system == ASCENSION and name in ASCENSION_LABELS:
        new, nc, ns, v = ascension_site(row.get("description") or "", city, state)
        if not new:
            return None
        return new, nc, ns, f"posting site line: {v}"
    if system in TOWN_SYSTEMS:
        new, nc, ns, how = FT.decide(system, row.get("hospital_name"), city, state, row.get("title") or "",
                                     row.get("description") or "", row.get("url") or "")
        if not new:
            return None
        return new, nc, ns, f"town table ({how})"
    return None
