"""The nightly shortfall, part A (2026-10-30, facility scale phase 6).

The owner, 2026-10-29: every hospital inside a multi-hospital system is named
and placed correctly on the site and in the dashboard. Decisions: (1) cards
and job pages show the HOSPITAL or site ("Apply to <site>"); the system name
and logo stay on the job page and the system hub (hospital_system is never
touched); (2) named sites (hospitals, named clinics and centres) show their
own name; generic offices, home-based and remote jobs, multi-site pools and
regional roles show the system name with the town; a town or a site is never
guessed.

The 10-30 nightly named fewer rows than phases 2 to 4 expected. For three
systems of this group the reason is a field the earlier modules waited for
the live feed to give, while the posting already carried it in a place that
is stored on the row (so the stored rows can be named now, and the nightly
names them on every night it reads the body):

  INTEGRIS Health (Oracle HCM, ertr CX_3001; 655 label rows, 0 named by the
      nightly). Phase 2 counted on the Oracle work location (workLocation
      LocationName, facilities_oracle_other). On this tenant every
      requisition's workLocation child comes back with all fields null (read
      2026-10-30: 167 of 200 rows of a page with one empty child, 33 with
      none), although the board's facet lists the names; so the list read
      names nothing. The posting's own lead sentence names the work site:
      "Join our team as a night shift, full-time, Registered Nurse at INTEGRIS
      Health Edmond, in Edmond, OK." It is in the list payload every night
      (ShortDescriptionStr) and opens the stored body. Read here: the phrase
      after "at", matched whole against the table below (no prefix, no fuzzy
      match). A clinic, medical group, float pool, office, institute or the
      bare system name names nothing; a clinic, office or remote title names
      nothing whatever the phrase says (facilities_hospital_systems_a's
      offsite_title, the TOWN family's rules).
  Prisma Health (Workday prismahealth.wd5; 1,043 label rows). Phase 3 left the
      shared towns (Greenville: Greenville Memorial and Patewood; Columbia:
      Richland, Baptist, Baptist Parkridge) to the detail payload's
      jobRequisitionLocation, re-read once every 5 nights
      (facilities_leftovers.reread_slot). Two things kept the first night
      short: (a) the re-read reaches a fifth of the rows a night by design;
      (b) Prisma's Midlands work sites are spelled "Richland", "Baptist",
      "Parkridge", "Tuomey" (2026-10-30 detail reads), not "Richland
      Hospital" and the like, so no Columbia row was named. The same work
      site is printed in the stored body ("Location" then the site, then
      "Facility", "Department"): read here with facilities_leftovers'
      own table and rules (detail_site / place), so the stored rows and the
      nightly agree: a hospital names the row; a clinic, an office building,
      a practice or a street address keeps the system name and the town
      tables leave the row (the verdict the re-read gives).
      The Laurens County Hospital campus: the board files its postings at
      Laurens, SC; the hospital's own street address is in Clinton
      (CMS 420038). A posting at Laurens whose work site is "Laurens County
      Medical Campus" takes the hospital and keeps its own town (Laurens).
  Broward Health (Phenom; 182 label rows). The body opens with the business
      unit; "Children's Diagnostic & Treatment Center" is a named centre
      (decision 2: its own name). The other first lines (Broward Health
      Corporate ISC, Physician Group, Corporate Spectrum, Broward Health
      Point) are offices, practices and community programmes: they keep the
      system name.

Nights without a body (scraper._keep_stored_body sends a blank): INTEGRIS's
list sentence names the row every night; a row named from its body whose
list sentence is empty keeps its stored name (load_known_facilities, the
facilities_hospital_systems_a pattern). Prisma's stored decision is kept by
facilities_town.STORED.

hospital_system, job_id, url and slug are never touched.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time

import facilities_hospital_systems_a as HSA
import facilities_leftovers as LO
import facilities_text as T
import facilities_town as TOWN
import sb_keys  # the DATA project key, one reader for the repo

logger = logging.getLogger("scraper.facility_fac6a")

S = T.S
INTEGRIS = "INTEGRIS Health"
BROWARD = "Broward Health"
PRISMA = "Prisma Health"
_OKC = "Oklahoma City"


# ═══════════════════════════════ INTEGRIS ═══════════════════════════════════
# The hospitals the lead sentences name (2026-10-30: the 655 label rows'
# stored bodies and a 200-row list page). Towns: "cms <id>" = the CMS
# hospital list in the database (public.hospitals); the names are the ones
# the postings print (facilities_oracle_other already writes the first two
# and Jim Thorpe this way).
_INTEGRIS_SITES = (
    S("INTEGRIS Health Baptist Medical Center", _OKC, "OK", "cms 370028",
      "INTEGRIS Baptist Medical Center", "INTEGRIS Health Baptist Medical Center Hospital",
      # an inpatient unit of the hospital, as the sentence names it
      "INTEGRIS Health Baptist Cardiac Critical Care", "INTEGRIS Health Baptist Intermediate Care Unit",
      "INTEGRIS Health Baptist Mother/Baby", "INTEGRIS Health Baptist Trauma Med/Surg",
      "INTEGRIS Health Baptist Cardiac Transplant"),
    S("INTEGRIS Health Southwest Medical Center", _OKC, "OK", "cms 370106",
      "INTEGRIS Southwest Medical Center", "INTEGRIS Health Southwest Medical Center Cardiovascular Care Unit"),
    S("INTEGRIS Jim Thorpe Rehabilitation", _OKC, "OK", "address 4219 S Western Ave, Oklahoma City",
      "INTEGRIS Health Jim Thorpe Rehabilitation", "INTEGRIS Jim Thorpe Rehabilitation Hospital"),
    S("INTEGRIS Health Baptist Medical Center Portland Avenue", _OKC, "OK", "cms 370032",
      "INTEGRIS Baptist Medical Center Portland Avenue", "INTEGRIS Health Baptist Medical Center Portland Ave",
      "INTEGRIS Health Baptist Medical Center on Portland Avenue", "INTEGRIS Health Portland Avenue",
      "INTEGRIS Health Portland Avenue Hospital", "INTEGRIS Health Baptist Portland Ave",
      "INTEGRIS Health Baptist Portland Ave Hospital", "INTEGRIS Health Baptist Portland Avenue",
      "INTEGRIS Health Portland Medical Center"),
    S("INTEGRIS Heart Hospital", _OKC, "OK", "address 3300 NW Expressway, Oklahoma City (the Baptist campus)",
      "INTEGRIS Health Heart Hospital", "INTEGRIS Heart Hospital at INTEGRIS Health Baptist Medical Center",
      "INTEGRIS Heart Hospital at INTEGRIS Baptist Medical Center"),
    S("INTEGRIS Health Lakeside Women's Hospital", _OKC, "OK", "cms 370199",
      "Lakeside Women's Hospital", "INTEGRIS Lakeside Women's Hospital"),
    S("INTEGRIS Community Hospital - Council Crossing", _OKC, "OK", "cms 370240",
      "INTEGRIS Health Council Crossing", "INTEGRIS Community Hospital Council Crossing",
      "INTEGRIS Health Council Crossing Hospital"),
    S("INTEGRIS Health Edmond Hospital", "Edmond", "OK", "cms 370236",
      "INTEGRIS Health Edmond", "INTEGRIS Edmond Hospital", "INTEGRIS Health Edmond Medical Center"),
    S("INTEGRIS Health Canadian Valley Hospital", "Yukon", "OK", "cms 370211",
      "INTEGRIS Health Canadian Valley", "INTEGRIS Canadian Valley Hospital",
      # the one INTEGRIS hospital in Yukon
      "INTEGRIS Health Yukon Hospital"),
    S("INTEGRIS Health Enid Hospital", "Enid", "OK", "cms 370016 (600 S Monroe, the former Bass Baptist Health Center)",
      "INTEGRIS Health Enid", "INTEGRIS Health Enid Medical Center Hospital", "INTEGRIS Health Bass Baptist Hospital",
      "INTEGRIS Bass Baptist Health Center"),
    S("INTEGRIS Health Ponca City Hospital", "Ponca City", "OK", "cms 370006",
      "INTEGRIS Health Ponca City"),
    S("INTEGRIS Health Woodward Hospital", "Woodward", "OK",
      "cms 370002 (900 17th St, Woodward; the CMS list still prints AllianceHealth Woodward)",
      "INTEGRIS Health Woodward", "INTEGRIS Woodward Medical Center"),
    S("INTEGRIS Health Miami Hospital", "Miami", "OK", "cms 370004",
      "INTEGRIS Health Miami", "INTEGRIS Miami Hospital"),
    S("INTEGRIS Health Grove Hospital", "Grove", "OK", "cms 370113",
      "INTEGRIS Health Grove", "INTEGRIS Grove Hospital"),
    # the psychiatric hospital campus at Spencer ("... Registered Nurse at
    # INTEGRIS Mental Health in Spencer, OK"): its own name with its town
    S("INTEGRIS Mental Health Spencer", "Spencer", "OK",
      "board: every posting naming INTEGRIS Mental Health sits in Spencer (2026-10-30)",
      "INTEGRIS Mental Health", "INTEGRIS Mental Health - Spencer", "INTEGRIS Health Mental Health Spencer"),
)


def _with_variants(sites):
    """Each spelling also as the sentence writes it with its town or its
    emergency department after it ("INTEGRIS Health Enid Hospital Enid",
    "INTEGRIS Health Baptist Medical Center ER"): exact strings, nothing
    fuzzy."""
    out = []
    for site, spellings in sites:
        base = (site.name,) + tuple(spellings)
        extra = []
        for sp in base:
            extra += [f"{sp} {site.city}", f"{sp} {site.city} {site.state}", f"{sp} ER", f"{sp} ED",
                      f"{sp} Emergency Department"]
        seen, allsp = set(), []
        for sp in tuple(spellings) + tuple(extra):
            k = T.key(sp)
            if k not in seen and k != T.key(site.name):
                seen.add(k)
                allsp.append(sp)
        out.append((site, tuple(allsp)))
    return tuple(out)


INTEGRIS_SPEC = T.Spec(INTEGRIS, None, (), _with_variants(_INTEGRIS_SITES))

# "Join our team as a ... at <site>, in <town>, OK."
_JOIN_RX = re.compile(r"\bjoin\s+our\s+team\b", re.I)
_AT_RX = re.compile(r"\bat\s+(?=(?:the\s+)?(?:INTEGRIS|Lakeside)\b)", re.I)
_CUT_RX = re.compile(r"\s*(?:[,;!?(]|\.(?:\s|$)|\s(?:in|located|based|within|serving|where|and\s+(?:is|are|will))\s)",
                     re.I)
_LEAD_CHARS = 1500


_TOWN_TAIL_RX = re.compile(r"^\s*,?\s*in\s+([A-Z][A-Za-z .'-]{1,40}?)\s*,?\s*(?:OK|Oklahoma)\b")
_SENT_END_RX = re.compile(r"\.(?:\s|$)|\n")
_ANOTHER_RX = re.compile(r"\b(?:INTEGRIS|Lakeside)\b", re.I)
# "at INTEGRIS Health Hospital, in Miami, OK": the system's hospital in a
# town where it has exactly one
_BARE_HOSPITAL = frozenset({T.key("INTEGRIS Health Hospital"), T.key("INTEGRIS Hospital")})


def integris_lead(text) -> tuple:
    """(phrase, town, several) of the posting's lead sentence: the words
    after the first "at INTEGRIS ..." / "at Lakeside ..." of the "Join our
    team" sentence, up to a comma, a full stop or "in <town>"; the town the
    sentence names right after it ("in Edmond, OK"), else ""; several =
    the rest of the sentence names another INTEGRIS site (a posting for
    several sites). ("", "", False) when there is no such sentence."""
    t = str(text or "")[:_LEAD_CHARS]
    m = _JOIN_RX.search(t)
    if not m:
        return "", "", False
    sent = t[m.start():m.start() + 600]
    a = _AT_RX.search(sent)
    if not a or _SENT_END_RX.search(sent[:a.start()]):
        return "", "", False             # no "at INTEGRIS", or it is past the sentence
    rest = sent[a.end():]
    c = _CUT_RX.search(rest)
    phrase = T.clean_text(rest[:c.start()] if c else rest).strip()
    phrase = re.sub(r"(?i)^the\s+", "", phrase)
    after = rest[c.start():] if c else ""
    e = _SENT_END_RX.search(after)
    after = after[:e.start()] if e else after
    tm = _TOWN_TAIL_RX.search(after)
    return phrase, (T.clean_text(tm.group(1)) if tm else ""), bool(_ANOTHER_RX.search(after))


def integris_phrase(text) -> str:
    """The site phrase of the posting's lead sentence ("" when none)."""
    return integris_lead(text)[0]


def _one_site_town(town):
    """The one INTEGRIS hospital in `town`, else None (none, or several)."""
    if not town:
        return None
    hits = {s.name: s for s in INTEGRIS_SPEC.names.values()
            if T.key(s.city) == T.key(town) and s.name != "INTEGRIS Mental Health Spencer"}
    return next(iter(hits.values())) if len(hits) == 1 else None


def integris_site(text, title=""):
    """(Site or None, what, value). what: "site", "offsite" (a clinic,
    office or remote title), "several" (the sentence lists two or more
    sites), "unknown" (a phrase no entry spells: a clinic, the medical
    group, an institute, an office, the bare system name), "none" (no lead
    sentence). value: the phrase, with " - <town>" when the sentence names
    the town right after it (facilities_text.place reads it)."""
    phrase, town, several = integris_lead(text)
    if not phrase:
        return None, "none", ""
    value = f"{phrase} - {town}" if town else phrase
    if several:
        return None, "several", value
    k = T.key(phrase)
    site = INTEGRIS_SPEC.table.get(k)
    if site is None and k in _BARE_HOSPITAL:
        site = _one_site_town(town)
    if site is None:
        return None, "unknown", value
    if HSA.offsite_title(title):
        return None, "offsite", value
    return site, "site", value


def integris_facility(text, title="", city="", state=""):
    """(hospital_name, city, state) or None for one INTEGRIS lead sentence
    (the list's ShortDescriptionStr or the body). The posting keeps its own
    town when it is the site's; a posting with no town, or whose sentence
    names the site's town ("at INTEGRIS Health Edmond Hospital in Edmond,
    OK" on a row the requisition filed at Oklahoma City), takes the site's
    town; any other row stays on the label (facilities_text.place)."""
    site, what, v = integris_site(text, title)
    if site is None:
        return None
    return T.place(INTEGRIS_SPEC, site, "site", city, state, v)


def oracle_lead_facility(system, j, city="", state=""):
    """scrape_oracle's hook, after the work location named nothing: the
    hospital the requisition's ShortDescriptionStr names, as
    (hospital_name, city, state), else None. Only INTEGRIS."""
    if system != INTEGRIS or not isinstance(j, dict):
        return None
    title = str(j.get("Title", j.get("title", "")) or "")
    return integris_facility(j.get("ShortDescriptionStr") or "", title, city, state)


# ═══════════════════════════════ BROWARD ════════════════════════════════════
_BROWARD_SITES = (
    S("Children's Diagnostic & Treatment Center", "Fort Lauderdale", "FL",
      "board: the posting's own first line; every Broward posting naming it sits in Fort Lauderdale",
      "Childrens Diagnostic & Treatment Center", "Children's Diagnostic and Treatment Center"),
)
BROWARD_SPEC = T.Spec(BROWARD, None, (), _BROWARD_SITES)


def broward_site(description):
    """(Site or None, what, first line): the body's first line, matched whole."""
    lines = [x for x in T.clean_text_lines(description) if x.strip()]
    if not lines:
        return None, "none", ""
    first = re.sub(r"\s*[#-]\s*\d{4,6}\s*$", "", lines[0]).strip()    # "... #31599", "... - 32235"
    site = BROWARD_SPEC.table.get(T.key(first))
    return (site, "site", first) if site else (None, "unknown", first)


# ═══════════════════════════════ PRISMA ═════════════════════════════════════
# The block the Workday template prints at the end of the body:
#   Location\n<work site>\n\nFacility\n<cost centre>\n\nDepartment\n...
_PRISMA_BLOCK_RX = re.compile(
    r"(?m)^[ \t]*Location[ \t]*:?[ \t]*\n+[ \t]*([^\n]+?)[ \t]*\n+(?:[ \t]*\n)*[ \t]*Facility[ \t]*:?[ \t]*$")
# A work site the board files at a town that is not the hospital's street
# town: {clean name: {town keys}} (the posting keeps its own town)
BOARD_TOWNS = {
    PRISMA: {"Prisma Health Laurens County Hospital": {TOWN.town_key("Laurens")}},
}


def prisma_location(description) -> str:
    """The work site the body's Location / Facility block names, or ""."""
    m = None
    for m in _PRISMA_BLOCK_RX.finditer(str(description or "")):
        pass                     # the template's block is the last one
    return T.clean_text(m.group(1)).strip() if m else ""


def prisma_site(system, description, city="", state="", title=""):
    """(verdict, (name, city, state) or None, work site). verdict: "site"
    (a hospital of facilities_leftovers' table, placed), "elsewhere" (a
    hospital in another town than the posting's), "office", "unknown",
    "none" (no block).

    2026-10-30 (release 6 review A): a remote, home, clinic, practice or
    office posting, by its title or its body's "Facility" cost centre, is
    "office", and a cost centre at another facility is "unknown"
    (facilities_leftovers.prisma_gate, the detail re-read's own rule). The
    body's line never moves a row's town: a row the board filed at the
    stamped default (Greenville) has a Workday location that is no town
    ("Richland-Saluda-Point", "HillcrestSimpsonville-Medical-Campus" in its
    link), which the body line may contradict; the detail re-read reads that
    location itself."""
    v = prisma_location(description)
    if not v:
        return "none", None, ""
    verdict, entry = LO.detail_site(system, v)
    if verdict != "site":
        return verdict, None, v
    gate = LO.prisma_gate(title, description, entry)
    if gate:
        return gate, None, v
    placed = LO.place(system, entry, city, state)
    if placed is None:
        towns = BOARD_TOWNS.get(PRISMA, {}).get(entry[0], set())
        jc, js = T.clean_text(city), (state or "").strip().upper()
        if jc and js == entry[2] and TOWN.town_key(jc) in towns:
            placed = (entry[0], jc, js)
    jc = T.clean_text(city)
    if placed is not None and jc and (TOWN.town_key(placed[1]), placed[2]) != (
            TOWN.town_key(jc), (state or "").strip().upper()):
        placed = None
    if placed is None:
        return "elsewhere", None, v
    return "site", placed, v


# ═══════════════════════════ applying it (finalize) ═════════════════════════
TEXT_SPECS = {INTEGRIS: INTEGRIS_SPEC, BROWARD: BROWARD_SPEC}
SYSTEMS = frozenset(TEXT_SPECS) | {PRISMA}
NO_BODY_CHARS = T.NO_BODY_CHARS
UNKNOWN: dict = {}       # system -> {phrase: rows} no entry names, this run

_KNOWN: dict = {}
KNOWN_PAGE = 1000
KNOWN_MAX_ROWS = 20000


def set_known_facilities(rows) -> int:
    """{(hospital_system, job_id): (stored hospital_name, city, state)} for
    INTEGRIS and Broward rows whose stored name is one of the sites here."""
    global _KNOWN
    out = {}
    for r in rows or []:
        spec = TEXT_SPECS.get((r.get("hospital_system") or "").strip())
        name, jid = T.clean_text(r.get("hospital_name")), r.get("job_id")
        if spec is None or jid in (None, "") or name not in spec.names:
            continue
        out[(spec.system, str(jid))] = (name, T.clean_text(r.get("city")), T.clean_text(r.get("state")).upper())
    _KNOWN = out
    return len(out)


def known_facility(system, job_id) -> str:
    return (_KNOWN.get(((system or "").strip(), str(job_id))) or ("",))[0]


def _kept_place(spec, job):
    """(name, city, state) to keep on a night with no text: the stored site,
    in the posting's own town when that is the site's, else in the stored
    town when that is the site's (the text named it on an earlier night:
    "at INTEGRIS Health Edmond Hospital in Edmond, OK" on a requisition the
    list files at Oklahoma City). None when nothing is stored."""
    kept = _KNOWN.get((spec.system, str(job.job_id)))
    if not kept:
        return None
    name, kc, ks = kept
    site = spec.names[name]
    placed = T.place(spec, site, "site", job.city, job.state)
    if placed is None and kc and ks == site.state and T.key(kc) == T.key(site.city):
        placed = (site.name, kc, ks)
    return (name, placed[1], placed[2]) if placed else None


def load_known_facilities() -> int:
    """Read-only PostgREST passes over the active INTEGRIS and Broward rows
    whose hospital_name is not the system label, keyset-paged on id. Without
    credentials the map stays empty and nothing is kept. Never raises."""
    sb_url = os.environ.get("SUPABASE_URL", "")
    sb_key = sb_keys.supabase_key()
    if not sb_url or not sb_key:
        set_known_facilities([])
        return 0
    import urllib.parse as _urlparse
    import urllib.request as _urlreq
    rows = []
    for system in sorted(TEXT_SPECS):
        flt = _urlparse.quote(f"eq.{system}", safe=".")
        nflt = _urlparse.quote(f"neq.{system}", safe=".")
        last, got = 0, 0
        try:
            while len(rows) < KNOWN_MAX_ROWS:
                u = (f"{sb_url.rstrip('/')}/rest/v1/hospital_jobs?select=id,hospital_system,job_id,hospital_name"
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
            logger.warning(f"Phase 6 A: {system} stored names stopped after {got:,} rows ({e}); "
                           f"its rows past that keep a site only on nights their text is read")
    try:
        return set_known_facilities(rows)
    except Exception as e:          # never let this read end the run
        logger.warning(f"Phase 6 A: stored names not installed ({e})")
        set_known_facilities([])
        return 0


def _set(job, name, city, state) -> None:
    job.hospital_name = name
    if (city, state) != ((job.city or "").strip(), (job.state or "").strip().upper()):
        job.city, job.state, job.location = city, state, f"{city}, {state}"


def _note(system, value) -> None:
    if value:
        d = UNKNOWN.setdefault(system, {})
        d[value[:80]] = d.get(value[:80], 0) + 1


def _apply_text(job, spec) -> str:
    cur = T.clean_text(job.hospital_name)
    if cur and cur != spec.system:
        return "other"
    body = job.description or ""
    if spec.system == INTEGRIS:
        site, what, v = integris_site(body, job.title)
        kind = "site"
    else:
        site, what, v = broward_site(body)
        kind = "site"
    if site is not None:
        placed = T.place(spec, site, kind, job.city, job.state, v)
        if placed is None:
            return "elsewhere"
        _set(job, *placed)
        return "text"
    if len(body.strip()) < NO_BODY_CHARS:
        kept = _kept_place(spec, job)
        if kept:
            _set(job, *kept)
            return "kept"
        return "no-body"
    if what == "unknown" and spec.system == INTEGRIS:
        _note(spec.system, v)
    return what


def _apply_prisma(job) -> str:
    spec = TOWN.SPECS.get(PRISMA)
    if spec is None or not spec.is_label(job.hospital_name):
        return "other"
    body = job.description or ""
    if len(body.strip()) < NO_BODY_CHARS:
        return "no-body"          # facilities_town keeps the stored decision
    verdict, placed, v = prisma_site(PRISMA, body, job.city, job.state, job.title)
    if verdict == "site":
        _set(job, *placed)
        return "body-site"
    if verdict == "office":
        # the same verdict the detail re-read gives on the nights it reads
        # this row (facilities_leftovers.apply_detail): the town tables leave it
        setattr(job, LO.SKIP_ATTR, True)
    elif verdict == "unknown":
        _note(PRISMA, v)
    return verdict


def apply_facility(job, canon=None) -> str:
    """One Job: "" for another system, else how ("text", "kept",
    "body-site", "elsewhere", "offsite", "office", "unknown", "none",
    "no-body", "other")."""
    system = (job.hospital_system or "").strip()
    if canon:
        system = canon(system)
    if system == PRISMA:
        return _apply_prisma(job)
    spec = TEXT_SPECS.get(system)
    if spec is None:
        return ""
    return _apply_text(job, spec)


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
    """apply_facility over a run's rows (finalize_jobs, after the adapters'
    hooks and before the town tables). Returns {system: {how: rows}}."""
    stats: dict = {}
    for job in jobs:
        how = _row_guard(apply_facility, job, canon)
        if how:
            system = canon(job.hospital_system) if canon else job.hospital_system
            d = stats.setdefault(system, {})
            d[how] = d.get(how, 0) + 1
    return stats


def unknown_summary(top=4) -> str:
    parts = []
    for system, d in sorted(UNKNOWN.items()):
        vals = sorted(d.items(), key=lambda kv: -kv[1])[:top]
        parts.append(f"{system}: " + "; ".join(f"{v} ({n})" for v, n in vals))
    return " | ".join(parts)


# ═══════════════════════ the stored-row change list ═════════════════════════
def stored_change(row):
    """(new_hospital_name, new_city, new_state, source) for one stored row
    whose name is its system's label (Prisma: or its providers board label),
    or None. Read only from what hospital_jobs stores, through the same
    readers the nightly runs."""
    system = (row.get("hospital_system") or "").strip()
    body, title = row.get("description") or "", row.get("title") or ""
    city, state = row.get("city") or "", row.get("state") or ""
    name = T.clean_text(row.get("hospital_name"))
    if system == INTEGRIS and name == INTEGRIS:
        site, what, v = integris_site(body, title)
        if site is None:
            return None
        placed = T.place(INTEGRIS_SPEC, site, "site", city, state, v)
        return (*placed, f"posting lead sentence: at {v} ({site.source})") if placed else None
    if system == BROWARD and name == BROWARD:
        site, what, v = broward_site(body)
        if site is None:
            return None
        placed = T.place(BROWARD_SPEC, site, "site", city, state, v)
        return (*placed, f"posting first line: {v} ({site.source})") if placed else None
    if system == PRISMA:
        spec = TOWN.SPECS.get(PRISMA)
        if spec is None or not spec.is_label(name):
            return None
        verdict, placed, v = prisma_site(PRISMA, body, city, state, title)
        if verdict != "site":
            return None
        return (*placed, f"posting work site (Location line): {v}")
    return None
