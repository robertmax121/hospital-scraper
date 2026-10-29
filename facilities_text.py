"""The hospital a posting's own text names, for systems whose feed names only
the system (2026-10-29, the description-text family).

The owner, 2026-10-29: every hospital inside a multi-hospital system is named
and placed correctly on the site and in the dashboard, and applies are
retro-assigned to the right hospital ("any multi system hospital is subject
to this"). His decisions: (1) cards and job pages show the HOSPITAL, the
system name and logo stay on the job page and the system hub; (2) named sites
(hospitals, named clinics and centers) show their own name, generic offices,
home-based and remote jobs show the system name with the town; never guess a
town.

These systems' feeds stamp every posting with the system's label, but the
posting's own body names the site, in one fixed place per system:

  UNC Health         "Entity: UNC REX Healthcare"
  Bayhealth          "Location: Kent Campus Hospital"
  Southcoast Health  "Location: St. Luke's Hospital - New Bedford, MA"
  Kaleida Health     "Location: Buffalo General Medical Center"
  Cook County Health "LOCATION: John H. Stroger, Jr. Hospital"
  Hawaii HHSC        "LOCATION : KONA COMMUNITY HOSPITAL | Respiratory Therapy"
  CoxHealth          "Facility:" then "CoxHealth South: 3801 S National Ave, ..."
  Lee Health         "Location: Cape Coral Hospital - 636 Del Prado Blvd ..."
  NCH                "Location: 350 7th Street North, Naples, FL, 34102"
  Premier Health     "Facility: Miami Valley Hospital North"
  ... (SPECS below, one entry per system)

How a body is read (read_text): each system lists where to look, in order:
  label   the first line "<Label>: <value>" (or "<Label>" alone with the
          value on the next line); the value is cut at the first ";" (what
          follows is a note: "...; Includes coverage at Tobey") and looked
          up whole, then each prefix ending at a separator ("John H.
          Stroger, Jr. Hospital (Medical/Surgical - 6E Unit)"), then each
          segment between separators ("Operating Room - Tobey Hospital -
          Wareham, MA"). Separators: " - ", ",", ";", ":", "|", "(", ")",
          "/", "@", a period before a street number ("Duke University
          Hospital. 2301 Erwin Rd"), a hyphen between a lower-case and an
          upper-case letter ("Diagnostic Imaging-St. Anthony's Hospital")
          and a hyphen before a closing code ("Med Surg Unit I-WHH").
  code    the same line, its first word only (Monument's cost-centre code
          "RCH PCU-Nursing").
  first   the first non-empty line(s), whole or before " | " (MaineHealth's
          Infor bodies open with the entity: "Mid Coast Hospital").
  lead    the first few non-empty lines, one that STARTS with a site's
          spelling ("Aspirus Rhinelander Hospital in Rhinelander, WI is
          seeking", "St. Mary's Medical Center is seeking").
Every lookup is an exact match of the folded spelling (case, punctuation,
"Saint"/"St", "&"/"and" aside) against the system's table; nothing is fuzzy.
A value that segments into two different sites names none. A value the table
does not know names none (and is counted in the run log, so it can be added).

Each system's table (SPECS): source spelling or code -> Site(clean public
name, city, state, source of the town). The town source is "cms <id>" (the
database's CMS hospital list), "board" (the posting line itself names that
town, or every posting naming the site sits in it), or "address <street>"
(the site's published street address).

Where the row is placed (place): the posting keeps its own town when it is
one of the site's own towns (a campus town listed in the table may carry its
campus name: "Maine Medical Center" at Biddeford is MaineHealth Maine Medical
Center Biddeford). The table's town is written when the posting has no town
or sits at the system default town (the head office the scraper stamps when a
feed gives none); on a site line (Location / Facility: where the job is) also
when the line itself names the site's town ("Charlton Memorial Hospital -
Fall River, MA" on rows stored at New Bedford); on Hawaii's lines always (its
rows sat at an island, "Big Island", "Kauai", "Oahu", not a town). Any other
posting elsewhere stays on the system label with its own town: an entity
line (an Entity line, the first line, a lead sentence, a cost-centre code:
who employs, not where) on a posting in another town is a clinic or office of
that entity (the owner's rule 2), and a site line that disagrees with the
row's town is not trusted over it. No town is guessed.

Nights without a body: a known body is sent blank (scraper._keep_stored_body)
and the enrichment trigger keeps the stored one, so on those nights the
hospital stored on the row is kept (load_known_facilities), as
facility_names does for its body-derived systems. A night whose own body
names no site leaves the label.

hospital_system is never touched (the upsert key, the hub, the filters, the
logo). Shared with the stored-row change list
(claud_outputinput/facility_scale_2026-10-29/phase2/text_work/changes.py),
so the database holds what the scraper keeps.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import NamedTuple

import sb_keys  # the DATA project key, one reader for the repo
from facility_names import clean_text, fkey as _fkey

logger = logging.getLogger("scraper.facility_text")


def key(s) -> str:
    """Lookup key: facility_names.fkey after "&" -> "and" ("Wheeler Heart &
    Vascular" and "Wheeler Heart and Vascular" are one spelling)."""
    return _fkey(clean_text(s).replace("&", " and "))


def lead_key(s) -> str:
    """key() with a possessive kept apart ("St. Mary's Medical Center is"
    and "Mount Sinai Hospital's Emergency Department" start with their
    site's spelling word for word)."""
    return key(re.sub(r"'s\b", " s", clean_text(s)))


class Site(NamedTuple):
    name: str          # the clean public name written to hospital_name
    city: str          # the site's own town
    state: str
    source: str        # where the town comes from: "cms <id>", "board", "address <street>"
    campuses: tuple    # ((town, state, campus name or ""), ...): other towns of the site


class Read(NamedTuple):
    mode: str          # "label", "code", "first", "lead"
    arg: object        # label regex alternation (label, code) or a line count (first, lead)
    kind: str          # "site" (where the job is), "site!" (and the row's own town is not a town), "entity" (who employs)


class Spec:
    def __init__(self, system, default, reads, sites, offices=(), canon=None, clinics=(), clinic_rx=None):
        self.system = system
        self.default = default          # (town, state) the scraper stamps when the feed gives none, or None
        self.reads = tuple(reads)
        self.table: dict[str, Site] = {}
        self.lead: list[tuple[str, Site]] = []
        self.names: dict[str, Site] = {}
        # 2026-10-29 (owner calls, "go with recs"): named clinics the postings
        # name in full (decision 2: a named clinic shows its own name), read
        # like the sites, and the marker of a site line that names a clinic or
        # an outpatient site (clinic_rx). With clinic_rx set, the adapter's own
        # hospital value (canon) names the hospital only where the posting is
        # at the hospital (_canon_site).
        self.clinic_names = frozenset(site.name for site, _sp in clinics)
        self.clinic_rx = re.compile(clinic_rx, re.I) if clinic_rx else None
        for site, spellings in tuple(sites) + tuple(clinics):
            self.names[site.name] = site
            for sp in (site.name,) + tuple(spellings):
                k = key(sp)
                old = self.table.get(k)
                if old is not None and old.name != site.name:
                    raise ValueError(f"{system}: spelling {sp!r} names {old.name!r} and {site.name!r}")
                self.table[k] = site
                self.lead.append((lead_key(sp), site))
            for _t, _s, cname in site.campuses:
                if cname:
                    self.names[cname] = site
        self.lead.sort(key=lambda kv: -len(kv[0]))
        self.offices = frozenset(key(o) for o in offices)
        # the adapter's own spellings of a site (BayCare's Infor list names it
        # "St Josephs"): one clean name, the row's town kept
        self.canon = {clean_text(k): self.names[v] for k, v in (canon or {}).items()}
        self.rx = {}
        for r in self.reads:
            if r.mode in ("label", "code"):
                self.rx[r.arg] = re.compile(
                    r"^[ \t*•\-]*(?:" + r.arg + r")[ \t]*(?::[ \t]*(.*?)|)[ \t]*$", re.I)


def S(name, city, state, source, *spellings, campuses=()):
    return Site(name, city, state, source, tuple(campuses)), spellings


# ── Reading a body ──────────────────────────────────────────────────────────
_SEP = re.compile(r"\s+[-–—]+\s+|\s*[,;:|()\[\]/]\s*|\s+@\s+|\.\s+(?=\d)|(?<=[a-z.])-(?=[A-Z])|(?<=\S)-(?=[A-Z]{2,6}\s*$)")
_LABELISH = re.compile(r"^[A-Za-z][A-Za-z /&#.'-]{0,40}:")
_LEAD_LINES_MAX = 6


def _lines(description) -> list[str]:
    return [ln.strip() for ln in clean_text_lines(description) if ln.strip()]


def clean_text_lines(description) -> list[str]:
    """The body's lines, each cleaned (facility_names.clean_text collapses
    newlines, so lines are split first)."""
    return [clean_text(ln) for ln in str(description or "").replace("\r", "\n").split("\n")]


def _label_value(rx, lines) -> str | None:
    """The value of the first line the label regex matches: on the line
    itself, else the next non-empty line (unless that is another label).
    None when no line matches."""
    for i, ln in enumerate(lines):
        m = rx.match(ln)
        if not m:
            continue
        v = (m.group(1) or "").strip()
        if v:
            return v
        for nx in lines[i + 1:i + 3]:
            nx = nx.strip()
            if nx:
                # another label with its value still to come ("Department:")
                return "" if _LABELISH.match(nx) and not nx.split(":", 1)[1].strip() else nx
        return ""
    return None


def _candidates(value) -> tuple[list[str], list[str]]:
    """(whole and prefixes, longest first; segments in order) of one value."""
    v = value.split(";")[0].strip()
    if not v:
        return [], []
    cuts = [m.start() for m in _SEP.finditer(v)]
    prefixes = [v[:c].strip() for c in sorted(set(cuts), reverse=True) if v[:c].strip()]
    segments = [s.strip() for s in _SEP.split(v) if s and s.strip()]
    return [v] + prefixes, segments


def match_value(spec, value):
    """The Site a value names, else None ("office" or "unknown" when it is
    a value of the system's own that names no site, for the run log)."""
    heads, segments = _candidates(value)
    hit = None
    for c in heads:
        site = spec.table.get(key(c))
        if site is not None:
            hit = site
            if c == heads[0]:
                return site          # the whole value: nothing else to weigh
            break
    seg_sites = {spec.table[key(s)].name: spec.table[key(s)] for s in segments if key(s) in spec.table}
    if hit is None and len(seg_sites) == 1:
        hit = next(iter(seg_sites.values()))
    if hit is not None and any(n != hit.name for n in seg_sites):
        return None                  # two sites in one value: names none
    return hit


def read_text(spec, description):
    """(Site or None, kind, what, raw value) for one body. what: "site"
    (a site named), "office" (a value the table lists as no site), "unknown"
    (a value naming no site the table knows), "none" (no line to read)."""
    lines = _lines(description)
    if not lines:
        return None, "", "none", ""
    seen_value = None
    for r in spec.reads:
        if r.mode in ("label", "code"):
            v = _label_value(spec.rx[r.arg], lines)
            if v is None or not v.strip():
                continue
            if r.mode == "code":
                site = spec.table.get(key(v.split()[0]))
            else:
                site = match_value(spec, v)
            if site is not None:
                return site, r.kind, "site", v
            seen_value = seen_value or v
        elif r.mode == "first":
            for ln in lines[:int(r.arg)]:
                v = ln.split(" | ")[0].strip()
                site = spec.table.get(key(v))
                if site is not None:
                    return site, r.kind, "site", ln
                seen_value = seen_value or v
        elif r.mode == "lead":
            for ln in lines[:min(int(r.arg), _LEAD_LINES_MAX)]:
                lk = lead_key(ln) + " "
                for k, site in spec.lead:
                    if lk.startswith(k + " "):
                        return site, r.kind, "site", ln
    if seen_value is None:
        return None, "", "none", ""
    if key(seen_value) in spec.offices or any(key(s) in spec.offices for s in _candidates(seen_value)[0]):
        return None, "", "office", seen_value
    return None, "", "unknown", seen_value


# ── Placing a row ───────────────────────────────────────────────────────────
def _at_default(spec, city, state) -> bool:
    if not spec.default:
        return False
    town, st = spec.default
    return bool((city or "").strip()) and key(city) == key(town) and (state or "").strip().upper() in ("", st)


def _names_town(spec, value, town) -> bool:
    """True when a segment of the line (before its first ";") other than a
    site's own spelling names the town as whole words ("Charlton Memorial
    Hospital - Fall River, MA" names Fall River; "Lee Memorial Hospital -
    2776 Cleveland Ave Fort Myers FL" names Fort Myers; "Duke Raleigh
    Hospital" alone names no town)."""
    if not value or not town:
        return False
    tk = f" {key(town)} "
    _heads, segments = _candidates(value)
    return any(key(x) not in spec.table and tk in f" {key(x)} " for x in segments)


def place(spec, site, kind, city, state, value=""):
    """(hospital_name, city, state) for a row whose text names `site`, or
    None when the row stays on the label.

    The posting keeps its own town when it is one of the site's towns (a
    campus town may carry its campus name). The table's town is written
    when the posting has no town or sits at the system default; on a "site"
    line also when the line itself names the site's town ("Charlton Memorial
    Hospital - Fall River, MA" on a row stored at New Bedford); on a "site!"
    line (Hawaii, whose rows sat at an island, not a town) always. Any other
    posting elsewhere stays on the label with its own town: a clinic or an
    office of that site, or a line that disagrees with the row, so no town
    is guessed."""
    jc, js = (city or "").strip(), (state or "").strip().upper()
    if jc and js == site.state and key(jc) == key(site.city):
        return site.name, jc, js
    for t, st, cname in site.campuses:
        if jc and js == st and key(jc) == key(t):
            return (cname or site.name), jc, js
    if not jc or _at_default(spec, jc, js) or kind == "site!"             or (kind == "site" and _names_town(spec, value, site.city)):
        return site.name, site.city, site.state
    return None


# ── The systems ─────────────────────────────────────────────────────────────
_LOC = "Location"
_SPECS = (
    # UNC Health (Infor): "Entity: <entity>", 1,773 of 1,784 active rows on
    # 2026-10-29. "Location of Job: US:NC:<town>" gives the posting's town.
    Spec("UNC Health", ("Chapel Hill", "NC"), (Read("label", "Entity", "entity"),), (
        S("UNC Medical Center", "Chapel Hill", "NC", "cms 340061"),
        S("UNC REX Healthcare", "Raleigh", "NC", "cms 340114", "UNC REX Healthcare Organization"),
        S("UNC Rex Holly Springs Hospital", "Holly Springs", "NC", "board"),
        S("UNC Medical Center Hillsborough", "Hillsborough", "NC", "board"),
        S("Johnston Health", "Smithfield", "NC", "cms 340090",
          campuses=(("Clayton", "NC", "Johnston Health Clayton"),)),
        S("Caldwell Memorial Hospital", "Lenoir", "NC", "cms 340041"),
        S("UNC Rockingham Health Care", "Eden", "NC", "cms 340060"),
        S("UNC Health Southeastern", "Lumberton", "NC", "cms 340050"),
        S("Chatham Hospital", "Siler City", "NC", "cms 341311"),
        S("Wayne UNC Health Care", "Goldsboro", "NC", "address 2700 Wayne Memorial Dr, Goldsboro"),
        S("UNC Lenoir Health Care", "Kinston", "NC", "cms 340027"),
        S("Nash UNC Health Care", "Rocky Mount", "NC", "address 2460 Curtis Ellis Dr, Rocky Mount"),
        S("Onslow Memorial Hospital", "Jacksonville", "NC", "cms 340042"),
    ), offices=("UNC Health Medical Group", "UNC Health Medical Group Organization", "UNC Faculty Physicians",
                "Shared Services", "UNC Health MG Group Practices", "UNC Physicians Network",
                "UNC Physicians Network Organization", "Support Operations", "UNC Health Complete Care",
                "UNC Physician Group Practices", "Health Alliance", "UNCHMG", "UNC Medical Group")),
    # Bayhealth (Infor): "Location: Kent Campus Hospital" (Dover), "Sussex
    # Campus Hospital" (Milford).
    Spec("Bayhealth", None, (Read("label", _LOC, "site"),), (
        S("Bayhealth Hospital Kent Campus", "Dover", "DE", "cms 080004", "Kent Campus Hospital"),
        S("Bayhealth Hospital Sussex Campus", "Milford", "DE", "cms 080009", "Sussex Campus Hospital"),
    ), offices=("BH Medical Group Physician Practices",)),
    # Southcoast Health (iCIMS): "Location: <site> - <town>, MA".
    Spec("Southcoast Health", None, (Read("label", _LOC, "site"),), (
        S("St. Luke's Hospital", "New Bedford", "MA", "board; address 101 Page St, New Bedford"),
        S("Charlton Memorial Hospital", "Fall River", "MA", "board; address 363 Highland Ave, Fall River"),
        S("Tobey Hospital", "Wareham", "MA", "board; address 43 High St, Wareham"),
    )),
    # Kaleida Health (Infor): "Location: <site>" ("Location of Job : US:NY:
    # <town>" is the town, not this line).
    Spec("Kaleida Health", None, (Read("label", _LOC, "site"),), (
        S("Buffalo General Medical Center", "Buffalo", "NY", "cms 330005"),
        S("Olean General Hospital", "Olean", "NY", "cms 330103"),
        S("Millard Fillmore Suburban Hospital", "Williamsville", "NY", "board", "Millard Fillmore Suburban"),
        # "Golisano Children's Hospital of Buffalo (GCH Buffalo), formerly
        # Oishei Children's Hospital" (Kaleida's own posting text)
        S("Golisano Children's Hospital of Buffalo", "Buffalo", "NY", "board",
          "Golisano Children's Hospital", "Oishei Children's Hospital", "John R. Oishei Children's Hospital"),
        S("HighPointe on Michigan", "Buffalo", "NY", "board", "HighPointe"),
        S("DeGraff Medical Park", "North Tonawanda", "NY", "board"),
        S("Bradford Regional Medical Center", "Bradford", "PA", "board", "Bradford Regional Medical Cntr"),
    ), offices=("Larkin Bldg @ Exchange Street",)),
    # Cook County Health (Taleo): "LOCATION: <site>[, street, town]".
    Spec("Cook County Health", None, (Read("label", _LOC, "site"),), (
        S("John H. Stroger Jr. Hospital of Cook County", "Chicago", "IL", "address 1969 W Ogden Ave, Chicago",
          "John H. Stroger, Jr. Hospital", "John H. Stroger Jr. Hospital", "John H Stroger Jr Hospital",
          "John H. Stroger Hospital", "John H Stroger Hospital", "John Stroger Hospital", "Stroger Hospital",
          "John H. Stroger, Jr. Hospital of Cook County", "John H. Stroger, Jr. Hospital-Chicago",
          "John H. Stroger Jr. Hospital of Cook County"),
        S("Provident Hospital of Cook County", "Chicago", "IL", "address 500 E 51st St, Chicago",
          "Provident Hospital"),
        S("Cermak Health Services", "Chicago", "IL", "board 2800 S California Ave, Chicago"),
        S("Ruth M. Rothstein CORE Center", "Chicago", "IL", "board 2020 W Harrison St, Chicago"),
    ), offices=("Department of Public Health", "System Float Pool-Operating Room")),
    # Hawaii Health Systems (Infor): "LOCATION : <FACILITY> | <department>";
    # the rows sat at an island ("Big Island", "Kauai", "Oahu"), not a town.
    Spec("Hawaii Health Systems", None, (Read("label", _LOC, "site!"),), (
        S("Kona Community Hospital", "Kealakekua", "HI", "cms 120019"),
        S("Samuel Mahelona Memorial Hospital", "Kapaa", "HI", "cms 121306",
          "Samuel Mahelona Mem Hosp", "Samuel Mahelona Memorial Hosp"),
        S("Kauai Veterans Memorial Hospital", "Waimea", "HI", "cms 121300",
          "Kauai Veterans Memorial Hosp", "Kauai Veterans Mem Hosp"),
        S("Kohala Hospital", "Kapaau", "HI", "cms 121302"),
        S("Hilo Benioff Medical Center", "Hilo", "HI", "cms 120005", "Hilo Medical Center"),
        S("Hale Ho'ola Hamakua", "Honokaa", "HI", "cms 121307"),
        S("Ka'u Hospital", "Pahala", "HI", "cms 121301", "Kau Hospital"),
        S("Leahi Hospital", "Honolulu", "HI", "address 3675 Kilauea Ave, Honolulu"),
        S("Maluhia", "Honolulu", "HI", "address 1027 Hala Dr, Honolulu"),
    ), offices=("Kauai Region Pharmacy West", "Kaua'i Region Medical Group")),
    # CoxHealth (Workday): "Facility:" then "<site>: <street>, <town>,
    # Missouri, ..." (the board's own address gives each town).
    Spec("CoxHealth", ("Springfield", "MO"), (Read("label", "Facility", "site"),), (
        S("Cox Medical Center South", "Springfield", "MO", "board 3801 S National Ave", "CoxHealth South", "Cox South"),
        S("Cox Medical Center North", "Springfield", "MO", "board 1423 N Jefferson Ave", "Cox North"),
        S("Cox Medical Center Branson", "Branson", "MO", "cms 260094; board 525 Branson Landing Blvd"),
        S("Cox Barton County Hospital", "Lamar", "MO", "cms 261325; board 29 NW 1st Lane"),
        S("Cox Monett Hospital", "Monett", "MO", "cms 261329; board 1000 E Hwy 60", "Cox Monett"),
        S("Hulston Cancer Center", "Springfield", "MO", "board 3850 S National Ave"),
        S("Ferrell-Duncan Clinic", "Springfield", "MO", "board 1001 E Primrose St", "Ferrell Duncan Clinic"),
        S("Meyer Orthopedic Surgical Center", "Springfield", "MO", "board 3535 S National Ave"),
    ), offices=("Home Support", "Remote Missouri", "Sunset Building", "CoxHealth National Building",
                "CoxHealth Distribution Center Receiving", "CoxHealth Plans")),
    # Lee Health (Infor): "Location: <site> - <street> <town> FL <zip>".
    Spec("Lee Health", ("Fort Myers", "FL"), (Read("label", _LOC, "site"),), (
        S("Gulf Coast Medical Center", "Fort Myers", "FL", "board 13681 Doctor's Way"),
        S("HealthPark Medical Center", "Fort Myers", "FL", "board 9981 S HealthPark Dr"),
        S("Cape Coral Hospital", "Cape Coral", "FL", "cms 100244"),
        S("Lee Memorial Hospital", "Fort Myers", "FL", "cms 100012"),
        S("Golisano Children's Hospital of Southwest Florida", "Fort Myers", "FL", "board 9981 S HealthPark Dr",
          "Golisano Children's Hospital"),
        S("HealthPark Care and Rehabilitation Center", "Fort Myers", "FL", "board 16131 Roserush Ct"),
    ), offices=("Lee Health", "Lee Health Corporate Center", "Remote", "Multiple Locations", "Various Locations")),
    # NCH (Infor): "Location: <street>, Naples, FL, <zip>": the two hospitals
    # by their street address.
    Spec("NCH Healthcare System", ("Naples", "FL"), (Read("label", _LOC, "site"),), (
        S("NCH Baker Hospital", "Naples", "FL", "board 350 7th St N",
          "350 7th Street North", "350 7th Street N", "350 7th St N", "350 7th St North"),
        S("NCH North Naples Hospital", "Naples", "FL", "board 11190 Health Park Blvd",
          "11190 Health Park Blvd", "11190 Health Park Boulevard"),
    )),
    # Premier Health (Phenom-style board): "Facility: <hospital>".
    Spec("Premier Health", ("Dayton", "OH"), (Read("label", "Facility", "site"),), (
        S("Miami Valley Hospital", "Dayton", "OH", "cms 360051", "MVH", "MVH Main", "Miami Valley Hospital Main"),
        S("Miami Valley Hospital North", "Dayton", "OH", "address 9000 N Main St, Dayton",
          "Miami Valley North", "Miami Valley North Hospital", "MVHN"),
        S("Miami Valley Hospital South", "Centerville", "OH", "address 2400 Miami Valley Dr, Centerville",
          "Miami Valley South", "Miami Valley South Hospital", "MVHS"),
        S("Atrium Medical Center", "Middletown", "OH", "cms 360076; address 1 Medical Center Dr, Middletown", "AMC"),
        S("Upper Valley Medical Center", "Troy", "OH", "cms 360174", "Upper Valley Hospital", "UVMC"),
    ), offices=("Premier System Support", "Fidelity Health Care", "All locations")),
    # Children's Health (Infor behind Phenom): "Location: Children's Health
    # Dallas" / "Hosp-Dal-C" / "Children's Health Plano". A bare town
    # ("Dallas, TX") names no site here.
    Spec("Children's Health", None, (Read("label", _LOC, "site"),), (
        S("Children's Medical Center Dallas", "Dallas", "TX", "cms 453302",
          "Children's Health Dallas", "Children's Health Dallas Campus", "Hosp-Dal-B", "Hosp-Dal-C",
          "Hosp-Dal", "Dallas Campus", "Children's Medical Center of Dallas"),
        S("Children's Medical Center Plano", "Plano", "TX", "cms 453316",
          "Children's Health Plano", "Children's Health Plano Campus", "Hosp-Plano", "Plano Campus"),
    )),
    # UChicago Medicine (Oracle): "Work Location: Hyde Park" (the medical
    # centre's campus), "Comer Children's Hospital", "Ingalls Memorial".
    Spec("UChicago Medicine", ("Chicago", "IL"),
         (Read("label", "Work Location|Job Location|Location", "site"),), (
        S("University of Chicago Medical Center", "Chicago", "IL", "cms 140088",
          "Hyde Park", "Hyde Park Campus", "Center for Care and Discovery", "The University of Chicago Medical Center",
          "UChicago Medicine Hyde Park"),
        S("UChicago Medicine Comer Children's Hospital", "Chicago", "IL", "address 5721 S Maryland Ave, Chicago",
          "Comer", "Comer Children's Hospital", "Comer Children's"),
        S("UChicago Medicine Ingalls Memorial", "Harvey", "IL", "cms 140191",
          "Ingalls Memorial Hospital", "Ingalls Memorial", "Ingalls"),
    ), offices=("Onsite", "Remote")),
    # Duke Health (Phenom): "Job Location:" / "Work Location:" / "Location:".
    Spec("Duke Health", None, (Read("label", "Job Location|Work Location|Location", "site"),), (
        S("Duke University Hospital", "Durham", "NC", "cms 340030", "2301 Erwin Rd", "2301 Erwin Road"),
        S("Duke Regional Hospital", "Durham", "NC", "cms 340155"),
        S("Duke Raleigh Hospital", "Raleigh", "NC", "address 3400 Wake Forest Rd, Raleigh", "Duke Raleigh"),
        S("Duke Health Cary Hospital", "Cary", "NC", "board", "Duke Health Cary"),
        S("Duke Health Lake Norman Hospital", "Mooresville", "NC", "cms 340129"),
    )),
    # Monument Health (Workday): "Department" then "<code> <unit>"; the
    # hospital's cost-centre code (RCH Rapid City Hospital, SPH Spearfish ...).
    Spec("Monument Health", ("Rapid City", "SD"), (Read("code", "Department", "entity"),), (
        S("Monument Health Rapid City Hospital", "Rapid City", "SD", "cms 430077", "RCH"),
        S("Monument Health Spearfish Hospital", "Spearfish", "SD", "cms 430048", "SPH"),
        S("Monument Health Custer Hospital", "Custer", "SD", "cms 431323", "CUH"),
        S("Monument Health Lead-Deadwood Hospital", "Deadwood", "SD", "cms 431320", "LDH"),
        S("Monument Health Sturgis Hospital", "Sturgis", "SD", "cms 431321", "STH"),
        # "70 Monument Health Orthopedic and Specialty Hospital" (its postings' entity line)
        S("Monument Health Orthopedic and Specialty Hospital", "Rapid City", "SD", "board", "MHOSH"),
    )),
    # BayCare (Infor): "Facility:" / "Location:" lines ("Winter Haven Hospital",
    # "BayCare Health System, Med Surg Unit I-WHH"), and the adapter's own
    # short names for rows the list names ("St Josephs").
    # 2026-10-29 (owner calls, "go with recs"): the list's short name is the
    # hospital's business unit, which also runs clinics and outpatient sites
    # ("Largo:Morton Plant" on the Bardmoor outpatient center, "South
    # Pasadena:St Anthonys" on a primary care office, "Lutz:Morton Plant North
    # Bay" on the recovery center). Such a row keeps "BayCare" with its own
    # town, unless the posting names the clinic itself in full (the six
    # clinics below, decision 2). A site line naming a clinic or an outpatient
    # site (clinic_rx: "Bardmoor Outpt and Surgery", "Free Standing ER, 102 S.
    # Dale Mabry", "Mease Countryside Eastlake Outpt") is a clinic; one opening
    # with the hospital's own name ("Morton Plant Hospital Axelrod Pavillon -
    # Outpatient Imaging") is the hospital. See _canon_site.
    Spec("BayCare", ("Clearwater", "FL"), (Read("label", "Facility", "site"), Read("label", _LOC, "site")), (
        S("St. Joseph's Hospital", "Tampa", "FL", "board", "St Joseph's Hospital", "SJH", "St. Joseph's Hospital Tampa"),
        S("St. Joseph's Hospital-North", "Lutz", "FL", "board", "St. Joseph's Hospital - North",
          "St. Joseph's Hospital North", "SJHN"),
        S("St. Joseph's Hospital-South", "Riverview", "FL", "board", "St. Joseph's Hospital - South",
          "St. Joseph's Hospital South", "SJHS"),
        S("St. Joseph's Children's Hospital", "Tampa", "FL", "board", "SJCH"),
        S("St. Joseph's Women's Hospital", "Tampa", "FL", "board", "SJWH"),
        S("Morton Plant Hospital", "Clearwater", "FL", "board", "MPH"),
        S("Morton Plant North Bay Hospital", "New Port Richey", "FL", "board", "North Bay Hospital", "NBH"),
        S("Mease Countryside Hospital", "Safety Harbor", "FL", "board"),
        S("Mease Dunedin Hospital", "Dunedin", "FL", "board"),
        S("St. Anthony's Hospital", "St Petersburg", "FL", "board", "St Anthony's Hospital"),
        S("South Florida Baptist Hospital", "Plant City", "FL", "board", "SFBH"),
        S("Winter Haven Hospital", "Winter Haven", "FL", "board", "WHH"),
        S("Winter Haven Women's Hospital", "Winter Haven", "FL", "board", "Winter Haven Womens", "WHWH", "WHW"),
        S("Bartow Regional Medical Center", "Bartow", "FL", "board", "BRMC"),
        S("BayCare Hospital Wesley Chapel", "Wesley Chapel", "FL", "board"),
    ), offices=("BayCare Health System", "BCHS", "BayCare HomeCare", "Remote"),
         canon={"St Josephs": "St. Joseph's Hospital", "St Josephs North": "St. Joseph's Hospital-North",
                "St Josephs South": "St. Joseph's Hospital-South", "Morton Plant": "Morton Plant Hospital",
                "Morton Plant North Bay": "Morton Plant North Bay Hospital",
                "Mease Countryside": "Mease Countryside Hospital", "Mease Dunedin": "Mease Dunedin Hospital",
                "St Anthonys": "St. Anthony's Hospital", "South Florida Baptist": "South Florida Baptist Hospital",
                "Bartow Med Ctr": "Bartow Regional Medical Center",
                "Wesley Chapel Hospital": "BayCare Hospital Wesley Chapel"},
         # each spelled in full on its own postings' "Facility:" / "Location:"
         # line, in the town every one of those postings sits in
         clinics=(
             S("Morton Plant North Bay Recovery Center", "Lutz", "FL", "board", "North Bay Recovery Center",
               "Morton Plant North Bay Recover Center"),
             S("Bardmoor Outpatient Center", "Largo", "FL", "board"),
             S("Wesley Chapel Outpatient Imaging", "Wesley Chapel", "FL", "board"),
             S("BayCare Life Management Countryside", "Safety Harbor", "FL", "board"),
             S("St. Anthony's Primary Care Pasadena", "South Pasadena", "FL", "board",
               "St Anthonys Primary Care Pasadena"),
             S("St. Anthony's Primary Care Baywalk", "St Petersburg", "FL", "board",
               "St Anthonys Primary Care Baywalk"),
         ),
         # review 2026-10-29: home care and home-based lines ("Mastectomy -
         # HomeCare", "Home base county: Hillsborough County ...") too
         # (decision 2: home-based jobs show the system name with the town)
         clinic_rx=(r"\b(?:outpt|outpatient|primary care|medical group|recovery? cent(?:er|re)|urgent care|"
                    r"free[- ]?standing|wellness center|child care|life management|home ?care|home base)\b"
                    r"|\bbaycare e[rd]\b")),
    # Allegheny Health Network (Workday): "Location: Allegheny General Hospital".
    Spec("Allegheny Health Network", ("Pittsburgh", "PA"), (Read("label", _LOC, "site"),), (
        S("Allegheny General Hospital", "Pittsburgh", "PA", "address 320 E North Ave, Pittsburgh"),
        S("West Penn Hospital", "Pittsburgh", "PA", "cms 390090"),
        S("Forbes Hospital", "Monroeville", "PA", "cms 390267"),
        S("Jefferson Hospital", "Jefferson Hills", "PA", "address 565 Coal Valley Rd, Jefferson Hills"),
        S("Canonsburg Hospital", "Canonsburg", "PA", "cms 390160", "Canonsburg General Hospital"),
        S("Allegheny Valley Hospital", "Natrona Heights", "PA", "address 1301 Carlisle St, Natrona Heights"),
        S("AHN Wexford Hospital", "Wexford", "PA", "cms 390334"),
        S("Saint Vincent Hospital", "Erie", "PA", "cms 390009"),
        S("Grove City Hospital", "Grove City", "PA", "cms 390266", "Grove City Medical Center"),
    )),
    # Aultman (HealthcareSource): "Entity: Aultman Hospital, Canton Campus".
    Spec("Aultman Health Foundation", ("Canton", "OH"), (Read("label", "Entity", "entity"),), (
        S("Aultman Hospital", "Canton", "OH", "cms 360084"),
        S("Aultman Alliance Community Hospital", "Alliance", "OH", "cms 360131", "Alliance Community Hospital"),
        S("Aultman Orrville Hospital", "Orrville", "OH", "cms 361323"),
    )),
    # Nebraska Medicine (Workday): "Location: Nebraska Medical Center[ | ...]".
    Spec("Nebraska Medicine", ("Omaha", "NE"), (Read("label", _LOC, "site"),), (
        S("Nebraska Medical Center", "Omaha", "NE", "cms 280013", "The Nebraska Medical Center"),
        S("Bellevue Medical Center", "Bellevue", "NE", "cms 280132"),
    )),
    # Insight Health (Paycor): "Location: Coldwater Regional Hospital", "Company:".
    Spec("Insight Health", None, (Read("label", _LOC + "|Company", "site"),), (
        S("Insight Hospital & Medical Center Coldwater", "Coldwater", "MI", "cms 230022",
          "Coldwater Regional Hospital", "Insight Hospital and Medical Center Coldwater"),
        S("Insight Hospital & Medical Center Trumbull", "Warren", "OH", "cms 360055",
          "Insight Hospital and Medical Center Trumbull"),
        S("Insight Hospital & Medical Center Chicago", "Chicago", "IL", "cms 140158",
          "Insight Hospital and Medical Center Chicago"),
    )),
    # MaineHealth (Infor): the body's first line is the entity ("Mid Coast
    # Hospital", "MaineHealth Memorial Hospital | Med Surg").
    Spec("MaineHealth", ("Portland", "ME"), (Read("first", 1, "entity"),), (
        S("MaineHealth Maine Medical Center", "Portland", "ME", "cms 200009",
          "Maine Medical Center", "MaineHealth Maine Medical Center Portland", "Maine Medical Center Portland",
          campuses=(("Biddeford", "ME", "MaineHealth Maine Medical Center Biddeford"),
                    ("Sanford", "ME", "MaineHealth Maine Medical Center Sanford"))),
        S("MaineHealth Mid Coast Hospital", "Brunswick", "ME", "cms 200021", "Mid Coast Hospital"),
        S("MaineHealth Pen Bay Hospital", "Rockport", "ME", "cms 200063", "Pen Bay Hospital", "Pen Bay Medical Center"),
        S("MaineHealth Memorial Hospital", "North Conway", "NH", "cms 301307", "Memorial Hospital"),
        S("MaineHealth Lincoln Hospital", "Damariscotta", "ME", "cms 201302", "Lincoln Hospital", "LincolnHealth"),
        S("MaineHealth Behavioral Health at Spring Harbor", "Westbrook", "ME", "cms 204005", "Spring Harbor Hospital"),
        S("MaineHealth Waldo Hospital", "Belfast", "ME", "cms 201312", "Waldo Hospital", "Waldo County General Hospital"),
        S("MaineHealth Franklin Hospital", "Farmington", "ME", "cms 201316", "Franklin Hospital"),
        S("MaineHealth Stephens Hospital", "Norway", "ME", "cms 201315", "Stephens Hospital",
          "Stephens Memorial Hospital"),
    ), offices=("Medical Group Practices", "MaineHealth Corporate", "NorDx", "MaineHealth Behavioral Health",
                "Home Health and Hospice", "MaineHealth Primary Care", "Maine Medical Partners")),
    # Cabell Huntington Hospital (ADP; the Marshall Health Network's label):
    # the opening sentence names the hiring hospital ("St. Mary's Medical
    # Center is seeking", "Rivers Health").
    Spec("Cabell Huntington Hospital", ("Huntington", "WV"), (Read("lead", 2, "entity"),), (
        S("Cabell Huntington Hospital", "Huntington", "WV", "cms 510055"),
        S("St. Mary's Medical Center", "Huntington", "WV", "cms 510007"),
        S("Rivers Health", "Point Pleasant", "WV", "cms 510012"),
    )),
    # HealthPartners (Oracle): "Regions Hospital in Saint Paul, MN is
    # seeking", "Methodist Hospital is looking to hire".
    Spec("HealthPartners", ("Saint Paul", "MN"), (Read("lead", 3, "entity"),), (
        S("Regions Hospital", "Saint Paul", "MN", "cms 240106"),
        S("Park Nicollet Methodist Hospital", "Saint Louis Park", "MN", "cms 240053", "Methodist Hospital"),
        S("Lakeview Hospital", "Stillwater", "MN", "cms 240066"),
        S("Olivia Hospital & Clinic", "Olivia", "MN", "cms 241306"),
        S("Hudson Hospital & Clinic", "Hudson", "WI", "cms 521335", "Hudson Hospital"),
        S("Westfields Hospital & Clinic", "New Richmond", "WI", "cms 521345", "Westfields Hospital"),
        S("Amery Hospital & Clinic", "Amery", "WI", "cms 521308"),
        S("Hutchinson Health", "Hutchinson", "MN", "cms 240187"),
        S("Glencoe Regional Health", "Glencoe", "MN", "cms 241355"),
    )),
    # Aspirus Health (Infor): "Aspirus Rhinelander Hospital in Rhinelander, WI
    # is seeking"; names the adapter already writes are kept as written.
    Spec("Aspirus Health", ("Wausau", "WI"), (Read("lead", 4, "entity"),), (
        S("Aspirus Wausau Hospital", "Wausau", "WI", "board"),
        S("Aspirus Rhinelander Hospital", "Rhinelander", "WI", "cms 520019"),
        S("Aspirus Stevens Point Hospital", "Stevens Point", "WI", "cms 520002"),
        S("Aspirus Medford Hospital", "Medford", "WI", "cms 521324"),
        S("Aspirus Ironwood Hospital", "Ironwood", "MI", "cms 231333"),
        S("Aspirus Keweenaw Hospital", "Laurium", "MI", "cms 231319"),
        S("Aspirus Tomahawk Hospital", "Tomahawk", "WI", "cms 521313"),
        S("Aspirus Merrill Hospital", "Merrill", "WI", "cms 521339"),
        S("Aspirus Stanley Hospital", "Stanley", "WI", "cms 521311"),
        S("Aspirus Iron River Hospital", "Iron River", "MI", "cms 231318"),
        S("Aspirus Eagle River Hospital", "Eagle River", "WI", "cms 521300"),
        S("Aspirus Divine Savior Hospital", "Portage", "WI", "board"),
        S("Aspirus Wisconsin Rapids Hospital", "Wisconsin Rapids", "WI", "cms 520033"),
        S("Langlade Hospital", "Antigo", "WI", "board", "Aspirus Langlade Hospital"),
        S("Howard Young Medical Center", "Woodruff", "WI", "board", "Aspirus Howard Young Medical Center"),
        S("Aspirus St. Luke's Hospital", "Duluth", "MN", "cms 240047"),
        S("Aspirus Lake View Hospital", "Two Harbors", "MN", "cms 241308"),
    )),
    # UW Medicine (Workday): the opening line under "Job Description"
    # ("Harborview Medical Center has an exciting opportunity").
    Spec("UW Medicine", None, (Read("lead", 3, "entity"),), (
        S("Harborview Medical Center", "Seattle", "WA", "cms 500064"),
        S("UW Medical Center - Montlake", "Seattle", "WA", "cms 500008", "UW Medical Center-Montlake",
          "UWMC Montlake", "UWMC - Montlake", "University of Washington Medical Center - Montlake"),
        S("UW Medical Center - Northwest", "Seattle", "WA", "cms 500008", "UW Medical Center-Northwest",
          "UWMC Northwest", "UWMC - Northwest", "University of Washington Medical Center - Northwest"),
        S("Valley Medical Center", "Renton", "WA", "cms 500088"),
    )),
    # UC Health (Oracle): "Location: UC Medical Center".
    Spec("UC Health", ("Cincinnati", "OH"), (Read("label", _LOC, "site"),), (
        S("University of Cincinnati Medical Center", "Cincinnati", "OH", "cms 360003", "UC Medical Center"),
        S("West Chester Hospital", "West Chester", "OH", "cms 360354"),
        S("Lindner Center of HOPE", "Mason", "OH", "cms 364044"),
    )),
    # Sinai Chicago (HealthcareSource): the posting's opening names the
    # hospital ("Mt. Sinai Hospital is a fast-paced Level 1 ...", "Holy Cross
    # Hospital is looking for"); the "About Sinai" paragraph that lists every
    # hospital never starts with one.
    Spec("Sinai Chicago", None, (Read("lead", 3, "entity"),), (
        S("Mount Sinai Hospital", "Chicago", "IL", "address 1500 S Fairfield Ave, Chicago", "Mt. Sinai Hospital",
          "Mt Sinai Hospital"),
        S("Holy Cross Hospital", "Chicago", "IL", "address 2701 W 68th St, Chicago"),
        S("Schwab Rehabilitation Hospital", "Chicago", "IL", "address 1401 S California Ave, Chicago",
          "Schwab Rehabilitation", "Schwab Rehab Hospital", "Schwab Rehab"),
        S("Sinai Children's Hospital", "Chicago", "IL", "address 1500 S Fairfield Ave, Chicago"),
    )),
)
SPECS: dict[str, Spec] = {s.system: s for s in _SPECS}
SYSTEMS = frozenset(SPECS)


def spec_for(system) -> Spec | None:
    return SPECS.get((system or "").strip())


def text_facility(system, description, city="", state=""):
    """(hospital_name, city, state) the body names for a row of `system` at
    (city, state), or None. Pure: no stored name, no Job."""
    spec = spec_for(system)
    if spec is None:
        return None
    site, kind, _what, v = read_text(spec, description)
    return place(spec, site, kind, city, state, v) if site else None


# ── Stored names (nights without a body) ────────────────────────────────────
_KNOWN: dict = {}
KNOWN_PAGE = 1000
KNOWN_MAX_ROWS = 60000
# Under this many characters the row carries no body tonight: blank (the
# stored body is kept, scraper._keep_stored_body) or a list teaser.
NO_BODY_CHARS = 600


_KNOWN_CANON: dict = {}


def set_known_facilities(rows) -> int:
    """{(hospital_system, job_id): stored hospital_name} for rows whose
    stored name is one of the table's site or campus names. 2026-10-29: and,
    for the systems with clinic rules (Spec.clinic_rx), every stored name
    (the system label too) in _KNOWN_CANON, the stored decision a row the
    adapter names by its hospital's business unit keeps on a night without
    a body (_canon_site)."""
    global _KNOWN, _KNOWN_CANON
    out, canon = {}, {}
    for r in rows or []:
        spec = spec_for(r.get("hospital_system"))
        name, jid = clean_text(r.get("hospital_name")), r.get("job_id")
        if spec is None or jid in (None, ""):
            continue
        if spec.clinic_rx is not None and name:
            canon[(spec.system, str(jid))] = name
        if name not in spec.names:
            continue
        out[(spec.system, str(jid))] = name
    _KNOWN = out
    _KNOWN_CANON = canon
    return len(out)


def known_facility(system, job_id) -> str:
    return _KNOWN.get(((system or "").strip(), str(job_id)), "")


def load_known_facilities() -> int:
    """Read-only PostgREST passes (the SUPABASE_URL / key the upsert uses)
    over the active rows of each system here whose hospital_name is not the
    system label, one system at a time, keyset-paged on id (idx_system).
    Without credentials the map stays empty and nothing is kept. Never
    raises: a system that fails keeps the rows read before it."""
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
        # 2026-10-29: a system with clinic rules reads its label rows too
        # (_KNOWN_CANON: a clinic row the label was stored on stays there)
        name_flt = "" if SPECS[system].clinic_rx is not None else f"&hospital_name={nflt}"
        last, got = 0, 0
        try:
            while len(rows) < KNOWN_MAX_ROWS:
                u = (f"{sb_url.rstrip('/')}/rest/v1/hospital_jobs?select=id,hospital_system,job_id,hospital_name"
                     f"&is_active=is.true&hospital_system={flt}{name_flt}"
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
            logger.warning(f"Text facilities: {system} load stopped after {got:,} rows ({e}); "
                           f"its rows past that keep a hospital only on nights their body is read")
    try:
        return set_known_facilities(rows)
    except Exception as e:          # never let this read end the run
        logger.warning(f"Text facilities: stored names not installed ({e})")
        set_known_facilities([])
        return 0


# ── Applying it to a Job (finalize_jobs, before normalize_job) ──────────────
UNKNOWN: dict = {}       # system -> {raw value: rows} named by no table entry, this run


def apply_facility(job) -> str:
    """Store the site on one Job of a SYSTEMS system. Returns what happened:
    "text" (the body named it), "canon" (the adapter's short name, written
    clean), "kept" (the stored name, no body tonight), "elsewhere" (an entity
    line on a posting in another town: stays on the label), "office",
    "unknown", "none", "other" (the adapter's own name, left as it is), or ""
    for any other system. hospital_system is never changed."""
    spec = spec_for(job.hospital_system)
    if spec is None:
        return ""
    cur = clean_text(job.hospital_name)
    if cur and cur != spec.system:
        site = spec.canon.get(cur)
        if site is None:
            return "other"
        if spec.clinic_rx is not None:
            return _canon_site(spec, site, job)
        job.hospital_name = site.name
        return "canon"
    site, kind, what, raw = read_text(spec, job.description)
    if site is not None:
        placed = place(spec, site, kind, job.city, job.state, raw)
        if placed is None:
            return "elsewhere"
        _set(job, *placed)
        return "text"
    if len((job.description or "").strip()) < NO_BODY_CHARS:
        name = known_facility(spec.system, job.job_id)
        if name:
            site = spec.names[name]
            placed = place(spec, site, "entity", job.city, job.state)
            if placed is not None:
                # a campus name stays the campus name it was stored as
                _set(job, name if name != site.name else placed[0], placed[1], placed[2])
                return "kept"
    if what == "unknown" and raw:
        d = UNKNOWN.setdefault(spec.system, {})
        d[raw[:80]] = d.get(raw[:80], 0) + 1
    return what


def _lead_site(spec, value):
    """The table site (hospital or clinic) whose spelling the value opens
    with, word for word ("Morton Plant Hospital Axelrod Pavillon - ..."), or
    None."""
    lk = lead_key(value) + " "
    for k, site in spec.lead:
        if lk.startswith(k + " "):
            return site
    return None


def _inner_clinic(spec, value):
    """A clinic of the table spelled whole inside the value ("Behavioral
    Health- Morton Plant North Bay Recover Center"), or None."""
    vk = f" {key(value)} "
    for k, site in spec.lead:
        if site.name in spec.clinic_names and f" {k} " in vk:
            return site
    return None


# a hospital department's code at the end of a system-office line ("-MPH",
# "-MPM SRVCS", "- SJHN", "-BCHS")
_DEPT_CODE_RX = re.compile(r"[-–]\s*[A-Z]{2,5}(?:\s+[A-Z]{2,6})?\s*$")


def _site_line(spec, body):
    """(site or None, value) for a clinic-rules system: the first label line
    (Facility, then Location) that decides. A value naming a table site is
    that site (a hospital or a clinic); else one opening with a site's
    spelling; else one holding a clinic's spelling; else a value naming a
    clinic or an outpatient site (clinic_rx) is (None, value), the label. A
    system-office line ("BayCare Health System, Trinity Nuc Med-MPM SRVCS",
    a department) or a bare town decides nothing, and the next line is read.
    (None, "") when no line decides."""
    lines = _lines(body)
    for r in spec.reads:
        if r.mode != "label":
            continue
        v = _label_value(spec.rx[r.arg], lines)
        if not v or not v.strip():
            continue
        heads = _candidates(v)[0]
        office = next((h for h in heads if key(h) in spec.offices), None)
        if office is not None:
            # review 2026-10-29: a system-office line names a hospital
            # department ("BayCare Health System, Outpatient Testing-MPH", the
            # hospital's code last) or, with no code, the site itself
            # ("BayCare Health System, Cheek Powell Wellness Center",
            # "BayCare Health System, Mastectomy - HomeCare"): the latter is
            # a clinic when clinic_rx reads it, else the next line is read
            rest = v[len(office):] if v.startswith(office) else ""
            if rest.strip(" ,-–") and spec.clinic_rx.search(rest) and not _DEPT_CODE_RX.search(rest):
                return None, v
            continue
        site = match_value(spec, v) or _lead_site(spec, v) or _inner_clinic(spec, v)
        if site is not None:
            return site, v
        if spec.clinic_rx.search(v):
            return None, v
    return None, ""


def _canon_site(spec, site, job) -> str:
    """The adapter's hospital value (`site`, from spec.canon) on one row of a
    system with clinic rules (owner 2026-10-29, "go with recs": a clinic job
    that carries a hospital's name keeps the system name with its town; a
    named clinic keeps its own name only when the posting names the clinic
    itself).

    With a body tonight, its site line decides first (_site_line): a clinic
    of the table is that clinic, placed as a site is (a posting in another
    town than the clinic keeps the label); a line naming a clinic or an
    outpatient site (clinic_rx) is the label; a line naming a hospital, or
    opening with a hospital's own name ("Morton Plant Hospital Axelrod
    Pavillon - Outpatient Imaging"), leaves the hospital. Without a body (the
    stored one is kept, scraper._keep_stored_body), the stored decision
    stands when it is the label or a clinic, so a row never flips between
    nights. Then the hospital, only on a posting in its own town (or with no
    town, or at the system default); a posting in any other town is one of
    the business unit's clinics and keeps the label. Its own town is kept
    either way. Returns "canon", "clinic", "elsewhere" or "kept"."""
    body = job.description or ""
    if len(body.strip()) >= NO_BODY_CHARS:
        found, raw = _site_line(spec, body)
        if found is None and raw:
            job.hospital_name = spec.system
            return "clinic"
        if found is not None and found.name in spec.clinic_names:
            placed = place(spec, found, "site", job.city, job.state, raw)
            if placed is None:
                job.hospital_name = spec.system
                return "elsewhere"
            _set(job, *placed)
            return "clinic"
    else:
        stored = _KNOWN_CANON.get((spec.system, str(job.job_id)), "")
        if stored and (stored == spec.system or stored in spec.clinic_names):
            job.hospital_name = stored
            return "kept"
    jc, js = (job.city or "").strip(), (job.state or "").strip().upper()
    at_site = (not jc or _at_default(spec, jc, js) or (js in ("", site.state) and key(jc) == key(site.city))
               or any(js in ("", st) and key(jc) == key(t) for t, st, _c in site.campuses))
    job.hospital_name = site.name if at_site else spec.system
    return "canon" if at_site else "elsewhere"


def _set(job, name, city, state) -> None:
    job.hospital_name = name
    if (city, state) != ((job.city or "").strip(), (job.state or "").strip().upper()):
        job.city, job.state, job.location = city, state, f"{city}, {state}"


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
    """The values no table entry names, most frequent first, for the run log."""
    parts = []
    for system, d in sorted(UNKNOWN.items()):
        vals = sorted(d.items(), key=lambda kv: -kv[1])[:top]
        parts.append(f"{system}: " + "; ".join(f"{v} ({n})" for v, n in vals))
    return " | ".join(parts)
