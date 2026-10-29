"""Hospitals inside the big systems still on the system label, part A
(2026-10-29, facility scale phase 3): Lifepoint Health, Providence and
Baptist Memorial Health Care. Sanford, Piedmont, OSF and UMMS are read by the
code already on main (facilities_town, facilities_icims, facilities_phenom);
nothing here changes them.

The owner, 2026-10-29: every hospital inside a multi-hospital system must be
named and placed correctly on the site and in the dashboard, and applies
retro-assigned to the right hospital ("i thought you were gonna edit all the
large multihospital systems"). Decisions: (1) cards and job pages show the
HOSPITAL or site ("Apply to <site>"), the system name and logo stay on the
job page and the system hub; (2) named sites (hospitals, named clinics and
centers) show their own name, generic offices, home-based and remote jobs
show the system name with the town; never guess a town.

Where each posting names its site:

  Lifepoint Health  (Oracle HCM, ibnjjb)  The body opens with the employing
      hospital: "Sovah Danville is part of Lifepoint Health, a diversified
      healthcare delivery network ..." (2,840 of 3,886 label rows on
      2026-10-29), some with a "Facility Name: Frye Regional Medical Center"
      line (500). The work location (in the list payload every night) is the
      hospital's own name for the ten biggest (facilities_oracle_other); the
      other sites' spellings are added here (extend_oracle), so the nightly
      list read names them too.
      Not named: a Lifepoint Rehabilitation unit inside a partner hospital
      ("Facility Name: Kindred Hospital The Palm Beaches", "McLeod Health
      Cheraw", "Mosaic Life Care St Joseph Medical Center"): the partner's
      hospital is not Lifepoint's, so the row keeps "Lifepoint Health" and its
      town (decision 2). Physician groups and clinics ("Conemaugh Physicians
      Group", "Saline Clinic") and the Brentwood office keep it too.
  Providence Health (Oracle HCM, evac)  The work location: "Kadlec Regional
      Medical Ctr-Richland(HR)", "Swedish Edmonds 21601 76th(HR)". Main's
      table lists twelve; the tenant's work-location facet, read per state
      and for Seattle and Spokane on 2026-10-29 (10 requests), gives the rest
      of the hospitals' spellings. Office parks, medical office buildings,
      clinics and the PACE sites keep the system label.
  Baptist Memorial  (Oracle HCM, fa-ewpe)  The work location "BMH - Tipton
      Hospital"; main lists nine, the facet per state (MS, TN, AR) the rest.
      The three Mississippi hospitals the facet did not reach (Yazoo, Leake,
      Attala) and the Memphis restorative-care and Germantown rehab hospitals
      are read by rules on their own place names. "BMHCC Corporate Office",
      "BMG - Central Business Office", "BMG Clinics", "NEA - Clinic" and
      Baptist Health Sciences University keep the system label.

How the Lifepoint body is read (read_lifepoint), in order:
  1. a "Facility Name:" (or "Facility:") line: where the job is (kind
     "site", facilities_text.place);
  2. the subject of the first "<X> is part of Lifepoint" sentence: who
     employs (kind "entity"), after the "Your experience matters" lead and
     any " is a 167-bed hospital located in ...", ", formerly ..." tail are
     cut. The subject is looked up whole, then by its prefixes and segments
     (facilities_text.match_value: a value naming two sites names none).
Every lookup is an exact match of the folded spelling against LIFEPOINT_SITES;
nothing is fuzzy. A posting is placed as facilities_text places it: the
row keeps its own town when it is the site's town (or a campus town listed
for the site), else it stays on "Lifepoint Health" with its own town (a
clinic or office of that hospital elsewhere; no town is guessed).
An entity sentence does not name a hospital on a posting whose title is a
clinic, practice, office, senior-care or remote job (facilities_town's
OFFSITE_RX, CARE_SITE_RX, REMOTE_RX): the hospital employs the practice
staff too, so those rows keep the system name with their town.

Nights without a body: the scraper sends a known body blank
(scraper._keep_stored_body), so the site stored on the row is kept
(load_known_facilities, the facilities_text pattern), and a row never flips
between nights.

hospital_system is never touched (the upsert key, the hub, the filters, the
logo). Shared with the stored-row change list (claud_outputinput/
facility_scale_2026-10-29/phase3/hsa_work/changes.py), so the database holds
what the scraper writes on the next run.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time

import facilities_text as T
import facilities_town as TOWN
import facility_names as F
import sb_keys  # the DATA project key, one reader for the repo

logger = logging.getLogger("scraper.facility_hsa")

S = T.S
key = T.key

LIFEPOINT = "Lifepoint Health"
PROVIDENCE = "Providence Health"
BMH = "Baptist Memorial Health Care"


# ═════════════════════════════ LIFEPOINT ════════════════════════════════════
# Every site the Lifepoint bodies name as the employer (2026-10-29: the
# 2,840 "part of Lifepoint" subjects and 500 "Facility Name:" lines of the
# label rows), with its own town: "cms <id>" the database's CMS hospital list
# (system_hospitals), "board" every posting naming the site sits in that town.
# The ten names main's Oracle table already writes (facilities_oracle_other)
# are spelled exactly as it writes them.
LIFEPOINT_SITES = (
    # acute care hospitals
    S("Haywood Regional Medical Center", "Clyde", "NC", "cms 340184", "Haywood Regional", "Haywood Regional Hospital"),
    S("Ottumwa Regional Health Center", "Ottumwa", "IA", "cms 160089", "Ottumwa Health Group, LLC",
      "Ottumwa Health Group"),
    S("Raleigh General Hospital", "Beckley", "WV", "cms 510070", "Raleigh General"),
    S("Conemaugh Memorial Medical Center", "Johnstown", "PA", "cms 390110"),
    S("Conemaugh Nason Medical Center", "Roaring Spring", "PA", "cms 390062"),
    S("Conemaugh Meyersdale Medical Center", "Meyersdale", "PA", "cms 391302"),
    S("Conemaugh Miners Medical Center", "Hastings", "PA", "cms 391317"),
    S("Frye Regional Medical Center", "Hickory", "NC", "cms 340116"),
    S("North Alabama Medical Center", "Florence", "AL", "cms 010006", "N orth Alabama Medical Center"),
    S("North Alabama Shoals Hospital", "Muscle Shoals", "AL", "cms 010157"),
    S("Harris Regional Hospital", "Sylva", "NC", "cms 340016", "Harris Regional Medical Center", "Harris Regional"),
    S("Swain Community Hospital", "Bryson City", "NC", "cms 341305", "Swain Community"),
    S("Lake Cumberland Regional Hospital", "Somerset", "KY", "cms 180132"),
    S("Havasu Regional Medical Center", "Lake Havasu City", "AZ", "cms 030069", "H avasu Regional Medical Center"),
    S("UP Health System-Marquette", "Marquette", "MI", "cms 230054", "UP Health System - Marquette",
      "UP Health System Marquette", "Marquette General Hospital", "UP Health System - Marquette General Hospital"),
    S("UP Health System - Portage", "Hancock", "MI", "cms 230108", "UP Health Systems - Portage"),
    S("UP Health System - Bell", "Ishpeming", "MI", "cms 231321", "UPHS Bell", "UPHS - Bell"),
    S("Memorial Medical Center", "Las Cruces", "NM", "cms 320018"),
    S("Highpoint Health - Winchester", "Winchester", "TN", "cms 440058",
      "Highpoint Health Winchester with Ascension Saint Thomas", "Highpoint Health - Winchester with Ascension Saint Thomas"),
    S("Highpoint Health - Sumner", "Gallatin", "TN", "cms 440003",
      "Highpoint Health - Sumner with Ascension Saint Thomas", "Highpoint - Sumner Regional Medical Center",
      "Highpoint Health - Sumner Regional Medical Center"),
    S("Highpoint Health - Riverview", "Carthage", "TN", "cms 441307",
      "Highpoint Health - Riverview with Ascension Saint Thomas", "Highpoint - Riverview Regional Medical Center"),
    S("Highpoint Health - Trousdale", "Hartsville", "TN", "cms 441301",
      "Highpoint Health - Trousdale with Ascension Saint Thomas", "Highpoint - Trousdale Medical Center"),
    S("Community Medical Center", "Missoula", "MT", "cms 270023"),
    S("Canyon Vista Medical Center", "Sierra Vista", "AZ", "cms 030043"),
    S("Valley View Medical Center", "Fort Mohave", "AZ", "cms 030117", "Valley View Regional Medical Center"),
    S("Rutherford Regional Health System", "Rutherfordton", "NC", "cms 340013", "Rutherford Regional",
      "Rutherford Regional Medical Center"),
    S("Wilson Medical Center", "Wilson", "NC", "cms 340126"),
    S("Los Alamos Medical Center", "Los Alamos", "NM", "cms 320033"),
    S("Jackson Purchase Medical Center", "Mayfield", "KY", "cms 180116", "Jackson Purchase"),
    # one hospital on two campuses (CMS 440068 Athens, 440054 Etowah); the
    # work location says "Starr Regional Medical Center" on both
    S("Starr Regional Medical Center", "Athens", "TN", "cms 440068", "Starr Regional Medical Center-Etowah Campus",
      campuses=(("Etowah", "TN", ""),)),
    S("Paris Regional Medical Center", "Paris", "TX", "cms 450196", "Paris Regional"),
    S("Maria Parham Health", "Henderson", "NC", "cms 340132", "Maria Parham", "Maria Parham Medical Center",
      campuses=(("Louisburg", "NC", "Maria Parham Franklin"),)),
    S("Maria Parham Franklin", "Louisburg", "NC", "board"),
    S("Central Carolina Hospital", "Sanford", "NC", "cms 340020"),
    S("Person Memorial Hospital", "Roxboro", "NC", "cms 340159"),
    S("National Park Medical Center", "Hot Springs", "AR", "cms 040078"),
    S("Saline Memorial Hospital", "Benton", "AR", "cms 040084", "Saline Memorial"),
    S("Saint Mary's Regional Medical Center", "Russellville", "AR", "cms 040041", "Saint Mary's Regional Health System",
      "Saint Mary's", "St. Mary's Regional Medical Center"),
    S("Wythe County Community Hospital", "Wytheville", "VA", "cms 490111"),
    S("Clinch Valley Health", "Richlands", "VA", "cms 490060", "Clinch Valley Medical Center"),
    S("Twin County Regional Healthcare", "Galax", "VA", "cms 490115", "Twin County Regional Hospital", "TCRH"),
    S("Sovah Health - Danville", "Danville", "VA", "cms 490075", "Sovah Health Danville", "Sovah Danville", "Sovah Health",
      "Sovah Health - Danville - ARU - R362J",
      campuses=(("Martinsville", "VA", "Sovah Health - Martinsville"),)),
    S("Sovah Health - Martinsville", "Martinsville", "VA", "cms 490079", "Sovah Health Martinsville", "Sovah Martinsville"),
    S("Northeastern Nevada Regional Hospital", "Elko", "NV", "cms 290008"),
    S("Trios Health", "Kennewick", "WA", "cms 500053", "Trios"),
    S("Lourdes Medical Center", "Pasco", "WA", "cms 501337", "Lourdes Health"),
    S("Lourdes Counseling Center", "Richland", "WA", "cms 504008"),
    S("Meadowview Regional Medical Center", "Maysville", "KY", "cms 180019"),
    S("Spring View Hospital", "Lebanon", "KY", "cms 180024", "Spring View Regional Hospital"),
    S("Fleming County Hospital", "Flemingsburg", "KY", "cms 181332"),
    S("Centerpoint Health - Winchester", "Winchester", "KY", "cms 180092", "Centerpoint - Winchester",
      "Centerpoint Health - Winchester Hospital", "Clark Regional Medical Center"),
    S("Centerpoint Health - Georgetown", "Georgetown", "KY", "cms 180101", "Centerpoint - Georgetown",
      "Centerpoint Health - Georgetown Hospital", "Georgetown Community Hospital"),
    S("Centerpoint Health - Versailles", "Versailles", "KY", "cms 181308", "Centerpoint - Versailles",
      "Centerpoint Health - Versailles Hospital", "Bluegrass Community Hospital"),
    S("Centerpoint Health - Paris", "Paris", "KY", "cms 180046", "Centerpoint - Paris",
      "Centerpoint Health - Paris Hospital", "Bourbon Community Hospital"),
    S("Southern Tennessee Regional Health System - Pulaski", "Pulaski", "TN", "cms 440020",
      "Southern TN Regional Health System - Pulaski"),
    S("Southern Tennessee Regional Health System - Lawrenceburg", "Lawrenceburg", "TN", "cms 440175",
      "Southern TN Regional Health System - Lawrenceburg", "Southern Tennessee Regional - Lawrenceburg"),
    S("Ashley Regional Medical Center", "Vernal", "UT", "cms 460030", "Ashley Regional Hospital"),
    S("Castleview Hospital", "Price", "UT", "cms 460011"),
    S("Willamette Valley Medical Center", "McMinnville", "OR", "cms 380071"),
    # behavioral health hospitals (Lifepoint Behavioral Health)
    S("Triangle Springs", "Raleigh", "NC", "cms 344032"),
    S("Beckett Springs", "West Chester", "OH", "cms 364051", "Beckett Springs Behavioral Health Hospital"),
    S("Cottonwood Springs", "Olathe", "KS", "cms 174020", "Cottonwood Spring Behavioral Health",
      "Cottonwood Springs Behavioral Health"),
    S("Denver Springs", "Englewood", "CO", "cms 064028"),
    S("El Dorado Springs", "Tucson", "AZ", "cms 034033", "El Dorado Springs Behavioral Health"),
    S("Rainier Springs", "Vancouver", "WA", "cms 504013", "Rainer Springs", "Rainier Springs Behavioral Health Hospital"),
    S("Sycamore Springs", "Lafayette", "IN", "cms 154059"),
    S("Valley Springs Behavioral Health Hospital", "Holyoke", "MA", "cms 224047", "Valley Spring Behavioral Health",
      "Valley Springs Behavioral Health", "Valley Springs Behavioral Hospital"),
    S("Highland Springs", "Highland Hills", "OH", "board", "Highland Springs Behavioral Health Hospital"),
    S("Columbus Springs Dublin", "Dublin", "OH", "board", "Dublin Springs", "Columbus Springs - Dublin"),
    S("Columbus Springs East", "Columbus", "OH", "board", "Columbus Springs - East"),
    S("Woodland Springs", "Conroe", "TX", "board"),
    S("Westpark Springs", "Richmond", "TX", "board"),
    S("Mesa Springs", "Fort Worth", "TX", "board"),
    S("Collin Springs", "McKinney", "TX", "board"),
    S("Carrollton Springs", "Carrollton", "TX", "board"),
    S("Rock Springs", "Georgetown", "TX", "board"),
    S("Oakwood Springs", "Oklahoma City", "OK", "board"),
    S("Copper Springs East", "Gilbert", "AZ", "board", "Copper Springs - East", "Copper Springs East Behavioral Health"),
    S("Copper Springs Avondale", "Avondale", "AZ", "board", "Copper Springs"),
    S("WellBridge Healthcare Fort Worth", "Fort Worth", "TX", "cms 454128"),
    S("Centra Behavioral Health Hospital", "Lynchburg", "VA", "board", "Centra BH MSA JV"),
    S("TGH Behavioral Health Hospital", "Tampa", "FL", "board"),
    # inpatient rehabilitation hospitals Lifepoint runs (its joint ventures)
    S("Centra Rehabilitation Hospital", "Lynchburg", "VA", "board", "CHC PR & MSA - Centra Health Rehab Hospital - Lynchburg VA"),
    S("Rehabilitation Hospital of Oak Creek", "Oak Creek", "WI", "board"),
    S("HSHS Rehabilitation Hospital", "Green Bay", "WI", "board", "HSHS Rehabilitation",
      "Hospital Sisters Health System Rehabilitation Hospital"),
    S("UW Health Rehabilitation Hospital", "Madison", "WI", "board", "UW Health Rehabilitation"),
    S("Texas Rehabilitation Hospital of Plano", "Plano", "TX", "board", "Texas Rehabilitation of Plano"),
    S("Texas Rehabilitation Hospital of Fort Worth", "Fort Worth", "TX", "cms 673048"),
    S("Texas Rehabilitation Hospital of Arlington", "Arlington", "TX", "cms 673060"),
    S("Texas Rehabilitation Hospital of Keller", "Keller", "TX", "cms 673077"),
    S("Texas Rehabilitation Hospital of Denton", "Denton", "TX", "board"),
    S("Methodist Rehabilitation Hospital", "Dallas", "TX", "cms 673031", "Methodist Rehab Hospital"),
    S("Central Texas Rehabilitation Hospital", "Austin", "TX", "cms 673027", "Central Texas Rehabilitation"),
    S("Temple Rehabilitation Hospital", "Temple", "TX", "cms 673074"),
    S("The Hospitals of Providence Rehabilitation Hospital East", "El Paso", "TX", "board"),
    S("Baptist Health Rehabilitation Hospital", "South Miami", "FL", "board"),
    S("North Florida Rehabilitation Hospital", "Jacksonville", "FL", "board"),
    S("TGH Rehabilitation Hospital", "Tampa", "FL", "board"),
    S("PeaceHealth Springfield Rehabilitation Hospital", "Springfield", "OR", "board"),
    S("Ascension Saint Thomas Rehabilitation Hospital", "Nashville", "TN", "board",
      "Ascension St. Thomas Rehabilitation Hospital - IRF"),
    S("Knoxville Rehabilitation Hospital", "Knoxville", "TN", "board"),
    S("Baptist Memorial Rehabilitation Hospital", "Germantown", "TN", "cms 443034"),
    S("Peak Rehabilitation Hospital", "Apex", "NC", "board"),
    S("Mercy Rehabilitation Hospital Springfield", "Springfield", "MO", "board",
      "Mercy Rehabilitation Hospital in Springfield MO"),
    S("Mercy Rehabilitation Hospital St. Louis", "Chesterfield", "MO", "board"),
    S("Mercy Rehabilitation Hospital South", "St Louis", "MO", "board"),
    S("Mercy Rehabilitation Hospital Northwest Arkansas", "Rogers", "AR", "board"),
    S("Mercy Rehabilitation Hospital Fort Smith", "Fort Smith", "AR", "board"),
    S("Community Rehabilitation Hospital North", "Indianapolis", "IN", "board"),
    S("Community Rehabilitation Hospital South", "Greenwood", "IN", "board"),
    S("Community Rehabilitation Hospital West", "Brownsburg", "IN", "board", "Community Rehabilitation Hospitality West"),
    S("Rehabilitation Hospital of Montana", "Billings", "MT", "cms 273025"),
    S("Northeast Georgia Rehabilitation Hospital", "Gainesville", "GA", "board",
      "Northeast Georgia Rehabilitation Institute"),
    S("Lancaster Rehabilitation Hospital", "Lancaster", "PA", "board"),
    S("Liberty Rehabilitation Hospital", "Liberty Township", "OH", "cms 363045"),
    S("University Hospitals Rehabilitation Hospital", "Beachwood", "OH", "board", "University Hospital Beachwood Rehab"),
    S("Eastern Iowa Rehabilitation Hospital", "Coralville", "IA", "board", "Eastern Iowa Rehab Hospital"),
    S("MercyOne Clive Rehabilitation Hospital", "Clive", "IA", "board", "MercyOne Clive Rehabilitation Hospital - IRF"),
    S("Anderson Rehabilitation Hospital", "Edwardsville", "IL", "cms 143029", "Anderson Rehabilitation Institute"),
    S("Dignity Health East Valley Rehabilitation Hospital - Chandler", "Chandler", "AZ", "board"),
    S("El Camino Health Rehabilitation Hospital", "Sunnyvale", "CA", "board"),
    S("Virginia Mason Franciscan Rehabilitation Hospital", "Tacoma", "WA", "board"),
    S("Dignity Health East Valley Rehabilitation Hospital - Gilbert", "Gilbert", "AZ", "board"),
    S("Mercy Rehabilitation Hospital Oklahoma City North", "Oklahoma City", "OK", "board"),
    S("Mercy Health Rehabilitation Hospital Youngstown", "Youngstown", "OH", "board",
      "Mercy Health Rehabilitation Hospital"),
    S("University Hospitals Avon Rehabilitation Hospital", "Avon", "OH", "board"),
    S("Palomar Health Rehabilitation Institute", "Escondido", "CA", "board"),
    S("CHRISTUS Health Rehabilitation Hospital", "Longview", "TX", "board", "Longview Rehabilitation Hospital"),
    S("ProHealth Rehabilitation Hospital of Wisconsin", "Waukesha", "WI", "board", "Rehabilitation Hospital of Wisconsin"),
    S("Providence Swedish Rehabilitation Hospital", "Lynnwood", "WA", "board"),
    S("UC Davis Rehabilitation Hospital", "Sacramento", "CA", "board"),
    S("Mercy Rehabilitation Hospital Oklahoma City South", "Oklahoma City", "OK", "board"),
    # a named senior-care site (decision 2: its own name; the title rules
    # that keep a clinic or a nursing home off a hospital do not apply)
    S("Starr Regional Health and Rehab", "Etowah", "TN", "board"),
)
# Named sites that are themselves a care site: an entity sentence names them
# whatever the title says (offsite_title is not applied)
CARE_SITES = frozenset({"Starr Regional Health and Rehab"})

# The "Facility Name:" line (where the job is), then the employer sentences
# (who employs), then the opening lines: read_lifepoint.
LIFEPOINT_SPEC = T.Spec(LIFEPOINT, None, (T.Read("label", r"Facility(?:[ \t]+Name)?", "site"),), LIFEPOINT_SITES)

# "<X> is part of Lifepoint Health", "<X> part of Lifepoint Health", "<X> is
# operated jointly with Lifepoint Health and Mercy", "<X>, a Duke LifePoint
# hospital"
_PART_RX = re.compile(r"\b(?:(?:is|are)\s+)?(?:a\s+|proud(?:ly)?\s+)?(?:part\s+of|a\s+member\s+of|operated\s+jointly\s+with)"
                      r"\s+(?:the\s+)?Life\s?point\b|,\s+an?\s+(?:Duke\s+)?Life\s?point\s+(?:hospital|facility)\b", re.I)
# "<X> is a 154-bed general acute-care hospital"
_BEDS_RX = re.compile(r"\s+is\s+(?:a|an)\s+\d+\s*-?\s*bed\b", re.I)
# "At <X>, we are committed"
_AT_RX = re.compile(r"\bAt\s+([A-Z][^,.;]{3,80}?),\s+we\b")
_LEAD_RX = re.compile(r"^(?:your\s+experience\s+matters\b[\s,.:;!-]*|who\s+we\s+are\b[\s,.:;!-]*|at\s+)+", re.I)
_TAIL_RX = re.compile(r"\s+(?:is|are|was)\s|,?\s+formerly\b|,?\s+formelly\b|\s+located\b|,?\s+(?:a|an)\s+\d", re.I)
_SENTENCE_RX = re.compile(r"(?:(?<=[a-z]{3})|(?<=[0-9)]))[.!?:]\s+(?=[A-Z])")   # "St." and "Mt." end no sentence
_LEAD_LINES = 3
# Lifepoint's own practice brands, read off the titles
_LP_OFFSITE_RX = re.compile(r"\bFryeCare\b", re.I)


def _subject(text) -> str:
    """The subject of one sentence head: its lead ("Your experience
    matters", "Who we are:") and tail (" is a 154 bed hospital located in
    ...", ", formerly ...") cut."""
    subj = _SENTENCE_RX.split(text)[-1]
    subj = _LEAD_RX.sub("", subj.strip())
    t = _TAIL_RX.search(subj)
    if t:
        subj = subj[:t.start()]
    return subj.strip(" ,;:-")


def lifepoint_subjects(description) -> list[str]:
    """The employer the body names, best first: the subject of the first
    "part of Lifepoint" / "operated jointly with Lifepoint" / "a Duke
    LifePoint hospital" sentence, of the first "<X> is a 25-bed ..."
    sentence, and of the first "At <X>, we ..." sentence."""
    lines = T.clean_text_lines(description)
    out = []
    for rx in (_PART_RX, _BEDS_RX):
        for ln in lines:
            m = rx.search(ln)
            if m:
                subj = _subject(ln[:m.start()])
                if subj:
                    out.append(subj)
                break
    for ln in lines:
        m = _AT_RX.search(ln)
        if m:
            out.append(_subject(m.group(1)))
            break
    return out


def lifepoint_subject(description) -> str:
    """The first of lifepoint_subjects, or ""."""
    subs = lifepoint_subjects(description)
    return subs[0] if subs else ""


def offsite_title(title) -> bool:
    """A clinic, practice, office, senior-care or remote job by its title
    (facilities_town's rules, the TOWN family's)."""
    t = title or ""
    return bool(TOWN.REMOTE_RX.search(t) or TOWN.OFFSITE_RX.search(t) or TOWN.CARE_SITE_RX.search(t)
                or _LP_OFFSITE_RX.search(t))


def _lead_site(description):
    """A Site whose spelling one of the first lines starts with ("Wilson
    Medical Center Registered Nurse (RN), House Supervisor", "Fleming County
    Hospital, Acute Cat Scan Technologist"), and the line."""
    for ln in [x for x in T.clean_text_lines(description) if x.strip()][:_LEAD_LINES]:
        if _PART_RX.search(ln) or _BEDS_RX.search(ln) or _AT_RX.search(ln):
            continue            # an employer sentence: lifepoint_subjects already weighed it
        lk = T.lead_key(ln) + " "
        for k, site in LIFEPOINT_SPEC.lead:
            if lk.startswith(k + " "):
                # a line that also names another site names none
                rest = " " + lk[len(k):]
                if any(f" {k2} " in rest for k2, s2 in LIFEPOINT_SPEC.lead if s2.name != site.name):
                    return None, ""
                return site, ln
    return None, ""


def read_lifepoint(description):
    """(Site or None, kind, what, raw value) for one Lifepoint body, the
    facilities_text.read_text shape: the "Facility Name:" line first, then
    the employer sentences, then the opening lines."""
    site, kind, what, raw = T.read_text(LIFEPOINT_SPEC, description)
    if site is not None:
        return site, kind, what, raw
    subs = lifepoint_subjects(description)
    for subj in subs:
        hit = T.match_value(LIFEPOINT_SPEC, subj)
        if hit is not None:
            return hit, "entity", "site", subj
    lead, ln = _lead_site(description)
    if lead is not None:
        return lead, "entity", "site", ln
    if subs:
        return None, "", "unknown", subs[0]
    return site, kind, what, raw


# ── Text specs (bodies), one per system read here ──────────────────────────
TEXT_SPECS = {LIFEPOINT: LIFEPOINT_SPEC}
TEXT_READERS = {LIFEPOINT: read_lifepoint}
SYSTEMS = frozenset(TEXT_SPECS)


def text_facility(system, description, title="", city="", state=""):
    """(hospital_name, city, state) the body names for a row of `system` at
    (city, state), or None. Pure: no stored name, no Job. An entity
    sentence on a clinic, office or remote title names nothing."""
    spec = TEXT_SPECS.get((system or "").strip())
    if spec is None:
        return None
    site, kind, _what, v = TEXT_READERS[spec.system](description)
    if site is None or (kind == "entity" and site.name not in CARE_SITES and offsite_title(title)):
        return None
    return T.place(spec, site, kind, city, state, v)


# ── Stored names (nights without a body) ────────────────────────────────────
_KNOWN: dict = {}
KNOWN_PAGE = 1000
KNOWN_MAX_ROWS = 20000


def set_known_facilities(rows) -> int:
    """{(hospital_system, job_id): stored hospital_name} for rows whose
    stored name is one of the system's site or campus names here."""
    global _KNOWN
    out = {}
    for r in rows or []:
        spec = TEXT_SPECS.get((r.get("hospital_system") or "").strip())
        name, jid = T.clean_text(r.get("hospital_name")), r.get("job_id")
        if spec is None or jid in (None, "") or name not in spec.names:
            continue
        out[(spec.system, str(jid))] = name
    _KNOWN = out
    return len(out)


def known_facility(system, job_id) -> str:
    return _KNOWN.get(((system or "").strip(), str(job_id)), "")


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
            logger.warning(f"Hospital systems A: {system} load stopped after {got:,} rows ({e}); "
                           f"its rows past that keep a site only on nights their body is read")
    try:
        return set_known_facilities(rows)
    except Exception as e:          # never let this read end the run
        logger.warning(f"Hospital systems A: stored names not installed ({e})")
        set_known_facilities([])
        return 0


# ── Applying it to a Job (finalize_jobs, after facilities_text) ─────────────
UNKNOWN: dict = {}       # system -> {raw value: rows} named by no table entry, this run


def apply_facility(job) -> str:
    """Store the site on one Job of a SYSTEMS system still on the system
    label. Returns "text" (the body named it), "kept" (the stored name, no
    body tonight), "offsite" (an entity sentence on a clinic, office or
    remote title), "elsewhere" (the site is in another town: stays on the
    label), "unknown", "none", "other" (already a site: the Oracle work
    location named it), or "" for any other system. hospital_system is never
    changed."""
    spec = TEXT_SPECS.get((job.hospital_system or "").strip())
    if spec is None:
        return ""
    cur = T.clean_text(job.hospital_name)
    if cur and cur != spec.system:
        return "other"
    site, kind, what, raw = TEXT_READERS[spec.system](job.description)
    if site is not None:
        if kind == "entity" and site.name not in CARE_SITES and offsite_title(job.title):
            return "offsite"
        placed = T.place(spec, site, kind, job.city, job.state, raw)
        if placed is None:
            return "elsewhere"
        _set(job, *placed)
        return "text"
    if len((job.description or "").strip()) < T.NO_BODY_CHARS:
        name = known_facility(spec.system, job.job_id)
        if name:
            site = spec.names[name]
            placed = T.place(spec, site, "entity", job.city, job.state)
            if placed is not None:
                _set(job, name if name != site.name else placed[0], placed[1], placed[2])
                return "kept"
    if what == "unknown" and raw:
        d = UNKNOWN.setdefault(spec.system, {})
        d[raw[:80]] = d.get(raw[:80], 0) + 1
    return what


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
    parts = []
    for system, d in sorted(UNKNOWN.items()):
        vals = sorted(d.items(), key=lambda kv: -kv[1])[:top]
        parts.append(f"{system}: " + "; ".join(f"{v} ({n})" for v, n in vals))
    return " | ".join(parts)


# ═════════════════════════ ORACLE WORK LOCATIONS ════════════════════════════
# Added to facilities_oracle_other's tables before it builds its Specs
# (extend_oracle), so scrape_oracle names these sites from the work location
# in the list payload every night. Same shape as that module's _sys():
#   hospitals {clean public name: (town, state, source of the town)}
#   sites     {work location as the tenant spells it: clean name}, or
#             {spelling: {facility_names.fkey(town): clean name}} for a
#             spelling several sites share
#   rules     ((regex on the work location, clean name), ...), searched only
#             when no spelling matched (facility_names.resolve)

def _lifepoint_oracle():
    hospitals, sites = {}, {}
    generic: dict = {}
    for site, spellings in LIFEPOINT_SITES:
        src = site.source
        if src == "board":
            src = f"board: every Lifepoint posting naming it sits in {site.city} (2026-10-29)"
        hospitals[site.name] = (site.city, site.state, src)
    for site, spellings in LIFEPOINT_SITES:
        for sp in spellings:
            sites[sp] = site.name
        for t, st, cname in site.campuses:
            if cname and cname in hospitals:
                generic.setdefault(site.name, {})[F.fkey(t)] = cname
    # a spelling that names no town ("Sovah Health", "Maria Parham") is the
    # campus's own hospital in the campus's town; "Sovah Danville" stays
    # Danville's
    for site, spellings in LIFEPOINT_SITES:
        if site.name in generic:
            towns = {F.fkey(site.city): site.name, **generic[site.name]}
            for sp in spellings:
                if F.fkey(site.city) not in F.fkey(sp):
                    sites[sp] = dict(towns)
    return dict(hospitals=hospitals, sites=sites, rules=())


ORACLE_EXTRA = {
    LIFEPOINT: _lifepoint_oracle(),
    # Providence: the work-location facet per state (WA, OR, CA, AK, TX, MT,
    # NM) and for Seattle and Spokane, 2026-10-29. Names as Providence prints
    # them; a town the CMS list gives ("cms <id>") or the work location's own
    # address ("address").
    PROVIDENCE: dict(hospitals={
        "Providence St. Patrick Hospital":                  ("Missoula", "MT", "cms 270014"),
        "Providence St. Joseph Medical Center Polson":      ("Polson", "MT", "cms 271343"),
        "Swedish Edmonds":                                  ("Edmonds", "WA", "cms 500026"),
        "Swedish Issaquah":                                 ("Issaquah", "WA", "cms 500152"),
        "Swedish Cherry Hill":                              ("Seattle", "WA", "cms 500025"),
        "Swedish Ballard":                                  ("Seattle", "WA", "address 5300 Tallman Ave NW, Seattle"),
        "Kadlec Regional Medical Center":                   ("Richland", "WA", "cms 500058"),
        "Providence Centralia Hospital":                    ("Centralia", "WA", "cms 500019"),
        "Providence Holy Family Hospital":                  ("Spokane", "WA", "cms 500077"),
        "Providence St. Luke's Rehabilitation Medical Center": ("Spokane", "WA", "cms 503025"),
        "Providence Mount Carmel Hospital":                 ("Colville", "WA", "cms 501326"),
        "Providence St. Joseph Hospital Chewelah":          ("Chewelah", "WA", "cms 501309"),
        "Providence Saint John's Health Center":            ("Santa Monica", "CA", "cms 050290"),
        "Providence St. Joseph Hospital Orange":            ("Orange", "CA", "cms 050069"),
        "Providence Little Company of Mary Medical Center Torrance": ("Torrance", "CA", "cms 050353"),
        "Providence Little Company of Mary Medical Center San Pedro": ("San Pedro", "CA", "cms 050078"),
        "Providence Santa Rosa Memorial Hospital":          ("Santa Rosa", "CA", "cms 050174"),
        "Providence Queen of the Valley Medical Center":    ("Napa", "CA", "cms 050009"),
        "Providence Holy Cross Medical Center":             ("Mission Hills", "CA", "cms 050278"),
        "Providence St. Mary Medical Center Apple Valley":  ("Apple Valley", "CA", "cms 050300"),
        "Providence St. Jude Medical Center":               ("Fullerton", "CA", "cms 050168"),
        "Providence Cedars-Sinai Tarzana Medical Center":   ("Tarzana", "CA", "cms 050761"),
        "Providence Mission Hospital Laguna Beach":         ("Laguna Beach", "CA", "cms 050193"),
        "Providence Redwood Memorial Hospital":             ("Fortuna", "CA", "cms 051318"),
        "Providence Petaluma Valley Hospital":              ("Petaluma", "CA", "cms 050136"),
        "Providence Medford Medical Center":                ("Medford", "OR", "cms 380075"),
        "Providence Milwaukie Hospital":                    ("Milwaukie", "OR", "cms 380082"),
        "Providence Newberg Medical Center":                ("Newberg", "OR", "cms 380037"),
        "Providence Willamette Falls Medical Center":       ("Oregon City", "OR", "cms 380038"),
        "Providence Seaside Hospital":                      ("Seaside", "OR", "cms 381303"),
        "Providence Hood River Memorial Hospital":          ("Hood River", "OR", "cms 381318"),
        "Providence Kodiak Island Medical Center":          ("Kodiak", "AK", "cms 021306"),
        "Providence Valdez Medical Center":                 ("Valdez", "AK", "cms 021301"),
        "Providence Seward Medical Center":                 ("Seward", "AK", "cms 021302"),
        "Providence Seward Mountain Haven":                 ("Seward", "AK", "board: work location Providence Seward Mountain Haven-Seward(HR)"),
        "Providence St. Elias Specialty Hospital":          ("Anchorage", "AK", "cms 022001"),
        "Covenant Children's Hospital":                     ("Lubbock", "TX", "cms 453306"),
        "Covenant Specialty Hospital":                      ("Lubbock", "TX", "cms 452102"),
        "Covenant Health Plainview":                        ("Plainview", "TX", "cms 450539"),
        "Covenant Health Levelland":                        ("Levelland", "TX", "cms 450755"),
        "Covenant Health Hobbs Hospital":                   ("Hobbs", "NM", "cms 320065"),
    }, sites={
        "St Patrick Hospital-Broadway Bldg-Missoula MT(HR)":      "Providence St. Patrick Hospital",
        "St Joseph Medical Ctr Polso-Polson(HR)":                 "Providence St. Joseph Medical Center Polson",
        "Swedish Edmonds 21601 76th(HR)":                         "Swedish Edmonds",
        "Swedish Issaquah(HR)":                                   "Swedish Issaquah",
        "Swedish Cherry Hill 500 17th-Seattle(HR)":               "Swedish Cherry Hill",
        "Swedish Ballard 5300 Tallman-Seattle(HR)":               "Swedish Ballard",
        "Kadlec Regional Medical Ctr-Richland(HR)":               "Kadlec Regional Medical Center",
        "Centralia Hospital-Centralia(HR)":                       "Providence Centralia Hospital",
        "Holy Family Hospital-Spokane(HR)":                       "Providence Holy Family Hospital",
        "St Lukes Rehab-Spokane(HR)":                             "Providence St. Luke's Rehabilitation Medical Center",
        "Providence Saint John's Health Ctr-Santa Monica(HR)":    "Providence Saint John's Health Center",
        "St Joseph Hospital-Orange(HR)":                          "Providence St. Joseph Hospital Orange",
        "Providence Little Co of Mary Medical Ctr-Torrance(HR)":  "Providence Little Company of Mary Medical Center Torrance",
        "Santa Rosa Memorial Hospital(HR)":                       "Providence Santa Rosa Memorial Hospital",
        "Queen of the Valley Medical Center(HR)":                 "Providence Queen of the Valley Medical Center",
        "Providence Holy Cross Medical Ctr-Mission Hills(HR)":    "Providence Holy Cross Medical Center",
        "St Mary Medical Center-Apple Valley(HR)":                "Providence St. Mary Medical Center Apple Valley",
        "Providence Medford Medical Center(HR)":                  "Providence Medford Medical Center",
        "Providence Milwaukie Hospital-Milwaukie(HR)":            "Providence Milwaukie Hospital",
        "Providence Newberg Medical Ctr-Newberg(HR)":             "Providence Newberg Medical Center",
        "Providence Willamette Falls Medical Ctr-Oregon City(HR)": "Providence Willamette Falls Medical Center",
        "Providence Seaside Hospital-Seaside(HR)":                "Providence Seaside Hospital",
        "Providence Hood River Memorial Hosp-Hood River(HR)":     "Providence Hood River Memorial Hospital",
        "Providence Kodiak Island Medical Ctr-Kodiak(HR)":        "Providence Kodiak Island Medical Center",
        "Providence Valdez Medical Ctr-Valdez(HR)":               "Providence Valdez Medical Center",
        "Providence Seward Medical Ctr-Seward(HR)":               "Providence Seward Medical Center",
        "Providence Seward Mountain Haven-Seward(HR)":            "Providence Seward Mountain Haven",
        "Providence St Elias Specialty Hospital-Anchorage(HR)":   "Providence St. Elias Specialty Hospital",
        "Covenant Children's Hospital(HR)":                       "Covenant Children's Hospital",
        "Covenant Specialty Hospital(HR)":                        "Covenant Specialty Hospital",
        "Covenant Health Plainview & Ctr for Outpt Diabetes Ed(HR)": "Covenant Health Plainview",
        "Covenant Health Levelland Hospital(HR)":                 "Covenant Health Levelland",
        "Covenant Hobbs Hospital-4900 N Lovington Hwy(HR)":       "Covenant Health Hobbs Hospital",
    }, rules=(
        # hospitals the facet did not reach: their own place name and
        # "Hospital" / "Medical Ctr" in the work location, never a clinic
        (r"^(?!.*\b(?:clinic|clinics|medical\s+(?:group|grp)|pacmed|office|bldg)\b).*"
         r"\bSt\.?\s*Jude\s+(?:Medical|Hosp)", "Providence St. Jude Medical Center"),
        (r"^(?!.*\b(?:clinic|clinics|office|bldg)\b).*\bTarzana\s+Medical", "Providence Cedars-Sinai Tarzana Medical Center"),
        (r"^(?!.*\b(?:clinic|clinics|office|bldg)\b).*\bLittle\s+Co(?:mpany)?\s+of\s+Mary\b.*\bSan\s+Pedro\b",
         "Providence Little Company of Mary Medical Center San Pedro"),
        (r"^(?!.*\b(?:clinic|clinics|office|bldg)\b).*\bMission\s+Hosp\w*\b.*\bLaguna\b",
         "Providence Mission Hospital Laguna Beach"),
        (r"^(?!.*\b(?:clinic|clinics|office|bldg)\b).*\bRedwood\s+Memorial\s+Hosp", "Providence Redwood Memorial Hospital"),
        (r"^(?!.*\b(?:clinic|clinics|office|bldg)\b).*\bPetaluma\s+Valley\s+Hosp", "Providence Petaluma Valley Hospital"),
        (r"^(?!.*\b(?:clinic|clinics|office|bldg)\b).*\b(?:Mount|Mt\.?)\s+Carmel\s+Hosp",
         "Providence Mount Carmel Hospital"),
        (r"^(?!.*\b(?:clinic|clinics|office|bldg)\b).*\bSt\.?\s+Joseph\s+Hosp\w*\b.*\bChewelah\b",
         "Providence St. Joseph Hospital Chewelah"),
    )),
    # Baptist Memorial: the work-location facet per state (MS, TN, AR),
    # 2026-10-29. Names written the way main writes the others
    # ("BMH - Desoto Hospital" = Baptist Memorial Hospital-DeSoto).
    BMH: dict(hospitals={
        "Baptist Memorial Hospital for Women":            ("Memphis", "TN", "cms 440222"),
        "Baptist Memorial Hospital-Carroll County":       ("Huntingdon", "TN", "cms 440016"),
        "Baptist Memorial Hospital-Tipton":               ("Covington", "TN", "cms 440131"),
        "Baptist Memorial Hospital-Union City":           ("Union City", "TN", "cms 440130"),
        "Baptist Memorial Hospital-Booneville":           ("Booneville", "MS", "cms 250044"),
        "Baptist Memorial Hospital-Calhoun":              ("Calhoun City", "MS", "cms 251331"),
        "Baptist Memorial Hospital-Oktibbeha County":     ("Starkville", "MS", "cms 250050"),
        "Baptist Memorial Hospital-Rankin County":        ("Brandon", "MS", "board: every BMH Rankin County posting sits in Brandon"),
        "Baptist Memorial Hospital-Crittenden":           ("West Memphis", "AR", "cms 040156"),
        "Baptist Memorial Hospital-Paragould":            ("Paragould", "AR", "board: every BMH Paragould posting sits in Paragould"),
        "Baptist Medical Center Yazoo":                   ("Yazoo City", "MS", "cms 251313"),
        "Baptist Medical Center Leake":                   ("Carthage", "MS", "cms 251315"),
        "Baptist Medical Center Attala":                  ("Kosciusko", "MS", "cms 251336"),
        "Baptist Memorial Restorative Care Hospital":     ("Memphis", "TN", "cms 442010"),
        "Baptist Memorial Rehabilitation Hospital":       ("Germantown", "TN", "cms 443034"),
    }, sites={
        "BMH - Women's Hospital":           "Baptist Memorial Hospital for Women",
        "BMH - Huntingdon Hospital":        "Baptist Memorial Hospital-Carroll County",
        "BMH - Carroll County Hospital":    "Baptist Memorial Hospital-Carroll County",
        "BMH - Tipton Hospital":            "Baptist Memorial Hospital-Tipton",
        "BMH - Union City Hospital":        "Baptist Memorial Hospital-Union City",
        "BMH - Booneville Hospital":        "Baptist Memorial Hospital-Booneville",
        "BMH - Calhoun Hospital":           "Baptist Memorial Hospital-Calhoun",
        "BMH - Oktibbeha County Hospital":  "Baptist Memorial Hospital-Oktibbeha County",
        "BMH - Rankin County Hospital":     "Baptist Memorial Hospital-Rankin County",
        "BMH - Crittenden Hospital":        "Baptist Memorial Hospital-Crittenden",
        "BMH - Paragould Hospital":         "Baptist Memorial Hospital-Paragould",
    }, rules=(
        (r"^(?!.*\b(?:clinic|clinics|BMG|office)\b).*\bYazoo\b", "Baptist Medical Center Yazoo"),
        (r"^(?!.*\b(?:clinic|clinics|BMG|office)\b).*\bLeake\b", "Baptist Medical Center Leake"),
        (r"^(?!.*\b(?:clinic|clinics|BMG|office)\b).*\bAttala\b", "Baptist Medical Center Attala"),
        (r"^(?!.*\b(?:clinic|clinics|BMG|office)\b).*\bRestorative\b", "Baptist Memorial Restorative Care Hospital"),
        (r"^(?!.*\b(?:clinic|clinics|BMG|office|outpatient)\b).*\bRehab\w*\s+Hosp\w*\b.*\bGermantown\b|"
         r"^(?!.*\b(?:clinic|clinics|BMG|office|outpatient)\b).*\bGermantown\b.*\bRehab\w*\s+Hosp",
         "Baptist Memorial Rehabilitation Hospital"),
        (r"^BAMC\b(?!.*\b(?:clinic|clinics|office)\b)", "Baptist Anderson Regional Medical Center"),
    )),
}


def extend_oracle(systems: dict) -> dict:
    """Add ORACLE_EXTRA to facilities_oracle_other's SYSTEMS (before its
    _build). A clean name already there must be in the same town; a
    spelling already there must name the same hospital. All or nothing:
    every table is checked on a copy first, and `systems` changes only when
    all of them pass (ValueError otherwise). Returns {system: hospitals
    added}."""
    out = {}
    staged = {}
    for system, extra in ORACLE_EXTRA.items():
        src = systems.get(system)
        if src is None:
            raise ValueError(f"extend_oracle: {system} is not in facilities_oracle_other")
        t = dict(src, hospitals=dict(src["hospitals"]), sites=dict(src["sites"]), rules=tuple(src["rules"]))
        staged[system] = t
        hosp = t["hospitals"]
        added = 0
        for name, place in extra["hospitals"].items():
            old = hosp.get(name)
            if old is not None:
                if (F.fkey(old[0]), old[1]) != (F.fkey(place[0]), place[1]):
                    raise ValueError(f"extend_oracle: {system} {name!r} is in {old[0]}, not {place[0]}")
                continue
            clash = next((n for n in hosp if F.fkey(n) == F.fkey(name)), None)
            if clash:
                raise ValueError(f"extend_oracle: {system} {name!r} folds like {clash!r}")
            hosp[name] = place
            added += 1
        known = {F.fkey(raw): tgt for raw, tgt in t["sites"].items()}
        for raw, target in extra["sites"].items():
            for name in (target.values() if isinstance(target, dict) else (target,)):
                if name not in hosp:
                    raise ValueError(f"extend_oracle: {system} {raw!r} names unknown {name!r}")
            prev = known.get(F.fkey(raw))
            if prev is not None and prev != target:
                raise ValueError(f"extend_oracle: {system} {raw!r} already names {prev!r}")
            if F.fkey(raw) in {F.fkey(n) for n in hosp} and not isinstance(target, dict) and \
                    next(n for n in hosp if F.fkey(n) == F.fkey(raw)) != target:
                raise ValueError(f"extend_oracle: {system} {raw!r} is another hospital's own name")
            t["sites"][raw] = target
            known[F.fkey(raw)] = target
        for _rx, name in extra["rules"]:
            if name not in hosp:
                raise ValueError(f"extend_oracle: {system} rule names unknown {name!r}")
        t["rules"] = tuple(t["rules"]) + tuple(extra["rules"])
        out[system] = added
    for system, t in staged.items():
        systems[system].update(hospitals=t["hospitals"], sites=t["sites"], rules=t["rules"])
    return out
