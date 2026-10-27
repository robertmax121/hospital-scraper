"""One clean name per VA facility on USAJOBS postings (2026-10-27).

The owner, 2026-10-27, on a Clearwater, FL posting: "for VA jobs, using 'va
hospitals' is not acceptable. Scrape the real facility name."

USAJOBS names the VA facility that hires on the hiring-organization line under
"Veterans Health Administration" (the API's UserArea.Details.SubAgencyName,
the line under the agency on the job page): "Bay Pines VA Healthcare System".
On 2026-10-27 3,473 active rows were filed under the system "VA Hospitals":
1,288 named only the agency ("Veterans Health Administration": the posting's
line was the agency itself or blank), and the rest spelled their facilities
630 ways ("North Florida/South Georgia VA Healthcare System" and "...
Veterans Health System"; "VA North Texas Health Care System (VANTHCS)";
"Kansas City VA Medical Center - Sterile Processing Services"; "Veterans
Health Administration-Bronx, NY VAMC").

This module is the one place that turns any of those values into one name per
facility, for the USAJOBS adapter (scraper.usajobs_facility, _usajobs_jobs)
and for the one-time cleanup of stored rows
(claud_outputinput/va_facility_2026-10-27/make_sql.py):

  facility_segment(raw)   the part of a raw value that names a facility,
                          without departments, towns, acronyms or the
                          network / agency in front ("" when it names none)
  canonical(raw, state)   the facility's one name: a medical center with a
                          name of its own when the value names one
                          (HOSPITALS, hospital_for), else a station of
                          STATIONS (every spelling of the station gives the
                          station's name), else the cleaned segment; "" for
                          the agency itself
  station_for(raw, state) the station only (a medical center gives its
                          station); station_of(name) the same for a name
                          canonical gave
  town_facility(city, st) the station that runs the VA's care in a town, only
                          where that is certain (TOWNS); "" elsewhere
  is_generic(name)        True for a value that names no facility

The level is the station: the medical center or health care system with its
own number, as USAJOBS's hiring-organization line names it, except where the
value names one of its medical centers by a name of its own (rule of
2026-10-27, the owner wants the real facility: "C.W. Bill Young VA Medical
Center" stays C.W. Bill Young VA Medical Center, not Bay Pines VA Healthcare
System). Its clinics in other towns carry the station's name ("located at the
North Pinellas VA Clinic in Clearwater" is Bay Pines VA Healthcare System), as
do agency-only postings named by their town or summary. A national program or a
network office that hires in its own name ("National Oncology Program",
"VISN 4 VA Heart of America Health Network") keeps that name: it is the
employer the posting names. hospital_system ("VA Hospitals") is never
changed: the scraper upserts on (hospital_system, job_id).
"""
from __future__ import annotations

import re
import unicodedata

_QUOTES = {"\u2018": "'", "\u2019": "'", "\u201c": '"', "\u201d": '"', "\u2013": "-", "\u2014": "-",
           "\u00a0": " "}

# The agency itself, in the ways USAJOBS writes it: no facility.
GENERIC_LABELS = frozenset({
    "veterans health administration", "veterans' health administration", "department of veterans affairs",
    "veterans affairs", "va", "vha", "veterans health administration (vha)", "va hospitals",
})

# A segment that names a station: a medical center, a health care system, a
# hospital. A clinic ("North Pinellas VA Clinic", "Hamilton CBOC") is a site
# of a station and is named by its town (TOWNS).
FACILITY_WORD_RX = re.compile(
    r"\b(?:medical\s+cent(?:er|re)s?|health\s*care\s+systems?|health\s+systems?|hospitals?|VAMC|HCS|"
    r"medical\s+facility|health\s*care\s+cent(?:er|re)|health\s+cent(?:er|re)|veterans'?\s+home|"
    r"rehabilitation\s+cent(?:er|re)|ambulatory\s+care\s+cent(?:er|re)|ACC)\b", re.I)
_LAST_FACILITY_WORD_RX = re.compile(
    r"^(.*\b(?:medical\s+cent(?:er|re)s?|health\s*care\s+systems?|health\s+systems?|hospitals?|VAMC|HCS|"
    r"medical\s+facility|health\s*care\s+cent(?:er|re)|health\s+cent(?:er|re)|veterans'?\s+home|"
    r"rehabilitation\s+cent(?:er|re)(?:\s+and\s+clinics)?|ambulatory\s+care\s+cent(?:er|re)|ACC))"
    r"(?:\s+of\s+the\s+[A-Z][\w'-]*)?", re.I)
# Separators between the facility and a department, a town or the network.
# A hyphen inside a name ("Mann-Grandstaff", "Nebraska-Western Iowa") is not
# one; ", Jr." is part of a name ("Edward Hines, Jr. VA Hospital").
_SEP_RX = re.compile(r"\s+[-|:;]+\s*|\s*[|;]\s*|(?<=[A-Za-z.)])[-:]\s+|,(?!\s*(?:Jr|Sr)\b)\s*")
_LEAD_RX = re.compile(
    r"^(?:purpose\.\s+)?(?:the\s+)?"
    r"(?:(?:veterans'?\s+health\s+administration(?:'s)?|vha|veterans\s+health(?!\s+(?:care|system)))"
    r"(?=[\s,/-]|$)[\s,/-]*)?"
    r"(?:VISN\s*\d+\s*[-/:]?\s*|V\d+\s+|\(VISN\s*\d+\)\s*)?(?:the\s+)?", re.I)
_TAIL_RX = re.compile(
    r"\s+(?:in|is\s+located|located|at)\s+[A-Z].*$|\s*\([^()]*\)\s*$|\s+veterans\s+health\s+administration$|"
    r"\s+(?:primary|specialty)\s+care$|[\s.,;:|-]+$", re.I)
_FILLER = {"the", "va", "vha", "veterans", "veteran", "veterans'", "affairs", "affair's", "administration",
           "department", "of", "medical", "center", "centre", "health", "care", "healthcare", "system",
           "systems", "hospital", "vamc", "us", "u.s.", "d", "and", "administrator"}
_STATE_CODES = ("AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH NJ "
                "NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY PR GU VI").split()
_STATE_NAMES = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA", "colorado": "CO",
    "connecticut": "CT", "delaware": "DE", "district of columbia": "DC", "florida": "FL", "georgia": "GA",
    "hawaii": "HI", "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD", "massachusetts": "MA",
    "michigan": "MI", "minnesota": "MN", "mississippi": "MS", "missouri": "MO", "montana": "MT",
    "nebraska": "NE", "nevada": "NV", "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM",
    "new york": "NY", "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC", "south dakota": "SD",
    "tennessee": "TN", "texas": "TX", "utah": "UT", "vermont": "VT", "virginia": "VA", "washington": "WA",
    "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY", "puerto rico": "PR",
}
_STATE_WORDS = {c.lower() for c in _STATE_CODES}
_STATE_NAME_RX = re.compile(r"\b(?:%s)\b" % "|".join(sorted((re.escape(n) for n in _STATE_NAMES), key=len,
                                                            reverse=True)), re.I)


def clean_text(s) -> str:
    """NFKC, straight quotes and dashes, whitespace collapsed."""
    s = unicodedata.normalize("NFKC", str(s or ""))
    for a, b in _QUOTES.items():
        s = s.replace(a, b)
    return re.sub(r"\s+", " ", s).strip()


def _proper_words(seg: str, states: bool = False) -> set:
    """The words of a segment that name something: not filler, not a state
    code, and (unless states) not a state name ("Alaska", "West Virginia")."""
    s = seg if states else _STATE_NAME_RX.sub(" ", seg)
    return {w.lower().strip(".,'()\"") for w in s.split()} - _FILLER - _STATE_WORDS - {""}


def _trim(seg: str) -> str:
    seg = _LEAD_RX.sub("", seg.strip())
    prev = None
    while prev != seg:
        prev = seg
        seg = _TAIL_RX.sub("", seg).strip()
    m = _LAST_FACILITY_WORD_RX.match(seg)
    if m:
        seg = m.group(0)
    return seg.strip()


def _town_part(seg: str) -> str:
    """A segment that is only a town (and maybe its state): "Augusta",
    "Lexington KY", "Beckley". "" otherwise."""
    s = _STATE_NAME_RX.sub(" ", seg)
    words = [w for w in s.replace(".", " ").split() if w.lower() not in _STATE_WORDS]
    if not words or len(words) > 3 or not all(re.fullmatch(r"[A-Z][a-z'-]+", w) for w in words):
        return ""
    return " ".join(words)


def segment_candidates(raw) -> list:
    """The readings of a raw hiring-organization value that may name its
    facility, best first, trimmed of departments, towns, acronyms and the
    network / agency in front. A segment whose only proper words are a state
    ("Alaska VA Healthcare System", "West Virginia VA Medical Center") comes
    with the town beside it as a second reading ("Beckley, West Virginia VA
    Medical Center" is also "Beckley VA Medical Center"; "VA Medical Center,
    Lexington KY" is only "Lexington VA Medical Center")."""
    s = clean_text(raw)
    if not s or s.lower() in GENERIC_LABELS:
        return []
    out = []
    segs = [x.strip() for x in _SEP_RX.split(s) if x and x.strip()]
    for i, seg in enumerate(segs):
        if not FACILITY_WORD_RX.search(seg):
            continue
        t = _trim(seg)
        if not t or not FACILITY_WORD_RX.search(t):
            continue
        if _proper_words(t):
            out.append(t)
            break
        if _proper_words(t, states=True):
            out.append(t)
        for j in (i - 1, i + 1):
            if 0 <= j < len(segs) and not FACILITY_WORD_RX.search(segs[j]):
                town = _town_part(_LEAD_RX.sub("", segs[j]))
                if town:
                    body = _STATE_NAME_RX.sub(" ", t)
                    body = " ".join(w for w in body.split() if w == "VA" or w.lower() not in _STATE_WORDS)
                    out.append(f"{town} {body}".strip())
                    break
        if out:
            break
    return out


def facility_segment(raw) -> str:
    """The first reading of segment_candidates, or "" when there is none."""
    c = segment_candidates(raw)
    return c[0] if c else ""


def name_key(s) -> str:
    """Spelling-blind key of a facility name: case, punctuation, "VA" /
    "Veterans Affairs" / "VAMC", "Health Care" / "Healthcare", "Saint" / "St",
    plurals and parentheticals do not count."""
    s = clean_text(s).lower()
    s = re.sub(r"\([^()]*\)", " ", s)
    s = s.replace("&", " and ")
    s = re.sub(r"\b(?:department\s+of\s+veterans'?\s+affairs|department\s+of\s+the\s+va|"
               r"veterans?'?\s+affair'?s?|veterans?'?\s+administration)\b", " va ", s)
    s = re.sub(r"\bvamc\b", " va medical center ", s)
    s = re.sub(r"\bhcs\b", " healthcare system ", s)
    s = re.sub(r"\bacc\b", " ambulatory care center ", s)
    s = re.sub(r"\bmedical\s+facility\b", " medical center ", s)
    s = re.sub(r"\bhealth\s*care\b", " healthcare ", s)
    s = re.sub(r"\bsystems\b", " system ", s)
    s = re.sub(r"\bhealth\s+system\b", " healthcare system ", s)
    s = re.sub(r"\bsaint\b", " st ", s)
    s = re.sub(r"\blieutenant\s+colonel\b|\blt\.?\s*col\.?\b", " ltc ", s)
    s = re.sub(r"\bjunior\b", " jr ", s)
    s = re.sub(r"\bwm\b", " william ", s)
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    drop = {"va", "vha", "veterans", "veteran", "the", "jr", "sr", "s", "us"}
    toks = [t for t in s.split() if t not in drop and t not in _STATE_WORDS]
    # a state after a town ("Martinsburg West Virginia VA Medical Center",
    # "Birmingham, Alabama VA Health Care System") does not count; a state
    # that is the name ("VA Maine Healthcare System", "Central Alabama",
    # "Nebraska-Western Iowa", "Iowa City") does
    out, i = [], 0
    while i < len(toks):
        hit = 0
        for n in (3, 2, 1):
            if " ".join(toks[i:i + n]) in _STATE_NAMES:
                hit = n
                break
        after_town = bool(out) and out[-1] not in _DIRECTIONS and out[-1] not in _FILLER
        if hit and after_town and i + hit < len(toks) and toks[i + hit] in _FILLER | {"healthcare"}:
            i += hit
            continue
        out.append(toks[i])
        i += 1
    return " ".join(out)


_DIRECTIONS = {"north", "south", "east", "west", "northern", "southern", "eastern", "western", "central",
               "northeast", "southeast", "northwest", "southwest", "greater", "upper", "lower", "new", "of"}


# ── Stations ────────────────────────────────────────────────────────────────
# (name, states, other names). The name is mostly the spelling the VA's
# hiring-organization lines used most on active rows on 2026-10-27; where
# they disagree, the health care system's name over its main medical
# center's, an abbreviation written out ("Lieutenant Colonel", not "LTC"),
# the station's own punctuation ("W.G. (Bill) Hefner"), and its current name
# where it was renamed or the spellings split (North Florida/South Georgia
# Veterans Health System, Ralph H. Johnson VA Health Care System,
# Fayetteville VA Coastal Health Care System). Every other name is a spelling
# of the same station seen on USAJOBS (hiring-organization lines, the agency
# contact address), or a medical-center spelling that names no medical center
# of its own (see HOSPITALS). states: the states its sites are in (a clinic
# across a state line included). A name is only listed here when it is the
# same station; a clinic town goes in TOWNS, a medical center with a name of
# its own in HOSPITALS.
STATIONS: tuple = (
    # Alabama
    ("Birmingham VA Health Care System", "AL", ("Birmingham VA Medical Center", "Birmingham VA Healthcare Service")),
    ("Central Alabama Veterans Health Care System", "AL GA", ("Central Alabama VA Medical Center",)),
    ("Tuscaloosa VA Medical Center", "AL", ("Tuscaloosa VA Healthcare System",)),
    # Alaska
    ("Alaska VA Healthcare System", "AK", ("Alaska VA Medical Center", "Anchorage VA Medical Center")),
    # Arizona
    ("VA Phoenix Healthcare System", "AZ", ("Phoenix VA Health Care System", "Phoenix/Gilbert AZ VA Medical Center")),
    ("Southern Arizona VA Health Care System", "AZ", ()),
    ("Northern Arizona VA Health Care System", "AZ", ()),
    # Arkansas / Ozarks
    ("Central Arkansas Veterans Health Care System", "AR", (
        "Central Arkansas Veterans Healthcare System", "Central Arkansas Veteran Healthcare System")),
    ("Veterans Healthcare System of the Ozarks", "AR MO OK", ("Veterans Health Care System of the Ozarks",)),
    # California
    ("Greater Los Angeles VA Health Care System", "CA", ("Greater Los Angeles VA Medical Center",)),
    ("Long Beach VA Health Care System", "CA", ()),
    ("VA Loma Linda Healthcare System", "CA", ("Hemet VA Medical Center",)),
    ("San Diego VA Healthcare System", "CA", ()),
    ("VA Palo Alto Health Care System", "CA", ("Livermore VA Medical Center",)),
    ("San Francisco VA Health Care System", "CA", ("San Francisco VA Medical Center",)),
    ("VA Northern California Healthcare System", "CA", ()),
    ("VA Central California Health Care System", "CA", ()),
    # Colorado / Wyoming
    ("VA Eastern Colorado Health Care System", "CO", ("Eastern Colorado VA Health Care System",)),
    ("VA Western Colorado Health Care System", "CO", ()),
    ("Cheyenne VA Health Care System", "WY CO", ("Cheyenne VA Medical Center",)),
    ("Sheridan VA Health Care System", "WY", ("Sheridan VA Medical Center",)),
    # Connecticut / New England
    ("VA Connecticut Healthcare System", "CT", ()),
    ("VA Boston Healthcare System", "MA", ()),
    ("VA Bedford Healthcare System", "MA", ()),
    ("VA Central Western Massachusetts Healthcare System", "MA", ()),
    ("VA Maine Healthcare System", "ME", ()),
    ("VA Manchester Healthcare System", "NH", ("Manchester VA Medical Center",)),
    ("VA Providence Healthcare System", "RI MA", ("Providence VA Medical Center", "Providence Healthcare System")),
    ("VA White River Junction Healthcare System", "VT NH", ("White River Junction VA Medical Center",)),
    # DC / Delaware / Maryland
    ("Washington DC VA Medical Center", "DC", ()),
    ("Wilmington VA Medical Center", "DE NJ", ("VA Wilmington Healthcare System",)),
    ("VA Maryland Health Care System", "MD", ("VA Baltimore Health Care System",)),
    # Florida
    ("Bay Pines VA Healthcare System", "FL", ()),
    ("North Florida/South Georgia Veterans Health System", "FL GA", ("North Florida/South Georgia VA Healthcare System",)),
    ("Miami VA Healthcare System", "FL", ()),
    ("West Palm Beach VA Healthcare System", "FL", ()),
    ("Orlando VA Healthcare System", "FL", ("Orlando VA Medical Center", "Lake Nona VA Medical Center")),
    ("James A. Haley VA Medical Center", "FL", (
        "James A. Haley Veterans' Hospital", "Tampa VA Hospital", "Tampa VA Medical Center",
        "James A. VA Healthcare System")),
    # Georgia
    ("Atlanta VA Health Care System", "GA", ("Altanta VA Healthcare System",)),
    ("Charlie Norwood VA Medical Center", "GA", (
        "Augusta VA Health Care System", "Augusta VA Medical Center", "Charlie A. Norwood VA Medical Center")),
    ("Carl Vinson VA Medical Center", "GA", ("Dublin VA Medical Center", "Dublin Carl Vinson VA Medical Center")),
    # Hawaii / Pacific, Caribbean
    ("VA Pacific Islands Health Care System", "HI", ()),
    ("VA Caribbean Healthcare System", "PR VI", ()),
    # Idaho
    ("Boise VA Medical Center", "ID", ("VA Boise Healthcare System",)),
    # Illinois
    ("Edward Hines Jr. VA Hospital", "IL", ("Hines VA Hospital", "Edward Hines Junior Hospital")),
    ("Jesse Brown VA Medical Center", "IL", ("Jesse Brown VA Hospital",)),
    ("Marion VA Health Care System", "IL IN KY", ("Marion VA Medical Center",)),
    ("VA Illiana Health Care System", "IL", ()),
    ("Captain James A. Lovell Federal Health Care Center", "IL WI", ()),
    # Indiana
    ("Richard L. Roudebush VA Medical Center", "IN", ()),
    ("VA Northern Indiana Health Care System", "IN", ()),
    # Iowa / Nebraska / Dakotas / Minnesota
    ("Iowa City VA Health Care System", "IA IL", ("Iowa City VA Medical Center",)),
    ("VA Central Iowa Health Care System", "IA", ()),
    ("VA Nebraska-Western Iowa Health Care System", "NE IA", ()),
    ("Fargo VA Health Care System", "ND MN", ("Fargo VA Medical Center",)),
    ("Sioux Falls VA Health Care System", "SD", ("Royal C. Johnson VA Health Care System",)),
    ("VA Black Hills Health Care System", "SD", ()),
    ("Minneapolis VA Health Care System", "MN WI", (
        "Minneapolis VA Medical Center", "Minneapolis VA Health Center", "Minneapolis/Maplewood VA Medical Center")),
    ("St. Cloud VA Health Care System", "MN", ("St. Cloud VA Medical Center",)),
    # Kansas / Missouri
    ("Robert J. Dole VA Medical Center", "KS", ()),
    ("VA Eastern Kansas Health Care System", "KS", ("Easern Kansas Health Care System",)),
    ("Kansas City VA Medical Center", "MO KS", ("Kansas City VA Healthcare System",)),
    ("Harry S. Truman Memorial Veterans' Hospital", "MO", (
        "Harry S. Truman VA Medical Center", "Harry Truman VA Medical Center", "Truman VA Medical Center",
        "Rolla VA Medical Center")),
    ("John J. Pershing VA Medical Center", "MO", ("Poplar Bluff VA Medical Center",)),
    ("VA St. Louis Health Care System", "MO IL", ("St. Louis VA Medical Center",)),
    # Kentucky
    ("Robley Rex VA Medical Center", "KY IN", ("Louisville VA Medical Center",)),
    ("Lexington VA Health Care System", "KY", ("Lexington VA Medical Center",)),
    # Louisiana
    ("Alexandria VA Health Care System", "LA", ("Alexandria VA Medical Center",)),
    ("Overton Brooks VA Medical Center", "LA TX", ()),
    ("Southeast Louisiana Veterans Health Care System", "LA", ()),
    # Michigan
    ("Battle Creek VA Medical Center", "MI", ()),
    ("John D. Dingell VA Medical Center", "MI", ("Detroit VA Medical Center",)),
    ("Aleda E. Lutz VA Medical Center", "MI", ()),
    ("Oscar G. Johnson VA Medical Center", "MI", ()),
    ("Lieutenant Colonel Charles S. Kettles VA Medical Center", "MI OH", (
        "VA Ann Arbor Healthcare System", "Ann Arbor VA Medical Center")),
    # Mississippi / Gulf Coast
    ("G.V. (Sonny) Montgomery VA Medical Center", "MS", (
        "G.V. Montgomery VA Medical Center", "G.V. Sonny Montgomery VA Medical Center",
        "Jackson VA Medical Center")),
    ("Gulf Coast Veterans Health Care System", "MS AL FL", ()),
    # Montana
    ("Montana VA Health Care System", "MT", ()),
    # Nevada
    ("VA Sierra Nevada Health Care System", "NV", ()),
    ("VA Southern Nevada Healthcare System", "NV", ()),
    # New Jersey
    ("New Jersey VA Healthcare System", "NJ", ()),
    # New Mexico
    ("New Mexico VA Health Care System", "NM", ("VA Albuquerque Healthcare System", "New Mexico Veterans Hospital")),
    # New York
    ("James J. Peters VA Medical Center", "NY", ("Bronx VA Medical Center", "Bronx NY VA Medical Center")),
    ("VA NY Harbor Healthcare System", "NY", ("New York Harbor VA Healthcare System", "New York Harbor VA Medical Center")),
    ("Northport VA Medical Center", "NY", ("Northport VA Healthcare System",)),
    ("Syracuse VA Medical Center", "NY", ("Syracuse VA Health Care System", "Syracuse New York VA Medical Center")),
    ("VA Western New York Healthcare System", "NY", (
        "WNY VA Healthcare System", "Western New York Health Care System",
        "Western New York Veterans Health Administration Health Care Systems")),
    ("VA Finger Lakes Healthcare System", "NY", ("Fingerlakes VA Medical Center",)),
    ("Samuel S. Stratton VA Medical Center", "NY", (
        "Albany Stratton VA Medical Center", "Samuel S. Stratton Albany VA Medical Center",
        "Albany VA Health Care System", "Albany VA Medical Center")),
    ("Hudson Valley VA Healthcare System", "NY", ("Hudson Valley VA Medical Center",)),
    # North Carolina
    ("Durham VA Medical Center", "NC", ("Durham VA Health Care System",)),
    ("W.G. (Bill) Hefner VA Medical Center", "NC", (
        "WG Bill Hefner VA Medical Center", "W.G. Hefner VA Medical Center", "Salisbury VA Medical Center",
        "Salisbury VA Healthcare System", "WG Bill Hefner Salisbury VA Medical Center")),
    ("Charles George VA Medical Center", "NC", ("Asheville VA Medical Center",)),
    # Fayetteville, NC: the station's name since 2020 (the medical center's
    # name is also Fayetteville, AR's, the Ozarks' main campus)
    ("Fayetteville VA Coastal Health Care System", "NC", (
        "Fayetteville VA Medical Center", "NC Coastal VA Health Care System",
        "Fayetteville NC VA Coastal Healthcare System")),
    # Ohio
    ("Cincinnati VA Medical Center", "OH KY IN", ("Cincinnati Veterans Affair's Medical Center",)),
    ("Dayton VA Medical Center", "OH IN", ()),
    ("Chillicothe VA Medical Center", "OH", ()),
    ("VA Northeast Ohio Healthcare System", "OH", ()),
    ("VA Central Ohio Healthcare System", "OH", (
        "Chalmers P. Wylie VA Ambulatory Care Center", "Columbus VA Medical Center",
        "Chalmers P. Wylie Veterans Outpatient Clinic")),
    # Oklahoma
    ("Oklahoma City VA Health Care System", "OK TX", ("Oklahoma City VA Medical Center",)),
    ("Eastern Oklahoma VA Health Care System", "OK", ()),
    # Oregon / Washington
    ("VA Portland Health Care System", "OR WA", ()),
    ("Roseburg VA Health Care System", "OR", ("Roseburg VA Medical Center",)),
    ("VA Southern Oregon Rehabilitation Center and Clinics", "OR", ("White City VA Medical Center",)),
    ("VA Puget Sound Health Care System", "WA", ("Puget Sound VA Medical Center",)),
    ("Mann-Grandstaff VA Medical Center", "WA", ("Mann-Granstaff VA Medical Center",)),
    ("Jonathan M. Wainwright Memorial VA Medical Center", "WA OR", ("Walla Walla VA Medical Center",)),
    # Pennsylvania
    ("Coatesville VA Medical Center", "PA", ()),
    ("Lebanon VA Medical Center", "PA", ()),
    ("Erie VA Medical Center", "PA OH", ()),
    ("James E. Van Zandt VA Medical Center", "PA", ("Altoona VA Medical Center",)),
    ("Corporal Michael J. Crescenz VA Medical Center", "PA NJ", ("Philadelphia VA Medical Center",)),
    ("VA Pittsburgh Healthcare System", "PA", ("Pittsburgh VA Medical Center",)),
    ("Butler VA Medical Center", "PA", ("Butler VA Health Care System",)),
    ("Wilkes-Barre VA Medical Center", "PA", ("Wilkes-Barre VA Healthcare System",)),
    # South Carolina
    ("Columbia VA Health Care System", "SC", ()),
    ("Ralph H. Johnson VA Health Care System", "SC GA", (
        "Ralph H. Johnson VA Medical Center", "Charleston VA Health Care System", "Charleston VA Medical Center")),
    # Tennessee
    ("Tennessee Valley Healthcare System", "TN", ()),
    ("James H. Quillen VA Medical Center", "TN VA", ("James H. Quillen VA Healthcare System",)),
    ("Lt. Col. Luke Weathers, Jr. VA Medical Center", "TN", ("Memphis VA Medical Center",)),
    # Texas
    ("Michael E. DeBakey VA Medical Center", "TX", ("Houston VA Medical Center",)),
    ("VA North Texas Health Care System", "TX", ("Dallas VA Health Care System",)),
    ("South Texas Veterans Health Care System", "TX", ()),
    ("Central Texas Veterans Health Care System", "TX", ()),
    ("VA Texas Valley Coastal Bend Health Care System", "TX", ("VA TX Valley Coastal Bend HCS",)),
    ("West Texas VA Health Care System", "TX", ()),
    ("El Paso VA Health Care System", "TX NM", ()),
    ("Amarillo VA Health Care System", "TX", ()),
    # Utah
    ("VA Salt Lake City Health Care System", "UT ID", ()),
    # Virginia
    ("Hampton VA Medical Center", "VA NC", ("Hampton VA Healthcare System", "Hampton Veterans Medical Center")),
    ("Central Virginia VA Health Care System", "VA", ()),
    ("VA Salem Health Care System", "VA", ("Salem VA Medical Center",)),
    # West Virginia
    ("Louis A. Johnson VA Medical Center", "WV", ("Clarksburg VA Medical Center",)),
    ("Hershel \"Woody\" Williams VA Medical Center", "WV", ("Huntington VA Medical Center",)),
    ("Martinsburg VA Medical Center", "WV MD VA", ()),
    ("Beckley VA Medical Center", "WV", ("Beckley's VA Medical Center",)),
    # Wisconsin
    ("Clement J. Zablocki VA Medical Center", "WI", (
        "Celment J. Zablocki VA Medical Center", "Clement J. Zablocki VA Health Care System",
        "Milwaukee VA Medical Center")),
    ("William S. Middleton Memorial Veterans Hospital", "WI", (
        "William S. Middleton Veterans Hospital", "Madison VA Medical Center")),
    ("Tomah VA Medical Center", "WI", ()),
)

# ── Medical centers with a name of their own ────────────────────────────────
# Rule of 2026-10-27 (the owner wants the real facility): a row whose own
# name, or whose job page's hiring-organization line, names a specific VA
# medical center or hospital keeps that medical center, under one name, its
# current one (Ex: "C.W. Bill Young VA Medical Center", not Bay Pines VA
# Healthcare System; "Miami VA Healthcare System (Bruce W. Carter VA Medical
# Center)" is Bruce W. Carter VA Medical Center). A medical center is listed
# here when its name is its own: a person's or a place's name the station's
# name does not carry ("Rocky Mountain Regional", "Tucson" of Southern Arizona,
# "Bath" and "Canandaigua" of Finger Lakes), or the hospital was renamed while
# the station kept the town ("West Palm Beach VA Medical Center" is now
# Thomas H. Corey West Palm Beach VA Medical Center; "Salt Lake City VA
# Medical Center" is George E. Wahlen; "San Diego VA Medical Center" is
# Jennifer Moreno; "Long Beach VA Medical Center" is Tibor Rubin; "Atlanta VA
# Medical Center" is Joseph Maxwell Cleland Atlanta). Not listed, so the
# station's one name: a medical-center spelling that is only the station's
# own name on a medical-center word, at a station with one medical center
# (the VA renamed those stations to health care systems and their hiring
# lines use the new name: "Cheyenne VA Medical Center" is Cheyenne VA Health
# Care System, likewise Alexandria, Birmingham, Fargo, Iowa City, Lexington,
# Manchester, Marion, Minneapolis, Providence, Roseburg, Salem, San Francisco,
# Sheridan, St. Cloud, White River Junction, Oklahoma City, Ralph H. Johnson,
# Fayetteville VA Coastal); a station's regional name on a medical-center
# word where it has several, so the name picks none ("New York Harbor",
# "Puget Sound", "Greater Los Angeles", "Hudson Valley", "Central Alabama",
# "Fingerlakes", "St. Louis", "Pittsburgh"); a clinic or an ambulatory care
# center (Chalmers P. Wylie); a town on a bare facility word. Clinics, towns,
# agency-only rows and summaries still give the station.
# (name, station, other spellings); a medical center's states are its
# station's.
HOSPITALS: tuple = (
    ("Central Alabama VA Medical Center-Montgomery", "Central Alabama Veterans Health Care System", (
        "Central VA Medical Center Montgomery", "Central Alabama VA Medical Center Montgomery")),
    ("Carl T. Hayden VA Medical Center", "VA Phoenix Healthcare System", (
        "Carl T Hayden Veterans' Administration Medical Center",)),
    ("Tucson VA Medical Center", "Southern Arizona VA Health Care System", ("VA Tucson Medical Center",)),
    ("John L. McClellan Memorial Veterans Hospital", "Central Arkansas Veterans Health Care System", (
        "Central Arkansas Veteran Healthcare System John L. McClellan Memorial Veterans Hospital",)),
    ("Fayetteville VA Medical Center", "Veterans Healthcare System of the Ozarks", ()),       # Fayetteville, AR
    ("Sepulveda VA Medical Center", "Greater Los Angeles VA Health Care System", ()),
    ("Tibor Rubin VA Medical Center", "Long Beach VA Health Care System", ("Long Beach VA Medical Center",)),
    ("Jerry L. Pettis Memorial Veterans' Hospital", "VA Loma Linda Healthcare System", (
        "Jerry L. Pettis Memorial Veterans Hospital", "Jerry L Pettis Memorial VA Medical Center")),
    ("Jennifer Moreno VA Medical Center", "San Diego VA Healthcare System", ("San Diego VA Medical Center",)),
    ("Palo Alto VA Medical Center", "VA Palo Alto Health Care System", ()),
    ("Sacramento VA Medical Center", "VA Northern California Healthcare System", ()),
    ("Fresno VA Medical Center", "VA Central California Health Care System", ()),
    ("Rocky Mountain Regional VA Medical Center", "VA Eastern Colorado Health Care System", ()),
    ("Grand Junction VA Medical Center", "VA Western Colorado Health Care System", ()),
    ("Edith Nourse Rogers Memorial Veterans Hospital", "VA Bedford Healthcare System", ()),
    ("Togus VA Medical Center", "VA Maine Healthcare System", ()),
    ("Baltimore VA Medical Center", "VA Maryland Health Care System", ()),
    ("C.W. Bill Young VA Medical Center", "Bay Pines VA Healthcare System", (
        "CW Bill Young Department of Veterans Affairs Medical Center", "Bay Pines VA Medical Center")),
    ("Malcom Randall VA Medical Center", "North Florida/South Georgia Veterans Health System", (
        "Malcolm Randall VA Medical Center",)),
    ("Lake City VA Medical Center", "North Florida/South Georgia Veterans Health System", ()),
    ("Bruce W. Carter VA Medical Center", "Miami VA Healthcare System", ()),
    ("Thomas H. Corey West Palm Beach VA Medical Center", "West Palm Beach VA Healthcare System", (
        "West Palm Beach VA Medical Center", "Thomas H. Corey VA Medical Center")),
    ("Joseph Maxwell Cleland Atlanta VA Medical Center", "Atlanta VA Health Care System", (
        "Atlanta VA Medical Center",)),
    ("Spark M. Matsunaga VA Medical Center", "VA Pacific Islands Health Care System", ()),
    ("Danville VA Medical Center", "VA Illiana Health Care System", ()),
    ("Des Moines VA Medical Center", "VA Central Iowa Health Care System", ()),
    ("Omaha VA Medical Center", "VA Nebraska-Western Iowa Health Care System", ()),
    ("Royal C. Johnson Veterans' Memorial Hospital", "Sioux Falls VA Health Care System", (
        "Royal C. Johnson Veterans Memorial Hospital",)),
    ("John Cochran VA Medical Center", "VA St. Louis Health Care System", ("John Cochran VA Hospital",)),
    ("New Orleans VA Medical Center", "Southeast Louisiana Veterans Health Care System", ()),
    ("Biloxi VA Medical Center", "Gulf Coast Veterans Health Care System", ()),
    ("Fort Harrison VA Medical Center", "Montana VA Health Care System", ()),
    ("Ioannis A. Lougaris VA Medical Center", "VA Sierra Nevada Health Care System", ("Reno VA Medical Center",)),
    ("North Las Vegas VA Medical Center", "VA Southern Nevada Healthcare System", ("Las Vegas VA Medical Center",)),
    ("East Orange VA Medical Center", "New Jersey VA Healthcare System", ()),
    ("Lyons VA Medical Center", "New Jersey VA Healthcare System", ()),
    ("Raymond G. Murphy VA Medical Center", "New Mexico VA Health Care System", (
        "Raymond G. Murphy Albuquerque VA Medical Center", "Albuquerque VA Medical Center",
        "Raymond G. Murphy D Veterans Health Administrator Medical Center", "Raymond G Murphy Department of the VAMC")),
    ("Manhattan VA Medical Center", "VA NY Harbor Healthcare System", ()),
    ("Brooklyn VA Medical Center", "VA NY Harbor Healthcare System", ()),
    ("Buffalo VA Medical Center", "VA Western New York Healthcare System", ()),
    ("Canandaigua VA Medical Center", "VA Finger Lakes Healthcare System", ("Canandaigua NY VA Medical Center",)),
    ("Bath VA Medical Center", "VA Finger Lakes Healthcare System", ()),
    ("Franklin Delano Roosevelt Hospital", "Hudson Valley VA Healthcare System", ()),
    ("Castle Point VA Medical Center", "Hudson Valley VA Healthcare System", ()),
    ("Louis Stokes Cleveland VA Medical Center", "VA Northeast Ohio Healthcare System", (
        "Cleveland VA Medical Center",)),
    ("Jack C. Montgomery VA Medical Center", "Eastern Oklahoma VA Health Care System", (
        "Muskogee VA Medical Center",)),
    ("James Mountain Inhofe VA Medical Center", "Eastern Oklahoma VA Health Care System", ()),
    ("Portland VA Medical Center", "VA Portland Health Care System", ()),
    ("Vancouver VA Medical Center", "VA Portland Health Care System", ()),
    ("Seattle VA Medical Center", "VA Puget Sound Health Care System", ()),
    ("Wm. Jennings Bryan Dorn VA Medical Center", "Columbia VA Health Care System", (
        "William Jennings Bryan Dorn VA Medical Center",)),
    ("Alvin C. York VA Medical Center", "Tennessee Valley Healthcare System", ()),
    ("Nashville VA Medical Center", "Tennessee Valley Healthcare System", ()),
    ("Dallas VA Medical Center", "VA North Texas Health Care System", ()),
    ("Audie L. Murphy Memorial Veterans' Hospital", "South Texas Veterans Health Care System", (
        "Audie L. Murphy VA Hospital",)),
    ("Olin E. Teague Veterans' Medical Center", "Central Texas Veterans Health Care System", (
        "Temple VA Medical Center",)),
    ("Doris Miller VA Medical Center", "Central Texas Veterans Health Care System", ("Waco VA Medical Center",)),
    ("George H. O'Brien, Jr. VA Medical Center", "West Texas VA Health Care System", ()),
    ("Thomas E. Creek VA Medical Center", "Amarillo VA Health Care System", ()),
    ("George E. Wahlen VA Medical Center", "VA Salt Lake City Health Care System", (
        "Salt Lake City VA Medical Center",)),
    ("Richmond VA Medical Center", "Central Virginia VA Health Care System", (
        "Hunter Holmes McGuire VA Medical Center",)),
)

# The towns a medical center stands in, as a posting's duty location names
# them (_town_key form). A posting under a medical center's hiring line lists
# the station's other towns too (West Palm Beach VA Medical Center's in Vero
# Beach, Seattle's in Tacoma, Malcom Randall's in Lake City): a row in a town
# not listed here is at another site of the station and takes the station's
# name (at_town).
HOSPITAL_TOWNS: dict = {
    "Central Alabama VA Medical Center-Montgomery": ("montgomery",),
    "Carl T. Hayden VA Medical Center": ("phoenix",),
    "Tucson VA Medical Center": ("tucson",),
    "John L. McClellan Memorial Veterans Hospital": ("little rock",),
    "Fayetteville VA Medical Center": ("fayetteville",),
    "Sepulveda VA Medical Center": ("north hills", "los angeles", "sepulveda"),
    "Tibor Rubin VA Medical Center": ("long beach",),
    "Jerry L. Pettis Memorial Veterans' Hospital": ("loma linda",),
    "Jennifer Moreno VA Medical Center": ("san diego", "la jolla"),
    "Palo Alto VA Medical Center": ("palo alto",),
    "Sacramento VA Medical Center": ("mather", "sacramento", "rancho cordova"),
    "Fresno VA Medical Center": ("fresno",),
    "Rocky Mountain Regional VA Medical Center": ("aurora",),
    "Grand Junction VA Medical Center": ("grand junction",),
    "Edith Nourse Rogers Memorial Veterans Hospital": ("bedford",),
    "Togus VA Medical Center": ("togus", "augusta", "chelsea"),
    "Baltimore VA Medical Center": ("baltimore",),
    "C.W. Bill Young VA Medical Center": ("bay pines", "st petersburg", "seminole"),
    "Malcom Randall VA Medical Center": ("gainesville",),
    "Lake City VA Medical Center": ("lake city",),
    "Bruce W. Carter VA Medical Center": ("miami",),
    "Thomas H. Corey West Palm Beach VA Medical Center": ("west palm beach", "riviera beach"),
    "Joseph Maxwell Cleland Atlanta VA Medical Center": ("decatur", "atlanta"),
    "Spark M. Matsunaga VA Medical Center": ("honolulu",),
    "Danville VA Medical Center": ("danville",),
    "Des Moines VA Medical Center": ("des moines",),
    "Omaha VA Medical Center": ("omaha",),
    "Royal C. Johnson Veterans' Memorial Hospital": ("sioux falls",),
    "John Cochran VA Medical Center": ("st louis",),
    "New Orleans VA Medical Center": ("new orleans",),
    "Biloxi VA Medical Center": ("biloxi",),
    "Fort Harrison VA Medical Center": ("fort harrison", "helena"),
    "Ioannis A. Lougaris VA Medical Center": ("reno",),
    "North Las Vegas VA Medical Center": ("north las vegas", "las vegas"),
    "East Orange VA Medical Center": ("east orange",),
    "Lyons VA Medical Center": ("lyons",),
    "Raymond G. Murphy VA Medical Center": ("albuquerque",),
    "Manhattan VA Medical Center": ("new york", "manhattan"),
    "Brooklyn VA Medical Center": ("brooklyn", "kings"),
    "Buffalo VA Medical Center": ("buffalo",),
    "Canandaigua VA Medical Center": ("canandaigua",),
    "Bath VA Medical Center": ("bath",),
    "Franklin Delano Roosevelt Hospital": ("montrose",),
    "Castle Point VA Medical Center": ("castle point", "wappingers falls"),
    "Louis Stokes Cleveland VA Medical Center": ("cleveland",),
    "Jack C. Montgomery VA Medical Center": ("muskogee",),
    "James Mountain Inhofe VA Medical Center": ("tulsa",),
    "Portland VA Medical Center": ("portland",),
    "Vancouver VA Medical Center": ("vancouver",),
    "Seattle VA Medical Center": ("seattle",),
    "Wm. Jennings Bryan Dorn VA Medical Center": ("columbia",),
    "Alvin C. York VA Medical Center": ("murfreesboro",),
    "Nashville VA Medical Center": ("nashville",),
    "Dallas VA Medical Center": ("dallas",),
    "Audie L. Murphy Memorial Veterans' Hospital": ("san antonio",),
    "Olin E. Teague Veterans' Medical Center": ("temple",),
    "Doris Miller VA Medical Center": ("waco",),
    "George H. O'Brien, Jr. VA Medical Center": ("big spring",),
    "Thomas E. Creek VA Medical Center": ("amarillo",),
    "George E. Wahlen VA Medical Center": ("salt lake city",),
    "Richmond VA Medical Center": ("richmond",),
}

# Names whose spelling the VA itself varies but that are not a station's.
NAME_FIXES: dict = {
    "VA Great Lakes Health Care System": ("Great Lakes Health Care System",),   # VISN 12, the network
}

# Acronyms the hiring-organization line uses alone ("Veterans Health
# Administration, VANCHCS, Pharmacy").
ACRONYMS: dict = {
    "VANCHCS": "VA Northern California Healthcare System", "VAPAHCS": "VA Palo Alto Health Care System",
    "VASDHS": "San Diego VA Healthcare System", "SFVAHCS": "San Francisco VA Health Care System",
    "VACCHCS": "VA Central California Health Care System", "VAGLAHS": "Greater Los Angeles VA Health Care System",
    "VANTHCS": "VA North Texas Health Care System", "STVHCS": "South Texas Veterans Health Care System",
    "CTVHCS": "Central Texas Veterans Health Care System", "WTVAHCS": "West Texas VA Health Care System",
    "EPVAHCS": "El Paso VA Health Care System", "ECHCS": "VA Eastern Colorado Health Care System",
    "NMVAHCS": "New Mexico VA Health Care System", "CAVHS": "Central Arkansas Veterans Health Care System",
    "CAVHCS": "Central Alabama Veterans Health Care System", "VHSO": "Veterans Healthcare System of the Ozarks",
    "GCVHCS": "Gulf Coast Veterans Health Care System", "NF/SGVHS": "North Florida/South Georgia Veterans Health System",
    "VAMHCS": "VA Maryland Health Care System", "VABHCS": "VA Boston Healthcare System",
    "VAPIHCS": "VA Pacific Islands Health Care System", "VAPHCS": "VA Providence Healthcare System",
    "VAWRJHCS": "VA White River Junction Healthcare System", "TVHS": "Tennessee Valley Healthcare System",
    "VAMEHCS": "VA Maine Healthcare System", "EKHCS": "VA Eastern Kansas Health Care System",
}


def _build():
    """_BY_KEY (spelling key -> [(station or NAME_FIXES name, states)]; a
    medical center's spellings give its station there, so every lookup at
    the station level reads as before), the station states, and the medical
    centers: their station, their states, and their spelling keys for
    hospital_for (whole key, the key station lookups use, the name), longest
    first."""
    by_key: dict = {}
    station_states: dict = {}

    def add(k, name, sts):
        by_key.setdefault(k, [])
        if all(x[0] != name for x in by_key[k]):
            by_key[k].append((name, sts))

    for name, states, others in STATIONS:
        sts = tuple(states.split())
        station_states[name] = sts
        for n in (name,) + tuple(others):
            add(name_key(facility_segment(n) or n), name, sts)
    hosp_station, hosp_states, hosp_keys = {}, {}, []
    for name, station, others in HOSPITALS:
        sts = station_states[station]
        hosp_station[name] = station
        hosp_states[name] = sts
        for n in (name,) + tuple(others):
            seg_key = name_key(facility_segment(n) or n)
            add(seg_key, station, sts)
            hosp_keys.append((name_key(n), seg_key, name))
    for name, others in NAME_FIXES.items():
        for n in (name,) + tuple(others):
            add(name_key(facility_segment(n) or n), name, ())
    hosp_keys = tuple(sorted(set(hosp_keys), key=lambda x: (-len(x[0]), x[0], x[2])))
    return by_key, station_states, hosp_station, hosp_states, hosp_keys


_BY_KEY: dict = {}
STATION_STATES: dict = {}
STATION_NAMES: frozenset = frozenset()
HOSPITAL_STATION: dict = {}      # medical center -> its station
HOSPITAL_STATES: dict = {}
HOSPITAL_NAMES: frozenset = frozenset()
_HOSP_KEYS: tuple = ()

# The words that make a cleaned name read the same way everywhere.
_DISPLAY_FIXES = (
    (re.compile(r"\bVAMC\b"), "VA Medical Center"),
    (re.compile(r"\bHCS\b"), "Health Care System"),
    (re.compile(r"(?<=[A-Za-z]) (?:%s)(?= VA )" % "|".join(_STATE_CODES)), ""),     # "Tyler TX VA Medical Center"
    (re.compile(r"\b(?:Department\s+of\s+Veterans'?\s+Affairs|Veterans?'?\s+Affairs|Veterans'?\s+Administration)"
                r"(?=\s+(?:Medical\s+Center|Health\s*Care|Hospital))", re.I), "VA"),
    (re.compile(r"\bHealth\s*Care\s+Systems\b", re.I), "Health Care System"),
    (re.compile(r"\bHealthcare\s+Systems\b", re.I), "Healthcare System"),
    (re.compile(r"\bHealthCare\b"), "Health Care"),
)


def _display(seg: str) -> str:
    s = seg
    for rx, rep in _DISPLAY_FIXES:
        s = rx.sub(rep, s)
    if s.isupper():
        s = s.title().replace("Va ", "VA ")
    return re.sub(r"\s+", " ", s).strip()


def _station_of(seg, state="", stations_only=False) -> str:
    """The station (or NAME_FIXES name) whose key the segment has; state
    picks between two stations that share a spelling."""
    return _station_of_key(name_key(seg), state, stations_only)


def _station_of_key(key, state="", stations_only=True) -> str:
    hits = [h for h in _BY_KEY.get(key, []) if not stations_only or h[0] in STATION_STATES]
    if len(hits) == 1:
        return hits[0][0]
    st = (state or "").strip().upper()
    for n, sts in hits:
        if st and st in sts:
            return n
    return ""


def _station_by_spelling(name, state="") -> str:
    for seg in segment_candidates(name):
        st = _station_of(seg, state, stations_only=True)
        if st:
            return st
    s = clean_text(name)
    for seg in [s] + [x.strip() for x in _SEP_RX.split(s) if x and x.strip()]:
        st = _station_of(seg, state, stations_only=True) or ACRONYMS.get(seg.strip("() ").upper(), "")
        if st:
            return st
    return ""


def station_for(name, state="") -> str:
    """The station a name is a spelling of, or "" (state picks between two
    stations that share a spelling). A name without a facility word is
    looked up whole ("Chalmers P. Wylie Veterans Outpatient Clinic") and by
    its acronym ("VANCHCS"); a medical center's name gives its station."""
    st = _station_by_spelling(name, state)
    if st:
        return st
    h = hospital_for(name, state)
    return HOSPITAL_STATION[h] if h else ""


def hospital_for(name, state="") -> str:
    """The medical center with a name of its own (HOSPITALS) a raw value
    names anywhere in it, by any of its spellings, longest first, or "":
    "VA Finger Lakes Healthcare System- Bath VA Medical Center" is Bath VA
    Medical Center; "Miami VA Healthcare System (Bruce W. Carter VA Medical
    Center)" is Bruce W. Carter VA Medical Center; "Fingerlakes VA Medical
    Center (Canandaigua)" is Canandaigua VA Medical Center. The state, when
    given, must be one of its station's; without it, a spelling two stations
    share ("Fayetteville VA Medical Center", AR and NC) names none."""
    s = clean_text(name)
    if not s or s.lower() in GENERIC_LABELS:
        return ""
    if _CLINIC_RX.search(s):
        # the posting is at a clinic of the medical center ("Central Alabama
        # VA Medical Center-Montgomery - Robert S. Poydasheff VA Clinic", in
        # Columbus, GA): a clinic carries its station's name
        return ""
    st = (state or "").strip().upper()
    tk = f" {name_key(re.sub(r'[()]', ' ', s))} "
    towns = set()
    # "Fingerlakes VA Medical Center (Canandaigua)": the town names the medical
    # center; after a system's name it does not ("VA Southern Nevada
    # Healthcare System (Las Vegas)" is the system)
    outer = re.sub(r"\([^()]*\)", " ", s)
    if _MC_WORD_RX.search(outer) and not _SYSTEM_WORD_RX.search(outer):
        for p in re.findall(r"\(([^()]*)\)", s):
            town = _town_part(p.strip())
            if town:
                towns.add(name_key(f"{town} VA Medical Center"))
    for key, seg_key, hosp in _HOSP_KEYS:
        if not key or (f" {key} " not in tk and key not in towns):
            continue
        if st and st not in HOSPITAL_STATES[hosp]:
            continue
        if not st and len({n for n, _ in _BY_KEY.get(seg_key, [])}) > 1:
            continue
        if key in towns and f" {key} " not in tk and _station_by_spelling(s, state) not in ("", HOSPITAL_STATION[hosp]):
            continue
        return hosp
    return ""


_CLINIC_RX = re.compile(r"\bclinics?\b|\bCBOCs?\b|\boutpatient\b", re.I)
_MC_WORD_RX = re.compile(r"\bmedical\s+cent(?:er|re)\b|\bhospital\b|\bVAMC\b", re.I)
_SYSTEM_WORD_RX = re.compile(r"\bhealth\s*care\s+systems?\b|\bhealth\s+systems?\b|\bHCS\b", re.I)


def station_of(name) -> str:
    """The station of a name canonical() gave: a medical center's station,
    else the name itself."""
    return HOSPITAL_STATION.get(name, name)


def at_town(name, city="", state="") -> str:
    """The name for a row in this town: a medical center's name only when
    the row's town is the medical center's own (HOSPITAL_TOWNS) or the row
    names no town; else its station's (a row in Vero Beach, FL under "West
    Palm Beach VA Medical Center" is West Palm Beach VA Healthcare System's).
    Any other name is returned as is."""
    if name not in HOSPITAL_STATION or not clean_text(city):
        return name
    return name if _town_key(city) in HOSPITAL_TOWNS.get(name, ()) else HOSPITAL_STATION[name]


def canonical(name, state="") -> str:
    """The one name of the facility a raw value names: the medical center
    when it names one with a name of its own (hospital_for), else its
    station's name, else the value cleaned (departments, towns, acronyms,
    the agency in front dropped; "VAMC" written out); "" when it names no
    facility (the agency, "VA Medical Center", "TX VA Medical Center", "Ohio
    Veterans Affairs Medical Center"). A town's name on a bare facility word
    ("Lake Charles LA VA Medical Center", where no VA medical center stands)
    is the station that serves that town, when TOWNS knows it."""
    h = hospital_for(name, state)
    if h:
        return h
    cands = segment_candidates(name)
    for seg in cands:
        st = _station_of(seg, state)
        if st:
            return st
    st = station_for(name, state)
    if st:
        return st
    for seg in cands:
        m = _TOWN_FACILITY_RX.match(_display(seg))
        if m and state:
            st = town_facility(m.group(1), state)
            if st:
                return st
    for seg in cands:
        if _proper_words(seg):
            return _display(seg)
    return ""


_TOWN_FACILITY_RX = re.compile(r"^([A-Z][A-Za-z.' -]*?)\s+(?:VA\s+)?(?:Medical\s+Center|Health\s*Care\s+System)$")


def is_generic(name, state="") -> bool:
    """True when a value names no facility."""
    return not canonical(name, state)


# ── VA units that hire in their own name ────────────────────────────────────
# A national program or office, a network (VISN) office, the Readjustment
# Counseling Service (Vet Centers), the Veterans Canteen Service: "National
# Oncology Program", "VISN 4 VA Heart of America Health Network", "VHA
# Central Office (VHACO), SCI/D NPO". A department or a site of a station
# ("Pharmacy Service", "Radiology", "Office of Research and Development",
# "Community Base Out-patient Clinics (CBOC), Waynesville or Marshfield",
# "Veterans Health Administration, Care Management") is not one: the station
# is read from the posting's contact address or town.
ORG_RX = re.compile(
    r"\bnational\b.*\b(?:program|network|service|center|office)\b|\bVISN\s*\d+|\bhealth\s*care\s+network\b|"
    r"\bhealth\s+network\b|\breadjustment\s+counseling\b|\bcanteen\s+service\b|\bcentral\s+office\b|"
    r"\bcrisis\s+line\b|\bsuicide\s+prevention\b|\btele-?\w+\s+(?:program|hub)\b", re.I)
_ORG_NOT_RX = re.compile(r"\bresearch\b|\bhuman\s+resources\b|\bclinics?\b|\bcboc\b|\bfloat\s+pool\b", re.I)
_VA_MARK_RX = re.compile(r"\b(?:VA|VHA|VISN|Veterans?'?)\b")


def org_name(raw) -> str:
    """The name of a VA unit that hires in its own name, cleaned (the
    acronym in brackets and the agency in front dropped; "VA " in front of a
    name that does not say VA): "VA National Oncology Program". "" when the
    value names none."""
    s = clean_text(raw)
    if not s or s.lower() in GENERIC_LABELS:
        return ""
    for seg in _SEP_RX.split(s):
        seg = (seg or "").strip()
        if not seg or not ORG_RX.search(seg) or FACILITY_WORD_RX.search(seg) or _ORG_NOT_RX.search(seg):
            continue
        t = re.sub(r"^(?:veterans\s+health\s+administration|vha|the)\s*[-,:]?\s+", "", seg, flags=re.I)
        t = re.sub(r"^the\s+", "", t, flags=re.I)
        t = re.sub(r"\s*\([^()]*\)", "", t).strip(" .,;:-")
        t = re.sub(r"\bServices\b", "Service", t)
        if re.match(r"(?i)central\s+office\b", t):
            t = "VHA " + t
        if not _proper_words(t, states=True) - {"national", "program", "network", "service", "office"}:
            continue
        if not _VA_MARK_RX.search(t):
            t = "VA " + t
        return re.sub(r"\s+", " ", t)
    return ""


# "<station or town> Veterans Health Administration" ("San Francisco Veterans
# Health Administration", "C. W. Bill Young Veterans Health Administration",
# "Maryland Veterans Health Care Administration")
_PLACE_VHA_RX = re.compile(r"^(.+?)\s+Veterans\s+Health\s+(?:Care\s+)?Administration\b", re.I)


def hiring_org_name(raw, state="") -> str:
    """The employer a hiring-organization value names: its facility
    (canonical), else a station named by its place in front of the agency
    ("San Francisco Veterans Health Administration"), else the VA unit that
    hires in its own name (org_name), else ""."""
    name = canonical(raw, state)
    if name:
        return name
    m = _PLACE_VHA_RX.match(clean_text(raw))
    if m:
        place = m.group(1).strip(" ,-")
        for suffix in (" VA Medical Center", " VA Health Care System"):
            st = hospital_for(place + suffix, state) or _station_of(place + suffix, state, stations_only=True)
            if st:
                return st
        st = town_facility(place, state)
        if st:
            return st
    return org_name(raw)


_NETWORK_RX = re.compile(r"\bVISN\b|\bnetwork\b", re.I)


def is_network(name) -> bool:
    """True for a network (VISN) office's name ("VISN 4 VA Heart of America
    Health Network", "VA Mid-Atlantic Health Care Network", VISN 12's "VA
    Great Lakes Health Care System"), not a national program's ("VA National
    CARDIO-VET Network"). A network's HR offices put its name on the
    postings of its stations too ("Physician (Cardiologist)" in North
    Chicago under "VISN 12 - VA Great Lakes Health Care System"): a posting
    of one town under a network's name is that town's station's."""
    s = clean_text(name)
    return bool(s) and (s in NAME_FIXES or bool(_NETWORK_RX.search(s))) and not re.search(r"\bnational\b", s, re.I)


def is_town_only(name) -> bool:
    """True for a cleaned name that is only a town on a facility word and no
    station's ("Tyler VA Medical Center", from "VCS Optical Shops at the
    Tyler TX VA Medical Center"): it names no facility of its own."""
    s = clean_text(name)
    return (bool(s) and s not in STATION_NAMES and s not in HOSPITAL_NAMES and not is_network(s)
            and bool(_TOWN_FACILITY_RX.match(s)))


def text_station(text, state="") -> str:
    """The first station a free text names by any of its names, longest name
    first ("Serve as ... for Eastern Colorado Health Care System (ECHCS)
    located in Pueblo" is VA Eastern Colorado Health Care System); "" when
    it names none (or only a name two stations share, without the state)."""
    tk = f" {name_key(text)} "
    if len(tk) < 4:
        return ""
    for key in _TEXT_KEYS:
        if f" {key} " in tk:
            st = _station_of_key(key, state)
            if st:
                return st
    return ""


# ── Towns ───────────────────────────────────────────────────────────────────
# (town, state) -> the station that runs the VA's care there, for a posting
# that names no facility (its hiring-organization line is the agency and its
# text names none). Only towns where one station runs every VA site of the
# town. Built 2026-10-27 from the active rows that named their station (a
# posting of one station listing towns of that station's states only), the
# agency contact address on the job pages read that day
# (claud_outputinput/va_facility_2026-10-27/page_facts.tsv), and the VA's
# station pages for the towns of the generic rows; a town two stations
# share is left out.
TOWNS: dict = {
    # AK
    ('anchorage', 'AK'): 'Alaska VA Healthcare System',
    ('fairbanks', 'AK'): 'Alaska VA Healthcare System',
    ('soldotna', 'AK'): 'Alaska VA Healthcare System',
    # AL
    ('birmingham', 'AL'): 'Birmingham VA Health Care System',
    ('childersburg', 'AL'): 'Birmingham VA Health Care System',
    ('dothan', 'AL'): 'Central Alabama Veterans Health Care System',
    ('huntsville', 'AL'): 'Birmingham VA Health Care System',
    ('mobile', 'AL'): 'Gulf Coast Veterans Health Care System',
    ('montgomery', 'AL'): 'Central Alabama Veterans Health Care System',
    ('oxford', 'AL'): 'Birmingham VA Health Care System',
    ('tuscaloosa', 'AL'): 'Tuscaloosa VA Medical Center',
    ('tuskegee', 'AL'): 'Central Alabama Veterans Health Care System',
    # AR
    ('conway', 'AR'): 'Central Arkansas Veterans Health Care System',
    ('el dorado', 'AR'): 'Central Arkansas Veterans Health Care System',
    ('fayetteville', 'AR'): 'Veterans Healthcare System of the Ozarks',
    ('fort smith', 'AR'): 'Veterans Healthcare System of the Ozarks',
    ('harrison', 'AR'): 'Veterans Healthcare System of the Ozarks',
    ('hot springs', 'AR'): 'Central Arkansas Veterans Health Care System',
    ('little rock', 'AR'): 'Central Arkansas Veterans Health Care System',
    ('mena', 'AR'): 'Central Arkansas Veterans Health Care System',
    ('mountain home', 'AR'): 'Central Arkansas Veterans Health Care System',
    ('north little rock', 'AR'): 'Central Arkansas Veterans Health Care System',
    ('ozark', 'AR'): 'Veterans Healthcare System of the Ozarks',
    ('pine bluff', 'AR'): 'Central Arkansas Veterans Health Care System',
    ('russellville', 'AR'): 'Central Arkansas Veterans Health Care System',
    # AZ
    ('anthem', 'AZ'): 'Northern Arizona VA Health Care System',
    ('chinle', 'AZ'): 'Northern Arizona VA Health Care System',
    ('cottonwood', 'AZ'): 'Northern Arizona VA Health Care System',
    ('flagstaff', 'AZ'): 'Northern Arizona VA Health Care System',
    ('gilbert', 'AZ'): 'VA Phoenix Healthcare System',
    ('goodyear', 'AZ'): 'VA Phoenix Healthcare System',
    ('kayenta', 'AZ'): 'Northern Arizona VA Health Care System',
    ('kingman', 'AZ'): 'Northern Arizona VA Health Care System',
    ('lake havasu city', 'AZ'): 'Northern Arizona VA Health Care System',
    ('page', 'AZ'): 'Northern Arizona VA Health Care System',
    ('phoenix', 'AZ'): 'VA Phoenix Healthcare System',
    ('prescott', 'AZ'): 'Northern Arizona VA Health Care System',
    ('scottsdale', 'AZ'): 'VA Phoenix Healthcare System',
    ('sierra vista', 'AZ'): 'Southern Arizona VA Health Care System',
    ('surprise', 'AZ'): 'VA Phoenix Healthcare System',
    ('tuba city', 'AZ'): 'Northern Arizona VA Health Care System',
    ('tucson', 'AZ'): 'Southern Arizona VA Health Care System',
    # CA
    ('bakersfield', 'CA'): 'Greater Los Angeles VA Health Care System',
    ('chico', 'CA'): 'VA Northern California Healthcare System',
    ('chula vista', 'CA'): 'San Diego VA Healthcare System',
    ('escondido', 'CA'): 'San Diego VA Healthcare System',
    ('eureka', 'CA'): 'San Francisco VA Health Care System',
    ('fairfield', 'CA'): 'VA Northern California Healthcare System',
    ('fresno', 'CA'): 'VA Central California Health Care System',
    ('hemet', 'CA'): 'VA Loma Linda Healthcare System',
    ('laguna hills', 'CA'): 'Long Beach VA Health Care System',
    ('lancaster', 'CA'): 'Greater Los Angeles VA Health Care System',
    ('livermore', 'CA'): 'VA Palo Alto Health Care System',
    ('loma linda', 'CA'): 'VA Loma Linda Healthcare System',
    ('long beach', 'CA'): 'Long Beach VA Health Care System',
    ('los angeles', 'CA'): 'Greater Los Angeles VA Health Care System',
    ('marina', 'CA'): 'VA Palo Alto Health Care System',
    ('martinez', 'CA'): 'VA Northern California Healthcare System',
    ('mather', 'CA'): 'VA Northern California Healthcare System',
    ('mather afb', 'CA'): 'VA Northern California Healthcare System',
    ('mcclellan', 'CA'): 'VA Northern California Healthcare System',
    ('menlo park', 'CA'): 'VA Palo Alto Health Care System',
    ('monterey', 'CA'): 'VA Palo Alto Health Care System',
    ('north hills', 'CA'): 'Greater Los Angeles VA Health Care System',
    ('oceanside', 'CA'): 'San Diego VA Healthcare System',
    ('palo alto', 'CA'): 'VA Palo Alto Health Care System',
    ('placentia', 'CA'): 'Long Beach VA Health Care System',
    ('redding', 'CA'): 'VA Northern California Healthcare System',
    ('redlands', 'CA'): 'VA Loma Linda Healthcare System',
    ('san bruno', 'CA'): 'San Francisco VA Health Care System',
    ('san diego', 'CA'): 'San Diego VA Healthcare System',
    ('san francisco', 'CA'): 'San Francisco VA Health Care System',
    ('san jose', 'CA'): 'VA Palo Alto Health Care System',
    ('san luis obispo', 'CA'): 'Greater Los Angeles VA Health Care System',
    ('santa maria', 'CA'): 'Greater Los Angeles VA Health Care System',
    ('santa rosa', 'CA'): 'San Francisco VA Health Care System',
    ('stockton', 'CA'): 'VA Northern California Healthcare System',
    ('ukiah', 'CA'): 'San Francisco VA Health Care System',
    ('visalia', 'CA'): 'VA Central California Health Care System',
    # CO
    ('alamosa', 'CO'): 'VA Eastern Colorado Health Care System',
    ('aurora', 'CO'): 'VA Eastern Colorado Health Care System',
    ('burlington', 'CO'): 'VA Eastern Colorado Health Care System',
    ('castle rock', 'CO'): 'VA Eastern Colorado Health Care System',
    ('colorado springs', 'CO'): 'VA Eastern Colorado Health Care System',
    ('denver', 'CO'): 'VA Eastern Colorado Health Care System',
    ('englewood', 'CO'): 'VA Eastern Colorado Health Care System',
    ('glendale', 'CO'): 'VA Eastern Colorado Health Care System',
    ('golden', 'CO'): 'VA Eastern Colorado Health Care System',
    ('grand junction', 'CO'): 'VA Western Colorado Health Care System',
    ('la junta', 'CO'): 'VA Eastern Colorado Health Care System',
    ('lamar', 'CO'): 'VA Eastern Colorado Health Care System',
    ('loveland', 'CO'): 'Cheyenne VA Health Care System',
    ('pueblo', 'CO'): 'VA Eastern Colorado Health Care System',
    ('salida', 'CO'): 'VA Eastern Colorado Health Care System',
    # CT
    ('newington', 'CT'): 'VA Connecticut Healthcare System',
    ('west haven', 'CT'): 'VA Connecticut Healthcare System',
    # DC
    ('washington', 'DC'): 'Washington DC VA Medical Center',
    # DE
    ('wilmington', 'DE'): 'Wilmington VA Medical Center',
    # FL
    ('bay pines', 'FL'): 'Bay Pines VA Healthcare System',
    ('bradenton', 'FL'): 'Bay Pines VA Healthcare System',
    ('broward', 'FL'): 'Miami VA Healthcare System',
    ('cape coral', 'FL'): 'Bay Pines VA Healthcare System',
    ('charlotte', 'FL'): 'Bay Pines VA Healthcare System',
    ('clearwater', 'FL'): 'Bay Pines VA Healthcare System',
    ('clermont', 'FL'): 'Orlando VA Healthcare System',
    ('daytona beach', 'FL'): 'Orlando VA Healthcare System',
    ('deerfield beach', 'FL'): 'Miami VA Healthcare System',
    ('delray beach', 'FL'): 'West Palm Beach VA Healthcare System',
    ('deltona', 'FL'): 'Orlando VA Healthcare System',
    ('eglin afb', 'FL'): 'Gulf Coast Veterans Health Care System',
    ('gainesville', 'FL'): 'North Florida/South Georgia Veterans Health System',
    ('hollywood', 'FL'): 'Miami VA Healthcare System',
    ('homestead', 'FL'): 'Miami VA Healthcare System',
    ('jacksonville', 'FL'): 'North Florida/South Georgia Veterans Health System',
    ('key largo', 'FL'): 'Miami VA Healthcare System',
    ('key west', 'FL'): 'Miami VA Healthcare System',
    ('kissimmee', 'FL'): 'Orlando VA Healthcare System',
    ('lake city', 'FL'): 'North Florida/South Georgia Veterans Health System',
    ('lee', 'FL'): 'Bay Pines VA Healthcare System',
    ('marianna', 'FL'): 'North Florida/South Georgia Veterans Health System',
    ('miami', 'FL'): 'Miami VA Healthcare System',
    ('middleburg', 'FL'): 'North Florida/South Georgia Veterans Health System',
    ('monroe', 'FL'): 'Miami VA Healthcare System',
    ('naples', 'FL'): 'Bay Pines VA Healthcare System',
    ('oakland park', 'FL'): 'Miami VA Healthcare System',
    ('orlando', 'FL'): 'Orlando VA Healthcare System',
    ('palm bay', 'FL'): 'Orlando VA Healthcare System',
    ('panama city beach', 'FL'): 'Gulf Coast Veterans Health Care System',
    ('pensacola', 'FL'): 'Gulf Coast Veterans Health Care System',
    ('port richey', 'FL'): 'James A. Haley VA Medical Center',
    ('riverview', 'FL'): 'James A. Haley VA Medical Center',
    ('sarasota', 'FL'): 'Bay Pines VA Healthcare System',
    ('sebring', 'FL'): 'Bay Pines VA Healthcare System',
    ('st augustine', 'FL'): 'North Florida/South Georgia Veterans Health System',
    ('st petersburg', 'FL'): 'Bay Pines VA Healthcare System',
    ('sunrise', 'FL'): 'Miami VA Healthcare System',
    ('tallahassee', 'FL'): 'North Florida/South Georgia Veterans Health System',
    ('tampa', 'FL'): 'James A. Haley VA Medical Center',
    ('tavares', 'FL'): 'Orlando VA Healthcare System',
    ('the villages', 'FL'): 'North Florida/South Georgia Veterans Health System',
    ('vero beach', 'FL'): 'West Palm Beach VA Healthcare System',
    ('viera', 'FL'): 'Orlando VA Healthcare System',
    ('west palm beach', 'FL'): 'West Palm Beach VA Healthcare System',
    ('winter park', 'FL'): 'Orlando VA Healthcare System',
    ('zephyrhills', 'FL'): 'James A. Haley VA Medical Center',
    # GA
    ('athens', 'GA'): 'Charlie Norwood VA Medical Center',
    ('atlanta', 'GA'): 'Atlanta VA Health Care System',
    ('augusta', 'GA'): 'Charlie Norwood VA Medical Center',
    ('columbus', 'GA'): 'Central Alabama Veterans Health Care System',
    ('decatur', 'GA'): 'Atlanta VA Health Care System',
    ('dublin', 'GA'): 'Carl Vinson VA Medical Center',
    ('duluth', 'GA'): 'Atlanta VA Health Care System',
    ('flowery branch', 'GA'): 'Atlanta VA Health Care System',
    ('hinesville', 'GA'): 'Ralph H. Johnson VA Health Care System',
    ('marietta', 'GA'): 'Atlanta VA Health Care System',
    ('savannah', 'GA'): 'Ralph H. Johnson VA Health Care System',
    ('st marys', 'GA'): 'North Florida/South Georgia Veterans Health System',
    ('stockbridge', 'GA'): 'Atlanta VA Health Care System',
    ('valdosta', 'GA'): 'North Florida/South Georgia Veterans Health System',
    # HI
    ('honolulu', 'HI'): 'VA Pacific Islands Health Care System',
    ('kapolei', 'HI'): 'VA Pacific Islands Health Care System',
    # IA
    ('burlington', 'IA'): 'Iowa City VA Health Care System',
    ('carroll', 'IA'): 'VA Central Iowa Health Care System',
    ('coralville', 'IA'): 'Iowa City VA Health Care System',
    ('davenport', 'IA'): 'Iowa City VA Health Care System',
    ('des moines', 'IA'): 'VA Central Iowa Health Care System',
    ('dubuque', 'IA'): 'Iowa City VA Health Care System',
    ('fort dodge', 'IA'): 'VA Central Iowa Health Care System',
    ('iowa city', 'IA'): 'Iowa City VA Health Care System',
    ('ottumwa', 'IA'): 'Iowa City VA Health Care System',
    ('shenandoah', 'IA'): 'VA Nebraska-Western Iowa Health Care System',
    ('waterloo', 'IA'): 'Iowa City VA Health Care System',
    # ID
    ('boise', 'ID'): 'Boise VA Medical Center',
    ('caldwell', 'ID'): 'Boise VA Medical Center',
    ('mountain home', 'ID'): 'Boise VA Medical Center',
    ('pocatello', 'ID'): 'VA Salt Lake City Health Care System',
    # IL
    ('carbondale', 'IL'): 'Marion VA Health Care System',
    ('chicago', 'IL'): 'Jesse Brown VA Medical Center',
    ('danville', 'IL'): 'VA Illiana Health Care System',
    ('hines', 'IL'): 'Edward Hines Jr. VA Hospital',
    ('hoffman estates', 'IL'): 'Edward Hines Jr. VA Hospital',
    ('joliet', 'IL'): 'Edward Hines Jr. VA Hospital',
    ('kankakee', 'IL'): 'Edward Hines Jr. VA Hospital',
    ('marion', 'IL'): 'Marion VA Health Care System',
    ('mount vernon', 'IL'): 'Marion VA Health Care System',
    ('north aurora', 'IL'): 'Edward Hines Jr. VA Hospital',
    ('north chicago', 'IL'): 'Captain James A. Lovell Federal Health Care Center',
    ('oak lawn', 'IL'): 'Edward Hines Jr. VA Hospital',
    ('peru', 'IL'): 'Edward Hines Jr. VA Hospital',
    ('quincy', 'IL'): 'Iowa City VA Health Care System',
    ('scott afb', 'IL'): 'VA St. Louis Health Care System',
    ('shiloh', 'IL'): 'VA St. Louis Health Care System',
    ('sterling', 'IL'): 'Iowa City VA Health Care System',
    # IN
    ('brownsburg', 'IN'): 'Richard L. Roudebush VA Medical Center',
    ('dearborn', 'IN'): 'Cincinnati VA Medical Center',
    ('evansville', 'IN'): 'Marion VA Health Care System',
    ('fort wayne', 'IN'): 'VA Northern Indiana Health Care System',
    ('indianapolis', 'IN'): 'Richard L. Roudebush VA Medical Center',
    ('marion', 'IN'): 'VA Northern Indiana Health Care System',
    ('mishawaka', 'IN'): 'VA Northern Indiana Health Care System',
    ('new albany', 'IN'): 'Robley Rex VA Medical Center',
    ('peru', 'IN'): 'VA Northern Indiana Health Care System',
    ('richmond', 'IN'): 'Dayton VA Medical Center',
    ('shelbyville', 'IN'): 'Richard L. Roudebush VA Medical Center',
    # KS
    ('dodge city', 'KS'): 'Robert J. Dole VA Medical Center',
    ('fort riley', 'KS'): 'VA Eastern Kansas Health Care System',
    ('kansas city', 'KS'): 'Kansas City VA Medical Center',
    ('leavenworth', 'KS'): 'VA Eastern Kansas Health Care System',
    ('lenexa', 'KS'): 'Kansas City VA Medical Center',
    ('shawnee', 'KS'): 'Kansas City VA Medical Center',
    ('topeka', 'KS'): 'VA Eastern Kansas Health Care System',
    ('wichita', 'KS'): 'Robert J. Dole VA Medical Center',
    # KY
    ('clarkson', 'KY'): 'Robley Rex VA Medical Center',
    ('florence', 'KY'): 'Cincinnati VA Medical Center',
    ('fort knox', 'KY'): 'Robley Rex VA Medical Center',
    ('fort thomas', 'KY'): 'Cincinnati VA Medical Center',
    ('lexington', 'KY'): 'Lexington VA Health Care System',
    ('louisville', 'KY'): 'Robley Rex VA Medical Center',
    ('somerset', 'KY'): 'Lexington VA Health Care System',
    # LA
    ('alexandria', 'LA'): 'Alexandria VA Health Care System',
    ('fort polk', 'LA'): 'Alexandria VA Health Care System',
    ('hammond', 'LA'): 'Southeast Louisiana Veterans Health Care System',
    ('jennings', 'LA'): 'Alexandria VA Health Care System',
    ('lafayette', 'LA'): 'Alexandria VA Health Care System',
    ('lake charles', 'LA'): 'Alexandria VA Health Care System',
    ('monroe', 'LA'): 'Overton Brooks VA Medical Center',
    ('natchitoches', 'LA'): 'Alexandria VA Health Care System',
    ('new orleans', 'LA'): 'Southeast Louisiana Veterans Health Care System',
    ('shreveport', 'LA'): 'Overton Brooks VA Medical Center',
    # MA
    ('bedford', 'MA'): 'VA Bedford Healthcare System',
    ('boston', 'MA'): 'VA Boston Healthcare System',
    ('brockton', 'MA'): 'VA Boston Healthcare System',
    ('greenfield', 'MA'): 'VA Central Western Massachusetts Healthcare System',
    ('hyannis', 'MA'): 'VA Providence Healthcare System',
    ('jamaica plain', 'MA'): 'VA Boston Healthcare System',
    ('northampton', 'MA'): 'VA Central Western Massachusetts Healthcare System',
    ('pittsfield', 'MA'): 'VA Central Western Massachusetts Healthcare System',
    ('springfield', 'MA'): 'VA Central Western Massachusetts Healthcare System',
    ('west roxbury', 'MA'): 'VA Boston Healthcare System',
    ('worcester', 'MA'): 'VA Central Western Massachusetts Healthcare System',
    # MD
    ('baltimore', 'MD'): 'VA Maryland Health Care System',
    ('cambridge', 'MD'): 'VA Maryland Health Care System',
    ('fort detrick', 'MD'): 'Martinsburg VA Medical Center',
    ('fort meade', 'MD'): 'VA Maryland Health Care System',
    ('perry point', 'MD'): 'VA Maryland Health Care System',
    # ME
    ('augusta', 'ME'): 'VA Maine Healthcare System',
    ('bangor', 'ME'): 'VA Maine Healthcare System',
    ('lincoln', 'ME'): 'VA Maine Healthcare System',
    ('portland', 'ME'): 'VA Maine Healthcare System',
    ('presque isle', 'ME'): 'VA Maine Healthcare System',
    ('togus', 'ME'): 'VA Maine Healthcare System',
    # MI
    ('adrian', 'MI'): 'Lieutenant Colonel Charles S. Kettles VA Medical Center',
    ('ann arbor', 'MI'): 'Lieutenant Colonel Charles S. Kettles VA Medical Center',
    ('battle creek', 'MI'): 'Battle Creek VA Medical Center',
    ('canton', 'MI'): 'Lieutenant Colonel Charles S. Kettles VA Medical Center',
    ('detroit', 'MI'): 'John D. Dingell VA Medical Center',
    ('gaylord', 'MI'): 'Aleda E. Lutz VA Medical Center',
    ('iron mountain', 'MI'): 'Oscar G. Johnson VA Medical Center',
    ('lansing', 'MI'): 'Battle Creek VA Medical Center',
    ('muskegon', 'MI'): 'Battle Creek VA Medical Center',
    ('saginaw', 'MI'): 'Aleda E. Lutz VA Medical Center',
    ('traverse city', 'MI'): 'Aleda E. Lutz VA Medical Center',
    ('wyoming', 'MI'): 'Battle Creek VA Medical Center',
    ('yale', 'MI'): 'John D. Dingell VA Medical Center',
    # MN
    ('alexandria', 'MN'): 'St. Cloud VA Health Care System',
    ('fergus falls', 'MN'): 'Fargo VA Health Care System',
    ('maplewood', 'MN'): 'Minneapolis VA Health Care System',
    ('minneapolis', 'MN'): 'Minneapolis VA Health Care System',
    ('montevideo', 'MN'): 'St. Cloud VA Health Care System',
    ('st cloud', 'MN'): 'St. Cloud VA Health Care System',
    # MO
    ('branson', 'MO'): 'Veterans Healthcare System of the Ozarks',
    ('camdenton', 'MO'): "Harry S. Truman Memorial Veterans' Hospital",
    ('cape girardeau', 'MO'): 'John J. Pershing VA Medical Center',
    ('columbia', 'MO'): "Harry S. Truman Memorial Veterans' Hospital",
    ('florissant', 'MO'): 'VA St. Louis Health Care System',
    ('jefferson barracks', 'MO'): 'VA St. Louis Health Care System',
    ('joplin', 'MO'): 'Veterans Healthcare System of the Ozarks',
    ('kansas city', 'MO'): 'Kansas City VA Medical Center',
    ('marshfield', 'MO'): "Harry S. Truman Memorial Veterans' Hospital",
    ('poplar bluff', 'MO'): 'John J. Pershing VA Medical Center',
    ('rolla', 'MO'): "Harry S. Truman Memorial Veterans' Hospital",
    ('sikeston', 'MO'): 'John J. Pershing VA Medical Center',
    ('springfield', 'MO'): 'Veterans Healthcare System of the Ozarks',
    ('st james', 'MO'): "Harry S. Truman Memorial Veterans' Hospital",
    ('st louis', 'MO'): 'VA St. Louis Health Care System',
    ('warrensburg', 'MO'): 'Kansas City VA Medical Center',
    ('waynesville', 'MO'): "Harry S. Truman Memorial Veterans' Hospital",
    # MS
    ('biloxi', 'MS'): 'Gulf Coast Veterans Health Care System',
    ('jackson', 'MS'): 'G.V. (Sonny) Montgomery VA Medical Center',
    # MT
    ('billings', 'MT'): 'Montana VA Health Care System',
    ('bozeman', 'MT'): 'Montana VA Health Care System',
    ('butte', 'MT'): 'Montana VA Health Care System',
    ('fort harrison', 'MT'): 'Montana VA Health Care System',
    ('great falls', 'MT'): 'Montana VA Health Care System',
    ('helena', 'MT'): 'Montana VA Health Care System',
    ('kalispell', 'MT'): 'Montana VA Health Care System',
    ('miles city', 'MT'): 'Montana VA Health Care System',
    ('missoula', 'MT'): 'Montana VA Health Care System',
    # NC
    ('asheville', 'NC'): 'Charles George VA Medical Center',
    ('charlotte', 'NC'): 'W.G. (Bill) Hefner VA Medical Center',
    ('clayton', 'NC'): 'Durham VA Medical Center',
    ('durham', 'NC'): 'Durham VA Medical Center',
    ('elizabeth city', 'NC'): 'Hampton VA Medical Center',
    ('fayetteville', 'NC'): 'Fayetteville VA Coastal Health Care System',
    ('garner', 'NC'): 'Durham VA Medical Center',
    ('goldsboro', 'NC'): 'Fayetteville VA Coastal Health Care System',
    ('greenville', 'NC'): 'Durham VA Medical Center',
    ('jacksonville', 'NC'): 'Fayetteville VA Coastal Health Care System',
    ('kernersville', 'NC'): 'W.G. (Bill) Hefner VA Medical Center',
    ('pembroke', 'NC'): 'Fayetteville VA Coastal Health Care System',
    ('salisbury', 'NC'): 'W.G. (Bill) Hefner VA Medical Center',
    ('wilmington', 'NC'): 'Fayetteville VA Coastal Health Care System',
    # ND
    ('bismarck', 'ND'): 'Fargo VA Health Care System',
    ('fargo', 'ND'): 'Fargo VA Health Care System',
    ('grand forks', 'ND'): 'Fargo VA Health Care System',
    # NE
    ('grand island', 'NE'): 'VA Nebraska-Western Iowa Health Care System',
    ('holdrege', 'NE'): 'VA Nebraska-Western Iowa Health Care System',
    ('lincoln', 'NE'): 'VA Nebraska-Western Iowa Health Care System',
    ('norfolk', 'NE'): 'VA Nebraska-Western Iowa Health Care System',
    ('north platte', 'NE'): 'VA Nebraska-Western Iowa Health Care System',
    ('omaha', 'NE'): 'VA Nebraska-Western Iowa Health Care System',
    ('papillion', 'NE'): 'VA Nebraska-Western Iowa Health Care System',
    # NH
    ('littleton', 'NH'): 'VA White River Junction Healthcare System',
    ('manchester', 'NH'): 'VA Manchester Healthcare System',
    ('portsmouth', 'NH'): 'VA Manchester Healthcare System',
    # NJ
    ('east orange', 'NJ'): 'New Jersey VA Healthcare System',
    ('gloucester city', 'NJ'): 'Corporal Michael J. Crescenz VA Medical Center',
    ('hamilton', 'NJ'): 'New Jersey VA Healthcare System',
    ('lyons', 'NJ'): 'New Jersey VA Healthcare System',
    ('tinton falls', 'NJ'): 'New Jersey VA Healthcare System',
    ('toms river', 'NJ'): 'New Jersey VA Healthcare System',
    ('vineland', 'NJ'): 'Wilmington VA Medical Center',
    # NM
    ('albuquerque', 'NM'): 'New Mexico VA Health Care System',
    ('las cruces', 'NM'): 'El Paso VA Health Care System',
    ('rio rancho', 'NM'): 'New Mexico VA Health Care System',
    # NV
    ('fallon', 'NV'): 'VA Sierra Nevada Health Care System',
    ('las vegas', 'NV'): 'VA Southern Nevada Healthcare System',
    ('reno', 'NV'): 'VA Sierra Nevada Health Care System',
    # NY
    ('albany', 'NY'): 'Samuel S. Stratton VA Medical Center',
    ('batavia', 'NY'): 'VA Western New York Healthcare System',
    ('bath', 'NY'): 'VA Finger Lakes Healthcare System',
    ('bronx', 'NY'): 'James J. Peters VA Medical Center',
    ('brooklyn', 'NY'): 'VA NY Harbor Healthcare System',
    ('buffalo', 'NY'): 'VA Western New York Healthcare System',
    ('canandaigua', 'NY'): 'VA Finger Lakes Healthcare System',
    ('castle point', 'NY'): 'Hudson Valley VA Healthcare System',
    ('catskill', 'NY'): 'Samuel S. Stratton VA Medical Center',
    ('goshen', 'NY'): 'Hudson Valley VA Healthcare System',
    ('jamestown', 'NY'): 'VA Western New York Healthcare System',
    ('kings', 'NY'): 'VA NY Harbor Healthcare System',
    ('monticello', 'NY'): 'Hudson Valley VA Healthcare System',
    ('montrose', 'NY'): 'Hudson Valley VA Healthcare System',
    ('new york', 'NY'): 'VA NY Harbor Healthcare System',
    ('northport', 'NY'): 'Northport VA Medical Center',
    ('olean', 'NY'): 'VA Western New York Healthcare System',
    ('oswego', 'NY'): 'Syracuse VA Medical Center',
    ('queens', 'NY'): 'VA NY Harbor Healthcare System',
    ('rochester', 'NY'): 'VA Finger Lakes Healthcare System',
    ('rome', 'NY'): 'Syracuse VA Medical Center',
    ('syracuse', 'NY'): 'Syracuse VA Medical Center',
    ('wappingers falls', 'NY'): 'Hudson Valley VA Healthcare System',
    # OH
    ('ashtabula', 'OH'): 'Erie VA Medical Center',
    ('chillicothe', 'OH'): 'Chillicothe VA Medical Center',
    ('cincinnati', 'OH'): 'Cincinnati VA Medical Center',
    ('clermont', 'OH'): 'Cincinnati VA Medical Center',
    ('cleveland', 'OH'): 'VA Northeast Ohio Healthcare System',
    ('columbus', 'OH'): 'VA Central Ohio Healthcare System',
    ('dayton', 'OH'): 'Dayton VA Medical Center',
    ('findlay', 'OH'): 'Lieutenant Colonel Charles S. Kettles VA Medical Center',
    ('georgetown', 'OH'): 'Cincinnati VA Medical Center',
    ('grove city', 'OH'): 'VA Central Ohio Healthcare System',
    ('hamilton', 'OH'): 'Cincinnati VA Medical Center',
    ('lima', 'OH'): 'Dayton VA Medical Center',
    ('middletown', 'OH'): 'Dayton VA Medical Center',
    ('newark', 'OH'): 'VA Central Ohio Healthcare System',
    ('norwood', 'OH'): 'Cincinnati VA Medical Center',
    ('parma', 'OH'): 'VA Northeast Ohio Healthcare System',
    ('springfield', 'OH'): 'Dayton VA Medical Center',
    ('toledo', 'OH'): 'Lieutenant Colonel Charles S. Kettles VA Medical Center',
    ('willoughby', 'OH'): 'VA Northeast Ohio Healthcare System',
    ('youngstown', 'OH'): 'VA Northeast Ohio Healthcare System',
    # OK
    ('claremore', 'OK'): 'Eastern Oklahoma VA Health Care System',
    ('jay', 'OK'): 'Veterans Healthcare System of the Ozarks',
    ('lawton', 'OK'): 'Oklahoma City VA Health Care System',
    ('muskogee', 'OK'): 'Eastern Oklahoma VA Health Care System',
    ('oklahoma city', 'OK'): 'Oklahoma City VA Health Care System',
    ('tulsa', 'OK'): 'Eastern Oklahoma VA Health Care System',
    # OR
    ('astoria', 'OR'): 'VA Portland Health Care System',
    ('bend', 'OR'): 'VA Portland Health Care System',
    ('boardman', 'OR'): 'Jonathan M. Wainwright Memorial VA Medical Center',
    ('eugene', 'OR'): 'Roseburg VA Health Care System',
    ('fairview', 'OR'): 'VA Portland Health Care System',
    ('grants pass', 'OR'): 'VA Southern Oregon Rehabilitation Center and Clinics',
    ('hillsboro', 'OR'): 'VA Portland Health Care System',
    ('north bend', 'OR'): 'Roseburg VA Health Care System',
    ('portland', 'OR'): 'VA Portland Health Care System',
    ('roseburg', 'OR'): 'Roseburg VA Health Care System',
    ('salem', 'OR'): 'VA Portland Health Care System',
    ('the dalles', 'OR'): 'VA Portland Health Care System',
    ('west linn', 'OR'): 'VA Portland Health Care System',
    ('white city', 'OR'): 'VA Southern Oregon Rehabilitation Center and Clinics',
    # PA
    ('altoona', 'PA'): 'James E. Van Zandt VA Medical Center',
    ('aspinwall', 'PA'): 'VA Pittsburgh Healthcare System',
    ('butler', 'PA'): 'Butler VA Medical Center',
    ('coatesville', 'PA'): 'Coatesville VA Medical Center',
    ('erie', 'PA'): 'Erie VA Medical Center',
    ('lebanon', 'PA'): 'Lebanon VA Medical Center',
    ('mechanicsburg', 'PA'): 'Lebanon VA Medical Center',
    ('newtown square', 'PA'): 'Coatesville VA Medical Center',
    ('philadelphia', 'PA'): 'Corporal Michael J. Crescenz VA Medical Center',
    ('pittsburgh', 'PA'): 'VA Pittsburgh Healthcare System',
    ('rochester', 'PA'): 'VA Pittsburgh Healthcare System',
    ('state college', 'PA'): 'James E. Van Zandt VA Medical Center',
    ('wilkes barre', 'PA'): 'Wilkes-Barre VA Medical Center',
    # PR
    ('mayaguez', 'PR'): 'VA Caribbean Healthcare System',
    ('san juan', 'PR'): 'VA Caribbean Healthcare System',
    # RI
    ('middletown', 'RI'): 'VA Providence Healthcare System',
    ('providence', 'RI'): 'VA Providence Healthcare System',
    # SC
    ('anderson', 'SC'): 'Columbia VA Health Care System',
    ('beaufort', 'SC'): 'Ralph H. Johnson VA Health Care System',
    ('charleston', 'SC'): 'Ralph H. Johnson VA Health Care System',
    ('columbia', 'SC'): 'Columbia VA Health Care System',
    ('goose creek', 'SC'): 'Ralph H. Johnson VA Health Care System',
    ('greenville', 'SC'): 'Columbia VA Health Care System',
    ('myrtle beach', 'SC'): 'Ralph H. Johnson VA Health Care System',
    ('north charleston', 'SC'): 'Ralph H. Johnson VA Health Care System',
    ('orangeburg', 'SC'): 'Columbia VA Health Care System',
    ('spartanburg', 'SC'): 'Columbia VA Health Care System',
    # SD
    ('fort meade', 'SD'): 'VA Black Hills Health Care System',
    ('hot springs', 'SD'): 'VA Black Hills Health Care System',
    ('rapid city', 'SD'): 'VA Black Hills Health Care System',
    ('sioux falls', 'SD'): 'Sioux Falls VA Health Care System',
    ('union', 'SD'): 'Sioux Falls VA Health Care System',
    # TN
    ('chattanooga', 'TN'): 'Tennessee Valley Healthcare System',
    ('clarksville', 'TN'): 'Tennessee Valley Healthcare System',
    ('jacksboro', 'TN'): 'James H. Quillen VA Medical Center',
    ('jackson', 'TN'): 'Lt. Col. Luke Weathers, Jr. VA Medical Center',
    ('johnson city', 'TN'): 'James H. Quillen VA Medical Center',
    ('knoxville', 'TN'): 'James H. Quillen VA Medical Center',
    ('mcminnville', 'TN'): 'Tennessee Valley Healthcare System',
    ('memphis', 'TN'): 'Lt. Col. Luke Weathers, Jr. VA Medical Center',
    ('millington', 'TN'): 'Lt. Col. Luke Weathers, Jr. VA Medical Center',
    ('murfreesboro', 'TN'): 'Tennessee Valley Healthcare System',
    ('nashville', 'TN'): 'Tennessee Valley Healthcare System',
    ('sevierville', 'TN'): 'James H. Quillen VA Medical Center',
    ('tullahoma', 'TN'): 'Tennessee Valley Healthcare System',
    # TX
    ('abilene', 'TX'): 'West Texas VA Health Care System',
    ('amarillo', 'TX'): 'Amarillo VA Health Care System',
    ('austin', 'TX'): 'Central Texas Veterans Health Care System',
    ('balcones heights', 'TX'): 'South Texas Veterans Health Care System',
    ('big spring', 'TX'): 'West Texas VA Health Care System',
    ('bonham', 'TX'): 'VA North Texas Health Care System',
    ('cedar park', 'TX'): 'Central Texas Veterans Health Care System',
    ('college station', 'TX'): 'Central Texas Veterans Health Care System',
    ('conroe', 'TX'): 'Michael E. DeBakey VA Medical Center',
    ('corpus christi', 'TX'): 'VA Texas Valley Coastal Bend Health Care System',
    ('dallas', 'TX'): 'VA North Texas Health Care System',
    ('denton', 'TX'): 'VA North Texas Health Care System',
    ('duncanville', 'TX'): 'VA North Texas Health Care System',
    ('el paso', 'TX'): 'El Paso VA Health Care System',
    ('fort stockton', 'TX'): 'West Texas VA Health Care System',
    ('fort worth', 'TX'): 'VA North Texas Health Care System',
    ('garland', 'TX'): 'VA North Texas Health Care System',
    ('granbury', 'TX'): 'VA North Texas Health Care System',
    ('grand prairie', 'TX'): 'VA North Texas Health Care System',
    ('greenville', 'TX'): 'VA North Texas Health Care System',
    ('harlingen', 'TX'): 'VA Texas Valley Coastal Bend Health Care System',
    ('houston', 'TX'): 'Michael E. DeBakey VA Medical Center',
    ('kerrville', 'TX'): 'South Texas Veterans Health Care System',
    ('killeen', 'TX'): 'Central Texas Veterans Health Care System',
    ('lake jackson', 'TX'): 'Michael E. DeBakey VA Medical Center',
    ('laredo', 'TX'): 'VA Texas Valley Coastal Bend Health Care System',
    ('longview', 'TX'): 'Overton Brooks VA Medical Center',
    ('lubbock', 'TX'): 'Amarillo VA Health Care System',
    ('mcallen', 'TX'): 'VA Texas Valley Coastal Bend Health Care System',
    ('new braunfels', 'TX'): 'South Texas Veterans Health Care System',
    ('odessa', 'TX'): 'West Texas VA Health Care System',
    ('plano', 'TX'): 'VA North Texas Health Care System',
    ('san angelo', 'TX'): 'West Texas VA Health Care System',
    ('san antonio', 'TX'): 'South Texas Veterans Health Care System',
    ('sherman', 'TX'): 'VA North Texas Health Care System',
    ('temple', 'TX'): 'Central Texas Veterans Health Care System',
    ('texarkana', 'TX'): 'Overton Brooks VA Medical Center',
    ('tyler', 'TX'): 'VA North Texas Health Care System',
    ('victoria', 'TX'): 'South Texas Veterans Health Care System',
    ('waco', 'TX'): 'Central Texas Veterans Health Care System',
    ('wichita falls', 'TX'): 'Oklahoma City VA Health Care System',
    # UT
    ('salt lake city', 'UT'): 'VA Salt Lake City Health Care System',
    ('south jordan', 'UT'): 'VA Salt Lake City Health Care System',
    ('st george', 'UT'): 'VA Salt Lake City Health Care System',
    # VA
    ('bristol', 'VA'): 'James H. Quillen VA Medical Center',
    ('charlottesville', 'VA'): 'Central Virginia VA Health Care System',
    ('chesapeake', 'VA'): 'Hampton VA Medical Center',
    ('danville', 'VA'): 'VA Salem Health Care System',
    ('emporia', 'VA'): 'Central Virginia VA Health Care System',
    ('fredericksburg', 'VA'): 'Central Virginia VA Health Care System',
    ('hampton', 'VA'): 'Hampton VA Medical Center',
    ('harrisonburg', 'VA'): 'Martinsburg VA Medical Center',
    ('henrico', 'VA'): 'Central Virginia VA Health Care System',
    ('lynchburg', 'VA'): 'VA Salem Health Care System',
    ('norton', 'VA'): 'James H. Quillen VA Medical Center',
    ('richmond', 'VA'): 'Central Virginia VA Health Care System',
    ('salem', 'VA'): 'VA Salem Health Care System',
    ('staunton', 'VA'): 'VA Salem Health Care System',
    ('tazewell', 'VA'): 'VA Salem Health Care System',
    ('virginia beach', 'VA'): 'Hampton VA Medical Center',
    ('wytheville', 'VA'): 'VA Salem Health Care System',
    # VT
    ('burlington', 'VT'): 'VA White River Junction Healthcare System',
    ('white river junction', 'VT'): 'VA White River Junction Healthcare System',
    # WA
    ('auburn', 'WA'): 'VA Puget Sound Health Care System',
    ('everett', 'WA'): 'VA Puget Sound Health Care System',
    ('olympia', 'WA'): 'VA Puget Sound Health Care System',
    ('richland', 'WA'): 'Jonathan M. Wainwright Memorial VA Medical Center',
    ('seattle', 'WA'): 'VA Puget Sound Health Care System',
    ('spokane', 'WA'): 'Mann-Grandstaff VA Medical Center',
    ('tacoma', 'WA'): 'VA Puget Sound Health Care System',
    ('vancouver', 'WA'): 'VA Portland Health Care System',
    ('walla walla', 'WA'): 'Jonathan M. Wainwright Memorial VA Medical Center',
    # WI
    ('appleton', 'WI'): 'Clement J. Zablocki VA Medical Center',
    ('green bay', 'WI'): 'Clement J. Zablocki VA Medical Center',
    ('kenosha', 'WI'): 'Captain James A. Lovell Federal Health Care Center',
    ('madison', 'WI'): 'William S. Middleton Memorial Veterans Hospital',
    ('middleton', 'WI'): 'William S. Middleton Memorial Veterans Hospital',
    ('milwaukee', 'WI'): 'Clement J. Zablocki VA Medical Center',
    ('rice lake', 'WI'): 'Minneapolis VA Health Care System',
    ('tomah', 'WI'): 'Tomah VA Medical Center',
    ('wausau', 'WI'): 'Tomah VA Medical Center',
    # WV
    ('beckley', 'WV'): 'Beckley VA Medical Center',
    ('charleston', 'WV'): 'Hershel "Woody" Williams VA Medical Center',
    ('clarksburg', 'WV'): 'Louis A. Johnson VA Medical Center',
    ('huntington', 'WV'): 'Hershel "Woody" Williams VA Medical Center',
    ('martinsburg', 'WV'): 'Martinsburg VA Medical Center',
    ('parkersburg', 'WV'): 'Louis A. Johnson VA Medical Center',
    ('westover', 'WV'): 'Louis A. Johnson VA Medical Center',
    # WY
    ('casper', 'WY'): 'Sheridan VA Health Care System',
    ('cheyenne', 'WY'): 'Cheyenne VA Health Care System',
    ('cody', 'WY'): 'Sheridan VA Health Care System',
    ('gillette', 'WY'): 'Sheridan VA Health Care System',
    ('sheridan', 'WY'): 'Sheridan VA Health Care System',
}


def _town_key(city) -> str:
    s = clean_text(city).lower()
    s = re.sub(r"\bsaint\b", "st", s)
    s = re.sub(r"\bft\b", "fort", s)
    s = re.sub(r"\s+county$", "", s)
    return re.sub(r"[^a-z0-9]+", " ", s).strip()


def town_facility(city, state) -> str:
    """The station for a posting's town, where that is certain; else ""
    (the District of Columbia, one station, with or without its town)."""
    st = (state or "").strip().upper()
    if not st:
        return ""
    if not clean_text(city):
        return TOWNS.get(("washington", "DC"), "") if st == "DC" else ""
    return TOWNS.get((_town_key(city), st), "")


_BY_KEY, STATION_STATES, HOSPITAL_STATION, HOSPITAL_STATES, _HOSP_KEYS = _build()
STATION_NAMES = frozenset(STATION_STATES)
HOSPITAL_NAMES = frozenset(HOSPITAL_STATION)
# station names and their other names, as keys, longest first (text_station)
_TEXT_KEYS = tuple(sorted((k for k, v in _BY_KEY.items() if k and any(h[0] in STATION_STATES for h in v)),
                          key=lambda k: (-len(k), k)))
