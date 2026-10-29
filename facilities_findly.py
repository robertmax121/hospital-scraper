"""The hospital (or named site) on Findly and Findly-Google rows (2026-10-29).

The owner, 2026-10-29: every hospital inside a multi-hospital system is named
and placed correctly on the site and in the dashboard, and applies are
retro-assigned to the right hospital. Decisions: (1) cards and job pages show
the hospital; (2) named sites (hospitals, named clinics and centers) show
their own name, generic offices / home-based / remote jobs show the system
name with the town; never guess a town.

Findly-Google tenants (jobsapi-google.m-cloud.io, scrape_findly_google) and
where each search payload names the site, read from the live payloads
2026-10-29 (AdventHealth 2,219 of 4,419 rows, Scripps 339 of 339,
University Hospitals 300 of 1,164, UCLA Health 300 of 756, UPMC 200 of 2,856):

  AdventHealth    job.department, the hospital's cost-center company
                  ("AdventHealth Orlando", "AdventHealth East Orlando",
                  "UChicago Medicine AdventHealth Hinsdale"), read together
                  with job.education, the cost center's location ("Orlando",
                  "East Orlando", "Hinsdale JV", "ER"). company_name is
                  "Adventist Health System - Florida" on every row (the old
                  label, also on Colorado, Kansas and Illinois rows);
                  job.brand is the region and job.business_unit a recruiter's
                  name, so neither is read.
                  A hospital department names the site only when the
                  location is the hospital's own: the regional support cost
                  centers sit under a hospital's company ("AdventHealth
                  Porter" / "RMR Support" holds the Rocky Mountain Region's
                  float pools, physicians at Castle Rock, Littleton and Avista
                  and the Greenwood Village office: 49 of Porter's 95 sampled
                  rows; likewise "WFD Support" under Tampa, "East FL
                  Division" under Daytona Beach, "GLR Support JV" under
                  Hinsdale, "Mid America Region" and "ASC MAR Shawnee
                  Mission" under Shawnee Mission, "SWR Support" under Huguley,
                  "Southeast Region" under Gordon). Those rows are the
                  region's, not the hospital's, so they take the system
                  name. Orlando and Tampa hold several AdventHealth hospitals
                  (Orlando, East Orlando, Lake Nona; Tampa, Carrollwood); the
                  department tells them apart, the town never does.
                  Freestanding ERs (location "ER": "AdventHealth Millenia
                  ER", "AdventHealth Poinciana ER") are named sites and keep
                  the department's name. The medical groups, home health and
                  hospice, Centra Care, Corporate, Executives, the division
                  support companies and the internal site codes ("OSED WFD
                  Auburndale", "HFCC Brevard") are no named site.
  Scripps Health  job.business_unit, the cost-center company with its code
                  ("SCRIPPS MEMORIAL HOSP LA JOLLA (300)", "SCRIPPS CLIN-
                  TORREY PINES (202)"). job.store_id is the coarser brand
                  ("Scripps Clinic" for every clinic), not read.
  University      job.store_id, the facility ("Cleveland Medical Center",
  Hospitals       "Rainbow Babies & Children's Hospital", "Lake West Medical
                  Center"); company_name is "University Hospitals (SP)" on
                  every row. UH writes its sites "UH <name>" ("UH Lake West
                  Medical Center", "UH TriPoint Medical Center" in the stored
                  bodies), the names used here.
  UCLA Health     job.primary_address, the street ("757 Westwood Plaza,
                  City: Los Angeles, ..."); nothing else names a site
                  (business_unit is a bargaining unit, "AFSCME - EX"). A
                  hospital's own street in its own town names it: 757
                  Westwood Plaza (Ronald Reagan), 1250 16th St, Santa Monica,
                  7300 Medical Center Dr, West Hills, 150 Medical Plaza
                  (Resnick). Every other address (clinics, offices, the
                  school's buildings) keeps the system name.
  UPMC            business_unit refined by brand (2026-10-15,
                  facility_names.upmc_facility); when neither names a
                  hospital, the one hospital the title names (owner
                  2026-10-29, facility_names.upmc_title_facility).

Each entry: source spelling -> (clean public name, city, state, source of
the town). The town is used only when the posting gives none: every row of
these feeds carries the posting's own town (primary_city), which is kept, so
no row moves town. Names are the employer's own public names (the CMS
hospital list in the database where it lists the hospital, given by its CMS
id; else the board's own spelling).

hospital_system never changes (the upsert key, the hub, the filters, the
logo). A row that names no site takes the system name (FINDLY_GOOGLE_NAME in
scraper.py), never the payload's legal label.

  facility(system, job, city, state)   (name, city, state) or None
  unknown(system, job)                 the raw value of a site the table
                                       does not know ("" otherwise), for
                                       the run log
  stored_facility(system, row)         (name, city, state, source) or None
                                       for a STORED row (description, title,
                                       city, state): the change-list builder
                                       (facility_scale_2026-10-29/phase2)
                                       for expired rows that carry applies
"""
from __future__ import annotations

import re

ADVENTHEALTH = "AdventHealth"
SCRIPPS = "Scripps Health"
UH = "University Hospitals"
UCLA = "UCLA Health"
UPMC = "UPMC"

# The system name a row takes when it names no site (decision 2), for the
# tenants whose company_name is a legal label rather than the system's name.
SYSTEM_NAME = {
    ADVENTHEALTH: "AdventHealth",           # was "Adventist Health System - Florida"
    UH: "University Hospitals",             # was "University Hospitals (SP)"
    UPMC: "UPMC",                           # was "UPMC - Pittsburgh Medical Center"
}


def key(s) -> str:
    """Lookup key: letters and digits only, lower case."""
    return re.sub(r"[^a-z0-9]", "", str(s or "").lower())


def _clean(s) -> str:
    return " ".join(str(s or "").replace("’", "'").split())


# ══════════════════════════════════════════════════════════════════════════
#  AdventHealth: job.department + job.education
# ══════════════════════════════════════════════════════════════════════════
# department -> (name, city, state, source of the town)
ADVENTHEALTH_FACILITIES: dict[str, tuple[str, str, str, str]] = {}
# department key -> the location keys (job.education) that are the site's own
_AH_OWN: dict[str, frozenset] = {}
# (street key, town key) -> name: the hospital's own street, for STORED rows
_AH_CAMPUS: dict[tuple[str, str], str] = {}


def _ah(dept, city, state, source, own=(), name=None, campus=None):
    nm = name or dept
    ADVENTHEALTH_FACILITIES[key(dept)] = (nm, city, state, source)
    short = re.sub(r"^AdventHealth\s+", "", dept)
    _AH_OWN[key(dept)] = frozenset({key(short)} | {key(o) for o in own})
    if campus:
        _AH_CAMPUS[(_street_key(campus), key(city))] = nm


_SUFFIX = {"street": "st", "avenue": "ave", "drive": "dr", "boulevard": "blvd", "road": "rd",
           "parkway": "pkwy", "highway": "hwy", "place": "pl", "plaza": "plz", "trail": "trl",
           "lane": "ln", "court": "ct", "circle": "cir", "north": "n", "south": "s", "east": "e",
           "west": "w", "northwest": "nw", "northeast": "ne", "southwest": "sw", "southeast": "se",
           "fwy": "fwy", "freeway": "fwy", "us": "us", "pl": "plz"}


def _street_key(s) -> str:
    words = re.sub(r"[^a-z0-9 ]", " ", str(s or "").lower()).split()
    return " ".join(_SUFFIX.get(w, w) for w in words)


# Central Florida (cms 100007 is one license for several named campuses)
_ah("AdventHealth Orlando", "Orlando", "FL", "cms 100007", campus="601 E Rollins St")
_ah("AdventHealth East Orlando", "Orlando", "FL", "board address", campus="7727 Lake Underhill Rd")
_ah("AdventHealth Lake Nona", "Orlando", "FL", "board address", campus="10925 Narcoossee Rd")
_ah("AdventHealth Altamonte Springs", "Altamonte Springs", "FL", "board address", campus="601 E Altamonte Dr")
_ah("AdventHealth Apopka", "Apopka", "FL", "board address", campus="2100 Ocoee Apopka Rd")
_ah("AdventHealth Celebration", "Celebration", "FL", "board address", campus="400 Celebration Pl")
_ah("AdventHealth Kissimmee", "Kissimmee", "FL", "board address", campus="2450 N Orange Blossom Trl")
_ah("AdventHealth Winter Park", "Winter Park", "FL", "board address", campus="200 N Lakemont Ave")
_ah("AdventHealth Winter Garden", "Winter Garden", "FL", "board address", campus="2000 Fowler Grove Blvd")
_ah("AdventHealth Minneola", "Minneola", "FL", "board address", campus="1800 N Hancock Rd")
# East Florida
_ah("AdventHealth Waterman", "Tavares", "FL", "cms 100057", campus="1000 Waterman Way")
_ah("AdventHealth Fish Memorial", "Orange City", "FL", "cms 100072", campus="1055 Saxon Blvd")
_ah("AdventHealth DeLand", "Deland", "FL", "cms 100045", campus="701 W Plymouth Ave")
_ah("AdventHealth Daytona Beach", "Daytona Beach", "FL", "cms 100068", campus="301 Memorial Medical Pkwy")
_ah("AdventHealth New Smyrna Beach", "New Smyrna Beach", "FL", "cms 100014", campus="401 Palmetto St")
_ah("AdventHealth Palm Coast", "Palm Coast", "FL", "cms 100118", campus="60 Memorial Medical Pkwy")
_ah("AdventHealth Palm Coast Parkway", "Palm Coast", "FL", "cms 100363", campus="1 AdventHealth Way")
# West Florida
_ah("AdventHealth Ocala", "Ocala", "FL", "cms 100062", campus="1500 SW 1st Ave")
_ah("AdventHealth Tampa", "Tampa", "FL", "cms 100173", campus="3100 E Fletcher Ave")
_ah("AdventHealth Carrollwood", "Tampa", "FL", "cms 100069", campus="7171 N Dale Mabry Hwy")
_ah("AdventHealth Riverview", "Riverview", "FL", "cms 100364", campus="9330 US Highway 301 S")
_ah("AdventHealth Wesley Chapel", "Wesley Chapel", "FL", "cms 100319", campus="2600 Bruce B Downs Blvd")
_ah("AdventHealth Zephyrhills", "Zephyrhills", "FL", "cms 100046", campus="7050 Gall Blvd")
_ah("AdventHealth Dade City", "Dade City", "FL", "cms 100211", campus="13100 Fort King Rd")
_ah("AdventHealth North Pinellas", "Tarpon Springs", "FL", "cms 100055", campus="1395 S Pinellas Ave")
_ah("AdventHealth Connerton", "Land O Lakes", "FL", "board address", campus="9441 Health Center Dr")
_ah("AdventHealth Sebring", "Sebring", "FL", "cms 100109", campus="4200 Sun N Lake Blvd")
_ah("AdventHealth Lake Placid", "Lake Placid", "FL", "board address", campus="1210 US Hwy 27 N")
_ah("AdventHealth Wauchula", "Wauchula", "FL", "cms 101300", campus="735 S 5th Ave")
_ah("AdventHealth Heart of Florida", "Davenport", "FL", "cms 100137", campus="40100 Hwy 27")
_ah("AdventHealth Lake Wales", "Lake Wales", "FL", "cms 100099", campus="410 S 11th St")
_ah("AdventHealth Port Charlotte", "Port Charlotte", "FL", "cms 100077", campus="2500 Harbor Blvd")
# Rocky Mountain
_ah("AdventHealth Porter", "Denver", "CO", "cms 060064", campus="2525 S Downing St")
_ah("AdventHealth Littleton", "Littleton", "CO", "cms 060113", campus="7700 S Broadway")
_ah("AdventHealth Parker", "Parker", "CO", "cms 060114", campus="9395 Crown Crest Blvd")
_ah("AdventHealth Castle Rock", "Castle Rock", "CO", "cms 060125", campus="2350 Meadows Blvd")
_ah("AdventHealth Avista", "Louisville", "CO", "cms 060103", campus="100 Health Park Dr")
# Mid-America
_ah("AdventHealth Shawnee Mission", "Shawnee Mission", "KS", "cms 170104", campus="9100 W 74th St")
_ah("AdventHealth South Overland Park", "Overland Park", "KS", "cms 170208", campus="7820 W 165th St")
_ah("AdventHealth Ottawa", "Ottawa", "KS", "cms 170014", campus="1301 S Main St")
_ah("AdventHealth Lenexa City Center", "Lenexa", "KS", "board address", campus="16950 W 86th St")
_ah("AdventHealth Prairie Star", "Lenexa", "KS", "board address", own=("ASC MAR Prairie Star",))
_ah("AdventHealth Durand", "Durand", "WI", "cms 521307")
# Southeast
_ah("AdventHealth Hendersonville", "Hendersonville", "NC", "cms 340023", campus="100 Hospital Dr")
_ah("AdventHealth Polk", "Columbus", "NC", "board address", campus="101 Hospital Dr")
_ah("AdventHealth Gordon", "Calhoun", "GA", "cms 110023", campus="1035 Red Bud Rd NE")
_ah("AdventHealth Murray", "Chatsworth", "GA", "cms 110050", campus="707 Old Dalton Ellijay Rd")
_ah("AdventHealth Redmond", "Rome", "GA", "cms 110168", campus="501 Redmond Rd NW")
_ah("AdventHealth Manchester", "Manchester", "KY", "cms 180043", campus="210 Marie Langdon Dr")
# Southwest (the two Texas Health hospitals are AdventHealth joint ventures
# posted on this board)
_ah("AdventHealth Central Texas", "Killeen", "TX", "cms 450152", campus="2201 S Clear Creek Rd")
_ah("AdventHealth Rollins Brook", "Lampasas", "TX", "cms 451323")
_ah("Texas Health Huguley Hospital", "Fort Worth", "TX", "cms 450677", own=("Texas Health Huguley JV",),
    name="Texas Health Huguley Hospital Fort Worth South")
_AH_CAMPUS[(_street_key("11801 South Fwy"), key("Burleson"))] = "Texas Health Huguley Hospital Fort Worth South"
_ah("Texas Health Hospital Mansfield", "Mansfield", "TX", "cms 670309", own=("Texas Health Mansfield JV",),
    campus="2300 Lonestar Rd")
# Great Lakes (UChicago Medicine joint ventures)
_ah("UChicago Medicine AdventHealth Hinsdale", "Hinsdale", "IL", "cms 140122", own=("Hinsdale JV",),
    campus="120 N Oak St")
_ah("UChicago Medicine AdventHealth La Grange", "La Grange", "IL", "cms 140065", own=("La Grange JV",),
    campus="5101 Willow Springs Rd")
_ah("UChicago Medicine AdventHealth GlenOaks", "Glendale Heights", "IL", "cms 140292", own=("GlenOaks JV",),
    campus="701 Winthrop Ave")
_ah("UChicago Medicine AdventHealth Bolingbrook", "Bolingbrook", "IL", "cms 140304", own=("Bolingbrook JV",),
    campus="500 Remington Blvd")
# A named site that is no hospital (its own campus, its own name)
_ah("AdventHealth University", "Orlando", "FL", "board address", own=("AdventHealth University",))

_AH_ER_RX = re.compile(r"^(?:AdventHealth|UChicago Medicine AdventHealth)\s+\S")
# Departments that are no named site (medical groups, home health, support
# companies, offices, networks, internal site codes): logged as known.
_AH_NOT_SITE_RX = re.compile(
    r"medical group|home health|hospice|support|corporate|executive|primary care|sports med|"
    r"centra ?care|imaging|information technology|population health|well65|\blab\b|rural health|"
    r"^ahmg\b|^osed\b|^hfcc\b|pc network|portercare|medical associates|management", re.I)


def adventhealth(job, city="", state=""):
    """(name, city, state) for one AdventHealth search result, or None when
    the row names no site: the department's hospital when the location is
    the hospital's own, a freestanding ER by its department, else None (the
    system name). The posting's own town is kept."""
    if not isinstance(job, dict):
        return None
    dept = _clean(job.get("department"))
    loc = key(job.get("education"))
    if not dept:
        return None
    hit = ADVENTHEALTH_FACILITIES.get(key(dept))
    if hit:
        if loc not in _AH_OWN[key(dept)] and loc != "er":
            return None
        name, hcity, hst, _src = hit
    elif loc == "er" and _AH_ER_RX.match(dept) and not _AH_NOT_SITE_RX.search(dept):
        name, hcity, hst = dept, "", ""
    else:
        return None
    if (city or "").strip():
        return (name, city, state)
    return (name, hcity or city, hst or state)


def adventhealth_unknown(job) -> str:
    """The department of a row that names a site this table does not know:
    a department not in the table, not an ER and not a known non-site."""
    if not isinstance(job, dict):
        return ""
    dept = _clean(job.get("department"))
    if not dept or key(dept) in ADVENTHEALTH_FACILITIES or _AH_NOT_SITE_RX.search(dept):
        return ""
    if key(job.get("education")) == "er" and _AH_ER_RX.match(dept):
        return ""
    return dept


# Stored rows (expired, never re-read): the body's "Address: / City: / State:"
# lines. A posting at a hospital's own street, in its own town, is at that
# hospital, unless its title names a unit that is no hospital (the medical
# group, home health, a clinic or the region's float pool sit on the same
# campuses).
_ADDR_RX = re.compile(r"Address:\s*(?P<a>.{1,120}?)\s*City:\s*(?P<c>.{1,60}?)\s*State:", re.S)
_AH_TITLE_NOT_SITE_RX = re.compile(
    r"home health|hospice|\bAHMG\b|medical group|centra ?care|\bregional\b|\bdivision\b|corporate|"
    r"urgent care|primary care|sports med|imaging center|\bclinic\b|physician practice|\bAHU\b", re.I)


def _ah_stored(row):
    m = _ADDR_RX.search(str(row.get("description") or ""))
    if not m:
        return None
    street, town = _street_key(m.group("a")), key(m.group("c"))
    if _AH_TITLE_NOT_SITE_RX.search(str(row.get("title") or "")):
        return None
    for (cs, ct), name in _AH_CAMPUS.items():
        if ct == town and (street == cs or street.startswith(cs + " ")):
            return (name, row.get("city") or "", row.get("state") or "",
                    f"stored body Address line {m.group('a').strip()[:60]}, {m.group('c').strip()[:30]}")
    return None


# ══════════════════════════════════════════════════════════════════════════
#  Scripps Health: job.business_unit
# ══════════════════════════════════════════════════════════════════════════
SCRIPPS_FACILITIES: dict[str, tuple[str, str, str, str]] = {}


def _sc(unit, name, city, source):
    SCRIPPS_FACILITIES[key(unit)] = (name, city, "CA", source)


_sc("SCRIPPS MEMORIAL HOSP LA JOLLA (300)", "Scripps Memorial Hospital La Jolla", "La Jolla", "cms 050324")
_sc("SCRIPPS GREEN HOSP (310)", "Scripps Green Hospital", "La Jolla", "cms 050424")
_sc("SCRIPPS MEMORIAL HOSP ENC (320)", "Scripps Memorial Hospital Encinitas", "Encinitas", "cms 050503")
_sc("SCRIPPS MERCY HOSP CHULA VISTA (330)", "Scripps Mercy Hospital Chula Vista", "Chula Vista",
    "board address (435 H Street; a campus of cms 050077)")
_sc("SCRIPPS MERCY HOSP SAN DIEGO (350)", "Scripps Mercy Hospital San Diego", "San Diego", "cms 050077")
# Named clinics (Scripps Clinic, Scripps Coastal Medical Center sites)
_sc("SCRIPPS CLIN-TORREY PINES (202)", "Scripps Clinic Torrey Pines", "La Jolla", "board address")
_sc("SCRIPPS CLIN-RANCHO BERNARDO (203)", "Scripps Clinic Rancho Bernardo", "San Diego", "board address")
_sc("SCRIPPS CLIN-DEL MAR (205)", "Scripps Clinic Del Mar", "Del Mar", "board address")
_sc("SCRIPPS CLIN-CARMEL VLLY (207)", "Scripps Clinic Carmel Valley", "San Diego", "board address")
_sc("SCRIPPS CLIN-RANCHO SAN DIEGO (210)", "Scripps Clinic Rancho San Diego", "La Mesa", "board address")
_sc("SCRIPPS CLIN-MISSION VLLY (212)", "Scripps Clinic Mission Valley", "San Diego", "board address")
_sc("SCRIPPS CLIN-SANTEE (223)", "Scripps Clinic Santee", "Santee", "board address")
_sc("SCRIPPS CLIN-LA JOLLA (232)", "Scripps Clinic La Jolla", "La Jolla", "board address")
_sc("SCF-ANDERSON MEDICAL PAVILION (233)", "Scripps Clinic Anderson Medical Pavilion", "La Jolla", "board address")
_sc("SCF-LIBERTY STATION (258)", "Scripps Clinic Liberty Station", "San Diego", "board address")
_sc("SCRIPPS COASTAL MED CNTR-ESTLK (412)", "Scripps Coastal Medical Center Eastlake", "Chula Vista", "board address")
_sc("SCRIPPS COASTAL MED CNTR-HLLCR (419)", "Scripps Coastal Medical Center Hillcrest", "San Diego", "board address")
_sc("SCRIPPS COASTAL MED CNTR-OCEAN (444)", "Scripps Coastal Medical Center Oceanside", "Oceanside", "board address")
_sc("SCMC SOLANA BEACH (481)", "Scripps Coastal Medical Center Solana Beach", "Solana Beach", "board address")

# Units that are no named site (offices, centralized services, the medical
# foundation's administration, the lab, the health plan, a client's on-site
# clinic): logged as known.
_SC_NOT_SITE_RX = re.compile(
    r"corporate|systemwide|centralized|admin|laboratory|health plan|found dev|child care|qualcomm|"
    r"support svcs|executive hlth|integ medicine|ob/?gyn|rehab clinic|jefferson|diabetes", re.I)


def scripps(job, city="", state=""):
    if not isinstance(job, dict):
        return None
    hit = SCRIPPS_FACILITIES.get(key(job.get("business_unit")))
    if not hit:
        return None
    name, hcity, hst, _src = hit
    return (name, city, state) if (city or "").strip() else (name, hcity, hst)


def scripps_unknown(job) -> str:
    if not isinstance(job, dict):
        return ""
    unit = _clean(job.get("business_unit"))
    if not unit or key(unit) in SCRIPPS_FACILITIES or _SC_NOT_SITE_RX.search(unit):
        return ""
    return unit


# ══════════════════════════════════════════════════════════════════════════
#  University Hospitals: job.store_id
# ══════════════════════════════════════════════════════════════════════════
UH_FACILITIES: dict[str, tuple[str, str, str, str]] = {}


def _uh(store, name, city, source):
    UH_FACILITIES[key(store)] = (name, city, "OH", source)


_uh("Cleveland Medical Center", "UH Cleveland Medical Center", "Cleveland", "cms 360137")
_uh("Rainbow Babies & Children's Hospital", "UH Rainbow Babies & Children's Hospital", "Cleveland",
    "board address (11100 Euclid Ave)")
_uh("MacDonald Women's Hospital", "UH MacDonald Women's Hospital", "Cleveland", "board address (11100 Euclid Ave)")
_uh("Seidman Cancer Center", "UH Seidman Cancer Center", "Cleveland", "board address (11100 Euclid Ave)")
_uh("Parma Medical Center", "UH Parma Medical Center", "Parma", "cms 360041")
_uh("Ahuja Medical Center", "UH Ahuja Medical Center", "Beachwood", "cms 360359")
_uh("Geauga Medical Center Campus", "UH Geauga Medical Center", "Chardon", "cms 360075")
_uh("Geauga Medical Center", "UH Geauga Medical Center", "Chardon", "cms 360075")
_uh("Lake West Medical Center", "UH Lake West Medical Center", "Willoughby", "board address (36000 Euclid Ave)")
_uh("Tripoint Medical Center", "UH TriPoint Medical Center", "Concord Township", "cms 360098")
_uh("Beachwood Medical Center", "UH Beachwood Medical Center", "Beachwood", "cms 360367")
_uh("Elyria Medical Center", "UH Elyria Medical Center", "Elyria", "cms 360145")
_uh("St. John Medical Center", "UH St. John Medical Center", "Westlake", "cms 360123")
_uh("Portage Medical Center", "UH Portage Medical Center", "Ravenna", "cms 360078")
_uh("Geneva Medical Center", "UH Geneva Medical Center", "Geneva", "cms 361307")
_uh("Samaritan Medical Center", "UH Samaritan Medical Center", "Ashland", "cms 360002")
_uh("Conneaut Medical Center", "UH Conneaut Medical Center", "Conneaut", "board address (158 W Main Rd)")

# Stores that are no named site: the system, the medical practices and their
# management company, home care, the pharmacy company, a bare place
# ("Cleveland - Ohio - United States").
_UH_NOT_SITE_RX = re.compile(r"^university hospitals$|practices|home care|uh meds|primary_care|united states", re.I)


def university_hospitals(job, city="", state=""):
    if not isinstance(job, dict):
        return None
    hit = UH_FACILITIES.get(key(job.get("store_id")))
    if not hit:
        return None
    name, hcity, hst, _src = hit
    return (name, city, state) if (city or "").strip() else (name, hcity, hst)


def university_hospitals_unknown(job) -> str:
    if not isinstance(job, dict):
        return ""
    store = _clean(job.get("store_id"))
    if not store or key(store) in UH_FACILITIES or _UH_NOT_SITE_RX.search(store):
        return ""
    return store


# ══════════════════════════════════════════════════════════════════════════
#  UCLA Health: job.primary_address (a hospital's own street in its own town)
# ══════════════════════════════════════════════════════════════════════════
# (street key: house number + street name, suffix and tags dropped, town key)
UCLA_FACILITIES: dict[tuple[str, str], tuple[str, str, str, str]] = {}
_UCLA_DROP = {"st", "street", "dr", "drive", "pl", "plaza", "ave", "avenue", "blvd", "rd", "road",
              "way", "ucla", "rrumc", "rruclamc"}


def _ucla_street(addr) -> str:
    """"757 westwood" from "757 Westwood Plaza, City: Los Angeles, ...",
    "RRUMC 757 Westwood Pl", "RRUCLAMC, 757 Westwood Plaza"; "150 medical"
    from "150 Medical Plaza (RRUMC)" and "150 Medical Plaza-Rm-4230C"."""
    s = re.split(r",\s*City\s*:", str(addr or ""), maxsplit=1, flags=re.I)[0]
    s = re.sub(r"\([^)]*\)", " ", s)                    # "(RRUMC)"
    m = re.search(r"\b(\d+)\s+([^,#]+)", s)             # the house number and its street
    if not m:
        return ""
    street = re.split(r"-\s*rm\b|\bsuite\b|\bste\b|\broom\b", m.group(2), maxsplit=1, flags=re.I)[0]
    words = [w for w in re.sub(r"[^a-z0-9 ]", " ", street.lower()).split() if w not in _UCLA_DROP]
    return " ".join([m.group(1)] + words)


def _ucla(street, city, name, source):
    UCLA_FACILITIES[(_ucla_street(street), key(city))] = (name, city, "CA", source)


_ucla("757 Westwood Plaza", "Los Angeles", "Ronald Reagan UCLA Medical Center", "cms 050262")
_ucla("1250 16th Street", "Santa Monica", "UCLA Santa Monica Medical Center", "cms 050112")
_ucla("7300 Medical Center Drive", "West Hills", "UCLA West Valley Medical Center", "cms 050481")
_ucla("150 UCLA Medical Plaza", "Los Angeles", "Resnick Neuropsychiatric Hospital at UCLA", "cms 054009")


def ucla(job, city="", state=""):
    if not isinstance(job, dict):
        return None
    town = city or ""
    hit = UCLA_FACILITIES.get((_ucla_street(job.get("primary_address")), key(town)))
    if not hit:
        return None
    name, hcity, hst, _src = hit
    return (name, city, state) if town.strip() else (name, hcity, hst)


def ucla_unknown(job) -> str:
    return ""        # an address names a site only when it is a hospital's own street


# ══════════════════════════════════════════════════════════════════════════
#  UPMC: business_unit refined by brand (facility_names.upmc_facility,
#  2026-10-15); owner 2026-10-29 ("go with recs"): when neither names a
#  hospital, the one hospital the title names (facility_names.
#  upmc_title_facility: exactly one, in the posting's own town or at the
#  head office default / no town)
# ══════════════════════════════════════════════════════════════════════════
def upmc(job, city="", state=""):
    import facility_names
    if not isinstance(job, dict):
        return None
    name = facility_names.upmc_facility(job.get("brand"), job.get("business_unit"), city, state)
    if not name:
        name = facility_names.upmc_title_facility(job.get("title"), city, state)
    return (name, city, state) if name else None


# ══════════════════════════════════════════════════════════════════════════
HOOKS = {
    UPMC: upmc,
    ADVENTHEALTH: adventhealth,
    SCRIPPS: scripps,
    UH: university_hospitals,
    UCLA: ucla,
}
_UNKNOWN = {
    ADVENTHEALTH: adventhealth_unknown,
    SCRIPPS: scripps_unknown,
    UH: university_hospitals_unknown,
    UCLA: ucla_unknown,
}


def facility(system, job, city="", state=""):
    fn = HOOKS.get(system)
    return fn(job, city, state) if fn else None


def unknown(system, job) -> str:
    fn = _UNKNOWN.get(system)
    return fn(job) if fn else ""


def stored_facility(system, row):
    """(name, city, state, source) for a stored row, or None. Read only from
    what hospital_jobs stores (description, title, city, state). Only
    AdventHealth's expired rows need it today: its feed's department is not
    stored, but its body carries the street address."""
    if not isinstance(row, dict):
        return None
    if system == ADVENTHEALTH:
        return _ah_stored(row)
    return None
