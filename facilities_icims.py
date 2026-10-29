"""One clean hospital name per posting on the iCIMS and Jibe boards (2026-10-29).

The owner, 2026-10-29: every hospital inside a multi-hospital system is named
on its own postings (cards and job pages say the HOSPITAL; the system name and
logo stay on the job page and the hub), and applies are filed under the
hospital they were for. Named sites (hospitals, named clinics and centers)
show their own name; a generic office, a home-based or a remote job shows the
system name with its town; a town is never guessed.

The facility scan of 2026-10-29 (facility_scale_2026-10-29/scan/SCAN.txt)
found 14 Jibe boards and 5 classic iCIMS portals whose rows all said the
system's own name. Where each one names the site:

Jibe (/api/jobs list rows, read 2026-10-29, one page of 100 per board):
  Fairview Health      tags9 = the site's own name ("Fairview Southdale
                       Hospital", "Fairview Wyoming Clinic", "Willows Bend");
                       location_name is an abbreviation ("FV Southdale
                       Hospital")
  MedStar Health       location_name, abbreviated ("MedStar Washington Hosp
                       Ctr", "MSMHC-Hospital"); tags2 = the employing entity
                       ("MedStar Franklin Square Medical Center", "MedStar
                       Medical Group"; the board's facet lists all 21)
  MyMichigan Health    location_name ("Medical Center Midland")
  WakeMed              location_name ("WakeMed Cary Hospital", "WakeMed
                       Raleigh Campus", "MyCare 365 - Garner")
  Piedmont Healthcare  location_name = the street ("1255 Highway 54 West" is
                       Piedmont Fayette Hospital, CMS 110215's own address)
  Yale New Haven       tags7 = the street with its town ("267 Grant St,
                       Bridgeport, CT 06610" is Bridgeport Hospital);
                       location_name is a zip code ("CT06510")
  OSF HealthCare       location_name ("OSF St.Francis Med Center"; the facet
                       of tags1, the same value, lists all 183 sites)
  Carle Health         location_name ("BroMenn Medical Center")
  FirstHealth          location_name ("FirstHealth Moore Regional Hospital -
                       Richmond")
  Tanner Health        location_name with a site code ("Tanner Hospital
                       Villa Rica-V0001")
  USA Health           location_name ("University Hospital") and tags1, the
                       entity ("USA Health Providence Hospital")
  UCI Health           location_name = building and floor ("Irvine Hospital -
                       4th Floor", "Building 1 (MC)-3rd Fl") with
                       street_address ("101 The City Drive")
  Infirmary Health     location_name ("Mobile Infirmary", "Thomas Hospital")
  One Brooklyn Health  location_name ("The Brookdale Hospital Medical Center")

Classic iCIMS portals (their list and job pages answer this machine with the
AWS WAF CAPTCHA since 10-26, so only the stored text was read):
  Kettering Health     the body's first line, "Job Details" then "<site> |
                       <town> | <type> | <shift>" ("Kettering Health Soin |
                       Beavercreek | Full-Time | First Shift"); every row of
                       the portal sits at Kettering, OH, the list's one town
  Emory Healthcare     the body's "Location:" line ("Location: EMORY
                       MIDTOWN HOSPITAL") or the title ("RN Charge Nurse,
                       CVICU, Emory University Hospital, 7pm-7:30am")
  Huntsville Hospital  the title ("RN Staff, Marshall Medical Centers North,
                       ICU", "Imaging Technologist - Lincoln Medical Center")
  Northside Hospital   the title ("RN, PACU, Northside Gwinnett"); 950 of its
                       964 rows sit at Atlanta, GA
  Northern Arizona     the title ("RN CVICU - Flagstaff Medical Center")

Every value goes through one table per system: the employer's spelling (or
street) -> (one clean public name, city, state, source of the town). The
names are the hospitals' public names (the CMS hospital list's, written in
the employer's own style); the towns are the hospital's own street address:
"cms <id>" (the database's CMS hospital list), "address" (the published
street address), "body" (the postings' own "<site> | <town>" line).

  jibe_facility(system, j)   (hospital_name, how) for one /api/jobs row of a
                             JIBE_SYSTEMS board: the table's hospital, the
                             site's own name where the board's value is one
                             (KEEP systems), else the system label ("office",
                             "remote", "unknown")
  jibe_town(system, name, city, state)
                             the hospital's town for a row the feed gave none
  classic_facility(system, title, body, city, state)
                             (hospital_name, city, state, how) or None for one
                             classic portal row
  stored_facility(system, row)
                             the same from a stored row (title, description,
                             location, city, state; nothing the database does
                             not hold), for the change list of stored rows

hospital_system is never touched (the upsert key, the hub, the filters and
the logo).
"""
from __future__ import annotations

import re
from typing import Callable, Optional

FacilityHit = tuple  # (name, city, state, source)


def key(s) -> str:
    """Lookup key: lower case, "&" -> "and", "saint" -> "st", apostrophes and
    every other non-alphanumeric dropped."""
    s = str(s or "").lower().replace("’", "'").replace("&", " and ")
    s = re.sub(r"\bsaint\b", "st", s)
    s = re.sub(r"\bst\.", "st", s)
    return re.sub(r"[^a-z0-9]", "", s)


def clean(s) -> str:
    s = str(s or "").replace("’", "'").replace("–", "-").replace("—", "-")
    return re.sub(r"\s+", " ", s).strip()


def _table(rows) -> dict:
    """{key(spelling): (name, city, state, source)} from (spellings, name,
    city, state, source) rows; a spelling of "" is the name itself."""
    out = {}
    for spellings, name, city, state, source in rows:
        if isinstance(spellings, str):
            spellings = (spellings,)
        for sp in tuple(spellings) + (name,):
            out[key(sp)] = (name, city, state, source)
    return out


# A site value that is one of the system's offices, a remote or home-based
# post, a building or a suite (owner, 10-29: the system name with its town).
OFFICE_RX = re.compile(
    r"\b(?:corporate|corp|headquarters|hdqtrs|hq|offices?|remote|systemwide|system\s+location|"
    r"operations\s+center|corporate\s+center|administrative|admin\s+center|revenue\s+cycle|"
    r"work\s+from\s+home|home[-\s]based|telecommut\w*|virtual|multiple\s+locations|various\s+locations|"
    r"building|bldg|annex|atrium|suite|ste|warehouse|distribution\s+center|service\s+center|"
    r"data\s+center|resource\s+center|information\s+tech\w*|sys)\b", re.I)


def _is_office(v: str) -> bool:
    v = clean(v)
    return not v or bool(OFFICE_RX.search(v)) or bool(re.match(r"^\d", v))


def _first(j: dict, field: str) -> str:
    v = (j or {}).get(field)
    if isinstance(v, (list, tuple)):
        v = next((str(x) for x in v if str(x or "").strip()), "")
    return clean(v)


# ══════════════════════════════════════════════════════════════════════════
#  JIBE BOARDS
# ══════════════════════════════════════════════════════════════════════════

# ── Fairview Health (tags9 is the site's own name) ─────────────────────────
FAIRVIEW = "Fairview Health"
FAIRVIEW_HOSPITALS = _table([
    ((), "Fairview Southdale Hospital", "Edina", "MN", "cms 240078"),
    # the pharmacy inside the hospital (location_name "FV Southdale Hospital")
    (("Fairview Pharmacy-Edina, Fairview Southdale Hospital",), "Fairview Southdale Hospital", "Edina", "MN",
     "cms 240078"),
    ((), "Fairview Woodwinds Hospital", "Woodbury", "MN", "cms 240213"),
    (("Fairview St John's Hospital", "Fairview St. John's Hospital"), "Fairview St. John's Hospital",
     "Maplewood", "MN", "cms 240210"),
    ((), "Fairview Ridges Hospital", "Burnsville", "MN", "cms 240207"),
    (("Fairview Lakes Medical Center",), "Fairview Lakes Hospital", "Wyoming", "MN", "cms 240050"),
    (("Fairview Northland Medical Center",), "Fairview Northland Hospital", "Princeton", "MN", "cms 240141"),
    # the University campuses (CMS 240080, 2450 Riverside Ave, Minneapolis)
    ((), "Fairview East Bank University Hospital", "Minneapolis", "MN", "cms 240080"),
    ((), "Fairview Riverside University Hospital", "Minneapolis", "MN", "cms 240080"),
    # the West Bank's West Building: location_name "UMMC West Bank ..." on
    # the same postings as "Fairview Riverside University Hospital"
    (("University of Minnesota Medical Center-West Bank, West Building",
      "University of Minnesota Medical Center - West Bank, West Building",
      "University of Minnesota Medical Center-West Bank"),
     "Fairview Riverside University Hospital", "Minneapolis", "MN", "cms 240080"),
    ((), "Fairview Masonic Children's University Hospital", "Minneapolis", "MN", "cms 240080"),
    (("Grand Itasca Clinic and Hospital",), "Grand Itasca Clinic & Hospital", "Grand Rapids", "MN", "cms 240064"),
    ((), "Fairview Bethesda Hospital", "St Paul", "MN", "address 559 Capitol Blvd, St Paul (the feed's town)"),
    (("Fairview Range Hospital",), "Fairview Range Medical Center", "Hibbing", "MN", "cms 240040"),
])
# location_name, used only on a row whose tags9 is blank
FAIRVIEW_LOCATION = _table([
    (("FV Southdale Hospital",), "Fairview Southdale Hospital", "Edina", "MN", "cms 240078"),
    (("HealthEast -Woodwinds Hospital", "HealthEast - Woodwinds Hospital"), "Fairview Woodwinds Hospital",
     "Woodbury", "MN", "cms 240213"),
    (("FV St. John's Hospital",), "Fairview St. John's Hospital", "Maplewood", "MN", "cms 240210"),
    (("FV Ridges Hospital",), "Fairview Ridges Hospital", "Burnsville", "MN", "cms 240207"),
    (("Lakes Medical Center",), "Fairview Lakes Hospital", "Wyoming", "MN", "cms 240050"),
    (("FV Northland Medical Center",), "Fairview Northland Hospital", "Princeton", "MN", "cms 240141"),
    (("UMMC Hospital Unit J",), "Fairview East Bank University Hospital", "Minneapolis", "MN", "cms 240080"),
    (("U of M Masonic Children's Hosp",), "Fairview Masonic Children's University Hospital", "Minneapolis", "MN",
     "cms 240080"),
])


def fairview_facility(j: dict) -> tuple[str, str]:
    site = _first(j, "tags9")
    if site:
        hit = FAIRVIEW_HOSPITALS.get(key(site))
        if hit:
            return hit[0], "table"
        if _is_office(site):
            return FAIRVIEW, "office"
        return site, "site"          # a clinic, a center, an Ebenezer residence: its own name
    hit = FAIRVIEW_LOCATION.get(key(_first(j, "location_name")))
    return (hit[0], "table") if hit else (FAIRVIEW, "unknown")


# ── MedStar Health (location_name abbreviated; tags2 the entity) ───────────
MEDSTAR = "MedStar Health"
MEDSTAR_HOSPITALS = _table([
    (("MedStar Washington Hosp Ctr", "MedStar Washington Cancer Inst"), "MedStar Washington Hospital Center",
     "Washington", "DC", "cms 090011"),
    (("MedStar Georgetown Univ Hosp",), "MedStar Georgetown University Hospital", "Washington", "DC", "cms 090004"),
    (("MedStar Franklin Square Hosp",), "MedStar Franklin Square Medical Center", "Baltimore", "MD", "cms 210015"),
    (("MedStar Good Samaritan Hosp",), "MedStar Good Samaritan Hospital", "Baltimore", "MD", "cms 210056"),
    (("MedStar Harbor Hosp",), "MedStar Harbor Hospital", "Baltimore", "MD", "cms 210034"),
    (("MedStar Union Memorial Hosp",), "MedStar Union Memorial Hospital", "Baltimore", "MD", "cms 210024"),
    (("MedStar Montgomery Medical Ctr",), "MedStar Montgomery Medical Center", "Olney", "MD", "cms 210018"),
    (("MedStar St. Mary's Hosp", "MedStar Saint Mary's Hospital"), "MedStar St. Mary's Hospital", "Leonardtown",
     "MD", "cms 210028"),
    (("MSMHC-Hospital", "MSMHC - Hospital"), "MedStar Southern Maryland Hospital Center", "Clinton", "MD",
     "cms 210062"),
    (("MedStar National Rehab Hosp",), "MedStar National Rehabilitation Hospital", "Washington", "DC",
     "address 102 Irving St NW, Washington"),
])
MEDSTAR_URGENT_RX = re.compile(r"^Urgent\s+Care\s*[-–]?\s*([A-Z][A-Za-z .'-]{2,26})$")


def medstar_facility(j: dict) -> tuple[str, str]:
    loc = _first(j, "location_name")
    hit = MEDSTAR_HOSPITALS.get(key(loc))
    if hit:
        return hit[0], "table"
    entity = _first(j, "tags2")
    hit = MEDSTAR_HOSPITALS.get(key(entity))
    if hit:
        return hit[0], "entity"      # the hospital employs the post (an offsite department)
    m = MEDSTAR_URGENT_RX.match(loc)
    if m and key(entity) == key("MedStar Health Urgent Care"):
        return f"MedStar Health Urgent Care - {m.group(1).strip()}", "site"
    return MEDSTAR, ("office" if _is_office(loc) else "unknown")


# ── MyMichigan Health ("Medical Center <town>") ────────────────────────────
MYMICHIGAN = "MyMichigan Health"
_MYMI = [
    ("Midland", "Midland", "cms 230222"), ("Saginaw", "Saginaw", "cms 230077 (Ascension St. Mary's until 2024)"),
    ("Alma", "Alma", "cms 230030"), ("Alpena", "Alpena", "cms 230036"), ("Clare", "Clare", "cms 230180"),
    ("Gladwin", "Gladwin", "cms 231325"), ("West Branch", "West Branch", "cms 230095"),
    ("Mount Pleasant", "Mount Pleasant", "address 4851 E Pickard St, Mount Pleasant"),
    ("Sault", "Sault Ste. Marie", "cms 230239 (War Memorial until 2023)"),
    ("Standish", "Standish", "cms 231305 (Ascension Standish until 2024)"),
    ("Tawas", "Tawas City", "cms 230100 (Ascension St. Joseph until 2024)"),
]
MYMICHIGAN_HOSPITALS = _table(
    [((f"Medical Center {short}", f"Med Center {short}", f"MyMichigan Medical Center - {short}")
      + (("Medical Center Mt. Pleasant", "Medical Center Mt Pleasant") if short == "Mount Pleasant" else ()),
      f"MyMichigan Medical Center {short}", town, "MI", src) for short, town, src in _MYMI])


def mymichigan_facility(j: dict) -> tuple[str, str]:
    loc = _first(j, "location_name")
    hit = MYMICHIGAN_HOSPITALS.get(key(loc))
    if hit:
        return hit[0], "table"
    return MYMICHIGAN, ("office" if _is_office(loc) else "unknown")


# ── WakeMed (location_name is the site's own name) ─────────────────────────
WAKEMED = "WakeMed"
WAKEMED_HOSPITALS = _table([
    (("Physical Therapy & Rehab - Raleigh Campus",), "WakeMed Raleigh Campus", "Raleigh", "NC", "cms 340069"),
    ((), "WakeMed Cary Hospital", "Cary", "NC", "cms 340173"),
    ((), "WakeMed North Hospital", "Raleigh", "NC", "address 10000 Falls of Neuse Rd, Raleigh"),
])
WAKEMED_OFFICE_RX = re.compile(r"^WPP\b|\bCorporate\b|\bOperations Center\b", re.I)


def wakemed_facility(j: dict) -> tuple[str, str]:
    loc = _first(j, "location_name")
    hit = WAKEMED_HOSPITALS.get(key(loc))
    if hit:
        return hit[0], "table"
    if _is_office(loc) or WAKEMED_OFFICE_RX.search(loc):
        return WAKEMED, "office"
    return loc, "site"               # a Healthplex, a MyCare 365, a medical park


# ── Piedmont Healthcare (location_name is the street) ──────────────────────
PIEDMONT = "Piedmont Healthcare"
_STREET_WORDS = {"drive": "dr", "avenue": "ave", "av": "ave", "road": "rd", "street": "st", "boulevard": "blvd",
                 "parkway": "pkwy", "highway": "hwy", "court": "ct", "place": "pl", "lane": "ln", "way": "way",
                 "north": "n", "south": "s", "east": "e", "west": "w", "northeast": "ne", "northwest": "nw",
                 "southeast": "se", "southwest": "sw", "suite": "ste"}


def street_key(street: str) -> str:
    """"1255 Highway 54 West" -> "1255 hwy 54 w"; a suite, a building or a
    second line is dropped."""
    s = clean(street).lower().split("\n")[0]
    s = re.split(r",|\s(?:ste|suite|bldg|building|unit|#)\b", s)[0]
    words = [w for w in re.sub(r"[^a-z0-9 ]", " ", s).split() if w]
    return " ".join(_STREET_WORDS.get(w, w) for w in words)


def _street_table(rows) -> dict:
    return {(street_key(st), key(town)): (name, town, state, src) for st, town, name, state, src in rows}


PIEDMONT_STREETS = _street_table([
    ("1968 Peachtree Rd NW", "Atlanta", "Piedmont Atlanta Hospital", "GA", "cms 110083"),
    ("1199 Prince Avenue", "Athens", "Piedmont Athens Regional Medical Center", "GA", "cms 110074"),
    ("1350 Walton Way", "Augusta", "Piedmont Augusta Hospital", "GA", "cms 110028"),
    ("960 Joe Frank Harris Parkway", "Cartersville", "Piedmont Cartersville Medical Center", "GA", "cms 110030"),
    ("710 Center Street", "Columbus", "Piedmont Columbus Regional Midtown", "GA", "cms 110064"),
    ("100 Frist Court", "Columbus", "Piedmont Columbus Regional Northside", "GA", "cms 110200"),
    ("1412 Milstead Avenue", "Conyers", "Piedmont Rockdale Hospital", "GA", "cms 110091"),
    ("5126 Hospital Drive", "Covington", "Piedmont Newton Hospital", "GA", "cms 110018"),
    ("1255 Highway 54 West", "Fayetteville", "Piedmont Fayette Hospital", "GA", "cms 110215"),
    ("1266 Highway 515 South", "Jasper", "Piedmont Mountainside Hospital", "GA", "cms 110225"),
    ("400 Charter Boulevard", "Macon", "Piedmont Macon North Hospital", "GA", "cms 110201"),
    # CMS still lists 350 Hospital Drive as Coliseum Medical Centers (dba)
    ("350 Hospital Drive", "Macon", "Piedmont Macon Medical Center", "GA", "cms 110164"),
    ("2151 W Spring Street", "Monroe", "Piedmont Walton Hospital", "GA", "cms 110046"),
    ("745 Poplar Road", "Newnan", "Piedmont Newnan Hospital", "GA", "cms 110229"),
    ("1700 Medical Way", "Snellville", "Piedmont Eastside Medical Center", "GA", "cms 110192"),
    ("1133 Eagle's Landing Parkway", "Stockbridge", "Piedmont Henry Hospital", "GA", "cms 110191"),
    # CMS still lists 2460 Washington Road as University McDuffie County Regional
    ("2460 Washington Road", "Thomson", "Piedmont McDuffie Hospital", "GA", "cms 110111"),
])
# the Atlanta campus's buildings ("PH Campus 1984 Bldg" = 1984 Peachtree Rd)
PIEDMONT_CAMPUS_RX = re.compile(r"^PH\s+Campus\b", re.I)


def piedmont_facility(j: dict) -> tuple[str, str]:
    loc = _first(j, "location_name") or _first(j, "street_address")
    city = _first(j, "city")
    hit = PIEDMONT_STREETS.get((street_key(loc), key(city)))
    if not hit and _first(j, "street_address"):
        hit = PIEDMONT_STREETS.get((street_key(_first(j, "street_address")), key(city)))
    if hit:
        return hit[0], "street"
    if PIEDMONT_CAMPUS_RX.match(loc) and key(city) == key("Atlanta"):
        return "Piedmont Atlanta Hospital", "campus"
    return PIEDMONT, ("office" if re.search(r"remote", loc, re.I) else "unknown")


# ── Yale New Haven Health (tags7 is the street with its town) ──────────────
YNHHS = "Yale New Haven Health System"
# (house number, first street word, town) -> hospital
YNHHS_STREETS = {
    ("20", "york", "newhaven"): ("Yale New Haven Hospital", "New Haven", "CT", "cms 070022"),
    ("1450", "chapel", "newhaven"): ("Yale New Haven Hospital - Saint Raphael Campus", "New Haven", "CT",
                                     "address 1450 Chapel St, New Haven"),
    ("1", "park", "newhaven"): ("Yale New Haven Children's Hospital", "New Haven", "CT",
                                "address 1 Park St, New Haven"),
    ("35", "park", "newhaven"): ("Smilow Cancer Hospital", "New Haven", "CT", "address 35 Park St, New Haven"),
    ("184", "liberty", "newhaven"): ("Yale New Haven Psychiatric Hospital", "New Haven", "CT",
                                     "address 184 Liberty St, New Haven"),
    ("267", "grant", "bridgeport"): ("Bridgeport Hospital", "Bridgeport", "CT", "cms 070010"),
    ("300", "seaside", "milford"): ("Bridgeport Hospital - Milford Campus", "Milford", "CT",
                                    "address 300 Seaside Ave, Milford"),
    ("5", "perryridge", "greenwich"): ("Greenwich Hospital", "Greenwich", "CT", "cms 070018"),
    ("365", "montauk", "newlondon"): ("Lawrence + Memorial Hospital", "New London", "CT", "cms 070007"),
    ("25", "wells", "westerly"): ("Westerly Hospital", "Westerly", "RI", "cms 410013"),
}
_YNHHS_STREET_RX = re.compile(r"^\s*(\d+[A-Za-z]?)\s+([A-Za-z]+)")


def ynhhs_street_hit(street: str, city: str = "") -> Optional[FacilityHit]:
    """The hospital at one "<number> <street>, <town>, CT <zip>" value: the
    house number and the street's first word, in the table's town (named in
    the value itself or the row's city)."""
    m = _YNHHS_STREET_RX.match(clean(street))
    if not m:
        return None
    num, word = m.group(1).lower(), m.group(2).lower()
    low = key(street)
    for (n, w, town), hit in YNHHS_STREETS.items():
        if n == num and w == word and (town in low or town == key(city)):
            return hit
    return None


def ynhhs_facility(j: dict) -> tuple[str, str]:
    hit = ynhhs_street_hit(_first(j, "tags7"), _first(j, "city"))
    if hit:
        return hit[0], "street"
    return YNHHS, "unknown"


# ── OSF HealthCare (location_name, abbreviated) ────────────────────────────
OSF = "OSF HealthCare"
OSF_HOSPITALS = _table([
    (("OSF St.Francis Med Center", "OSF St. Francis Med Center"), "OSF Saint Francis Medical Center", "Peoria", "IL",
     "address 530 NE Glen Oak Ave, Peoria"),
    (("OSF Saint Anthony Med Ctr",), "OSF Saint Anthony Medical Center", "Rockford", "IL",
     "address 5666 E State St, Rockford"),
    (("OSF St. Joseph Med Center",), "OSF Saint Joseph Medical Center", "Bloomington", "IL",
     "address 2200 E Washington St, Bloomington"),
    (("OSF St. Mary Med Ctr",), "OSF Saint Mary Medical Center", "Galesburg", "IL",
     "address 3333 N Seminary St, Galesburg"),
    (("OSF Little Co of Mary Med",), "OSF Little Company of Mary Medical Center", "Evergreen Park", "IL",
     "cms 140179"),
    (("OSF St Katharine Med Ctr",), "OSF Saint Katharine Medical Center", "Dixon", "IL", "cms 140012"),
    (("Sacred Heart Med Center",), "OSF Sacred Heart Medical Center", "Danville", "IL", "cms 140093"),
    (("OSF Saint Elizabeth Md Ct",), "OSF Saint Elizabeth Medical Center", "Ottawa", "IL", "cms 140110"),
    (("OSF St.James-JWAlbrechtMC",), "OSF Saint James - John W. Albrecht Medical Center", "Pontiac", "IL",
     "address 2500 W Reynolds St, Pontiac"),
    (("OSF Saint Anthony HlthCtr",), "OSF Saint Anthony's Health Center", "Alton", "IL", "cms 140052"),
    (("Saint Luke Medical Center",), "OSF Saint Luke Medical Center", "Kewanee", "IL", "cms 141325"),
    (("OSF Holy Family Med Ctr",), "OSF Holy Family Medical Center", "Monmouth", "IL", "cms 141318"),
    (("OSF Saint Paul Med Center",), "OSF Saint Paul Medical Center", "Mendota", "IL", "cms 141310"),
    (("OSF Saint Clare Med Ctr",), "OSF Saint Clare Medical Center", "Princeton", "IL", "cms 141337"),
    (("OSF St. Francis Hospital",), "OSF St. Francis Hospital", "Escanaba", "MI", "cms 231337"),
    (("OSF Heart of Mary Med Ctr",), "OSF Heart of Mary Medical Center", "Urbana", "IL", "cms 140113"),
])


def osf_facility(j: dict) -> tuple[str, str]:
    loc = _first(j, "location_name") or _first(j, "tags1")
    hit = OSF_HOSPITALS.get(key(loc))
    if hit:
        return hit[0], "table"
    return OSF, ("office" if _is_office(loc) else "unknown")


# ── Carle Health (location_name is the site's own name) ────────────────────
CARLE = "Carle Health"
CARLE_HOSPITALS = _table([
    ((), "Carle Foundation Hospital", "Urbana", "IL", "cms 140091"),
    (("BroMenn Medical Center",), "Carle BroMenn Medical Center", "Normal", "IL", "cms 140127"),
    (("Methodist Medical Center",), "Carle Health Methodist Hospital", "Peoria", "IL", "cms 140209"),
    (("Proctor Hospital",), "Carle Health Proctor Hospital", "Peoria", "IL", "cms 140013"),
    (("Pekin Hospital",), "Carle Health Pekin Hospital", "Pekin", "IL", "cms 140120"),
    (("Hoopeston Regional Health Ctr", "Hoopeston Regional Health Center"), "Carle Hoopeston Regional Health Center",
     "Hoopeston", "IL", "cms 141316"),
    (("Richland Memorial Hospital",), "Carle Richland Memorial Hospital", "Olney", "IL", "cms 141355"),
    (("Eureka Hospital",), "Carle Eureka Hospital", "Eureka", "IL", "cms 141309"),
])


def carle_facility(j: dict) -> tuple[str, str]:
    loc = _first(j, "location_name")
    hit = CARLE_HOSPITALS.get(key(loc))
    if hit:
        return hit[0], "table"
    if _is_office(loc):
        return CARLE, "office"
    return loc, "site"


# ── FirstHealth of the Carolinas ───────────────────────────────────────────
FIRSTHEALTH = "FirstHealth of the Carolinas"
FIRSTHEALTH_HOSPITALS = _table([
    (("FirstHealth Moore Regional Hospital - Pinehurst",), "FirstHealth Moore Regional Hospital", "Pinehurst", "NC",
     "cms 340115"),
    (("FirstHealth Moore Regional Hospital- Richmond", "FirstHealth Moore Regional Hospital -Richmond"),
     "FirstHealth Moore Regional Hospital - Richmond", "Rockingham", "NC", "address 925 Long Dr, Rockingham"),
    (("FirstHealth Moore Regional Hospital- Hoke", "Hoke Campus"), "FirstHealth Moore Regional Hospital - Hoke",
     "Raeford", "NC", "address 6408 Fayetteville Rd, Raeford"),
    ((), "FirstHealth Montgomery Memorial Hospital", "Troy", "NC", "cms 341303"),
])
# values that name a kind of place, not a place
GENERIC_SITES = frozenset(key(v) for v in (
    "OB/GYN Clinic", "Child Development Center", "Emergency Medical Services", "Physician Office",
    "Patient Access", "Support Services", "System Services", "Central Scheduling", "Home Care", "Home Health"))


def firsthealth_facility(j: dict) -> tuple[str, str]:
    loc = _first(j, "location_name")
    hit = FIRSTHEALTH_HOSPITALS.get(key(loc))
    if hit:
        return hit[0], "table"
    if _is_office(loc) or key(loc) in GENERIC_SITES:
        return FIRSTHEALTH, "office"
    return loc, "site"


# ── Tanner Health (location_name with a site code) ─────────────────────────
TANNER = "Tanner Health"
TANNER_CODE_RX = re.compile(r"\s*-\s*[A-Z]\d{4}\s*$")
TANNER_HOSPITALS = _table([
    ((), "Tanner Hospital Carrollton", "Carrollton", "GA", "cms 110011"),
    ((), "Tanner Hospital Villa Rica", "Villa Rica", "GA", "cms 110015"),
    ((), "Tanner Hospital Bremen", "Bremen", "GA", "address 200 Allen Memorial Dr, Bremen"),
    ((), "Tanner Hospital East Alabama", "Wedowee", "AL", "cms 011306"),
    (("Willowbrooke Hospital",), "Willowbrooke at Tanner", "Villa Rica", "GA", "address 20 Herrell Rd, Villa Rica"),
])


def tanner_facility(j: dict) -> tuple[str, str]:
    loc = TANNER_CODE_RX.sub("", _first(j, "location_name"))
    hit = TANNER_HOSPITALS.get(key(loc))
    if hit:
        return hit[0], "table"
    if loc.lower().startswith("willowbrooke"):
        return "Willowbrooke at Tanner", "table"
    if _is_office(loc) or key(loc) in GENERIC_SITES:
        return TANNER, "office"
    return loc, "site"


# ── USA Health (location_name, then the entity in tags1) ───────────────────
USA_HEALTH = "USA Health"
USA_HOSPITALS = _table([
    (("University Hospital", "Mastin Building - University Hospital Campus"), "USA Health University Hospital",
     "Mobile", "AL", "cms 010087"),
    ((), "USA Health Providence Hospital", "Mobile", "AL", "cms 010090"),
    (("Children's and Women's Hospital", "USA Health Children's and Women's Hospital"),
     "USA Health Children's & Women's Hospital", "Mobile", "AL", "cms 013301"),
    (("Mitchell Cancer Institute",), "USA Health Mitchell Cancer Institute", "Mobile", "AL",
     "address 1660 Springhill Ave, Mobile"),
])


def usa_health_facility(j: dict) -> tuple[str, str]:
    loc = _first(j, "location_name")
    hit = USA_HOSPITALS.get(key(loc))
    if hit:
        return hit[0], "table"
    hit = USA_HOSPITALS.get(key(_first(j, "tags1")))
    if hit and hit[0] != "USA Health Mitchell Cancer Institute":
        return hit[0], "entity"
    if hit and key(loc).startswith(key("Mitchell Cancer Institute")):
        return hit[0], "entity"
    return USA_HEALTH, ("office" if _is_office(loc) else "unknown")


# ── UCI Health (building and floor, with the street) ───────────────────────
UCI = "UCI Health"
UCI_RULES = [
    (re.compile(r"^Irvine Hospital\b", re.I), "", ("UCI Health - Irvine", "Irvine", "CA", "board")),
    (re.compile(r"\(MC\b", re.I), "101 the city dr", ("UCI Medical Center", "Orange", "CA", "cms 050348")),
    (re.compile(r"^Fountain Valley (?:Med\b|Reg)", re.I), "",
     ("UCI Health - Fountain Valley", "Fountain Valley", "CA", "cms 050570")),
    (re.compile(r"^Los Alamitos Med(?:ical)? C(?:tr|enter)\b", re.I), "",
     ("UCI Health - Los Alamitos", "Los Alamitos", "CA", "cms 050551")),
    (re.compile(r"^Lakewood Medical C(?:tr|enter)\b", re.I), "", ("UCI Health - Lakewood", "Lakewood", "CA", "cms 050581")),
    (re.compile(r"^Placentia[- ]Linda Hospital\b", re.I), "",
     ("UCI Health - Placentia Linda", "Placentia", "CA", "cms 050589")),
]


def uci_facility(j: dict) -> tuple[str, str]:
    loc = _first(j, "location_name")
    street = street_key(_first(j, "street_address"))
    for rx, need_street, hit in UCI_RULES:
        if rx.search(loc) and (not need_street or street.startswith(need_street)):
            return hit[0], "table"
    return UCI, ("office" if _is_office(loc) else "unknown")


# ── Infirmary Health ───────────────────────────────────────────────────────
INFIRMARY = "Infirmary Health"
INFIRMARY_HOSPITALS = _table([
    ((), "Mobile Infirmary", "Mobile", "AL", "cms 010113"),
    ((), "Thomas Hospital", "Fairhope", "AL", "cms 010100"),
    ((), "North Baldwin Infirmary", "Bay Minette", "AL", "cms 010129"),
])


def infirmary_facility(j: dict) -> tuple[str, str]:
    loc = _first(j, "location_name")
    hit = INFIRMARY_HOSPITALS.get(key(loc))
    if hit:
        return hit[0], "table"
    return INFIRMARY, ("office" if _is_office(loc) else "unknown")


# ── One Brooklyn Health (location_name is the site's own name) ─────────────
OBH = "One Brooklyn Health"
OBH_SITES = _table([
    ((), "The Brookdale Hospital Medical Center", "Brooklyn", "NY", "address 1 Brookdale Plaza, Brooklyn"),
    ((), "Interfaith Medical Center", "Brooklyn", "NY", "address 1545 Atlantic Ave, Brooklyn"),
    ((), "Kingsbrook Jewish Medical Center", "Brooklyn", "NY", "address 585 Schenectady Ave, Brooklyn"),
    ((), "Rutland Nursing Home", "Brooklyn", "NY", "address 585 Schenectady Ave, Brooklyn"),
    (("Schulman and Schachne Nursing Homae",), "Schulman and Schachne Nursing Home", "Brooklyn", "NY",
     "address 555 Rockaway Pkwy, Brooklyn"),
])


def obh_facility(j: dict) -> tuple[str, str]:
    loc = _first(j, "location_name")
    hit = OBH_SITES.get(key(loc))
    if hit:
        return hit[0], "table"
    if _is_office(loc):
        return OBH, "office"
    return loc, "site"


JIBE_SYSTEMS: dict[str, Callable[[dict], tuple[str, str]]] = {
    FAIRVIEW: fairview_facility,
    MEDSTAR: medstar_facility,
    MYMICHIGAN: mymichigan_facility,
    WAKEMED: wakemed_facility,
    PIEDMONT: piedmont_facility,
    YNHHS: ynhhs_facility,
    OSF: osf_facility,
    CARLE: carle_facility,
    FIRSTHEALTH: firsthealth_facility,
    TANNER: tanner_facility,
    USA_HEALTH: usa_health_facility,
    UCI: uci_facility,
    INFIRMARY: infirmary_facility,
    OBH: obh_facility,
}

# every hospital of the Jibe tables, by system: {name: (city, state, source)}
JIBE_HOSPITALS: dict[str, dict[str, tuple[str, str, str]]] = {}
for _sys, _tables in {
        FAIRVIEW: (FAIRVIEW_HOSPITALS, FAIRVIEW_LOCATION), MEDSTAR: (MEDSTAR_HOSPITALS,),
        MYMICHIGAN: (MYMICHIGAN_HOSPITALS,), WAKEMED: (WAKEMED_HOSPITALS,), OSF: (OSF_HOSPITALS,),
        CARLE: (CARLE_HOSPITALS,), FIRSTHEALTH: (FIRSTHEALTH_HOSPITALS,), TANNER: (TANNER_HOSPITALS,),
        USA_HEALTH: (USA_HOSPITALS,), INFIRMARY: (INFIRMARY_HOSPITALS,), OBH: (OBH_SITES,)}.items():
    JIBE_HOSPITALS[_sys] = {v[0]: v[1:] for t in _tables for v in t.values()}
JIBE_HOSPITALS[PIEDMONT] = {v[0]: v[1:] for v in PIEDMONT_STREETS.values()}
JIBE_HOSPITALS[PIEDMONT]["Piedmont Atlanta Hospital"] = ("Atlanta", "GA", "cms 110083")
JIBE_HOSPITALS[YNHHS] = {v[0]: v[1:] for v in YNHHS_STREETS.values()}
JIBE_HOSPITALS[UCI] = {hit[0]: hit[1:] for _rx, _st, hit in UCI_RULES}
JIBE_HOSPITALS[WAKEMED]["WakeMed Raleigh Campus"] = ("Raleigh", "NC", "cms 340069")


def jibe_facility(system: str, j: dict) -> Optional[tuple[str, str]]:
    """(hospital_name, how) for one /api/jobs row of a JIBE_SYSTEMS board,
    None for any other board. how: "table" / "street" / "campus" / "entity"
    (a hospital of the table), "site" (the board's own name for a named
    clinic or center), "office" or "unknown" (the system label)."""
    fn = JIBE_SYSTEMS.get(system)
    if fn is None:
        return None
    try:
        name, how = fn(j or {})
    except Exception:
        name, how = system, "unknown"
    name = clean(name) or system
    if name == system and how == "unknown":
        # the board's field names no site we know: the posting's own site line
        # ("Location: Piedmont Newnan Hospital"), the rule stored_facility uses
        hit = body_line_hit(system, _plain(str((j or {}).get("description") or "")), _first(j, "city"))
        if hit:
            return hit[0], "body line"
        hit = title_table_hit(system, _first(j, "title"))
        if hit:
            return hit[0], "title"
    return name, how


def _plain(html: str) -> str:
    """A feed body as text lines (block tags become line breaks)."""
    import html as _h
    t = re.sub(r"(?i)<\s*(?:br|/p|/div|/li|/h\d|/tr)\b[^>]*>", "\n", html or "")
    t = re.sub(r"<[^>]+>", " ", t)
    return _h.unescape(t)


def jibe_town(system: str, name: str, city: str, state: str) -> tuple[str, str]:
    """(city, state): the row's own town; the hospital's town only when the
    feed gave none (a town is never moved, never guessed)."""
    if (city or "").strip():
        return city, state
    hit = JIBE_HOSPITALS.get(system, {}).get(name)
    if hit and (not state or state == hit[1]):
        return hit[0], hit[1]
    return city, state


# ══════════════════════════════════════════════════════════════════════════
#  CLASSIC iCIMS PORTALS (title and body)
# ══════════════════════════════════════════════════════════════════════════

# ── Kettering Health ("Job Details" then "<site> | <town> | <type> | ...") ─
KETTERING = "Kettering Health"
KETTERING_DEFAULT = ("Kettering", "OH")
KETTERING_SITES = _table([
    (("Kettering Health Main", "Kettering Health-Main Campus", "KH Main Campus"), "Kettering Health Main Campus",
     "Kettering", "OH", "cms 360079"),
    (("KH Dayton",), "Kettering Health Dayton", "Dayton", "OH", "cms 360133"),
    (("Kettering Health Soin Medical Center", "Soin Medical Center", "Soin Medical Cent/Kettering Health", "KH Soin"),
     "Kettering Health Soin", "Beavercreek", "OH", "cms 360360"),
    (("KH Hamilton",), "Kettering Health Hamilton", "Hamilton", "OH", "cms 360132"),
    (("KH Greene Memorial",), "Kettering Health Greene Memorial", "Xenia", "OH", "cms 360026"),
    (("KH Miamisburg",), "Kettering Health Miamisburg", "Miamisburg", "OH", "cms 360239"),
    (("KH Troy", "Kettering Health Troy Campus"), "Kettering Health Troy", "Troy", "OH", "cms 360368"),
    (("KH Washington Township", "Kettering Health Washington Township (Hand and Orthopedic)"),
     "Kettering Health Washington Township", "Centerville", "OH", "body"),
    (("Kettering Health Behavioral",), "Kettering Health Behavioral Medical Center", "Moraine", "OH", "body"),
    (("Kettering Health Huber",), "Kettering Health Huber Heights", "Huber Heights", "OH", "body"),
    ((), "Kettering Health Wilmington", "Wilmington", "OH", "body"),
    ((), "Kettering Health Springfield", "Springfield", "OH", "body"),
    ((), "Kettering Health Franklin", "Franklin", "OH", "body"),
    ((), "Kettering Health Preble", "Eaton", "OH", "body"),
    ((), "Kettering College", "Kettering", "OH", "body"),
    ((), "Kettering Health Ollie Davis MAE Center", "Beavercreek", "OH", "body"),
    # named on the body with no town; the row keeps its own town
    (("KH Middletown",), "Kettering Health Middletown", "", "OH", "name only"),
])
KETTERING_LINE_RX = re.compile(r"^\s*Job Details\s*\n+\s*([^\n|]{2,90}?)\s*\|\s*([^\n|]{2,40}?)\s*(?:\||\n|$)")
# Ohio towns the Kettering bodies name that city_utils' town list lacks
KETTERING_EXTRA_TOWNS = frozenset(key(t) for t in ("Springboro", "Clayton", "Eaton"))
# a second field that is the job type, not a town ("KH Miamisburg | Full-Time")
NOT_TOWN_RX = re.compile(r"time|resource|prn|per\s*diem|casual|temporary|remote|contract|fulltime|parttime|\d", re.I)


def kettering_line(body: str) -> tuple[str, str]:
    """(site, town) of a Kettering body's first line; town "" when the
    second field is not a town."""
    m = KETTERING_LINE_RX.match(body or "")
    if not m:
        return "", ""
    site, town = clean(m.group(1)), clean(m.group(2))
    town = re.sub(r",\s*OH$", "", town, flags=re.I)
    if NOT_TOWN_RX.search(town) or not re.fullmatch(r"[A-Za-z][A-Za-z .'-]{1,30}", town):
        return site, ""
    town = town.title() if town.islower() else town
    # a town of Ohio (city_utils' list, or one the bodies themselves name that
    # the list lacks); anything else ("Every Weekend Commitment") is not one
    try:
        from city_utils import known_town
        known = known_town(town, "OH")
    except Exception:
        known = None
    if known:
        return site, known
    return site, (town if key(town) in KETTERING_EXTRA_TOWNS else "")


def kettering_facility(title: str, body: str, city: str, state: str) -> Optional[tuple]:
    site, town = kettering_line(body)
    if not site:
        return None
    hit = KETTERING_SITES.get(key(site))
    if hit:
        name, hcity, hstate = hit[0], hit[1], hit[2]
        return name, (town or hcity or city), (hstate or state), "body"
    # an office, a department or "Physician Office": the system, in the body's town
    if town:
        return KETTERING, town, "OH", "body town"
    return None


# ── Emory Healthcare (the body's "Location:" line, else the title) ─────────
EMORY = "Emory Healthcare"
EMORY_HOSPITALS = _table([
    ((), "Emory University Hospital", "Atlanta", "GA", "cms 110010"),
    (("Emory Midtown Hospital", "Emory University Hospital Midtown", "EUHM"), "Emory University Hospital Midtown",
     "Atlanta", "GA", "cms 110078"),
    (("Emory Saint Joseph's Hospital", "Emory Saint Joseph Hospital", "Emory St. Joseph's Hospital", "ESJH",
      "Emory Saint Joseph's Hospital OPIC"),
     "Emory Saint Joseph's Hospital", "Atlanta", "GA", "cms 110082"),
    (("EDH",), "Emory Decatur Hospital", "Decatur", "GA", "cms 110076"),
    ((), "Emory Hillandale Hospital", "Lithonia", "GA", "cms 110226"),
    (("Emory John's Creek Hospital", "EJCH"), "Emory Johns Creek Hospital", "Johns Creek", "GA", "cms 110230"),
    (("Emory Houston Hospital Warner Robins",), "Emory Warner Robins Hospital", "Warner Robins", "GA",
     "cms 110069"),
    (("Emory Ortho Spine Hospital", "Emory University Orthopaedics and Spine Hospital"),
     "Emory University Orthopaedics & Spine Hospital", "Tucker", "GA", "address 1455 Montreal Rd, Tucker"),
    (("Winship at Emory Midtown Hospital",), "Winship at Emory Midtown", "Atlanta", "GA",
     "address 550 Peachtree St NE, Atlanta"),
])
EMORY_LOCATION_RX = re.compile(r"^\s*Location\s*:\s*(.{3,80}?)\s*$", re.I | re.M)
# title spellings, longest first (Midtown before the University Hospital)
EMORY_TITLE_NAMES = sorted(
    {"Emory University Hospital Midtown", "Emory Midtown Hospital", "Emory University Hospital",
     "Emory Saint Joseph's Hospital", "Emory Saint Joseph Hospital", "Emory Decatur Hospital",
     "Emory Hillandale Hospital", "Emory Johns Creek Hospital", "Emory John's Creek Hospital",
     "Emory Warner Robins Hospital", "ESJH", "EJCH", "EUHM"}, key=len, reverse=True)


def _title_hits(title: str, names, table) -> set:
    """The table's hospitals a title names (whole words, longest spelling
    first, each match taken out of the title before the next)."""
    t = " " + clean(title) + " "
    hits = set()
    for n in names:
        rx = re.compile(r"(?<![A-Za-z])" + re.escape(n).replace(r"\ ", r"\s+") + r"(?![A-Za-z])", re.I)
        if rx.search(t):
            hits.add(table[key(n)][0])
            t = rx.sub(" ", t)
    return hits


def emory_facility(title: str, body: str, city: str, state: str) -> Optional[tuple]:
    for m in EMORY_LOCATION_RX.finditer((body or "")[:12000]):
        v = m.group(1)
        # the whole value, or the value before a town or a schedule
        # ("Emory University Hospital (Atlanta, GA) Schedule: Full-time")
        for cand in (v, re.split(r"\s*[(,]|\s+-\s+|\s+Schedule\s*:", v)[0]):
            hit = EMORY_HOSPITALS.get(key(cand))
            if hit:
                return hit[0], hit[1], hit[2], "body"
        break                        # the first Location line is the posting's own
    hits = _title_hits(title, EMORY_TITLE_NAMES, EMORY_HOSPITALS)
    if len(hits) == 1:
        name = hits.pop()
        hit = EMORY_HOSPITALS[key(name)]
        return hit[0], hit[1], hit[2], "title"
    return None


# ── Title tables (Huntsville, Northside, Northern Arizona) ─────────────────
# A title names the hospital when one (and only one) of the system's hospital
# phrases stands in it as its own segment (between commas, dashes, "at" or
# parentheses), and that segment does not name a clinic, a practice, a
# nursing home, EMS or home care ("DeKalb Obstetric & Gynecology Clinic",
# "Lincoln Nursing Home", "Women's Specialists of Northside Gwinnett").
NOT_HOSPITAL_RX = re.compile(
    r"\b(?:clinic|practice|practices|physicians?|specialists|medical\s+group|nursing\s+home|ems|"
    r"home\s+health|home\s+care|hospice|urgent\s+care|family\s+medicine|family\s+practice|"
    r"breast\s+center|imaging\s+center|cancer\s+center|surgery\s+center|medical\s+office|"
    r"medical\s+assistant|opportunity|events?|hiring)\b", re.I)
_SEGMENT_SPLIT_RX = re.compile(r"\s*(?:,|\s-\s|\s–\s|\(|\)|/|\|)\s*")

TITLE_TABLES: dict[str, list] = {
    "Huntsville Hospital Health System": [
        (re.compile(r"^Marshall Medical Centers?,?\s+South\b", re.I),
         ("Marshall Medical Center South", "Boaz", "AL", "cms 010005")),
        (re.compile(r"^Marshall Medical Centers?,?\s+North\b", re.I),
         ("Marshall Medical Center North", "Guntersville", "AL", "address 8000 AL Hwy 69, Guntersville")),
        (re.compile(r"^Athens[- ]Limestone(?:\s+Hospital)?$", re.I),
         ("Athens-Limestone Hospital", "Athens", "AL", "cms 010079")),
        (re.compile(r"^Lincoln Medical Center$", re.I), ("Lincoln Medical Center", "Fayetteville", "TN", "cms 440102")),
        (re.compile(r"^Decatur Morgan Hospital\b", re.I), ("Decatur Morgan Hospital", "Decatur", "AL", "cms 010085")),
        (re.compile(r"^Helen Keller(?:\s+Hospital)?$", re.I), ("Helen Keller Hospital", "Sheffield", "AL", "cms 010019")),
        (re.compile(r"^Highlands(?:\s+Medical\s+Center)?(?:\s+\d\w*\s+Floor)?$", re.I),
         ("Highlands Medical Center", "Scottsboro", "AL", "cms 010061")),
        (re.compile(r"^DeKalb Regional(?:\s+Medical\s+Center)?$", re.I),
         ("DeKalb Regional Medical Center", "Fort Payne", "AL", "cms 010012")),
        (re.compile(r"^Madison Hospital$", re.I), ("Madison Hospital", "Madison", "AL", "address 8375 Hwy 72 W, Madison")),
        (re.compile(r"^(?:Huntsville Hospital (?:for )?)?Women\s*(?:&|and)\s*Children(?:'s)?(?:\s+Float\s+Pool)?$",
                    re.I),
         ("Huntsville Hospital for Women & Children", "Huntsville", "AL", "address 245 Governors Dr, Huntsville")),
        (re.compile(r"^Huntsville Hospital$", re.I), ("Huntsville Hospital", "Huntsville", "AL", "cms 010039")),
        (re.compile(r"^Red Bay Hospital$", re.I), ("Red Bay Hospital", "Red Bay", "AL", "cms 011302")),
    ],
    "Northside Hospital": [
        (re.compile(r"^(?:Northside(?:\s+Hospital)?\s+)?Gwinnett(?:\s+Campus)?$", re.I),
         ("Northside Hospital Gwinnett", "Lawrenceville", "GA", "cms 110087")),
        (re.compile(r"^(?:Northside(?:\s+Hospital)?\s+)?Forsyth(?:\s+Campus)?$", re.I),
         ("Northside Hospital Forsyth", "Cumming", "GA", "cms 110005")),
        (re.compile(r"^(?:Northside(?:\s+Hospital)?\s+)?Cherokee(?:\s+Campus)?$", re.I),
         ("Northside Hospital Cherokee", "Canton", "GA", "cms 110008")),
        (re.compile(r"^(?:Northside(?:\s+Hospital)?\s+)?Duluth(?:\s+Main|\s+Campus)?$", re.I),
         ("Northside Hospital Duluth", "Duluth", "GA", "cms 110252")),
        (re.compile(r"^Northside Hospital Atlanta$", re.I),
         ("Northside Hospital Atlanta", "Atlanta", "GA", "cms 110161")),
    ],
    "Northern Arizona Healthcare": [
        (re.compile(r"^(?:\S.*\s)?Flagstaff Medical Center$", re.I),
         ("Flagstaff Medical Center", "Flagstaff", "AZ", "cms 030023")),
        (re.compile(r"^(?:\S.*\s)?(?:Verde Valley Medical Center|VVMC)$", re.I),
         ("Verde Valley Medical Center", "Cottonwood", "AZ", "cms 030007")),
    ],
}


# a title that names two of the system's hospitals ("Anesthesiologist -
# Northside Hospital Atlanta, Cherokee and Forsyth") names none
TITLE_MENTIONS: dict[str, re.Pattern] = {
    "Northside Hospital": re.compile(r"\b(Gwinnett|Forsyth|Cherokee|Duluth|Northside Hospital Atlanta)\b", re.I),
    "Huntsville Hospital Health System": re.compile(
        r"\b(Marshall Medical Centers?,?\s+(?:North|South)|Athens[- ]Limestone|Lincoln Medical|Decatur Morgan|"
        r"Helen Keller|Highlands|DeKalb|Madison Hospital|Women\s*(?:&|and)\s*Children|Red Bay)\b", re.I),
    "Northern Arizona Healthcare": re.compile(r"\b(Flagstaff Medical Center|Verde Valley Medical Center|VVMC)\b", re.I),
}


def title_facility(system: str, title: str) -> Optional[FacilityHit]:
    """The one hospital a title names on a TITLE_TABLES system, else None."""
    rules = TITLE_TABLES.get(system)
    if not rules or not title:
        return None
    t = clean(title)
    t = re.sub(r"(Marshall Medical Centers?),\s+(North|South)\b", r"\1 \2", t, flags=re.I)
    if NOT_HOSPITAL_RX.search(t) and system != "Huntsville Hospital Health System":
        return None
    rx = TITLE_MENTIONS.get(system)
    if rx and len({re.sub(r"\W+", " ", m.lower()) for m in rx.findall(t)}) > 1:
        return None
    hits = {}
    for seg in _SEGMENT_SPLIT_RX.split(t):
        seg = re.sub(r"^(?:at|in|for)\s+", "", seg.strip(), flags=re.I)
        if not seg:
            continue
        for rx, hit in rules:
            if rx.search(seg):
                if NOT_HOSPITAL_RX.search(seg):
                    break
                hits[hit[0]] = hit
                break
    if len(hits) != 1:
        return None
    return next(iter(hits.values()))


def _title_classic(system: str):
    def fn(title, body, city, state):
        hit = title_facility(system, title)
        if not hit:
            return None
        return hit[0], hit[1], hit[2], "title"
    return fn


CLASSIC_SYSTEMS: dict[str, Callable] = {
    KETTERING: kettering_facility,
    EMORY: emory_facility,
    "Huntsville Hospital Health System": _title_classic("Huntsville Hospital Health System"),
    "Northside Hospital": _title_classic("Northside Hospital"),
    "Northern Arizona Healthcare": _title_classic("Northern Arizona Healthcare"),
}
# the town the list stamps on every row of a portal (the head office), which
# a named hospital's own town replaces
CLASSIC_DEFAULT_TOWN = {KETTERING: KETTERING_DEFAULT, "Northside Hospital": ("Atlanta", "GA")}


def classic_facility(system: str, title: str, body: str, city: str, state: str) -> Optional[tuple]:
    """(hospital_name, city, state, how) for one classic portal row of a
    CLASSIC_SYSTEMS system, None when its title and body name no site.
    The town: a named hospital's own town (a title that names one hospital,
    a body line), the body's own town for an office (Kettering), else the
    row's own. A title that names a hospital in another town than the row's
    own (the portal's head-office town aside) is two answers, so none
    (Huntsville: "Marshall Medical Centers South" on a Guntersville row)."""
    fn = CLASSIC_SYSTEMS.get(system)
    if fn is None:
        return None
    try:
        hit = fn(title or "", body or "", city or "", state or "")
    except Exception:
        return None
    if not hit:
        return None
    name, hcity, hstate, how = hit
    if how == "title" and hcity and (city or "").strip():
        dflt = CLASSIC_DEFAULT_TOWN.get(system)
        at_default = bool(dflt) and (key(city), (state or "").upper()) == (key(dflt[0]), dflt[1])
        if not at_default and key(city) != key(hcity):
            return None
    if not hcity:
        hcity, hstate = city, (hstate or state)
    return clean(name), clean(hcity), (hstate or state or "").upper(), how


def is_hospital(system: str, name: str) -> bool:
    """True when name is one of the tables' hospitals or named sites of the
    system (not its label)."""
    if not name or name == system:
        return False
    if name in JIBE_HOSPITALS.get(system, {}):
        return True
    tables = {KETTERING: KETTERING_SITES, EMORY: EMORY_HOSPITALS}
    if system in tables:
        return any(v[0] == name for v in tables[system].values())
    return any(hit[0] == name for _rx, hit in TITLE_TABLES.get(system, ()))


# ══════════════════════════════════════════════════════════════════════════
#  STORED ROWS (the change list; read-only, nothing the database does not hold)
# ══════════════════════════════════════════════════════════════════════════
MYMI_STORED_RX = re.compile(r"Medical Center\s+(Midland|Saginaw|Alma|Alpena|Clare|Gladwin|West Branch|"
                            r"Mount Pleasant|Mt\.? Pleasant|Sault|Standish|Tawas)\b", re.I)


# the tables' spellings, by system: {key(spelling): (name, city, state, source)}
JIBE_SPELLINGS: dict[str, dict] = {
    FAIRVIEW: {**FAIRVIEW_LOCATION, **FAIRVIEW_HOSPITALS}, MEDSTAR: MEDSTAR_HOSPITALS,
    MYMICHIGAN: MYMICHIGAN_HOSPITALS, WAKEMED: WAKEMED_HOSPITALS, OSF: OSF_HOSPITALS, CARLE: CARLE_HOSPITALS,
    FIRSTHEALTH: FIRSTHEALTH_HOSPITALS, TANNER: TANNER_HOSPITALS, USA_HEALTH: USA_HOSPITALS,
    INFIRMARY: INFIRMARY_HOSPITALS, OBH: OBH_SITES,
    PIEDMONT: {key(v[0]): v for v in PIEDMONT_STREETS.values()},
    YNHHS: {key(v[0]): v for v in YNHHS_STREETS.values()},
    UCI: {key(hit[0]): hit for _rx, _st, hit in UCI_RULES},
}
# a stored body's own site line ("Location: Piedmont Fayette Hospital",
# "Facility: OSF Saint Anthony Medical Center", "Work Location: 267 Grant St,
# Bridgeport, CT")
SITE_LINE_RX = re.compile(
    r"^[ \t]*(?:work\s+|primary\s+|job\s+|practice\s+)?(?:location|facility|hospital|campus|site|work\s*site)"
    r"[ \t]*:[ \t]*(.{3,120}?)[ \t]*$", re.I | re.M)


def _line_hit(system: str, value: str, city: str) -> Optional[FacilityHit]:
    """The table hospital one site-line value names, exactly (its whole value,
    or the value before a town: "Piedmont Fayette Hospital - Fayetteville,
    GA"); a street on the street tables."""
    v = clean(value)
    if re.search(r"\s/\s|;|\band\b\s+\d", v):
        return None          # two sites ("25 Wells St, Westerly RI / 46 Boston Post Road, Waterford")
    table = JIBE_SPELLINGS.get(system, {})
    for cand in (v, re.split(r"\s+[-\u2013|]\s+|,\s*", v)[0]):
        hit = table.get(key(cand))
        if hit:
            return hit
    if system == YNHHS:
        return ynhhs_street_hit(v, city)
    if system == PIEDMONT:
        m = re.match(r"^(.+?),\s*([A-Za-z .'-]+?)(?:,\s*GA\b.*)?$", v)
        street, town = (m.group(1), m.group(2)) if m else (v, city)
        return PIEDMONT_STREETS.get((street_key(street), key(town)))
    return None


def stored_facility(system: str, row: dict) -> Optional[FacilityHit]:
    """(name, city, state, source) for one stored row: a classic portal row
    by its title and body (classic_facility, what the nightly scrape does);
    a Jibe row (whose site field is not stored) only by its body's own site
    line naming one of the table's hospitals exactly ("Location: ...",
    "Facility: ..."), or an old MyMichigan row whose stored location still
    carries "Medical Center <town>". A mention of a hospital anywhere else in
    a body is not taken (2026-10-29 check against the live feed: a UCI body's
    boilerplate names "UCI Health - Irvine" on rows the feed places at the
    Orange medical center). None when the stored row names none."""
    title = row.get("title") or ""
    body = row.get("description") or ""
    city, state = row.get("city") or "", row.get("state") or ""
    if system in CLASSIC_SYSTEMS:
        hit = classic_facility(system, title, body, city, state)
        if hit:
            return hit[0], hit[1], hit[2], f"stored {hit[3]}"
        return None
    if system not in JIBE_SYSTEMS:
        return None
    if system == MYMICHIGAN:
        m = MYMI_STORED_RX.search(" ".join(str(row.get(k) or "") for k in ("location", "city")))
        if m:
            short = "Mount Pleasant" if m.group(1).lower().startswith("m") else m.group(1).title()
            hit = MYMICHIGAN_HOSPITALS.get(key(f"Medical Center {short}"))
            if hit:
                return hit[0], hit[1], hit[2], "stored location"
    hit = body_line_hit(system, body, city)
    if hit:
        return hit[0], hit[1], hit[2], "stored site line"
    hit = title_table_hit(system, title)
    if hit:
        return hit[0], hit[1], hit[2], "stored title"
    return None


def title_table_hit(system: str, title: str) -> Optional[FacilityHit]:
    """The one table hospital a Jibe title names by its full clean name
    ("Pharmacy Technician-Bridgeport Hospital"), longest name first, each
    match taken out before the next; None for none or two."""
    t = " " + clean(title) + " "
    found = {}
    for name in sorted(JIBE_HOSPITALS.get(system, {}), key=len, reverse=True):
        if len(name) < 12:
            continue
        rx = re.compile(r"(?<![A-Za-z])" + re.escape(name) + r"(?![A-Za-z])", re.I)
        if rx.search(t):
            hcity, hstate, src = JIBE_HOSPITALS[system][name]
            found[name] = (name, hcity, hstate, src)
            t = rx.sub(" ", t)
    return next(iter(found.values())) if len(found) == 1 else None


def body_line_hit(system: str, body: str, city: str = "") -> Optional[FacilityHit]:
    """The one table hospital a body's site lines name ("Location: ..."),
    None when they name none or two."""
    hits = {}
    for m in SITE_LINE_RX.finditer((body or "")[:12000]):
        hit = _line_hit(system, m.group(1), city)
        if hit:
            hits[hit[0]] = hit
    return next(iter(hits.values())) if len(hits) == 1 else None


# ══════════════════════════════════════════════════════════════════════════
#  APPLYING IT TO A CLASSIC PORTAL ROW (scraper._icims_tenant, after the
#  detail pass)
# ══════════════════════════════════════════════════════════════════════════
# The body-derived systems see the body only on nights it is fetched; a known
# body is sent blank (scraper._keep_stored_body). On those nights the row
# keeps the hospital and the town stored on it (load_stored at the start of
# the iCIMS run), so the list's head-office town does not come back.
STORED_SYSTEMS = (KETTERING, EMORY)
NO_BODY_CHARS = 200
STORED_PAGE = 1000
_STORED: dict = {}      # (system, job_id) -> (hospital_name, city, state)


def set_stored(rows) -> int:
    """Install {(system, job_id): (hospital_name, city, state)} from stored
    rows of STORED_SYSTEMS whose name is one of the tables' sites, or whose
    town is not the portal's head-office town. Returns the map's size."""
    global _STORED
    out = {}
    for r in rows or []:
        s, jid = r.get("hospital_system"), r.get("job_id")
        if s not in STORED_SYSTEMS or jid in (None, ""):
            continue
        name = clean(r.get("hospital_name"))
        city, state = clean(r.get("city")), clean(r.get("state")).upper()
        dflt = CLASSIC_DEFAULT_TOWN.get(s)
        ours = is_hospital(s, name)
        moved = name == s and city and dflt and (key(city), state) != (key(dflt[0]), dflt[1])
        if ours or moved:
            out[(s, str(jid))] = (name, city, state)
    _STORED = out
    return len(out)


def load_stored(logger=None) -> int:
    """One read-only PostgREST pass per STORED_SYSTEMS system over its active
    rows (job_id, hospital_name, city, state), keyset-paged on id. Without
    credentials the map stays empty and nothing is carried."""
    import json as _json
    import os as _os
    import time as _time
    import urllib.parse as _urlparse
    import urllib.request as _urlreq
    try:
        import sb_keys
        sb_url, sb_key = _os.environ.get("SUPABASE_URL", ""), sb_keys.supabase_key()
    except Exception:
        sb_url, sb_key = "", ""
    if not sb_url or not sb_key:
        set_stored([])
        return 0
    rows = []
    for system in STORED_SYSTEMS:
        flt = _urlparse.quote(f"eq.{system}", safe=".")
        last = 0
        try:
            while True:
                u = (f"{sb_url.rstrip('/')}/rest/v1/hospital_jobs?select=id,hospital_system,job_id,hospital_name,"
                     f"city,state&is_active=is.true&hospital_system={flt}&id=gt.{last}&order=id.asc"
                     f"&limit={STORED_PAGE}")
                rq = _urlreq.Request(u, headers={"apikey": sb_key, "Authorization": f"Bearer {sb_key}"})
                chunk = None
                for attempt in range(3):
                    try:
                        with _urlreq.urlopen(rq, timeout=60) as resp:
                            chunk = _json.loads(resp.read().decode())
                        break
                    except Exception:
                        if attempt == 2:
                            raise
                        _time.sleep(3 * (attempt + 1))
                if not chunk:
                    break
                rows.extend(chunk)
                last = chunk[-1]["id"]
                if len(chunk) < STORED_PAGE:
                    break
        except Exception as e:
            if logger:
                logger.warning(f"iCIMS stored facilities: {system} load stopped ({e}); its rows past that "
                               f"keep a hospital only on nights their body is read")
    try:
        n = set_stored(rows)
    except Exception as e:
        if logger:
            logger.warning(f"iCIMS stored facilities: not installed ({e})")
        set_stored([])
        return 0
    if logger:
        logger.info(f"iCIMS stored facilities: {n:,} of {len(rows):,} active rows of "
                    f"{', '.join(STORED_SYSTEMS)} carry a site or their own town")
    return n


def _set_town(job, city: str, state: str) -> None:
    if city and state:
        job.city, job.state, job.location = city, state, f"{city}, {state}"


def apply_classic(job) -> str:
    """Store the site (and its town) on one classic portal Job of a
    CLASSIC_SYSTEMS system. Returns how: "body" / "title" / "body town"
    (tonight's text), "kept" (the stored site or town, no body tonight),
    "other" (the card already named a facility), "none", or "" for any
    other system. hospital_system is never changed."""
    system = getattr(job, "hospital_system", "")
    if system not in CLASSIC_SYSTEMS:
        return ""
    cur = clean(job.hospital_name)
    if cur and cur != system:
        return "other"
    hit = classic_facility(system, job.title, job.description, job.city, job.state)
    if hit:
        job.hospital_name = hit[0]
        _set_town(job, hit[1], hit[2])
        return hit[3]
    if len((job.description or "").strip()) >= NO_BODY_CHARS:
        return "none"
    stored = _STORED.get((system, str(job.job_id)))
    if not stored:
        return "none"
    name, city, state = stored
    dflt = CLASSIC_DEFAULT_TOWN.get(system)
    at_default = not (job.city or "").strip() or (
        dflt and (key(job.city), (job.state or "").upper()) == (key(dflt[0]), dflt[1]))
    job.hospital_name = name or system
    if at_default:
        _set_town(job, city, state)
    return "kept"


def apply_classic_all(jobs) -> dict:
    """apply_classic over one portal's rows. Returns {how: rows}."""
    out: dict = {}
    for j in jobs or []:
        how = apply_classic(j)
        if how:
            out[how] = out.get(how, 0) + 1
    return out
