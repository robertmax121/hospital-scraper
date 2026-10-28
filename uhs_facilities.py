"""One clean name per UHS facility on jobs.uhsinc.com postings (2026-10-28).

The owner, 2026-10-28: "yes show facility names on UHS cards." Until now every
Universal Health Services row carried the system label in hospital_name
(4,827 of 4,827 active rows on 2026-10-28): the Jibe feed's location_name is
a street or a site code, and _jibe_facility fell back to the system. The feed
does name the facility, in the "tags4" list ("Manatee Memorial Hospital",
"STHS McAllen", "UHS of Delaware, Inc. - Corporate Office"), the same values
the careers board's own Facility filter shows. This module turns that value
into one clean name, and is shared with the stored-row cleanup
(claud_outputinput/uhs_site_crm_2026-10-28/make_sql.py), so the database
holds what the scraper keeps.

  facility(tag)        the facility a tags4 value names: one spelling per
                       facility (CANONICAL, then the board's own majority
                       spelling of 2026-10-28), the abbreviations written out
                       (RENAMES: "STHS McAllen" is South Texas Health System
                       McAllen), "" for a value that names no facility (the
                       system's own offices: the corporate office, the
                       billing offices, the call center; SYSTEM_LEVEL), and a
                       value the table does not know yet as written (a new
                       facility is still a facility)
  text_facility(text)  the facility a posting's own text names, for a row
                       whose tag is blank: the first known facility name in
                       the opening of the description ("Who We Are: Millwood
                       Hospital (a UHS Facility) is ..."), else ""
  is_facility(name)    True for a stored hospital_name that names a
                       facility (not the system label, not blank)

hospital_system stays "Universal Health Services" (the upsert key, the hub,
the filters and the logo). On 2026-10-28 the tag named a facility on 5,909 of
6,007 postings read (98.4%); the rest name an office or nothing.
"""
from __future__ import annotations

import html
import re

SYSTEM = "Universal Health Services"
FACILITY_TAG = "tags4"

# The system label and the spellings a row could carry for the system itself.
_SYSTEM_RX = re.compile(r"^\s*(?:universal\s+health\s+services(?:,?\s+inc\.?)?|uhs(?:,?\s+inc\.?)?)\s*$", re.I)

# Values that name no facility: UHS's own offices (the corporate office in
# King of Prussia, the regional billing offices, the shared-services group,
# the national call center) and the feed's blanks. A posting there is the
# system's own job; the row keeps the system label.
SYSTEM_LEVEL = re.compile(
    r"^\s*(?:"
    r"uhs\s+of\s+delaware\b.*"                 # UHS of Delaware, Inc. - Corporate Office (- Remote)
    r"|universal\s+health\s+services\b.*"
    r"|.*\bCBO\s*$"                            # Atlantic Region CBO, Western CBO (consolidated business offices)
    r"|georgia\s+business\s+center"
    r"|behavioral\s+health\s+shared\s+services\s+group"
    r"|the\s+national\s+call\s+center"
    r"|n\s*/?\s*a|none|tbd|-+"
    r")\s*$", re.I)

# Abbreviations and labels the board uses for a facility whose name is
# longer, and two-facility labels that name no one facility ("" = none).
RENAMES = {
    "sths mcallen": "South Texas Health System McAllen",
    "sths edinburg": "South Texas Health System Edinburg",
    "sths heart": "South Texas Health System Heart",
    "sths behavioral": "South Texas Health System Behavioral",
    "sths childrens": "South Texas Health System Children's",
    "sths er mccoll": "South Texas Health System ER McColl",
    "sths roc": "South Texas Health System",
    "south texas health system clinics": "South Texas Health System Clinics",
    # the freestanding ERs: "ER at Spanish Springs, an extension of Northern
    # Nevada Medical Center" (their own postings, 2026-10-28)
    "nnhs er at mccarran nw": "Northern Nevada Medical Center",
    "nnhs er at north valleys": "Northern Nevada Medical Center",
    "nnhs er at spanish springs": "Northern Nevada Medical Center",
    "the northern nevada health system": "Northern Nevada Health System",
    "the valley health system": "Valley Health System",
    "valley health system consolidated services": "Valley Health System",
    "the brook dupont and kmi": "",
    # the board's two spellings of one hospital (34 postings and 2 on 10-28)
    "southwest healthcare rancho springs hospital": "Rancho Springs Medical Center",
}

# How postings write a facility in their own text when it differs from the
# board's tag ("Summerlin Hospital is a 496-bed hospital ..." is Summerlin
# Hospital Medical Center). text_facility only.
TEXT_ALIASES = {
    "Summerlin Hospital": "Summerlin Hospital Medical Center",
    "Spring Valley Hospital": "Spring Valley Hospital Medical Center",
    "Centennial Hills Hospital": "Centennial Hills Hospital Medical Center",
    "Inland Valley Hospital": "Inland Valley Medical Center",
    "Southwest Healthcare Inland Valley Hospital": "Inland Valley Medical Center",
    "Rancho Springs Hospital": "Rancho Springs Medical Center",
    "Southwest Healthcare Rancho Springs Hospital": "Rancho Springs Medical Center",
    "Texoma Care": "TexomaCare",
}

# Names text_facility never takes: a market or a division that postings of
# its member facilities name first ("Spring Valley Hospital is a member of
# The Valley Health System", "Northwest Texas Healthcare System is a 495-bed
# system" on The Pavilion's postings), and a subsidiary that works inside
# other hospitals (Horizon Health). Their own postings carry the tag.
TEXT_SKIP = frozenset({
    "Valley Health System", "Southwest Healthcare System", "South Texas Health System",
    "Northern Nevada Health System", "Northwest Texas Healthcare System", "Horizon Health",
    "Foundations Health", "Foundations Recovery Network", "Patriot Support Programs of UHS",
    "Valley Hospital", "Behavioral Health Connections",
})

# One spelling per facility: the board's own, as its Facility filter and the
# majority of its postings wrote it on 2026-10-28 (a case or punctuation
# variant, "Doctors Hospital Of Laredo", "Prairie St Johns", "Hri Hospital",
# takes it). Also the names text_facility looks for.
CANONICAL = (
    "A Plus Urgent Care", "Aiken Physicians Alliance", "Aiken Professional Association",
    "Aiken Regional Medical Centers", "Alabama Clinical Schools", "Alan B. Miller Medical Center",
    "Alliance Health Center", "Anchor Hospital", "Arbour Counseling Services", "Arbour Hospital",
    "Arrowhead Behavioral Health", "Aspen Grove Behavioral Hospital", "Austin Oaks Hospital",
    "BHC Alhambra Hospital", "Behavioral Health Connections", "Behavioral Health Connections of Georgia",
    "Behavioral Hospital of Bellaire", "Belmont Pines Hospital", "Benchmark Behavioral Health System Hawaii",
    "Benchmark Behavioral Health Systems", "Black Bear Lodge", "Bloomington Meadows Hospital",
    "Brentwood Behavioral Healthcare", "Brentwood Hospital", "Brooke Glen Behavioral Hospital",
    "Brynn Marr Hospital", "Calvary Healing Center", "Canyon Creek Behavioral Health", "Canyon Ridge Hospital",
    "Capital Medical Group", "Cedar Creek Hospital", "Cedar Hill Regional Medical Center", "Cedar Hills Hospital",
    "Cedar Ridge Hospital", "Cedar Ridge at Bethany", "Cedar Springs Hospital",
    "Centennial Hills Hospital Medical Center", "Centennial Peaks Hospital", "Center for Change",
    "Central Florida Behavioral Hospital", "Clarion Psychiatric Center", "Clive Behavioral Health Hospital",
    "Coastal Behavioral Health", "Coastal Harbor Treatment Center", "Columbus Behavioral Center",
    "Compass Intervention Center", "Copper Hills Youth Center", "Coral Shores Behavioral Health",
    "Corona Regional Medical Center", "Cumberland Hall Hospital",
    "Cumberland Hospital for Children and Adolescents", "Cypress Creek Hospital",
    "Del Amo Behavioral Health System", "Desert View Hospital", "Diamond Grove Center",
    "Doctors Hospital Emergency Room South", "Doctors Hospital of Laredo", "Dover Behavioral Health System",
    "El Paso Behavioral Health System", "Emerald Coast Behavioral Hospital", "Fairfax Behavioral Health",
    "Fairmount Behavioral Health System", "First Health Care of Puerto Rico", "First Hospital Panamericano",
    "Forest View Hospital", "Fort Duncan Regional Medical Center", "Fort Lauderdale Behavioral Health Center",
    "Foundations Behavioral Health", "Foundations Health", "Foundations Minnesota",
    "Foundations Recovery Network", "Foundations San Francisco", "Foundations for Living",
    "Fox Run Center for Children & Adolescents", "Fremont Hospital", "Friends Hospital", "Fuller Hospital",
    "Garfield Park Behavioral Hospital", "Glen Oaks Hospital", "Granite Hills Hospital",
    "Greystone Foster Care", "Gulf Coast Treatment Center", "Gulfport Behavioral Health System", "HOPE Center",
    "HRI Hospital", "Hampton Academy", "Hampton Behavioral Health Center", "Hanover Hill Behavioral Health",
    "Harbor Point Behavioral Health Center", "Hartgrove Behavioral Health System", "Havenwyck Hospital",
    "HealthLinkNow", "Heartland Behavioral Health Services", "Henderson Hospital", "Heritage Oaks Hospital",
    "Hermitage Hall", "Hickory Trail Hospital", "Highlands Behavioral Health System",
    "Hill Crest Behavioral Health Services", "Holly Hill Hospital", "Horizon Health", "IPM SoCal Management",
    "Independence Physician Management", "Inland Northwest Behavioral Health", "Inland Valley Medical Center",
    "Intermountain Hospital", "Kempsville Center for Behavioral Health", "KeyStone Center",
    "Kingwood Pines Hospital", "La Amistad Behavioral Health Services", "Lakeside Behavioral Health System",
    "Lakewood Ranch Medical Center", "Lancaster Behavioral Health Hospital", "Laredo Physicians Group",
    "Laurel Heights Hospital", "Laurel Oaks Behavioral Health Center", "Laurel Ridge Treatment Center",
    "Liberty Point Behavioral Healthcare", "Lifeworks Schools", "Lighthouse Behavioral Health Hospital",
    "Lighthouse Care Center of Augusta", "Lincoln Prairie Behavioral Health Center",
    "Lincoln Trail Behavioral Health System", "Manatee Cardiology Associates", "Manatee Memorial Hospital",
    "Manatee Physician Alliance", "McDowell Center for Children", "Meridell Achievement Center",
    "Mesilla Valley Hospital", "Metropolitan Behavioral Health", "Michael's House",
    "Michiana Behavioral Health", "Mid-Atlantic Medicine", "Midwest Center for Youth and Families",
    "Millwood Hospital", "Mountain Youth Academy", "Mymindnow", "Newport News Behavioral Health Center",
    "North Spring Behavioral Healthcare", "North Star Behavioral Health", "Northern Nevada Health System",
    "Northern Nevada Medical Center", "Northern Nevada Medical Group", "Northwest Physician Group",
    "Northwest Specialty Hospital", "Northwest Texas Healthcare System", "Northwest Texas Physicians Network",
    "Oak Plains Academy", "Okaloosa Youth Academy", "Old Vineyard Behavioral Health Services",
    "Palm Point Behavioral Health", "Palm Shores Behavioral Health Center", "Palmdale Regional Medical Center",
    "Palmetto Lowcountry Behavioral Health", "Palo Verde Behavioral Health", "Parkwood Behavioral Health System",
    "Patriot Support Programs of UHS", "Peachford Hospital", "Pembroke Hospital", "Pinnacle Pointe Hospital",
    "Pinnacle Pointe Outpatient Services", "Poplar Springs Hospital", "Prairie St. John's", "Prominence Health",
    "Psychiatric Institute of Washington", "Quail Run Behavioral Health",
    "Quail Surgical and Pain Management Center", "Rancho Springs Medical Center",
    "Reasons Eating Disorder Center", "Rivendell Behavioral Health Hospital",
    "Rivendell Behavioral Health Services of Arkansas", "River Oaks Hospital", "River Park Hospital",
    "River Point Behavioral Health", "River Vista Behavioral Health", "Riveredge Hospital",
    "Riverside Medical Clinic", "Rockford Center", "Rolling Hills Hospital", "Roxbury Treatment Center",
    "Saint Simons By-The-Sea", "Salt Lake Behavioral Health", "San Marcos Treatment Center",
    "SandyPines Residential Treatment Center", "Sea Grove Recovery", "Sierra Medical Center",
    "Sierra Vista Hospital", "Skywood Outpatient", "Skywood Recovery", "South Texas Health System",
    "South Texas Health System Behavioral", "South Texas Health System Children's",
    "South Texas Health System Clinics", "South Texas Health System ER McColl",
    "South Texas Health System Edinburg", "South Texas Health System Heart", "South Texas Health System McAllen",
    "Southeast Behavioral Hospital", "Southridge Behavioral Hospital",
    "Southwest Healthcare Rancho Springs Hospital", "Southwest Healthcare System",
    "Spring Mountain Treatment Center", "Spring Valley Hospital Medical Center",
    "Spring Valley Physician Alliance", "Springwoods Behavioral Health", "St. Louis Behavioral Medicine Institute",
    "St. Mary's Physician Associates", "St. Mary's Regional Medical Center", "Stonington Institute",
    "Streamwood Behavioral Healthcare System", "Summerlin Hospital Medical Center", "Summit Oaks Hospital",
    "SummitRidge Hospital", "Suncoast Behavioral Health Center", "TMC Bonham Hospital", "Temecula Valley Hospital",
    "Texas NeuroRehab Center", "Texoma Medical Center", "TexomaCare", "Texomacare Specialty Physician",
    "The BridgeWay", "The Brook Hospital - Dupont", "The Brook Hospital - KMI", "The Canyon at Santa Monica",
    "The Carolina Center for Behavioral Health", "The Folsom Sierra Wellness Clinic",
    "The George Washington University Hospital", "The Horsham Clinic", "The Hughes Center",
    "The Meadows Psychiatric Center", "The Pavilion Behavioral Health System",
    "The Pavilion at Northwest Texas Healthcare System", "The Ridge Behavioral Health System",
    "The Vines Behavioral Health Hospital", "Thousand Branches Wellness", "Three Rivers Behavioral Health",
    "Three Rivers Midlands", "Three Trails Behavioral Hospital", "Turning Point Care Center",
    "University Behavioral Center", "University Behavioral Health of Denton", "Valle Vista Health System",
    "Valley Health Physician Alliance", "Valley Health System", "Valley Hospital", "Valley Hospital Medical Center",
    "Via Linda Behavioral Hospital", "Virginia Beach Psychiatric Center", "Wekiva Springs Center",
    "Wellington Physician Alliances", "Wellington Regional Medical Center", "Wellstone Regional Hospital",
    "West Henderson Hospital", "West Oaks Hospital", "Willow Springs Center", "Windmoor Healthcare",
    "Windsor Laurelwood Center for Behavioral Medicine", "Wyoming Behavioral Institute",
)

_QUOTES = {"\u2018": "'", "\u2019": "'", "\u201c": '"', "\u201d": '"', "\u2013": "-", "\u2014": "-",
           "\u00a0": " "}


def clean_text(s) -> str:
    """Entities decoded, curly quotes straightened, whitespace collapsed."""
    s = html.unescape(str(s or ""))
    for bad, good in _QUOTES.items():
        s = s.replace(bad, good)
    return re.sub(r"\s+", " ", s).strip()


def fkey(s) -> str:
    """The comparison key of a name: case, punctuation, "&", "Saint" and a
    leading "The" do not matter ("Prairie St Johns" is "Prairie St. John's")."""
    s = clean_text(s).lower().replace("&", " and ")
    s = re.sub(r"[.'`]", "", s)
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\bsaint\b", "st", s)
    s = re.sub(r"^the\s+", "", s.strip())
    return " ".join(s.split())


_BY_KEY = {fkey(n): n for n in CANONICAL}
_RENAME_BY_KEY = {fkey(k): v for k, v in RENAMES.items()}


def is_system_label(name) -> bool:
    return bool(_SYSTEM_RX.match(clean_text(name)))


def facility(tag) -> str:
    """One clean facility name for a tags4 value (a list or a string); ""
    when it names no facility."""
    if isinstance(tag, (list, tuple)):
        tag = next((t for t in tag if clean_text(t)), "")
    raw = clean_text(tag)
    if not raw or SYSTEM_LEVEL.match(raw) or is_system_label(raw):
        return ""
    k = fkey(raw)
    if k in _RENAME_BY_KEY:
        return _RENAME_BY_KEY[k]
    if k in _BY_KEY:
        return _BY_KEY[k]
    if raw.isupper() and len(raw) > 6:          # a new facility written in capitals
        raw = raw.title()
    return raw


def is_facility(name) -> bool:
    """A stored hospital_name that names a facility (not blank, not the
    system, not an office)."""
    return bool(facility(name))


# text_facility: the known names as one alternation on the key form, longest
# first, so "Spring Valley Hospital Medical Center" is never read as "Valley
# Hospital Medical Center" and "Valley Hospital Medical Center" never as
# "Valley Hospital".
_TEXT_NAMES = sorted({**{fkey(n): n for n in CANONICAL if n not in TEXT_SKIP},
                      **{fkey(a): n for a, n in TEXT_ALIASES.items()}}.items(), key=lambda kv: -len(kv[0]))
_TEXT_RX = re.compile(r"\b(?:" + "|".join(re.escape(k) for k, _ in _TEXT_NAMES) + r")\b")
_TEXT_BY_KEY = dict(_TEXT_NAMES)
# The opening of a description: where a UHS posting names its own facility
# ("Responsibilities Who We Are: Millwood Hospital (a UHS Facility) is ...",
# "About Manatee Memorial Hospital (MMH): ..."). Past it, a posting names
# its sister facilities too (a market's hospitals, a float pool's sites).
TEXT_HEAD_CHARS = 700
_TAGS_RX = re.compile(r"<[^>]+>")


def text_names(text, head_chars: int = TEXT_HEAD_CHARS) -> list:
    """Every known facility the opening of a posting's text names, in order."""
    t = clean_text(_TAGS_RX.sub(" ", str(text or "")))[:head_chars]
    if not t:
        return []
    out = []
    for m in _TEXT_RX.finditer(fkey(t)):
        name = facility(_TEXT_BY_KEY.get(m.group(0), ""))
        if name:
            out.append(name)
    return out


def text_facility(text, head_chars: int = TEXT_HEAD_CHARS, strict: bool = True) -> str:
    """The facility the opening of a posting's text names, or "". strict
    (the default): only when it names one facility and no other (a posting
    that names two, "RESOURCE RN POOL FOR HARTGROVE AND GARFIELD PARK", or a
    market's sister hospitals, names none). Names the table does not know
    are never guessed from text. Checked 2026-10-28 against the tag on the
    3,261 active rows where both name one: 3,210 the same facility (98.4%);
    the 51 others are sister facilities of one market (Murrieta's two
    hospitals, Horizon Health's contract staff named by the hospital they
    work in, a school of Foundations Behavioral Health)."""
    names = text_names(text, head_chars)
    if not names:
        return ""
    if strict and len(set(names)) != 1:
        return ""
    return names[0]
