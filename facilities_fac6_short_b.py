"""The hospital or named site on the label rows tonight's nightly left behind, part B (2026-10-30).

The owner, 2026-10-29: every hospital inside a multi-hospital system named
and placed right, on the site and in the dashboard. His rules stand: (1) cards
and job pages show the HOSPITAL or named site; the system name and logo stay
on the job page and the system hub (hospital_system is never touched here: the
upsert key, the hub, the filters, the logo); (2) named sites show their own
name; generic offices, home-based, remote, multi-site pools and regional roles
show the system name with the town; a town or a site is never guessed.

The 10-30 recount found these systems still on the system name after the
nightly, against the phase 2 estimates. Why each one stayed, and what names it
now (every rule below reads a field of the posting itself):

  TMC Healthcare (TaleoBE RSS)  phase 2 read "Tucson" as the stamped default,
        but the feed's own locationCity says Tucson, and Tucson holds two TMC
        hospitals (Tucson Medical Center on Grant Road, and TMC Rincon, the
        hospital TMC opened on the southeast side in 2024), so the town alone
        names neither. The feed's department does: TMC's cost-centre numbers
        ("634 Med-Surg Overflow", "602 Intensive Care Unit", "506 Rincon ED").
        TMC_DEPARTMENTS lists the main campus's inpatient units and services
        and the Rincon hospital's; clinics (TMCOne, the cancer centres, urgent
        cares, pain clinics), hospice, system offices and system float pools
        keep the label. Read at list time (_parse_taleo_be_rss), every night.
  Luminis Health (Greenhouse)  the board says "Annapolis, MD", where Anne
        Arundel Medical Center and the J. Kent McNew Family Medical Center
        both sit, and phase 2 read it as the stamp. The Greenhouse department
        names the business unit: "LH Anne Arundel Med Ctr - ICU", "LH Doctors
        Comm Med Ctr - CT Scan", "LH McNew Medical Center - Behavioral Health
        Inpt", "LH Pathways - Behavioral Health Inpt" (Luminis Health Pathways,
        2620 Riva Road, Annapolis). The unit is the employing entity: it names
        the hospital only when the posting's own location or office is that
        hospital's town, or its title names the hospital (" - DCMC",
        "@ LHAAMC"); outreach labs (Conexus), corporate services and the
        conference centre keep the label. Read at list time.
  UC Health (Oracle)  phase 2 waited for the Oracle work location; UC Health's
        requisitions carry none (LocationName null on all 510 on 10-30). The
        list payload's ShortDescriptionStr opens "Location: UC Medical Center"
        / "Location: West Chester Hospital": the two hospitals' own names,
        read at list time, in the hospital's own town only.
  Loma Linda University Health (Oracle)  the work location is "Campus/
        Building/Floor"; facilities_oracle_other cuts it at the first "/", so
        "Troesh/Tower Adult/13flr" (the adult floors of the Dennis and Carol
        Troesh Medical Campus tower: the Medical Center) and "Troesh/Tower
        Children's/9flr" (the Children's Hospital) became "Troesh", which
        names neither. Read whole here, with "Loma Linda University Surgical
        Hospital" (26780 Barton Rd, Redlands: LLUH's own location pages) and
        the Medical Center's central service ("MC CENTRAL SERVICE MCA742").
  LifeBridge Health (Jobvite)  Baltimore holds Sinai, Levindale and Grace
        Medical Center; the town never names one. Each posting closes with
        its employer's own "About <hospital>" heading ("About Sinai Hospital",
        "About Levindale Hebrew Geriatric Center", "About Grace Medical
        Center"): the hospital, in that hospital's own town only, never for a
        clinic, office, provider-group, remote or multi-site posting.
  Legacy Health (iCIMS)  Portland holds Emanuel, Good Samaritan, Randall
        Children's and the Unity Center for Behavioral Health. Named only when
        the posting's own text says where ("About Randall Children's
        Hospital", "Join Our Progressive Care Unit (PCU) Team at Legacy Good
        Samaritan", "Primary location will be Legacy Emanuel"), in exactly one
        site, outside the system's inventory sentences; a posting naming two
        sites, a float team, a clinic or an office keeps the label.
  Texas Children's (Oracle)  no work location either. The feed's own town is
        real (PrimaryLocation): The Woodlands and Austin each hold one TCH
        hospital (town table, registered in facilities_town: clinics, urgent
        cares, the health plan and offices keep the label); Houston holds the
        main campus, the Pavilion for Women and the West Campus (18200 Katy
        Fwy, Houston), so a Houston or Katy row is named only when the posting
        says "Pavilion for Women", "West Campus" or a main-campus tower.
  Mosaic Life Care (Oracle)  no work location; Saint Joseph, Maryville and
        Albany each hold one Mosaic hospital (town table). A requisition whose
        PrimaryLocation gives no town reaches finalize with no town (the
        Saint Joseph stamp comes later, in normalize), so the table never
        places it.
  Health First (Phenom)  the Phenom facility field writes codes: "HRMC" is
        Holmes Regional Medical Center (Health First's own postings: "Holmes
        Regional Medical Center (HRMC)"), "CCH" Cape Canaveral Hospital
        (Health First's page: "Health First's Cape Canaveral Hospital (CCH)"),
        "VH" Viera Hospital (Health First's own corporate listing: "viera
        hospital (vh)"); "GATEWAY" (an outpatient site, "HFMG Gateway OB Gyn")
        and "Viera MOB" (a medical office building) are the system name.

Nights without a body: the body readers (LifeBridge, Legacy, Texas Children's
campus cues) leave such a row to facilities_town's stored decision (STORED),
which keeps a stored site name, so a row never flips between nights. The list
readers (TMC, Luminis, UC Health, Loma Linda) read the list payload, which
every night carries.

Shared with the stored-row change list (claud_outputinput/
facility_scale_2026-10-29/phase6/short_b_work/changes.py), so the database
holds what the scraper writes on the next run.
"""
from __future__ import annotations

import logging
import re

import facilities_town as FT

logger = logging.getLogger("scraper.facility_fac6_short_b")

clean_text = FT.clean_text
_h = FT._h

TMC = "TMC Healthcare"
LUMINIS = "Luminis Health"
UCH = "UC Health"
LLU = "Loma Linda University Health"
LIFEBRIDGE = "LifeBridge Health"
LEGACY = "Legacy Health"
TCH = "Texas Children's"
MOSAIC = "Mosaic Life Care"
HEALTH_FIRST = "Health First"


def _tk(city) -> str:
    return FT.town_key(city)


def _no_site_title(system, title) -> bool:
    """A title no one hospital holds: remote, a care site, a multi-site pool,
    a system-office executive (facilities_town's tests)."""
    t = clean_text(title)
    return (FT.is_remote(t, "") or FT.is_care_site(system, t) or FT.is_multi_site(t, "")
            or bool(FT.SYSTEM_ROLE_RX.search(t)))


# ══════════════════════════════════════════════════════════════════════════
#  1. LIST READERS (called from the adapters, every night)
# ══════════════════════════════════════════════════════════════════════════
# TMC: the RSS department ("<cost centre> <name>"), read 2026-10-30 from the
# live feed (344 items). Only departments that are one hospital's own units
# or services are listed; a department not listed keeps the label.
TMC_MAIN = "Tucson Medical Center"          # cms 030006, 5301 E Grant Rd, Tucson
TMC_RINCON = "TMC Rincon"                    # TMC's own name (tmcaz.com, "TMC Rincon celebrates 2 years")
TMC_DEPARTMENTS = {
    # main campus (Grant Road): inpatient units, ED, ORs, and the services
    # that work inside the hospital
    "221 Adult Surg & Supp Staff": TMC_MAIN, "222 Emergency": TMC_MAIN,
    "250 Women & Childrens Svcs": TMC_MAIN, "261 Clinical Nutrition Svcs": TMC_MAIN,
    "302 Anesthesia Bill Only": TMC_MAIN, "314 Peds Hospitalist Bill Only": TMC_MAIN,
    "316 Laborist Bill Only": TMC_MAIN,
    "601 Emergency Svcs": TMC_MAIN, "602 Intensive Care Unit": TMC_MAIN, "603 Peds ED": TMC_MAIN,
    "605 Neonatal Intensive Care": TMC_MAIN, "609 Unit 500 Cardiac Care Unit": TMC_MAIN,
    "616 Bed Control": TMC_MAIN, "617 Operating Room Second Flr": TMC_MAIN, "618 Tower PACU": TMC_MAIN,
    "620 Clinical Decision Unit (CDU)": TMC_MAIN, "622 Pediatrics": TMC_MAIN, "623 Child Life Svcs": TMC_MAIN,
    "625 Unit 600 Cardiac Care Unit": TMC_MAIN, "628 PCCU": TMC_MAIN, "629 Unit 700 Adult Surgical": TMC_MAIN,
    "630 Unit 800 Adult Surgical": TMC_MAIN, "631 Orthopedics": TMC_MAIN,
    "632 Unit 850 Adult Med Surgical": TMC_MAIN, "633 Unit 950 Neuro-Neurosurgery": TMC_MAIN,
    "634 Med-Surg Overflow": TMC_MAIN, "635 Women's Surgery": TMC_MAIN, "636 Labor & Delivery": TMC_MAIN,
    "637 Mother-Baby": TMC_MAIN, "641 Operating Room Tower 3": TMC_MAIN,
    "647 Unit 750 Adult Med Surgical": TMC_MAIN, "656 GI Hospitalists": TMC_MAIN, "662 NICU Bill Only": TMC_MAIN,
    "685 Adm Critical Care": TMC_MAIN, "703 Cardiac Svcs-Non Invasive": TMC_MAIN, "705 Cat Scan-MRI": TMC_MAIN,
    "706 Central Processing": TMC_MAIN, "722 Surgical Support Svcs": TMC_MAIN, "741 Phlebotomy Services": TMC_MAIN,
    "758 Nuclear Medicine": TMC_MAIN, "762 Tower PreOp": TMC_MAIN, "764 PreAnes Tst-Surgery Sched": TMC_MAIN,
    "769 Clinical Pharmacy": TMC_MAIN, "787 Special Procedures-Cath Lab": TMC_MAIN,
    "789 Diagnostic Nursing Svcs": TMC_MAIN, "793 Acute Therapies": TMC_MAIN, "794 Respiratory Care Svcs": TMC_MAIN,
    "801 PST - Sitters": TMC_MAIN, "836 Environmental Svcs": TMC_MAIN, "837 Food Svcs": TMC_MAIN,
    "865 Lactation Services": TMC_MAIN, "906 ER & L&D IP Admitting - Grant": TMC_MAIN,
    "935 Spiritual Care": TMC_MAIN, "962 Transportation": TMC_MAIN, "963 Case Management": TMC_MAIN,
    # the Rincon hospital's own cost centres
    "256 ER & L&D Admitting - Rincon": TMC_RINCON, "502 Rincon Med/Surg 2nd Floor": TMC_RINCON,
    "503 Rincon Labor & Delivery": TMC_RINCON, "504 Rincon Med/Surg 3rd Floor": TMC_RINCON,
    "505 Rincon OR": TMC_RINCON, "506 Rincon ED": TMC_RINCON, "509 Rincon Radiology": TMC_RINCON,
    "512 Rincon Pharmacy": TMC_RINCON, "513 Rincon Food and Nutrition Svc": TMC_RINCON,
    "515 Rincon Facilities": TMC_RINCON, "516 Rincon Cath Lab": TMC_RINCON,
    "520 Rincon Case Management": TMC_RINCON, "521 Rincon Physical Therapy": TMC_RINCON,
    "526 Rincon ACNO": TMC_RINCON,
}
_TMC_DEPT_KEYS = {re.sub(r"\s+", " ", k).strip().lower(): v for k, v in TMC_DEPARTMENTS.items()}
# a clinic's, a cancer centre's, an urgent care's or hospice's posting, whatever its department
_TMC_CLINIC_RX = re.compile(r"\bTMC\s*One\b|\bTMCOne\b|\bclinics?\b|\burgent\s+care\b|\bcancer\s+cent(?:er|re)\b|"
                            r"\bhospice\b|\boffice\b|\boutpatient\b|\bwyatt\b", re.I)
_TMC_NAMES_RX = re.compile(r"\bTMC\b|\bTucson\s+Medical\s+Center\b|\bTMCH\b", re.I)


def tmc_department(dept, title, city="", state=""):
    """(site, city, state) for a TMC row whose RSS department is one of the
    hospitals' own (TMC_DEPARTMENTS), in Tucson; else None."""
    site = _TMC_DEPT_KEYS.get(re.sub(r"\s+", " ", clean_text(dept)).strip().lower())
    if not site:
        return None
    c, st = clean_text(city), clean_text(state).upper()
    if _tk(c) != _tk("Tucson") or st not in ("", "AZ"):
        return None
    t = clean_text(title)
    if _no_site_title(TMC, t) or _TMC_CLINIC_RX.search(t):
        return None
    stripped = _TMC_NAMES_RX.sub(" ", t)
    if FT.OFFSITE_RX.search(stripped) or FT.OFFICE_ROLE_RX.search(stripped):
        return None
    return site, c or "Tucson", "AZ"


def taleo_be_facility(system, dept, title, city="", state=""):
    """The TaleoBE hook (_parse_taleo_be_rss): (site, city, state) or None."""
    if system == TMC:
        return tmc_department(dept, title, city, state)
    return None


# Luminis: the Greenhouse department's business unit.
LUMINIS_UNITS = {
    "LH Anne Arundel Med Ctr": ("Luminis Health Anne Arundel Medical Center", "Annapolis", "MD"),      # cms 210023
    "LH Doctors Comm Med Ctr": ("Luminis Health Doctors Community Medical Center", "Lanham", "MD"),    # cms 210051
    "LH McNew Medical Center": ("Luminis Health J. Kent McNew Family Medical Center", "Annapolis", "MD"),  # cms 214020
    "LH Pathways": ("Luminis Health Pathways", "Annapolis", "MD"),                                       # 2620 Riva Rd
}
_LUMINIS_UNIT_KEYS = {k.lower(): v for k, v in LUMINIS_UNITS.items()}
# a title that names the hospital itself ("- DCMC", "@ LHAAMC", "AAMC - Pathways")
LUMINIS_TITLE_CUES = {
    "Luminis Health Anne Arundel Medical Center": re.compile(r"\b(?:LH)?AAMC\b", re.I),
    "Luminis Health Doctors Community Medical Center": re.compile(r"\b(?:LH)?DCMC\b", re.I),
    "Luminis Health J. Kent McNew Family Medical Center": re.compile(r"\bMcNew\b", re.I),
    "Luminis Health Pathways": re.compile(r"\bPathways\b", re.I),
}
# departments of a unit that are no hospital floor: the outreach lab, the
# conference centre, corporate services, community programs
_LUMINIS_DEPT_NOT = re.compile(r"\b(?:Conexus|Corporate|Doordan|Outreach|Community|Foundation|Home)\b", re.I)


def _towns_of(values) -> set:
    out = set()
    for v in values:
        for part in str(v or "").split(";"):
            part = part.split("|")[0].strip()
            m = re.match(r"^\s*([A-Za-z .'-]{2,40}?)\s*,\s*([A-Z]{2})\b", part)
            if m:
                out.add((_tk(m.group(1)), m.group(2)))
    return out


def _gh_towns(j) -> set:
    """The (town key, ST) pairs a Greenhouse job's own location names; its
    offices only when the location names none (review 2026-10-30: a posting
    located "Lanham, MD" with an Annapolis office and an Anne Arundel
    department names two towns, and is left as it was)."""
    towns = _towns_of([((j or {}).get("location") or {}).get("name")])
    if towns:
        return towns
    return _towns_of([(o or {}).get("location") or (o or {}).get("name") for o in (j or {}).get("offices") or []])


def luminis_department(j, city="", state="", title=""):
    """(hospital, city, state) for a Luminis Greenhouse job whose department's
    business unit is a hospital and whose own location or office is that
    hospital's town (or whose title names the hospital); else None."""
    deps = [str((d or {}).get("name") or "") for d in (j or {}).get("departments") or []]
    dep = clean_text(deps[0]) if deps else ""
    if not dep:
        return None
    unit, _, rest = dep.partition(" - ")
    hit = _LUMINIS_UNIT_KEYS.get(unit.strip().lower())
    if not hit or _LUMINIS_DEPT_NOT.search(rest):
        return None
    t = clean_text(title or (j or {}).get("title") or "")
    if _no_site_title(LUMINIS, t):
        return None
    stripped = re.sub(r"\b(?:LH)?(?:AAMC|DCMC)\b|\bLuminis\s+Health\b|\bPathways\b", " ", t, flags=re.I)
    if FT.OFFSITE_RX.search(stripped) or FT.OFFICE_ROLE_RX.search(stripped):
        return None
    name, hc, hs = hit
    towns = _gh_towns(j)
    # 2026-10-30 (release 6 review A): a posting located in two towns
    # ("Annapolis, MD; Lanham, MD") is no one hospital's unless its title names
    # the unit's hospital ("Environmental Attendant LHDCMC FT"), and a title
    # that names another hospital than the unit ("Occupational Therapist I -
    # PRN - AAMC" in "LH McNew Medical Center - Behavioral Health Inpt") names
    # none
    if (len(towns) > 1 and not LUMINIS_TITLE_CUES[name].search(t)) or \
            any(rx.search(t) for n, rx in LUMINIS_TITLE_CUES.items() if n != name):
        return None
    if (_tk(hc), hs) in towns or LUMINIS_TITLE_CUES[name].search(t):
        return name, hc, hs
    return None


def greenhouse_facility(system, j, city="", state=""):
    """The Greenhouse hook (scrape_greenhouse): (site, city, state) or None."""
    if system == LUMINIS:
        return luminis_department(j, city, state)
    return None


# UC Health: the list payload's ShortDescriptionStr "Location:" line.
UCH_LOCATIONS = {
    "University of Cincinnati Medical Center": ("Cincinnati", "OH", (     # cms 360003
        "uc medical center", "university of cincinnati medical center", "ucmc", "ucmc main",
        "main operating room uc medical center", "ucmc float pool")),
    "West Chester Hospital": ("West Chester", "OH", (                      # cms 360354
        "west chester hospital", "uc west chester hospital", "uc wester hospital")),
}
_UCH_KEYS = {k: (name, c, s) for name, (c, s, ks) in UCH_LOCATIONS.items() for k in ks}
_UCH_LINE_RX = re.compile(r"(?im)^[ \t]*Location[ \t]*:[ \t]*([^\n]*)")
_UCH_CUT_RX = re.compile(r"\s+(?:Department|Dept|Hours|Shift|Schedule|Status|FTE)\s*:.*$", re.I)


def uch_location_value(short_description) -> str:
    m = _UCH_LINE_RX.search(str(short_description or ""))
    if not m:
        return ""
    return _UCH_CUT_RX.sub("", clean_text(m.group(1))).strip()


def _vkey(v) -> str:
    return " ".join(re.findall(r"[a-z0-9]+", clean_text(v).lower()))


def uch_short_location(short_description, title="", city="", state=""):
    """(hospital, city, state) for a UC Health requisition whose short
    description's Location line is a hospital's own name, in that hospital's
    own town (or with no town); else None."""
    hit = _UCH_KEYS.get(_vkey(uch_location_value(short_description)))
    # 2026-10-30 (release 6 review A): and the clinic / office title words the
    # other phase 6 readers apply ("RN, Ambulatory Surgery Center", "Facilities
    # Compliance Specialist")
    if not hit or _no_site_title(UCH, title) or FT.OFFSITE_RX.search(clean_text(title)):
        return None
    name, hc, hs = hit
    c, st = clean_text(city), clean_text(state).upper()
    if c and (_tk(c) != _tk(hc) or st not in ("", hs)):
        return None
    return name, hc, hs


# Loma Linda: the raw work location, before facilities_oracle_other's cut.
LLU_MC = "Loma Linda University Medical Center"                  # cms 050327
LLU_CH = "Loma Linda University Children's Hospital"             # cms 050778
LLU_SH = "Loma Linda University Surgical Hospital"               # 26780 Barton Rd, Redlands (lluh.org locations)
LLU_RULES = (
    (re.compile(r"^\s*Troesh\s*/\s*Tower\s+Adult\b", re.I), (LLU_MC, "Loma Linda", "CA")),
    (re.compile(r"^\s*Troesh\s*/\s*Tower\s+Children", re.I), (LLU_CH, "Loma Linda", "CA")),
    (re.compile(r"^\s*Loma\s+Linda\s+University\s+Surgical\s+Hospital\b", re.I), (LLU_SH, "Redlands", "CA")),
    (re.compile(r"^\s*MC\s+CENTRAL\s+SERVICE\b", re.I), (LLU_MC, "Loma Linda", "CA")),
    # Redlands holds the Behavioral Medicine Center and the Surgical Hospital
    # (facilities_town now reads it as shared): the BMC's own work location
    # names it, as the town alone did before
    (re.compile(r"^\s*Loma\s+Linda\s+University\s+Behavioral\s+Medicine\s+Center\b", re.I),
     ("Loma Linda University Behavioral Medicine Center", "Redlands", "CA")),
)
_LLU_TOWNS = {_tk("Loma Linda"), _tk("Redlands")}


def llu_work_location(location_name, title="", city="", state=""):
    """(site, city, state) for a Loma Linda work location LLU_RULES names, for
    a row in Loma Linda or Redlands (or with no town); else None."""
    v = clean_text(location_name)
    hit = next((val for rx, val in LLU_RULES if rx.search(v)), None)
    if not hit or _no_site_title(LLU, title) or FT.OFFSITE_RX.search(clean_text(title)):    # review A, as UC Health
        return None
    c = clean_text(city)
    if c and _tk(c) not in _LLU_TOWNS:
        return None
    return hit


def _oracle_items(v) -> list:
    if isinstance(v, dict):
        v = v.get("items")
    return [x for x in v if isinstance(x, dict)] if isinstance(v, list) else []


def oracle_facility(system, j, city="", state=""):
    """The Oracle hook (scrape_oracle, for a row the work-location table left
    on the label): (site, city, state) or None."""
    title = (j or {}).get("Title", (j or {}).get("title", ""))
    if system == UCH:
        return uch_short_location((j or {}).get("ShortDescriptionStr", ""), title, city, state)
    if system == LLU:
        for w in _oracle_items((j or {}).get("workLocation", (j or {}).get("WorkLocation"))):
            name = str(w.get("LocationName") or "").strip()
            if name:
                return llu_work_location(name, title, city, state)
    return None


# ══════════════════════════════════════════════════════════════════════════
#  2. TOWN TABLES (registered in facilities_town at import)
# ══════════════════════════════════════════════════════════════════════════
TCH_MAIN = "Texas Children's Hospital"
TCH_PFW = "Texas Children's Pavilion for Women"
TCH_WEST = "Texas Children's Hospital West Campus"
TCH_WOODLANDS = "Texas Children's Hospital The Woodlands"
TCH_AUSTIN = "Texas Children's Hospital North Austin"
TCH_TOWNS = FT._spec(TCH, [
    _h(TCH_MAIN, "Houston", "TX", "cms 453304 (6621 Fannin St)"),
    _h(TCH_PFW, "Houston", "TX", "site (6651 Main St, Houston)"),
    _h(TCH_WEST, "Houston", "TX", "site (18200 Katy Fwy, Houston)"),
    _h(TCH_WOODLANDS, "The Woodlands", "TX", "site (17580 Interstate 45 S, The Woodlands)"),
    _h(TCH_AUSTIN, "Austin", "TX", "cms 453319 (9835 N Lake Creek Pkwy)"),
], labels=("Texas Children's Health System", "Texas Children's Hospital System"))

# Saint Joseph holds two (the medical center and the Heartland long term acute
# care hospital, cms 262019, the hospital registry's own list): a Saint Joseph
# row is named only when its title names one. Maryville, Albany and Bethany
# hold one each. Saint Joseph is also the scraper's stamp for a requisition
# that names no town (SYSTEM_LOCATION_DEFAULTS), so the table reads it as such.
MOSAIC_TOWNS = FT._spec(MOSAIC, [
    _h("Mosaic Life Care at St. Joseph", "Saint Joseph", "MO", "cms 260006 (5325 Faraon St)"),
    _h("Heartland Long Term Acute Care Hospital", "Saint Joseph", "MO", "cms 262019"),
    _h("Mosaic Medical Center - Maryville", "Maryville", "MO", "cms 261340"),
    _h("Mosaic Medical Center - Albany", "Albany", "MO", "cms 261328"),
    _h("Harrison County Community Hospital", "Bethany", "MO", "cms 261312"),
], default=("Saint Joseph", "MO"))
# Mosaic's own programs that are no hospital floor, by the posting's title:
# Family Life Services (outpatient behavioral health), the rural health
# transformation navigators, home services, the Children's Discovery Center,
# community health workers, administrative internships, the dermatology
# practice and the foundation's gift staff (the row keeps the label).
FT.CARE_SITE_BY_SYSTEM[MOSAIC] = re.compile(
    r"\b(?:Family\s+Life\s+Services|RHTP|Home\s+Services|Children'?s\s+Discovery\s+Center|"
    r"Community\s+Health\s+Worker|Administrative\s+Intern|Dermatology|Gifts?)\b", re.I)


# ══════════════════════════════════════════════════════════════════════════
#  3. BODY READERS (this module's pass, before the town tables)
# ══════════════════════════════════════════════════════════════════════════
_SENT_RX = re.compile(r"(?<=[.!?])\s+|\s*\n\s*|\s+[•*]\s+")
_FLOAT_RX = re.compile(r"\bfloat\w*\b|\brotat\w*\b|\bboth\b|\bmultiple\b|\bvarious\b|\beither\b|\bacross\b|"
                       r"\bas\s+well\s+as\b|\bas\s+needed\b|\bother\s+(?:sites|locations|facilities|hospitals)\b",
                       re.I)
_APOS = r"[’']"


def _sentences(description) -> list[str]:
    return [clean_text(x) for x in _SENT_RX.split(str(description or "")) if x and x.strip()]


class CueSpec:
    """One system's body cues: each site with its own spellings, the heading
    "About <site>", and the phrases that say a posting is at a site."""

    def __init__(self, system, sites, *, phrases=True, heading=True, inner=()):
        self.system = system
        self.sites = []                                   # (name, town, ST, compiled spelling rx)
        for name, town, st, spellings in sites:
            alt = "|".join(spellings)
            self.sites.append((name, town, st, alt))
        self.phrases = phrases
        self.heading = heading
        # (outer spelling rx, site): a site spelled inside another's name
        # ("Randall Children's Hospital at Legacy Emanuel" is Randall)
        self.inner = tuple((re.compile(rx, re.I), name) for rx, name in inner)
        self.town_of = {s[0]: (s[1], s[2]) for s in self.sites}

    def head_rx(self, alt):
        return re.compile(r"^(?:about)\s+(?:the\s+)?(?:" + alt + r")\s*:?\s*$", re.I)

    def phrase_rxs(self, alt):
        a = r"(?:" + alt + r")"
        return (
            re.compile(r"\bwork\s+location\s+for\s+this\s+(?:position|role)\s+is\s+(?:the\s+)?" + a + r"\b", re.I),
            re.compile(r"\b(?:primary|work)\s+location\s+(?:will\s+be|is)\s+(?:at\s+)?(?:the\s+)?" + a + r"\b", re.I),
            re.compile(r"^(?:work\s+)?location\s*:\s*(?:the\s+)?" + a + r"\b", re.I),
            re.compile(r"\b(?:team|unit|department|program|position|role|opening)\s+(?:at|for)\s+(?:the\s+)?" + a
                       + r"\b", re.I),
            re.compile(r"\bjoin\s+(?:our|the)\s+[^.]{0,80}?\b(?:at|for)\s+(?:the\s+)?" + a + r"\b", re.I),
            re.compile(r"\b(?:searching|looking|seeking)\s+for\s+[^.]{0,80}?\b(?:at|for|in)\s+(?:the\s+)?" + a + r"\b",
                       re.I),
            re.compile(r"\b" + a + _APOS + r"s\s+(?:[A-Z][\w&/()-]*\s+){0,4}(?:team|unit|department|program|"
                       r"services?)\b"),
        )


def cue_sites(cspec, title, description):
    """The sites the posting's own heading, location phrases or title name
    (a set of names); the system's inventory sentences ("six hospitals,
    including ...") and float or multi-site sentences are read past."""
    found = set()
    texts = [clean_text(title)] + _sentences(description)
    for i, s in enumerate(texts):
        if i and FT.INVENTORY_RX.search(s):
            continue
        work = s
        for rx, name in cspec.inner:
            if rx.search(work):
                if _FLOAT_RX.search(work):
                    return {"*multi*"}
                if i == 0 or (cspec.heading and cspec.head_rx(rx.pattern).search(work)) or \
                        (cspec.phrases and any(p.search(work) for p in cspec.phrase_rxs(rx.pattern))):
                    found.add(name)
                work = rx.sub(" ", work)
        for name, _town, _st, alt in cspec.sites:
            if i == 0:
                hit = bool(re.search(r"\b(?:" + alt + r")\b", work, re.I))
            else:
                hit = (cspec.heading and bool(cspec.head_rx(alt).search(work))) or \
                      (cspec.phrases and any(p.search(work) for p in cspec.phrase_rxs(alt)))
            if hit:
                if i and _FLOAT_RX.search(work):
                    return {"*multi*"}
                found.add(name)
    return found


LIFEBRIDGE_CUES = CueSpec(LIFEBRIDGE, [
    ("Sinai Hospital of Baltimore", "Baltimore", "MD", (r"Sinai\s+Hospital(?:\s+of\s+Baltimore)?",)),
    ("Levindale Hebrew Geriatric Center and Hospital", "Baltimore", "MD",
     (r"Levindale(?:\s+Hebrew\s+Geriatric\s+Center(?:\s+and\s+Hospital)?|\s+Hospital)?",)),
    ("Grace Medical Center", "Baltimore", "MD", (r"Grace\s+Medical\s+Center",)),
    ("Northwest Hospital", "Randallstown", "MD", (r"Northwest\s+Hospital",)),
    ("Carroll Hospital", "Westminster", "MD", (r"Carroll\s+Hospital(?:\s+Center)?",)),
], phrases=True)
# LifeBridge: only the heading and the explicit location lines count (its
# bodies name Sinai and Grace in passing: "the Emergency Departments at Sinai
# and Grace", "teaching opportunities ... at Sinai Hospital")
LIFEBRIDGE_CUES.phrase_rxs = lambda alt: (
    re.compile(r"\bwork\s+location\s+for\s+this\s+(?:position|role)\s+is\s+(?:the\s+)?(?:" + alt + r")\b", re.I),
    re.compile(r"^(?:work\s+)?location\s*:\s*(?:the\s+)?(?:" + alt + r")\b", re.I),
)
# 2026-10-30 (release 6 review A): those passing short names still say the
# posting works at two of the Baltimore hospitals ("placement of psychiatric
# patients in the Emergency Departments at Sinai and Grace" under an "About
# Grace Medical Center" heading): mentioned_sites counts them, so such a
# posting is no one hospital's (decision 2). Case-sensitive: "Grace" and
# "Sinai" as names, never the words.
LIFEBRIDGE_CUES.short = (
    (re.compile(r"(?-i:\bSinai\b)"), "Sinai Hospital of Baltimore"),
    (re.compile(r"(?-i:\bGrace\b)"), "Grace Medical Center"),
    (re.compile(r"(?-i:\bLevindale\b)"), "Levindale Hebrew Geriatric Center and Hospital"),
)

LEGACY_CUES = CueSpec(LEGACY, [
    ("Legacy Emanuel Medical Center", "Portland", "OR",
     (r"Legacy\s+Emanuel(?:\s+(?:Medical\s+Center|Hospital))?", r"Emanuel\s+(?:Medical\s+Center|Hospital)")),
    ("Legacy Good Samaritan Medical Center", "Portland", "OR",
     (r"Legacy\s+Good\s+Samaritan(?:\s+Medical\s+Center)?", r"Good\s+Samaritan\s+Medical\s+Center")),
    ("Randall Children's Hospital", "Portland", "OR", (r"Randall" + _APOS + r"?s?\s+Children" + _APOS + r"?s\s+Hospital",)),
    ("Unity Center for Behavioral Health", "Portland", "OR", (r"Unity\s+Center\s+for\s+Behavioral\s+Health",)),
    ("Legacy Mount Hood Medical Center", "Gresham", "OR", (r"Legacy\s+Mount\s+Hood(?:\s+Medical\s+Center)?",)),
    ("Legacy Meridian Park Medical Center", "Tualatin", "OR", (r"Legacy\s+Meridian\s+Park(?:\s+Medical\s+Center)?",)),
    ("Legacy Salmon Creek Medical Center", "Vancouver", "WA", (r"Legacy\s+Salmon\s+Creek(?:\s+Medical\s+Center)?",)),
    ("Legacy Silverton Medical Center", "Silverton", "OR", (r"Legacy\s+Silverton(?:\s+Medical\s+Center)?",)),
], inner=((r"Randall" + _APOS + r"?s?\s+Children" + _APOS + r"?s\s+Hospital\s+at\s+Legacy\s+Emanuel"
           r"(?:\s+Medical\s+Center)?", "Randall Children's Hospital"),))

TCH_CUES = CueSpec(TCH, [
    (TCH_PFW, "Houston", "TX", (r"(?:Texas\s+Children" + _APOS + r"s\s+)?Pavilion\s+for\s+Women",)),
    (TCH_WEST, "Houston", "TX", (r"(?:Texas\s+Children" + _APOS + r"s\s+(?:Hospital\s+)?)?West\s+Campus",)),
    (TCH_MAIN, "Houston", "TX", (r"Legacy\s+Tower", r"West\s+Tower", r"Abercrombie(?:\s+Building)?")),
], heading=False)
# Houston holds the three sites; the West Campus rows are written Katy (its
# street is the Katy Freeway); Houston is its address's town.
TCH_CUE_TOWNS = {_tk("Houston"), _tk("Katy")}

CUE_SPECS = {LIFEBRIDGE: LIFEBRIDGE_CUES, LEGACY: LEGACY_CUES, TCH: TCH_CUES}

# Titles a body cue never places (review 2026-10-30): an outpatient, residential
# or community program, an IS / applications office, credentialing (the system's
# verification office), a job fair, a motion analysis lab, audiology, outpatient
# therapy staff. Decision 2: the system name with the town.
_CUE_NOT_TITLE_RX = re.compile(
    r"\b(?:Otpt|Outpatient|Residential|Community|Credentialing|Applications?\s+(?:Analyst|Team|Lead)|"
    r"Job\s+Fair|Motion\s+Analysis|Audiology|Therapy\s+(?:Technician|Manager))\b|\b(?-i:IS|OP)\b", re.I)
# the same words keep a Texas Children's row off the town table's hospital
FT.CARE_SITE_BY_SYSTEM[TCH] = re.compile(
    r"\b(?:Outpatient|Job\s+Fair|Motion\s+Analysis|Audiology|Therapy\s+(?:Technician|Manager))\b", re.I)


def mentioned_sites(cspec, title, description):
    """Every site of the table the posting spells anywhere outside the
    system's inventory sentences (a posting naming two sites is no one
    site's: "multi-site facilities oversight for Sinai Hospital, Grace Medical
    Center, and Levindale", "born at the Pavilion for Women ... the West
    Tower")."""
    found = set()
    for i, s in enumerate([clean_text(title)] + _sentences(description)):
        if i and FT.INVENTORY_RX.search(s):
            continue
        work = s
        for rx, name in cspec.inner:
            if rx.search(work):
                found.add(name)
                work = rx.sub(" ", work)
        for name, _town, _st, alt in cspec.sites:
            if re.search(r"\b(?:" + alt + r")\b", work, re.I):
                found.add(name)
        for rx, name in getattr(cspec, "short", ()):
            if rx.search(work):
                found.add(name)
    return found


def _other_hospital(spec, title, description, site) -> bool:
    """The posting's title or location line names a hospital of the system
    other than `site` (facilities_town's test: it knows better)."""
    if spec is None:
        return False
    look = FT.name_key(" | ".join([clean_text(title)] + FT.where_lines(description)))
    named = set(FT._named_hospitals(spec, look, title))
    return bool(named - {site})


def cue_decide(system, hospital_name, city, state, title="", description=""):
    """(site, city, state, how) for a label row of a body-cue system, or
    (None, None, None, how)."""
    s = (system or "").strip()
    cspec = CUE_SPECS.get(s)
    if cspec is None:
        return None, None, None, "not-cue"
    spec = FT.SPECS.get(s)
    if clean_text(hospital_name) not in ("", s) and not (spec and spec.is_label(hospital_name)):
        return None, None, None, "named-already"
    if len(str(description or "").strip()) < FT.NO_BODY_CHARS:
        return None, None, None, "no-body"
    t = clean_text(title)
    if (FT.is_remote(t, description) or FT.is_care_site(s, t) or FT.is_multi_site(t, description)
            or FT.where_not_hospital(description) or FT.SYSTEM_ROLE_RX.search(t)):
        return None, None, None, "no-site"
    if FT.is_offsite(t, description, spec) or FT.is_group_provider(t) or _CUE_NOT_TITLE_RX.search(t):
        return None, None, None, "offsite"
    sites = cue_sites(cspec, t, description)
    if not sites:
        return None, None, None, "none"
    if len(sites) != 1 or "*multi*" in sites or len(sites | mentioned_sites(cspec, t, description)) != 1:
        return None, None, None, "several"
    site = next(iter(sites))
    if _other_hospital(spec, t, description, site):
        return None, None, None, "other-hospital"
    town, st = cspec.town_of[site]
    c, js = clean_text(city), clean_text(state).upper()
    if not c:
        return None, None, None, "no-town"
    if s == TCH:
        ok = _tk(c) in TCH_CUE_TOWNS and js in ("", "TX")
    else:
        ok = _tk(c) == _tk(town) and js in ("", st)
    if not ok:
        return None, None, None, "elsewhere"
    if s == TCH:
        return site, town, st, "cue"
    return site, FT._town_spelling(spec, c, st) if spec else c, st, "cue"


# Health First: the Phenom facility field's codes.
HEALTH_FIRST_CODES = {
    "HRMC": ("Holmes Regional Medical Center", "Melbourne", "FL"),     # cms 100019
    "CCH": ("Cape Canaveral Hospital", "Cocoa Beach", "FL"),           # cms 100177
    "VH": ("Viera Hospital", "Melbourne", "FL"),                       # cms 100315
    "GATEWAY": (HEALTH_FIRST, "", ""),
    "VIERA MOB": (HEALTH_FIRST, "", ""),
}
_NOT_A_TOWN_RX = re.compile(r"\bCounty\b|^\s*$", re.I)


def health_first_code(hospital_name, city="", state=""):
    """(name, city, state) for a Health First row whose name is a feed code;
    else None. A hospital keeps the row's own town when it is that hospital's,
    takes the hospital's town when the row has none (or a county); a row in
    another town keeps the system name there."""
    hit = HEALTH_FIRST_CODES.get(re.sub(r"\s+", " ", clean_text(hospital_name)).upper())
    if not hit:
        return None
    name, hc, hs = hit
    c, st = clean_text(city), clean_text(state).upper()
    if name == HEALTH_FIRST:
        return name, c, st
    if _NOT_A_TOWN_RX.search(c):
        return name, hc, hs
    if _tk(c) == _tk(hc) and st in ("", hs):
        return name, c, hs
    return HEALTH_FIRST, c, st


# ══════════════════════════════════════════════════════════════════════════
#  4. RUN TIME (scraper.finalize_jobs, after the phase 5 passes, before the town tables)
# ══════════════════════════════════════════════════════════════════════════
SYSTEMS = frozenset({TMC, LUMINIS, UCH, LLU, LIFEBRIDGE, LEGACY, TCH, MOSAIC, HEALTH_FIRST})


def _set(job, name, city, state) -> None:
    job.hospital_name = name
    if city and (city, state) != (clean_text(job.city), clean_text(job.state).upper()):
        job.city, job.state, job.location = city, state, f"{city}, {state}"


def apply_facility(job) -> str:
    s = (job.hospital_system or "").strip()
    if s == HEALTH_FIRST:
        hit = health_first_code(job.hospital_name, job.city, job.state)
        if not hit:
            return ""
        _set(job, *hit)
        return "code"
    if s not in CUE_SPECS:
        return ""
    name, city, state, how = cue_decide(s, job.hospital_name, job.city, job.state, job.title, job.description)
    if not name:
        return how if how not in ("not-cue", "named-already") else ""
    _set(job, name, city, state)
    return how


ROW_ERRORS: dict = {}


def apply_facilities(jobs) -> dict:
    """apply_facility over a run's rows. A row apply_facility raises on keeps
    the name and town it had ("error"). Returns {system: {how: rows}}."""
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
                logger.warning(f"Phase 6 B: one {job.hospital_system} row kept its name ({type(e).__name__}: {e})")
            how = "error"
        if how:
            d = stats.setdefault(job.hospital_system, {})
            d[how] = d.get(how, 0) + 1
    return stats
