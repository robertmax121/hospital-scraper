"""The hospital on the rows ten large systems still filed under the system name (2026-10-29, phase 3, group B).

The owner, 2026-10-29 17:30 UTC: "i thought you were gonna edit all the large
multihospital systems". His two rules stand: (1) cards and job pages show the
HOSPITAL or named site ("Apply to <site>"); the system name and logo stay on
the job page and the system hub (hospital_system is never touched here: the
upsert key, the hub, the filters, the logo); (2) named sites (hospitals,
named clinics and centers) show their own name; generic offices, home-based
and remote jobs show the system name with the town; a town is never guessed.

What each system's postings carry, and who names it (stored rows read
2026-10-29, after the phase 2 SQL):

  Geisinger (Workday)   the body opens "Location:" then the site ("Geisinger
                        Lewistown Hospital (GLH)", "Geisinger Primary Care
                        Pottsville", "Work from home (Pennsylvania)"); a few
                        Workday descriptors are the site itself, not a town
                        ("Geisinger-Medical-Center-GMC"). Here: a text Spec
                        (facilities_text) and a Workday site table.
  Sutter Health (Workday) the body's "Organization:" line is the employing
                        entity ("CPMC-California Pacific Med Center Van
                        Ness", "SMF-Sutter Med Foundation- Central"); a
                        hospital entity names its hospital in its own town,
                        a medical foundation, home care or system office
                        keeps the label. Here: a text Spec (entity line).
  Allegheny Health Network (Workday) the descriptor is "Town ST, zip, street"
                        ("Pittsburgh PA, 15212, 320 E N Ave."); the hospital
                        streets are listed, every other street is a clinic or
                        an office. Here: a Workday site table.
  Jefferson Health (Workday) the descriptor is a county or a neighbourhood
                        ("Montgomery County", "Northeast Philadelphia"); the
                        body's "Primary Location Address" line is the street
                        with its town ("1200 Old York Road, Abington,
                        Pennsylvania, United States of America"). Here: the
                        hospital streets name the hospital; any other street
                        gives a county row its real town (after the town
                        tables, so the town table never guesses from it).
  Ochsner Health (Workday) the descriptor is a town and a region; the title
                        often names the campus ("... - Jeff Hwy", "- Baptist",
                        "- OLGMC", "- OUHC Respiratory Therapy", "- Chabert").
                        Here: a title table, only for a title that names one
                        campus in the row's own town and is no clinic's or
                        office's (before the town tables).
  Kaiser Permanente (TalentBrew) the body's "Department:" line opens with
                        the site; phase 2 listed the medical centers, here
                        the named medical offices and specialty centers
                        ("Sunnybrook Medical Ofcs" = Kaiser Permanente
                        Sunnybrook Medical Offices), each with the town its
                        postings sit in (facilities_phenom's Kaiser table).
  Huntsville Hospital Health System (classic iCIMS) the title names a site
                        ("... - Crestwood Medical Center - FT", "Madison
                        Hospital Emergency Department", "DMH"); the towns of
                        its regional hospitals each hold one hospital. Here:
                        facilities_icims' title table gains Crestwood, the
                        Madison Hospital units and DMH, and a town table
                        (facilities_town) names a one-hospital town's rows.

Not here (the nightly names them from live feed fields the database does not
store, with the code already on main): Yale New Haven (Jibe tags7, the
street), Fairview (Jibe tags9, the site's own name), Adventist Health (the
Oracle work location "<code> - Hospital"; this module only adds the two
hospital towns its table lacked, The Dalles and Selma).

Hooks (scraper.py): this module registers its tables in the family modules
at import (before scraper.WD_SITE_FACILITY is built), and finalize_jobs calls
apply_before_town (after the text pass, before the town tables) and
apply_after_town (after the town tables). Shared with the stored-row change
list (claud_outputinput/facility_scale_2026-10-29/phase3/hsb/changes.py).
"""
from __future__ import annotations

import re

import facilities_icims
import facilities_phenom
import facilities_text
import facilities_town
import facilities_workday
from city_utils import known_town
from facility_names import clean_text, fkey

GEISINGER = "Geisinger"
SUTTER = "Sutter Health"
AHN = "Allegheny Health Network"
JEFFERSON = "Jefferson Health"
OCHSNER = "Ochsner Health"
KAISER = "Kaiser Permanente"
HUNTSVILLE = "Huntsville Hospital Health System"

SYSTEMS = frozenset({GEISINGER, SUTTER, AHN, JEFFERSON, OCHSNER, KAISER, HUNTSVILLE})


# ══════════════════════════════════════════════════════════════════════════
#  1. WORKDAY SITE TABLES (facilities_workday: the adapter's WD_SITE_FACILITY)
# ══════════════════════════════════════════════════════════════════════════
# Allegheny Health Network: the hospital streets of "Town ST, zip, street"
# descriptors (the externalPath segment is the same text slugged). Streets
# from the CMS hospital list (public.hospitals) unless noted. Every other
# street (420 E North Ave, 4815 Liberty Ave, 2580 Haymaker Rd ...) is an
# outpatient building or an office and is placed as before.
facilities_workday._t(AHN, """
Pittsburgh-PA-15212-320-E-N-Ave                     | Allegheny General Hospital | Pittsburgh | PA | cms 390050
Pittsburgh-PA-15224-West-Penn-4800-Friendship-Ave   | West Penn Hospital | Pittsburgh | PA | cms 390090
Erie-PA-16502-232-West-25th-St                      | Saint Vincent Hospital | Erie | PA | cms 390009
Jefferson-Hills-PA-15025-565-Coal-Valley-Road       | Jefferson Hospital | Jefferson Hills | PA | cms 390265
Monroeville-PA-15146-2570-Haymaker-Road             | Forbes Hospital | Monroeville | PA | cms 390267
Natrona-Heights-PA-15065-1301-Carlisle-Street       | Allegheny Valley Hospital | Natrona Heights | PA | cms 390032
Wexford-PA-15090-12351-Perry-Highway                | AHN Wexford Hospital | Wexford | PA | cms 390334
Canonsburg-PA-15317-100-Medical-Blvd                | Canonsburg Hospital | Canonsburg | PA | cms 390160
Grove-City-PA-16127-631-N-Broad-Street              | Grove City Hospital | Grove City | PA | cms 390266
Westfield-NY-14787-189-E-Main-St                    | Westfield Memorial Hospital | Westfield | NY | cms 330166
Greensburg-PA-15601-6321-Route-30                   | AHN Hempfield Neighborhood Hospital | Greensburg | PA | cms 390333
""")

# Geisinger: the few descriptors that are the site, not a town (the row's
# town parsed as the site's name, or the tenant default Danville).
facilities_workday._t(GEISINGER, """
Geisinger-Medical-Center-GMC                        | Geisinger Medical Center | Danville | PA | cms 390006
Geisinger-Wyoming-Valley-GWV                        | Geisinger Wyoming Valley Medical Center | Wilkes-Barre | PA | cms 390270
Geisinger-South-Wilkes-Barre-GSWB                   | Geisinger South Wilkes-Barre | Wilkes-Barre | PA | cms 390169
Geisinger-Community-Medical-Center-GCMC             | Geisinger Community Medical Center | Scranton | PA | cms 390001
Geisinger-Lewistown-Hospital-GLH                    | Geisinger Lewistown Hospital | Lewistown | PA | cms 390048
Geisinger-Bloomsburg-Hospital-GBH                   | Geisinger Bloomsburg Hospital | Bloomsburg | PA | cms 390003
Geisinger-Shamokin-Area-Community-Hospital-GSACH    | Geisinger Shamokin Area Community Hospital | Coal Township | PA | cms 390189
Geisinger-Medical-Center-Muncy-GMCM                 | Geisinger Medical Center Muncy | Muncy | PA | cms 390337
Geisinger-Jersey-Shore-Hospital-GJSH                | Geisinger Jersey Shore Hospital | Jersey Shore | PA | cms 391300
Geisinger-Healthplex-State-College                  | Geisinger Healthplex State College | Port Matilda | PA | rows
Geisinger-Primary-Care-Pottsville                   | Geisinger Primary Care Pottsville | Pottsville | PA | site
Geisinger-Multi-Specialty-Clinic-Wilkes-Barre       | Geisinger Multi-Specialty Clinic Wilkes-Barre | Wilkes-Barre | PA | site
Geisinger-Multi-Specialty-Clinic-Forty-Fort         | Geisinger Multi-Specialty Clinic Forty-Fort | Forty Fort | PA | site
Geisinger-Urology-Selinsgrove                       | Geisinger Urology Selinsgrove | Selinsgrove | PA | site
Geisinger-Wyoming-Valley-Courier-Hub                | - | Wilkes-Barre | PA | site
Geisinger-Emergency-Medical-Services-Danville       | - | Danville | PA | site
Geisinger-Emergency-Medical-Services-Rohrsburg      | - | Rohrsburg | PA | site
Geisinger-Life-Flight-Hangar-3-Avoca                | - | Avoca | PA | site
Geisinger-Life-Flight-Hangar-4-Montoursville        | - | Montoursville | PA | site
""")


# ══════════════════════════════════════════════════════════════════════════
#  2. TEXT SPECS (facilities_text: the body's own site or entity line)
# ══════════════════════════════════════════════════════════════════════════
S = facilities_text.S
Read = facilities_text.Read
Spec = facilities_text.Spec

# Geisinger: "Location:" then the site, the hospitals by their CMS names and
# the named clinics and centers by their own names (the town every posting
# naming the site sits in, or the town the name itself says: "board").
# Offices, office buildings, warehouses, hangars, EMS bases, schools and the
# health plan keep the label (decision 2), as do other systems'
# hospitals (Wayne Memorial, Mount Nittany) and unbranded programmes
# ("Community Care Wilkes-Barre"). No town is moved (default None): a
# posting whose own town is not the site's keeps the label.
_GEISINGER_SITES = (
    S("Geisinger Medical Center", "Danville", "PA", "cms 390006", "Geisinger Medical Center (GMC)"),
    S("Geisinger Wyoming Valley Medical Center", "Wilkes-Barre", "PA", "cms 390270", "Geisinger Wyoming Valley",
      "Geisinger Wyoming Valley (GWV)", "Geisinger Wyoming Valley Medical Center (GWV)"),
    S("Geisinger Community Medical Center", "Scranton", "PA", "cms 390001",
      "Geisinger Community Medical Center (GCMC)"),
    S("Geisinger South Wilkes-Barre", "Wilkes-Barre", "PA", "cms 390169", "Geisinger South Wilkes-Barre (GSWB)"),
    S("Geisinger Lewistown Hospital", "Lewistown", "PA", "cms 390048", "Geisinger Lewistown Hospital (GLH)"),
    S("Geisinger Bloomsburg Hospital", "Bloomsburg", "PA", "cms 390003", "Geisinger Bloomsburg Hospital (GBH)"),
    S("Geisinger Shamokin Area Community Hospital", "Coal Township", "PA", "cms 390189",
      "Geisinger Shamokin Area Community Hospital (GSACH)"),
    S("Geisinger Medical Center Muncy", "Muncy", "PA", "cms 390337", "Geisinger Medical Center Muncy (GMCM)"),
    S("Geisinger Jersey Shore Hospital", "Jersey Shore", "PA", "cms 391300", "Geisinger Jersey Shore Hospital (GJSH)"),
    S("Geisinger St. Luke's Hospital", "Orwigsburg", "PA", "cms 390332"),
    S("Geisinger Behavioral Health Center Northeast", "Moosic", "PA", "cms 394059",
      "Behavioral Health Center Northeast"),
    # named outpatient sites
    S("Geisinger Healthplex State College", "Port Matilda", "PA", "board",
      campuses=(("State College", "PA", ""),)),
    S("Geisinger Healthplex Woodbine", "Danville", "PA", "board"),
    S("Geisinger Healthplex CenterPoint", "Pittston", "PA", "board"),
    S("Geisinger Multi-Specialty Clinic Olive Street", "Scranton", "PA", "board"),
    S("Geisinger Multi-Specialty Clinic Ash Street", "Scranton", "PA", "board"),
    S("Geisinger Multi-Specialty Clinic Wilkes-Barre", "Wilkes-Barre", "PA", "board"),
    S("Geisinger Multi-Specialty Clinic Forty-Fort", "Forty Fort", "PA", "board"),
    S("Geisinger Multi-Specialty Clinic Lewistown", "Lewistown", "PA", "board"),
    S("Geisinger Multi-Specialty Tunkhannock", "Tunkhannock", "PA", "board"),
    S("Geisinger ConvenientCare+ & Multi-Specialty Clinic Selinsgrove", "Selinsgrove", "PA", "board"),
    S("Geisinger Wyoming Valley Specialty Clinic", "Wilkes-Barre", "PA", "board"),
    S("Geisinger Wyoming Valley Outpatient Specialty Center", "Wilkes-Barre", "PA", "board"),
    S("Geisinger South Wilkes-Barre Community Practice", "Wilkes-Barre", "PA", "board"),
    S("Geisinger Medical Clinic Lock Haven", "Lock Haven", "PA", "board"),
    S("Geisinger Medical Clinic Scenery Park", "State College", "PA", "board"),
    S("Geisinger Medical Clinic Buckhorn", "Bloomsburg", "PA", "board"),
    S("Geisinger Viewmont Clinic", "Scranton", "PA", "board"),
    S("Geisinger Viewmont Imaging", "Scranton", "PA", "board"),
    S("Geisinger Cancer Center Knapper Clinic", "Danville", "PA", "board"),
    S("Geisinger Cancer Center Dickson City", "Dickson City", "PA", "board"),
    S("Geisinger Cancer Center Lewisburg", "Lewisburg", "PA", "board"),
    S("Geisinger Cancer Center Hazleton", "Hazleton", "PA", "board"),
    S("Geisinger Behavioral Health Danville", "Danville", "PA", "board"),
    S("Geisinger Community Health Services & Sleep Disorder Center", "Danville", "PA", "board"),
    S("Geisinger Child Advocacy Center", "Sunbury", "PA", "board"),
    S("Geisinger Marworth", "Waverly", "PA", "board"),
    S("Geisinger Orthopaedics and Sports Medicine Steamtown", "Scranton", "PA", "board"),
    S("Geisinger Otolaryngology Pottsville", "Pottsville", "PA", "board"),
    S("Geisinger Gastroenterology & Endoscopy Lewistown", "Lewistown", "PA", "board"),
    S("Geisinger 65 Forward Kingston", "Kingston", "PA", "board"),
    S("LIFE Geisinger Wilkes-Barre", "Wilkes-Barre", "PA", "board"),
    S("LIFE Geisinger Lewistown", "Lewistown", "PA", "board"),
    S("LIFE Geisinger Minersville", "Minersville", "PA", "board"),
    S("Geisinger ConvenientCare Danville", "Danville", "PA", "board"),
    S("Geisinger ConvenientCare Lewistown", "Burnham", "PA", "board"),
    S("Geisinger ConvenientCare State College", "State College", "PA", "board"),
    S("Geisinger ConvenientCare Bellefonte", "Bellefonte", "PA", "board"),
    S("Geisinger ConvenientCare Berwick", "Berwick", "PA", "board"),
    S("Geisinger ConvenientCare Dallas", "Dallas", "PA", "board"),
    S("Geisinger ConvenientCare Williamsport", "Williamsport", "PA", "board"),
    S("Geisinger ConvenientCare McElhattan", "McElhattan", "PA", "board"),
    S("Geisinger Pediatrics Bloomsburg", "Bloomsburg", "PA", "board"),
    S("Geisinger Pediatrics Scranton", "Scranton", "PA", "board"),
    S("Geisinger Pediatrics Pittston", "Pittston", "PA", "board"),
    S("Geisinger Urology Selinsgrove", "Selinsgrove", "PA", "board"),
    *(S(f"Geisinger Primary Care {t}", t, "PA", "board") for t in (
        "Pottsville", "Mount Pocono", "Selinsgrove", "Pittston", "Mountain Top", "Elysburg", "Kingston",
        "Nanticoke", "Milton", "Huntingdon", "Philipsburg", "Berwick", "Lock Haven", "Lewisburg", "Tunkhannock",
        "Mount Carmel", "Benton", "Frackville", "Sunbury", "Orwigsburg", "Kulpmont")),
    S("Geisinger Primary Care Montage", "Moosic", "PA", "board"),
)
_GEISINGER_OFFICES = (
    "Work from home", "Work from home (Pennsylvania)", "Mill Street Office Building", "Justin Drive Office Building II",
    "620 Baltimore Drive", "Geisinger Office Building I (GOB I)", "Geisinger Office Building II (GOB II)",
    "Geisinger Centralized Logistics & Materials Distribution Center (CLAM)", "Geisinger Print Shop",
    "Geisinger Health Plan Hughes Center", "Henry Hood Center For Health Research",
    "Geisinger Medical Center Annex Building", "GWV Medical Office Building", "Windmere Office Building",
    "Medical Office Building Orwigsburg", "Geisinger CenterPoint Office Building",
    "Geisinger Commonwealth School of Medicine Scranton Campus", "Geisinger School of Nursing",
    "Geisinger Wyoming Valley Courier Hub", "Geisinger Life Flight Headquarters Danville",
)
GEISINGER_SPEC = Spec(GEISINGER, None, (Read("label", "Location", "site"),), _GEISINGER_SITES,
                      offices=_GEISINGER_OFFICES)

# Sutter Health: the "Organization:" entity. A hospital entity is the
# hospital (CMS names, Sutter's own spelling where it differs only by the
# prefix); campuses of one hospital in another town are listed. The medical
# foundations (SPMF, SMF, PAMF, SEBMF, SGMF, Sansum Clinic), Sutter Care at
# Home, the shared lab, outpatient services, the alliance and the system
# offices are no hospital: the label stays (decision 2).
_SUTTER_SITES = (
    S("California Pacific Medical Center - Van Ness Campus", "San Francisco", "CA", "cms 050047",
      "CPMC-California Pacific Med Center Van Ness"),
    S("California Pacific Medical Center - Davies Campus", "San Francisco", "CA", "cms 050008",
      "CPMC-California Pacific Med Center Davies"),
    S("California Pacific Medical Center - Mission Bernal Campus", "San Francisco", "CA", "cms 050055",
      "CPMC-California Pacific Med Center - Mission Bernal"),
    S("California Pacific Medical Center - Pacific Campus", "San Francisco", "CA", "address 2333 Buchanan St, San Francisco",
      "CPMC-California Pacific Med Center Pacific"),
    S("California Pacific Medical Center", "San Francisco", "CA", "cms 050047 (all campuses)",
      "CPMC-California Pacific Med Center"),
    S("Mills-Peninsula Medical Center", "Burlingame", "CA", "cms 050007",
      "SMPMC-Sutter Mills Peninsula Medical Center", "MPHS-Mills-Peninsula Medical Center",
      campuses=(("San Mateo", "CA", ""),)),
    S("Memorial Medical Center", "Modesto", "CA", "cms 050557", "MMC-Memorial Medical Center"),
    S("Alta Bates Summit Medical Center - Alta Bates Campus", "Berkeley", "CA", "cms 050305", "ABSMC-Ashby Campus"),
    S("Alta Bates Summit Medical Center - Summit Campus", "Oakland", "CA", "cms 050043", "ABSMC-Summit Campus"),
    S("Alta Bates Summit Medical Center - Herrick Campus", "Berkeley", "CA", "address 2001 Dwight Way, Berkeley",
      "ABSMC-Alta Bates Herrick Campus"),
    S("Alta Bates Summit Medical Center", "Oakland", "CA", "cms 050043", "ABSMC-Alta Bates Summit Med Center",
      campuses=(("Berkeley", "CA", ""),)),
    S("Eden Medical Center", "Castro Valley", "CA", "cms 050488", "SEMC - Sutter Eden Medical Center"),
    S("Sutter Medical Center Sacramento", "Sacramento", "CA", "cms 050108", "SMCS-Sutter Med Center - Sacramento"),
    S("Sutter Lakeside Hospital", "Lakeport", "CA", "cms 051329", "SLH-Sutter Lakeside Hospital"),
    S("Sutter Santa Rosa Regional Hospital", "Santa Rosa", "CA", "cms 050291", "SSRRH-Sutter Santa Rosa Reg Hospital"),
    S("Sutter Roseville Medical Center", "Roseville", "CA", "cms 050309", "SRMC-Sutter Roseville Med Center"),
    S("Sutter Coast Hospital", "Crescent City", "CA", "cms 050417", "SCH-Sutter Coast Hospital"),
    S("Sutter Delta Medical Center", "Antioch", "CA", "cms 050523", "SDMC-Sutter Delta Medical Center"),
    S("Sutter Amador Hospital", "Jackson", "CA", "cms 050014", "SAH-Sutter Amador Hospital"),
    S("Sutter Solano Medical Center", "Vallejo", "CA", "cms 050101", "SSMC-Sutter Solano Medical Center"),
    S("Novato Community Hospital", "Novato", "CA", "cms 050131", "NCH-Novato Community Hospital"),
    S("Sutter Auburn Faith Hospital", "Auburn", "CA", "cms 050498", "SAFH-Sutter Auburn Faith Hospital"),
    S("Sutter Center for Psychiatry", "Sacramento", "CA", "cms 054096", "SCP-Sutter Center for Psychiatry"),
    S("Memorial Hospital Los Banos", "Los Banos", "CA", "cms 050528", "MHLB-Memorial Hospital Los Banos"),
    S("Sutter Davis Hospital", "Davis", "CA", "cms 050537", "SDH-Sutter Davis Hospital"),
    S("Sutter Maternity & Surgery Center of Santa Cruz", "Santa Cruz", "CA", "cms 050714",
      "PAMF-Sutter Maternity and Surg Ctr"),
    S("Sutter Tracy Community Hospital", "Tracy", "CA", "cms 050313", "STCH-Sutter Tracy Community Hosp"),
    S("Sutter Surgical Hospital - North Valley", "Yuba City", "CA", "cms 050766",
      "SSHNV-Sutter Surgical Hospital of North Valley"),
)
_SUTTER_OFFICES = (
    "SHSO-Sutter Health System Office-Valley", "SHSO-Sutter Health System Office-Bay", "SMCS-Valley Administration",
    "SHSO-Administrative Payroll", "SHSO-Population Health Services-Valley", "Bay Administration",
    "SPMF-Sutter Pacific Medical Foundation - South", "SPMF-Sutter Pacific Medical Foundation - North",
    "SMF-Sutter Med Foundation- Central", "SMF-Sutter Med Foundation - North", "SMF-Sutter Med Foundation - West",
    "SC - Sansum Clinic", "PAMF-Palo Alto Medical Foundation CAD", "PAMF-Palo Alto Medical Foundation PAD",
    "PAMF-Palo Alto Medical Foundation ALD", "PAMF-Palo Alto Medical Foundation SCD",
    "PAMF-Palo Alto Medical Foundation MPD", "SEBMF-East Bay Medical Foundation", "SGMF-Sutter Gould Med Foundation",
    "SCAH-Sutter Care at Home - Bay", "SCAH-Sutter Care at Home - Valley", "SSL-Sutter Shared Lab-Bay",
    "Sutter Health Alliance - Valley", "Sutter Community Health", "SSC-Sutter Senior Care",
)
SUTTER_SPEC = Spec(SUTTER, None, (Read("label", "Organization", "entity"),), _SUTTER_SITES, offices=_SUTTER_OFFICES)


def _register_text_spec(spec) -> None:
    if spec.system in facilities_text.SPECS and facilities_text.SPECS[spec.system] is not spec:
        raise ValueError(f"facilities_text already has a spec for {spec.system}")
    facilities_text.SPECS[spec.system] = spec
    facilities_text.SYSTEMS = frozenset(facilities_text.SPECS)


for _spec in (GEISINGER_SPEC, SUTTER_SPEC):
    _register_text_spec(_spec)


# ══════════════════════════════════════════════════════════════════════════
#  3. KAISER PERMANENTE: named medical offices and specialty centers
#     (facilities_phenom's Kaiser "Department:" table, "prefix" match)
# ══════════════════════════════════════════════════════════════════════════
# The Department line's site part ("Sunnybrook Medical Ofcs - Day Surgery -
# 1001"). Each entry's town is the town its postings sit in (2026-10-29, the
# stored active rows); a posting elsewhere keeps the label, one at Kaiser's
# head-office default (Oakland) takes the entry's town. Not listed: medical
# office BUILDINGS ("Baldwin Park MOB", "Panorama City MOB #2"), regional and
# administrative offices, labs, call and service centers, other systems'
# hospitals (Virginia Hospital Center, Holy Cross Hospital), and a site part
# whose postings sit in several towns.
_kp = facilities_phenom._kp
KAISER_SITES = {
    "Sunnybrook Medical Ofcs": _kp("Kaiser Permanente Sunnybrook Medical Offices", "Clackamas", "OR"),
    "Wailuku Medical Offices": _kp("Kaiser Permanente Wailuku Medical Offices", "Wailuku", "HI"),
    "Kona Medical Offices": _kp("Kaiser Permanente Kona Medical Offices", "Kailua Kona", "HI"),
    "Franklin Medical Offices": _kp("Kaiser Permanente Franklin Medical Offices", "Denver", "CO"),
    "Southwood Medical Offices": _kp("Kaiser Permanente Southwood Medical Offices", "Jonesboro", "GA"),
    "Tacoma Specialty Center": _kp("Kaiser Permanente Tacoma Specialty Center", "Tacoma", "WA"),
    "Glenlake Medical Offices": _kp("Kaiser Permanente Glenlake Medical Offices", "Atlanta", "GA"),
    "San Jose Skyport Med Offices": _kp("Kaiser Permanente San Jose Skyport Medical Offices", "San Jose", "CA"),
    "Antelope Valley Medical Office": _kp("Kaiser Permanente Antelope Valley Medical Office", "Lancaster", "CA"),
    "Skyline Medical Offices": _kp("Kaiser Permanente Skyline Medical Offices", "Salem", "OR"),
    "Rock Creek Medical Offices": _kp("Kaiser Permanente Rock Creek Medical Offices", "Lafayette", "CO"),
    "Union City Medical Offices A": _kp("Kaiser Permanente Union City Medical Offices", "Union City", "CA"),
    "Daly City Medical Offices": _kp("Kaiser Permanente Daly City Medical Offices", "Daly City", "CA"),
    "Crescent Center Medical Ofcs": _kp("Kaiser Permanente Crescent Center Medical Offices", "Tucker", "GA"),
    "Stockton Medical Offices": _kp("Kaiser Permanente Stockton Medical Offices", "Stockton", "CA"),
    "Maui Lani Medical Offices": _kp("Kaiser Permanente Maui Lani Medical Offices", "Wailuku", "HI"),
    "Lone Tree Medical Offices": _kp("Kaiser Permanente Lone Tree Medical Offices", "Lone Tree", "CO"),
    "Lakewood II Medical Offices": _kp("Kaiser Permanente Lakewood Medical Offices", "Lakewood", "CO"),
    "Honolulu Medical Office": _kp("Kaiser Permanente Honolulu Medical Office", "Honolulu", "HI"),
    "Garfield Specialty Center": _kp("Kaiser Permanente Garfield Specialty Center", "San Diego", "CA"),
    "Orchard Medical Offices": _kp("Kaiser Permanente Orchard Medical Offices", "Vancouver", "WA"),
    "WALKER RD DENTAL/MED OFFICE": _kp("Kaiser Permanente Walker Road Medical and Dental Office", "Beaverton", "OR"),
    "San Marcos Outpatient Med Cntr": _kp("Kaiser Permanente San Marcos Outpatient Medical Center", "San Marcos",
                                          "CA"),
    "Anaheim Kraemer Med Office I": _kp("Kaiser Permanente Anaheim Kraemer Medical Office", "Anaheim", "CA"),
    "Anaheim Kraemer Med Office 2": _kp("Kaiser Permanente Anaheim Kraemer Medical Office", "Anaheim", "CA"),
    "Campbell Medical Offices I": _kp("Kaiser Permanente Campbell Medical Offices", "Campbell", "CA"),
    "Antioch Delta Fair Med Offices": _kp("Kaiser Permanente Antioch Delta Fair Medical Offices", "Antioch", "CA"),
    "Mt. Talbert Medical Offices": _kp("Kaiser Permanente Mt. Talbert Medical Offices", "Clackamas", "OR"),
    "Kailua Medical Offices": _kp("Kaiser Permanente Kailua Medical Offices", "Kailua", "HI"),
    "Salmon Creek Medical/Dental": _kp("Kaiser Permanente Salmon Creek Medical and Dental Office", "Vancouver", "WA"),
    "North Lancaster Medical Office": _kp("Kaiser Permanente North Lancaster Medical Office", "Salem", "OR"),
    "San Mateo Medical Offices": _kp("Kaiser Permanente San Mateo Medical Offices", "San Mateo", "CA"),
    "San Rafael Park Med Offices": _kp("Kaiser Permanente San Rafael Park Medical Offices", "San Rafael", "CA"),
    "Rockwood Medical Offices": _kp("Kaiser Permanente Rockwood Medical Offices", "Portland", "OR"),
    "Tracy Medical Offices": _kp("Kaiser Permanente Tracy Medical Offices", "Tracy", "CA"),
    "Rancho Cordova Medical Offices": _kp("Kaiser Permanente Rancho Cordova Medical Offices", "Rancho Cordova", "CA"),
    "Waimea Medical Offices": _kp("Kaiser Permanente Waimea Medical Offices", "Kamuela", "HI"),
    "Mountain View Medical Offices": _kp("Kaiser Permanente Mountain View Medical Offices", "Mountain View", "CA"),
    "MARKET STREET MEDICAL OFFICES": _kp("Kaiser Permanente Market Street Medical Offices", "Ventura", "CA"),
    "Santa Rosa Medical Offices 6": _kp("Kaiser Permanente Santa Rosa Medical Offices", "Santa Rosa", "CA"),
    "Martinez Hacienda Med Offices": _kp("Kaiser Permanente Martinez Hacienda Medical Offices", "Martinez", "CA"),
    "TANASBOURNE MED & DTL OFFICE": _kp("Kaiser Permanente Tanasbourne Medical and Dental Office", "Hillsboro", "OR"),
    "Gilroy Medical Offices": _kp("Kaiser Permanente Gilroy Medical Offices", "Gilroy", "CA"),
    "Bernardo Center Medical Office": _kp("Kaiser Permanente Bernardo Center Medical Office", "San Diego", "CA"),
    "West Cobb MO": _kp("Kaiser Permanente West Cobb Medical Office", "Austell", "GA"),
    "Petaluma Medical Offices I": _kp("Kaiser Permanente Petaluma Medical Offices", "Petaluma", "CA"),
    "West Salem Medical Offices": _kp("Kaiser Permanente West Salem Medical Offices", "Salem", "OR"),
    "Pinole Medical Offices": _kp("Kaiser Permanente Pinole Medical Offices", "Pinole", "CA"),
    "Hidden Lake Medical Offices": _kp("Kaiser Permanente Hidden Lake Medical Offices", "Westminster", "CO"),
    "Longview-Kelso Medical/Dental": _kp("Kaiser Permanente Longview-Kelso Medical and Dental Office", "Longview",
                                         "WA"),
    "Koolau Medical Offices": _kp("Kaiser Permanente Koolau Medical Offices", "Kaneohe", "HI"),
    "Cumberland Medical Offices": _kp("Kaiser Permanente Cumberland Medical Offices", "Atlanta", "GA"),
    "Elk Grove Medical Offices": _kp("Kaiser Permanente Elk Grove Medical Offices", "Elk Grove", "CA"),
    "Mapunapuna Medical Offices": _kp("Kaiser Permanente Mapunapuna Medical Offices", "Honolulu", "HI"),
    "Otay-Mesa Outpatient Med Cntr": _kp("Kaiser Permanente Otay Mesa Outpatient Medical Center", "San Diego", "CA"),
    "Wheat Ridge Medical Offices": _kp("Kaiser Permanente Wheat Ridge Medical Offices", "Wheat Ridge", "CO"),
    "Scotts Valley Medical Offices": _kp("Kaiser Permanente Scotts Valley Medical Offices", "Scotts Valley", "CA"),
    "Waipio Medical Offices": _kp("Kaiser Permanente Waipio Medical Offices", "Waipahu", "HI"),
    "Cascade Medical Offices": _kp("Kaiser Permanente Cascade Medical Offices", "Atlanta", "GA"),
    "Highlands Ranch Medical Office": _kp("Kaiser Permanente Highlands Ranch Medical Office", "Highlands Ranch",
                                          "CO"),
    "HOLLYWOOD ROMAINE MED OFFICES": _kp("Kaiser Permanente Hollywood Romaine Medical Offices", "Los Angeles", "CA"),
    "BEAVERTON MED & DENTAL OFFICE": _kp("Kaiser Permanente Beaverton Medical and Dental Office", "Beaverton", "OR"),
    "Henry County Medical Office": _kp("Kaiser Permanente Henry County Medical Office", "McDonough", "GA"),
    "CHINO GRAND MEDICAL OFFICES": _kp("Kaiser Permanente Chino Grand Medical Offices", "Chino", "CA"),
    "PALMDALE MEDICAL OFFICES": _kp("Kaiser Permanente Palmdale Medical Offices", "Palmdale", "CA"),
    "Dublin Medical Offices": _kp("Kaiser Permanente Dublin Medical Offices", "Dublin", "CA"),
    "So Sacramento Medical Offcs II": _kp("Kaiser Permanente South Sacramento Medical Offices", "Sacramento", "CA"),
    "Glendale Orange St Med Off": _kp("Kaiser Permanente Glendale Orange Street Medical Offices", "Glendale", "CA"),
    "Panola Medical offices": _kp("Kaiser Permanente Panola Medical Offices", "Lithonia", "GA"),
    "Sugar Hill Buford Medical Ofcs": _kp("Kaiser Permanente Sugar Hill Buford Medical Offices", "Sugar Hill", "GA"),
    "Tualatin Medical Offices": _kp("Kaiser Permanente Tualatin Medical Offices", "Tualatin", "OR"),
    "Alpharetta Medical Offices": _kp("Kaiser Permanente Alpharetta Medical Offices", "Alpharetta", "GA"),
    "South Los Angeles Med Offices": _kp("Kaiser Permanente South Los Angeles Medical Offices", "Los Angeles", "CA"),
    "Everett Specialty Center": _kp("Kaiser Permanente Everett Specialty Center", "Everett", "WA"),
    "Folsom Medical Offices": _kp("Kaiser Permanente Folsom Medical Offices", "Folsom", "CA"),
    "Alameda Medical Offices": _kp("Kaiser Permanente Alameda Medical Offices", "Alameda", "CA"),
    "Berkeley Medical Offices": _kp("Kaiser Permanente Berkeley Medical Offices", "Berkeley", "CA"),
    "Napa Medical Offices": _kp("Kaiser Permanente Napa Medical Offices", "Napa", "CA"),
    "Barranca Medical Office": _kp("Kaiser Permanente Barranca Medical Office", "Irvine", "CA"),
    "Wildomar Medical Offices": _kp("Kaiser Permanente Wildomar Medical Offices", "Wildomar", "CA"),
    "Lawrenceville MO": _kp("Kaiser Permanente Lawrenceville Medical Office", "Lawrenceville", "GA"),
    "Baseline Medical Offices": _kp("Kaiser Permanente Baseline Medical Offices", "Boulder", "CO"),
    "Lahaina Medical Offices": _kp("Kaiser Permanente Lahaina Medical Offices", "Lahaina", "HI"),
    "Decatur MO": _kp("Kaiser Permanente Decatur Medical Office", "Decatur", "GA"),
    "Fayetteville Medical Office": _kp("Kaiser Permanente Fayetteville Medical Office", "Sharpsburg", "GA"),
    "MURRIETA MEDICAL OFFICE 2": _kp("Kaiser Permanente Murrieta Medical Office", "Murrieta", "CA"),
    "Roseville Riverside Med Offcs": _kp("Kaiser Permanente Roseville Riverside Medical Offices", "Roseville", "CA"),
    "Hilo Medical Offices": _kp("Kaiser Permanente Hilo Medical Offices", "Hilo", "HI"),
    "Santa Ana Medical Office": _kp("Kaiser Permanente Santa Ana Medical Office", "Santa Ana", "CA"),
    "Watsonville Medical Offices": _kp("Kaiser Permanente Watsonville Medical Offices", "Watsonville", "CA"),
    "Aurora Centrepoint Medical Off": _kp("Kaiser Permanente Aurora Centrepoint Medical Offices", "Aurora", "CO"),
    "Smoky Hill Medical Offices": _kp("Kaiser Permanente Smoky Hill Medical Offices", "Aurora", "CO"),
    "CLAIREMONT MESA MEDICAL OFFICE": _kp("Kaiser Permanente Clairemont Mesa Medical Office", "San Diego", "CA"),
    "East Denver Medical Offices": _kp("Kaiser Permanente East Denver Medical Offices", "Denver", "CO"),
    "Livermore Medical Offices": _kp("Kaiser Permanente Livermore Medical Offices", "Livermore", "CA"),
    "San Ramon Medical Offices": _kp("Kaiser Permanente San Ramon Medical Offices", "San Ramon", "CA"),
}


def _register_kaiser() -> None:
    fam = facilities_phenom.FAMILIES[KAISER]
    table = fam.text[1]
    for spelling, site in KAISER_SITES.items():
        k = fkey(spelling)
        old = table.get(k)
        if old is not None and old != site:
            raise ValueError(f"Kaiser: {spelling!r} is already {old[0]!r}")
        table[k] = site
        fam.names.setdefault(fkey(site[0]), site)


_register_kaiser()


# ══════════════════════════════════════════════════════════════════════════
#  4. HUNTSVILLE HOSPITAL HEALTH SYSTEM: title table additions + town table
# ══════════════════════════════════════════════════════════════════════════
_HH_CRESTWOOD = ("Crestwood Medical Center", "Huntsville", "AL", "cms 010131")
_HH_MADISON = ("Madison Hospital", "Madison", "AL", "site (8375 Hwy 72 W, Madison)")
_HH_DMH = ("Decatur Morgan Hospital", "Decatur", "AL", "cms 010085")
# Huntsville's titles join their parts with a bare hyphen ("Env Services
# Aide-Crestwood Medical Center-FT-2nd shift"), which the title table's
# segment split keeps whole: these rules look for the site at a segment's
# start or after a hyphen.
_HH_RULES = [
    # "Post Surgical 3 East-LPN Staff-Crestwood Medical Center-FT-3rd shift"
    (re.compile(r"(?:^|-)\s*Crestwood(?:\s+Medical\s+Center)?\b", re.I), _HH_CRESTWOOD),
    # "Madison Hospital Emergency Department", "Madison Hospital Acute Medical Care 3rd FL"
    (re.compile(r"(?:^|-)\s*Madison\s+Hospital\b", re.I), _HH_MADISON),
    (re.compile(r"(?:^|-)\s*DMH\s*(?:-|$)"), _HH_DMH),
]


def _register_huntsville() -> None:
    rules = facilities_icims.TITLE_TABLES[HUNTSVILLE]
    if not any(hit[0] == _HH_CRESTWOOD[0] for _rx, hit in rules):
        # before the table's own "^Madison Hospital$" (same answer) and the rest
        rules[0:0] = _HH_RULES
    old = facilities_icims.TITLE_MENTIONS[HUNTSVILLE]
    facilities_icims.TITLE_MENTIONS[HUNTSVILLE] = re.compile(
        old.pattern[:-3] + r"|Crestwood|DMH)\b" if old.pattern.endswith(r")\b") else old.pattern, old.flags)


_register_huntsville()

_h = facilities_town._h
HUNTSVILLE_TOWNS = facilities_town._spec(HUNTSVILLE, [
    _h("Huntsville Hospital", "Huntsville", "AL", "cms 010039"),
    _h("Huntsville Hospital for Women & Children", "Huntsville", "AL", "site (245 Governors Dr, Huntsville)"),
    _h(*_HH_CRESTWOOD),
    _h(*_HH_MADISON),
    _h(*_HH_DMH),
    _h("Athens-Limestone Hospital", "Athens", "AL", "cms 010079"),
    _h("Highlands Medical Center", "Scottsboro", "AL", "cms 010061"),
    _h("DeKalb Regional Medical Center", "Fort Payne", "AL", "cms 010012"),
    _h("Helen Keller Hospital", "Sheffield", "AL", "cms 010019"),
    _h("Red Bay Hospital", "Red Bay", "AL", "cms 011302"),
    _h("Lawrence Medical Center", "Moulton", "AL", "cms 010059"),
    _h("Marshall Medical Center South", "Boaz", "AL", "cms 010005"),
    _h("Marshall Medical Center North", "Guntersville", "AL", "site (8000 AL Hwy 69, Guntersville)"),
    _h("Lincoln Medical Center", "Fayetteville", "TN", "cms 440102"),
])
# the system's own nursing homes and care centers by name: no hospital
facilities_town.CARE_SITE_BY_SYSTEM.setdefault(HUNTSVILLE, re.compile(
    r"\b(?:LMC\s+Nursing|Highlands\s+Care\s+Center|Care\s+Center|Nursing\s+Home|Huntsville\s+EMS|EMS)\b", re.I))


# ══════════════════════════════════════════════════════════════════════════
#  4b. ADVENTIST HEALTH (Oracle): two "<code> - Hospital" towns the table lacks
# ══════════════════════════════════════════════════════════════════════════
# The nightly names Adventist Health rows from the Oracle work location
# (facilities_oracle_other). A live page of 200 requisitions (2026-10-29): 145
# named; "AHCG - Hospital" (The Dalles, 7) and "AHSE - Hospital" (Selma, 1) are
# hospitals whose town the "site hospital" map does not list, so they kept the
# label. The rest are offices, clinics, home care, a care center and remote.
ADVENTIST = "Adventist Health"
_ADV_ADD = {
    "the dalles": ("Adventist Health Columbia Gorge", "The Dalles", "OR", "cms 380001"),
    "dalles": ("Adventist Health Columbia Gorge", "The Dalles", "OR", "cms 380001"),   # fkey drops "the"
    "selma": ("Adventist Health Selma", "Selma", "CA", "cms 050470"),
}


def _register_adventist() -> None:
    import facilities_oracle_other as fo
    t = fo.SYSTEMS[ADVENTIST]
    town_map = t["sites"]["Adventist Health site hospital"]
    spec = fo._SPECS[ADVENTIST]
    for town, (name, city, st, src) in _ADV_ADD.items():
        t["hospitals"].setdefault(name, (city, st, src))
        spec.hospitals.setdefault(name, (city, st))
        spec.exact.setdefault(fkey(name), name)
        spec.aliases.setdefault(spec._key(name), name)
        town_map.setdefault(town, name)           # the same dict the facility_names Spec reads


_register_adventist()


# ══════════════════════════════════════════════════════════════════════════
#  5. OCHSNER HEALTH: the campus a title names (before the town tables)
# ══════════════════════════════════════════════════════════════════════════
# (regex on the title, hospital, its own towns (the row must sit in one), source)
OCHSNER_TITLE_RULES = (
    (re.compile(r"\b(?:OMC\s+)?Jeff(?:erson)?\.?\s*(?:Hwy|Highway)\b", re.I),
     "Ochsner Medical Center", ("New Orleans", "Jefferson"), "cms 190036 (1516 Jefferson Hwy)"),
    (re.compile(r"\bBaptist\b(?!\s+(?:Memorial|Health\b|Hospital\s+of))", re.I),
     "Ochsner Baptist", ("New Orleans",), "cms 190135"),
    (re.compile(r"\bWest\s*bank(?:\s+Campus)?\b", re.I),
     "Ochsner Medical Center - West Bank Campus", ("Gretna",), "cms 190275"),
    (re.compile(r"\bOchsner\s+(?:Medical\s+Center\s*[-–]?\s*)?Kenner\b", re.I),
     "Ochsner Medical Center - Kenner", ("Kenner",), "cms 190274"),
    (re.compile(r"\bOMC[-\s]+North\s*shore\b|\bOchsner\s+(?:Medical\s+Center\s*[-–]?\s*)?North\s*shore\b", re.I),
     "Ochsner Medical Center - Northshore", ("Slidell",), "cms 190204"),
    (re.compile(r"\bOLGMC\b|\bOchsner\s+Lafayette\s+General\s+Medical\s+Center\b|\bOLG\b(?!\s*(?:Ortho|Surgical|"
                r"University|Women))", re.I),
     "Ochsner Lafayette General Medical Center", ("Lafayette",), "cms 190002"),
    (re.compile(r"\bOLG\s+Ortho(?:paedic|pedic)?\b", re.I),
     "Ochsner Lafayette General Orthopaedic Hospital", ("Lafayette",), "cms 190205"),
    (re.compile(r"\bOLG\s+Surgical\b|\bLafayette\s+General\s+Surgical\b", re.I),
     "Ochsner Lafayette General Surgical Hospital", ("Lafayette",), "cms 190268"),
    (re.compile(r"\bOUHC\b|\bOLG\s+University\b|\bOchsner\s+University\s+Hospital\b", re.I),
     "Ochsner University Hospital & Clinics", ("Lafayette",), "cms 190006"),
    (re.compile(r"\bChabert\b", re.I), "Leonard J. Chabert Medical Center", ("Houma",), "cms 190183"),
    (re.compile(r"\bSt\.?\s*Charles\s+Parish\s+Hospital\b", re.I), "St. Charles Parish Hospital", ("Luling",),
     "cms 190079"),
    (re.compile(r"\bSt\.?\s*Bernard\s+Parish\s+Hospital\b|\bSBPH\b", re.I), "St. Bernard Parish Hospital",
     ("Chalmette",), "cms 190308"),
    (re.compile(r"\bOchsner\s+Rush\s+(?:Medical\s+Center|Hospital)\b|\bRush\s+Foundation\s+Hospital\b", re.I),
     "Ochsner Rush Medical Center", ("Meridian",), "cms 250069"),
    (re.compile(r"\bSlidell\s+Memorial\b", re.I), "Slidell Memorial Hospital", ("Slidell",), "cms 190040"),
    (re.compile(r"\bOLHS\s+Academic\s+Medical\s+Center\b|\bOchsner\s+LSU\s+Health\s+Shreveport\s*[-–]?\s*"
                r"Academic\b", re.I),
     "Ochsner LSU Health Shreveport", ("Shreveport",), "cms 190098"),
    (re.compile(r"\bIberia\s+Medical\s+Center\b", re.I), "Iberia Medical Center", ("New Iberia",), "cms 190054"),
)


_HYBRID_ROOM_RX = re.compile(r"\bhybrid\s+(?:OR|operating|cath\w*|suite|room|lab)\b", re.I)
# Ochsner's own outpatient words the town tables' clinic list lacks ("LPN-
# OLG Community Health Center", "Ochsner Health Center - Kenner")
_OCHSNER_OFFSITE_RX = re.compile(r"\b(?:health\s+cent(?:er|re)|community\s+health|medical\s+complex|"
                                 r"concierge|65\s*plus|occupational)\b", re.I)


def ochsner_title(title, description, city, state):
    """(hospital_name, city, state, source) for an Ochsner row whose title names
    exactly one campus in the row's own town, else None. A remote, care-site,
    multi-site, clinic or office posting names none (facilities_town's rules)."""
    t = clean_text(title)
    if not t or (state or "").strip().upper() not in ("LA", "MS", ""):
        return None
    hits = {}
    for rx, name, towns, src in OCHSNER_TITLE_RULES:
        if rx.search(t):
            hits[name] = (towns, src)
    if len(hits) != 1:
        return None                       # none, or "Baptist/Clearview/Jeff Hwy campuses"
    # "Hybrid OR" is an operating room, not a hybrid (part-remote) job
    if facilities_town.is_remote(_HYBRID_ROOM_RX.sub(" ", t), "") or facilities_town.is_care_site(OCHSNER, t) \
            or facilities_town.is_multi_site(t, ""):
        return None
    stripped = facilities_town._strip_names(facilities_town.SPECS.get(OCHSNER), t) \
        if facilities_town.SPECS.get(OCHSNER) else t
    if facilities_town.OFFSITE_RX.search(stripped) or facilities_town.OFFICE_ROLE_RX.search(stripped) \
            or _OCHSNER_OFFSITE_RX.search(stripped):
        return None
    name, (towns, src) = next(iter(hits.items()))
    c = clean_text(city)
    if not c or facilities_town.town_key(c) not in {facilities_town.town_key(x) for x in towns}:
        return None                       # the row's own town is not the campus's: never guess
    st = (state or "").strip().upper() or ("MS" if "Meridian" in towns else "LA")
    return name, c, st, src


# ══════════════════════════════════════════════════════════════════════════
#  6. JEFFERSON HEALTH: "Primary Location Address" (after the town tables)
# ══════════════════════════════════════════════════════════════════════════
# (house number, first street word) -> hospital; the line's own town must be
# the hospital's. Streets from the CMS hospital list unless noted.
JEFFERSON_STREETS = {
    ("111", "s"): ("Thomas Jefferson University Hospital", "Philadelphia", "PA", "cms 390174 (111 S 11th St)"),
    ("132", "s"): ("Thomas Jefferson University Hospital", "Philadelphia", "PA",
                   "address 132 S 10th St, Philadelphia (the Main Building)"),
    ("5501", "old"): ("Jefferson Einstein Philadelphia Hospital", "Philadelphia", "PA", "cms 390142"),
    ("10800", "knights"): ("Jefferson Torresdale Hospital", "Philadelphia", "PA", "cms 390115"),
    ("1200", "old"): ("Jefferson Abington Hospital", "Abington", "PA", "cms 390231"),
    ("100", "medical"): ("Jefferson Lansdale Hospital", "Lansdale", "PA", "cms 390012"),
    ("559", "w"): ("Jefferson Einstein Montgomery Hospital", "East Norriton", "PA", "cms 390329"),
    ("559", "west"): ("Jefferson Einstein Montgomery Hospital", "East Norriton", "PA", "cms 390329"),
    ("18", "east"): ("Jefferson Stratford Hospital", "Stratford", "NJ", "cms 310086"),
    ("2301", "south"): ("Jefferson Methodist Hospital", "Philadelphia", "PA", "address 2301 S Broad St, Philadelphia"),
    ("4900", "frankford"): ("Jefferson Frankford Hospital", "Philadelphia", "PA",
                            "address 4900 Frankford Ave, Philadelphia"),
    ("380", "north"): ("Jefferson Bucks Hospital", "Langhorne", "PA", "address 380 N Oxford Valley Rd, Langhorne"),
    ("435", "hurffville"): ("Jefferson Washington Township Hospital", "Turnersville", "NJ",
                            "address 435 Hurffville-Cross Keys Rd, Turnersville"),
    ("2201", "chapel"): ("Jefferson Cherry Hill Hospital", "Cherry Hill", "NJ",
                         "address 2201 Chapel Ave W, Cherry Hill"),
    ("900", "walnut"): ("Jefferson Hospital for Neuroscience", "Philadelphia", "PA",
                        "address 900 Walnut St, Philadelphia"),
    ("1513", "race"): ("Magee Rehabilitation Hospital", "Philadelphia", "PA", "cms 393038 (1513 Race St)"),
}
# the street word after the number that must follow, where one number and
# word name two streets ("111 S 11th" is the hospital, "111 S 9th" is not)
JEFFERSON_STREET_CHECK = {("111", "s"): "11th", ("132", "s"): "10th", ("18", "east"): "laurel",
                          ("2301", "south"): "broad", ("380", "north"): "oxford", ("100", "medical"): "campus",
                          ("1200", "old"): "york", ("5501", "old"): "york"}
JEFFERSON_NAMES = frozenset(v[0] for v in JEFFERSON_STREETS.values())
_JEFF_LINE_RX = re.compile(r"(?mi)^[ \t]*Primary Location Address[ \t]*:?[ \t]*\n+[ \t]*([^\n]+)")
_JEFF_STATES = {"pennsylvania": "PA", "new jersey": "NJ", "delaware": "DE", "new york": "NY", "maryland": "MD"}
# a row town that is no town: a county, a part of Philadelphia, a building
_JEFF_NOT_TOWN_RX = re.compile(r"\bCounty$|^(?:Northeast|South|North|West|East|Center City)\s+Philadelphia$|"
                               r"\b(?:Building|Bldg|Center|Campus|Hospital|Pavilion|Tower)\b|^\s*$", re.I)
NO_BODY_CHARS = 1000


def jefferson_address(description):
    """(street, town, ST) of the body's "Primary Location Address" line, or None."""
    m = _JEFF_LINE_RX.search(str(description or ""))
    if not m:
        return None
    parts = [p.strip() for p in m.group(1).split(",") if p.strip()]
    parts = [p for p in parts if not re.fullmatch(r"United States(?: of America)?", p, re.I)]
    if len(parts) < 3:
        return None
    st = _JEFF_STATES.get(parts[-1].lower())
    if not st:
        return None
    town = parts[-2]
    if not re.fullmatch(r"[A-Za-z][A-Za-z .'-]{1,40}", town):
        return None
    return ", ".join(parts[:-2]), town, st


def jefferson_street_hit(street, town, st):
    toks = re.findall(r"[a-z0-9]+", (street or "").lower())
    if len(toks) < 2 or not toks[0].isdigit():
        return None
    k = (toks[0], toks[1])
    hit = JEFFERSON_STREETS.get(k)
    if not hit:
        return None
    need = JEFFERSON_STREET_CHECK.get(k)
    if need and need not in toks[2:4]:
        return None
    if facilities_town.town_key(town) != facilities_town.town_key(hit[1]) or st != hit[2]:
        return None
    return hit


def not_a_town(city, state="") -> bool:
    """A county, a part of Philadelphia, a building or nothing: no town. A
    known town ("Center Valley") is a town whatever its words."""
    c = clean_text(city)
    if not c:
        return True
    if state and known_town(c, state):
        return False
    return bool(_JEFF_NOT_TOWN_RX.search(c))


# A title that names one Jefferson hospital ("Infectious Disease Physician-
# Jefferson Abington Hospital") names it when the address line's town is that
# hospital's own (the street is then an outpatient building of its campus).
JEFFERSON_TITLE_CUES = (
    (re.compile(r"\bThomas Jefferson University Hospital\b|\bTJUH\b", re.I), "Thomas Jefferson University Hospital"),
    (re.compile(r"\bEinstein[- ]Philadelphia\b|\bEinstein Medical Center Philadelphia\b", re.I),
     "Jefferson Einstein Philadelphia Hospital"),
    (re.compile(r"\bEinstein[- ]Montgomery\b", re.I), "Jefferson Einstein Montgomery Hospital"),
    (re.compile(r"\bTorresdale\b", re.I), "Jefferson Torresdale Hospital"),
    (re.compile(r"\bAbington\s+Hospital\b|\bJefferson\s+Abington\b", re.I), "Jefferson Abington Hospital"),
    (re.compile(r"\bLansdale\s+Hospital\b|\bJefferson\s+Lansdale\b", re.I), "Jefferson Lansdale Hospital"),
    (re.compile(r"\bFrankford\s+Hospital\b|\bJefferson\s+Frankford\b", re.I), "Jefferson Frankford Hospital"),
    (re.compile(r"\bMethodist\s+Hospital\b|\bJefferson\s+Methodist\b", re.I), "Jefferson Methodist Hospital"),
    (re.compile(r"\bBucks\s+Hospital\b|\bJefferson\s+Bucks\b", re.I), "Jefferson Bucks Hospital"),
    (re.compile(r"\bWashington\s+Township\s+Hospital\b|\bJefferson\s+Washington\s+Township\b", re.I),
     "Jefferson Washington Township Hospital"),
    (re.compile(r"\bCherry\s+Hill\s+Hospital\b|\bJefferson\s+Cherry\s+Hill\b", re.I), "Jefferson Cherry Hill Hospital"),
    (re.compile(r"\bStratford\s+Hospital\b|\bJefferson\s+Stratford\b", re.I), "Jefferson Stratford Hospital"),
    (re.compile(r"\bHospital\s+for\s+Neuroscience\b", re.I), "Jefferson Hospital for Neuroscience"),
)
# a site on a hospital's campus that is no hospital (the Willowcrest nursing
# facility at 5501 Old York Road)
_JEFF_CARE_RX = re.compile(r"\bWillowcrest\b", re.I)


def jefferson_title_hit(title, town, st):
    t = clean_text(title)
    names = {name for rx, name in JEFFERSON_TITLE_CUES if rx.search(t)}
    if len(names) != 1:
        return None
    name = next(iter(names))
    home = next(v for v in JEFFERSON_STREETS.values() if v[0] == name)
    if facilities_town.town_key(town) != facilities_town.town_key(home[1]) or st != home[2]:
        return None
    stripped = facilities_town._strip_names(facilities_town.SPECS[JEFFERSON], t) \
        if JEFFERSON in facilities_town.SPECS else t
    if facilities_town.OFFSITE_RX.search(stripped) or facilities_town.OFFICE_ROLE_RX.search(stripped) \
            or facilities_town.REMOTE_RX.search(stripped):
        return None
    return name


def jefferson_decide(hospital_name, city, state, description, job_id=None, stored=None, title=""):
    """(hospital_name, city, state, how) for a Jefferson row after the town
    tables, or None (no change). "address": the line's street is a hospital;
    "title": the title names the hospital of the line's town; "town": a label
    row whose town is a county or a part of Philadelphia takes the line's
    town; "kept": no body tonight, the stored town (and the stored hospital)
    of a row whose feed town is no town. A care site on a campus (a nursing
    facility) or a remote posting never takes the hospital."""
    label = clean_text(hospital_name) in ("", JEFFERSON)
    addr = jefferson_address(description) if len(str(description or "").strip()) >= NO_BODY_CHARS else None
    if addr:
        street, town, st = addr
        care = bool(_JEFF_CARE_RX.search(clean_text(title))) or facilities_town.is_care_site(JEFFERSON, title) \
            or facilities_town.is_remote(title, "")
        hit = jefferson_street_hit(street, town, st)
        if label and hit and not care:
            return hit[0], town, st, "address"
        tname = jefferson_title_hit(title, town, st) if label and not care else None
        if tname:
            return tname, town, st, "title"
        if not_a_town(city, state) and (label or clean_text(hospital_name) in JEFFERSON_NAMES):
            return clean_text(hospital_name) or JEFFERSON, town, st, "town"
        return None
    if not_a_town(city, state) and stored:
        sname, scity, sst = stored
        if scity and not not_a_town(scity, sst) and (clean_text(sname) == clean_text(hospital_name) or
                                                (label and clean_text(sname) in JEFFERSON_NAMES)):
            return clean_text(sname) or JEFFERSON, scity, sst, "kept"
    return None


# ══════════════════════════════════════════════════════════════════════════
#  RUN TIME (scraper.finalize_jobs)
# ══════════════════════════════════════════════════════════════════════════
def _set(job, name, city, state):
    job.hospital_name = name
    if (city, state) != ((job.city or "").strip(), (job.state or "").strip().upper()):
        job.city, job.state, job.location = city, state, f"{city}, {state}"


# review B 2026-10-29 (facility release 3): one row nobody foresaw must not
# end the pass for every row after it. The row that raises keeps the name and
# town it came with and counts as "error" in the pass's stats.
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


def _ochsner_one(job) -> str:
    """apply_before_town on one Ochsner label row; returns how."""
    hit = ochsner_title(job.title, job.description, job.city, job.state)
    if hit:
        _set(job, *hit[:3])
    return "title" if hit else "none"


def _jefferson_one(job) -> str:
    """apply_after_town on one Jefferson row; returns how."""
    stored = facilities_town.STORED.get((JEFFERSON, str(job.job_id)))
    got = jefferson_decide(job.hospital_name, job.city, job.state, job.description, job.job_id, stored,
                           title=job.title)
    if got:
        _set(job, *got[:3])
    return got[3] if got else "none"


def apply_before_town(jobs) -> dict:
    """After the text pass, before the town tables: the Ochsner campus a title
    names. Returns {system: {how: rows}}."""
    stats: dict = {}
    for job in jobs:
        if job.hospital_system != OCHSNER or clean_text(job.hospital_name) not in ("", OCHSNER):
            continue
        how = _row_guard(_ochsner_one, job)
        d = stats.setdefault(OCHSNER, {})
        d[how] = d.get(how, 0) + 1
    return stats


def apply_after_town(jobs) -> dict:
    """After the town tables: Jefferson's "Primary Location Address" (the
    hospital at its street, else the real town of a county row). On a night
    without a body, the town tables' stored map (facilities_town.STORED) gives
    the stored town back to a row whose feed town is a county."""
    stats: dict = {}
    for job in jobs:
        if job.hospital_system != JEFFERSON:
            continue
        how = _row_guard(_jefferson_one, job)
        d = stats.setdefault(JEFFERSON, {})
        d[how] = d.get(how, 0) + 1
    return stats
