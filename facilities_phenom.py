"""The hospital on Phenom / TalentBrew rows filed under a system name (2026-10-29).

The owner, 2026-10-29: every hospital inside a multi-hospital system must be
named and placed correctly, on the site and in the dashboard, and the applies
retro-assigned to the right hospital. His decisions:
  1. cards and job pages show the HOSPITAL (Apply reads "Apply to
     <hospital>"); the system name and logo stay on the job page and the hub;
  2. named sites (hospitals, named clinics and centers) show their own name;
     generic offices, home-based and remote jobs show the system name with the
     town; never guess a town.

Where each employer of this family names the site (read 2026-10-29 off each
board's own search page and the stored rows):

  list field (the Phenom search list, read every night). An employing entity
  names its hospital only in that hospital's town; a work site is that site:
    Hartford HealthCare    businessUnit (entity)   "Hartford Hospital", "Manchester Hospital Inc."
    UMMS                   memberOrganization      "University Of Maryland Shore Regional Health"
    Spartanburg Regional   FacilityName (entity)   "Pelham Medical Center"
    ECU Health             facility (entity)       "ECUH BEA Campus of ECUH MC"
    LCMC Health            facility (entity)       "West Jefferson", "Lakeview"
    CentraCare             workLocation (site), companyName (entity)
    Children's Minnesota   workLocation (site)     "Children's - St. Paul"
    Broward Health         address (site)          "1600 S Andrews Avenue" (the hospital's own street)
    Franciscan Health      address (site)          "10777 Illinois St, Carmel, IN 46032"
  Workday link (the posting's primary site, the link's first path segment;
  stored on the row, so the stored rows and the nightly read agree):
    Children's Healthcare of Atlanta  "/job/Childrens-Hughes-Spalding-Hospital/..."
    Franciscan Health                 "/job/Franciscan-Health-Indianapolis-Orthopedic-Hospital/..."
  posting text (the job page body; read on the nights the detail pass fetches
  it, the stored name kept on the other nights):
    Kaiser Permanente     "Department: Sunnyside Medical Center - Emergency - 1001"
    Hartford HealthCare   "Location Detail: Windham Community Memorial (10183)"
    MU Health Care        "Facility: Capital Region Medical Center, 1125 Madison St, ..."
    MemorialCare          "Location: Saddleback Medical Center"
  Duke Health and Children's Health (Phenom fronts whose site is only in the
  body) are in facilities_text.py (the text family), not here.

Rules, the same for the scraper and the stored-row cleanup
(claud_outputinput/facility_scale_2026-10-29/phase2/make_changes_phenom.py):
  * a value the table does not list names no site: the row keeps the system
    label (and its own town); nothing is fuzzy-matched;
  * an employing entity (list field) names its hospital only when the posting
    is in that hospital's own town (a Hartford Hospital posting in Avon is an
    outpatient site: system label, Avon), so one hospital name never spreads
    over several towns (the CRM reads a name at 3+ towns as a system label);
  * a site (Workday link, posting text, workLocation, street address) is
    that site when the posting is in the site's own town; a posting with no
    town, or at the system's head-office default, takes the site's town; a
    posting in another town names no site (never guess a town);
  * a line that names two sites names none;
  * a hospital of another system is never named (Kaiser staff at Virginia
    Hospital Center keep the Kaiser label);
  * a town alone never names a hospital (decision 2: the posting names no
    site, so it shows the system name with its town).

hospital_system is never touched (the upsert key, the hub, the filters, the
logo). Every town below is the site's own address: the CMS hospital list in the
database ("cms <id>"), the town the board itself prints for the site ("board"),
or the site's published street address ("address").
"""
from __future__ import annotations

import json
import logging
import os
import re
import time

import facility_names
import sb_keys  # the DATA project key, one reader for the repo

logger = logging.getLogger("scraper.facility")

fkey = facility_names.fkey
clean_text = facility_names.clean_text

# A site: (public name, town, state, source of the town).
Site = tuple

# ── Hartford HealthCare ─────────────────────────────────────────────────────
_HHC_HH = ("Hartford Hospital", "Hartford", "CT", "cms 070025")
_HHC_IOL = ("Institute of Living", "Hartford", "CT", "address 200 Retreat Ave, Hartford")
_HHC_SVMC = ("St. Vincent's Medical Center", "Bridgeport", "CT", "cms 070028")
_HHC_HOCC = ("The Hospital of Central Connecticut", "New Britain", "CT", "cms 070035")
_HHC_BRADLEY = ("The Hospital of Central Connecticut, Bradley Memorial Campus", "Southington", "CT",
                "address 81 Meriden Ave, Southington")
_HHC_MMH = ("Manchester Memorial Hospital", "Manchester", "CT", "cms 070027")
_HHC_BACKUS = ("Backus Hospital", "Norwich", "CT", "cms 070024")
_HHC_WINDHAM = ("Windham Hospital", "Willimantic", "CT", "cms 070021")
_HHC_CHH = ("Charlotte Hungerford Hospital", "Torrington", "CT", "cms 070011")
_HHC_MIDSTATE = ("MidState Medical Center", "Meriden", "CT", "cms 070017")
_HHC_ROCKVILLE = ("Rockville General Hospital", "Vernon", "CT", "cms 070012")
_HHC_NATCHAUG = ("Natchaug Hospital", "Mansfield Center", "CT", "cms 074008")
_HHC_RUSHFORD = ("Rushford Center", "Meriden", "CT", "board Rushford Center Inc Meriden (10172)")
_HHC_SNSC = ("St. Vincent's Special Needs Services", "Trumbull", "CT", "board 95 Merritt Blvd Trumbull (10439)")
_HHC_SCC = ("Southington Care Center", "Southington", "CT", "board 45 Meriden Ave Southington CC (10174)")
_HHC_ORCHARDS = ("The Orchards at Southington", "Southington", "CT", "board The Orchards of Southington (10165)")
_HHC_MULBERRY = ("Mulberry Gardens of Southington", "Plantsville", "CT", "board 58 Mulberry Street Plantsville (10122)")
_HHC_JEFFERSON = ("Jefferson House", "Newington", "CT", "board Jefferson House 1 John Stewart (10009)")
_HHC_CEDAR = ("Cedar Mountain Commons", "Newington", "CT", "board Cedar Mountain Commons (10007)")

# businessUnit: the employing entity -> {posting town: site}. A town not listed
# keeps the system label (an outpatient site, a practice, an office).
_HHC_UNITS = {
    "Hartford Hospital": {"hartford": _HHC_HH},
    "Institute of Living": {"hartford": _HHC_IOL},
    "St. Vincent's Medical Center": {"bridgeport": _HHC_SVMC},
    "The Hospital of Central Connecticut": {"new britain": _HHC_HOCC},
    "Manchester Hospital Inc.": {"manchester": _HHC_MMH},
    "Manchester Memorial Hospital": {"manchester": _HHC_MMH},
    # Windham Hospital runs under Backus's license; a Backus posting in
    # Willimantic is at the Windham campus (job page 26164641: businessUnit
    # "Backus Hospital", Location Detail "Windham Community Memorial (10183)").
    "Backus Hospital": {"norwich": _HHC_BACKUS, "willimantic": _HHC_WINDHAM},
    "Windham Hospital": {"willimantic": _HHC_WINDHAM},
    "Charlotte Hungerford Hospital": {"torrington": _HHC_CHH},
    "MidState Medical Center": {"meriden": _HHC_MIDSTATE},
    "Rockville General": {"vernon": _HHC_ROCKVILLE},
    "Natchaug Hospital": {"mansfield center": _HHC_NATCHAUG},
    "Rushford": {"meriden": _HHC_RUSHFORD},
    "St. Vincent's Special Needs Center": {"trumbull": _HHC_SNSC},
    "Hartford HealthCare Senior Services at Southington Care Center": {"southington": _HHC_SCC},
    "Hartford HealthCare Senior Services at The Orchards": {"southington": _HHC_ORCHARDS},
    "Hartford HealthCare Senior Services at Mulberry Garden": {"plantsville": _HHC_MULBERRY,
                                                                "southington": _HHC_MULBERRY},
    "Jefferson House": {"newington": _HHC_JEFFERSON},
    "Cedar Mountain Commons": {"newington": _HHC_CEDAR},
}
# Location Detail codes that are a hospital campus or a named center (the
# body's "Location Detail: <site> (<code>)" line). Street addresses of
# practices, offices and outpatient buildings are not listed.
_HHC_CODES = {
    "10008": _HHC_HH, "10064": _HHC_HH, "10067": _HHC_HH, "10066": _HHC_HH, "10065": _HHC_HH,
    "10060": _HHC_HH, "10069": _HHC_HH, "10020": _HHC_HH, "10075": _HHC_HH,
    "10051": _HHC_IOL, "10023": _HHC_IOL,                  # HH Donnelly Bldg, HH Braceland Bldg
    "10427": _HHC_SVMC,
    "10126": _HHC_HOCC, "10148": _HHC_HOCC,                # 100 Grand St, 73 Cedar St NB - HOCC
    "10003": _HHC_BRADLEY,                                 # 81 Meriden Ave Bradley Memorial
    "10627": _HHC_MMH,                                     # 71 Haynes Street
    "10309": _HHC_BACKUS,
    "10183": _HHC_WINDHAM,                                 # Windham Community Memorial
    "10115": _HHC_CHH,
    "10123": _HHC_MIDSTATE,                                # 435 Lewis Avenue-Midstate Med
    "10619": _HHC_ROCKVILLE,                               # 31 Union Street, Vernon
    "10155": _HHC_NATCHAUG, "10187": _HHC_NATCHAUG, "10188": _HHC_NATCHAUG, "10190": _HHC_NATCHAUG,
    "10172": _HHC_RUSHFORD,
    "10439": _HHC_SNSC,
    "10174": _HHC_SCC,
    "10165": _HHC_ORCHARDS,
    "10122": _HHC_MULBERRY,
    "10009": _HHC_JEFFERSON,
    "10007": _HHC_CEDAR,
}
_HHC_CODE_RX = re.compile(r"(?mi)^[ \t]*Location Detail[ \t]*:?[ \t]*([^\n]*?)\((\d{5})\)")

# ── University of Maryland Medical System (memberOrganization) ─────────────
_UM_UMMC = ("University of Maryland Medical Center", "Baltimore", "MD", "cms 210002")
_UM_MIDTOWN = ("University of Maryland Medical Center Midtown Campus", "Baltimore", "MD", "cms 210038")
_UM_BWMC = ("University of Maryland Baltimore Washington Medical Center", "Glen Burnie", "MD", "cms 210043")
_UM_SJMC = ("University of Maryland St. Joseph Medical Center", "Towson", "MD", "cms 210063")
_UM_ROI = ("University of Maryland Rehabilitation & Orthopaedic Institute", "Baltimore", "MD", "cms 210058")
_UM_CRMC = ("University of Maryland Charles Regional Medical Center", "La Plata", "MD", "cms 210035")
_UM_CAPITAL = ("University of Maryland Capital Region Medical Center", "Largo", "MD",
               "address 901 Harry S Truman Dr, Largo (cms 210003)")
_UM_EASTON = ("University of Maryland Shore Medical Center at Easton", "Easton", "MD", "cms 210037")
_UM_CHESTERTOWN = ("University of Maryland Shore Medical Center at Chestertown", "Chestertown", "MD", "cms 210030")
_UM_UCMC = ("University of Maryland Upper Chesapeake Medical Center", "Bel Air", "MD", "cms 210049")
_UMMS_UNITS = {
    "University Of Maryland Medical Center": {"baltimore": _UM_UMMC},
    "University Of Maryland Medical Center Midtown Campus": {"baltimore": _UM_MIDTOWN},
    "University Of Maryland Baltimore Washington Medical Center": {"glen burnie": _UM_BWMC},
    "University Of Maryland Saint Joseph Medical Center": {"towson": _UM_SJMC},
    "University Of Maryland Rehabilitation And Orthopaedic Instit": {"baltimore": _UM_ROI},
    "University Of Maryland Charles Regional Medical Center": {"la plata": _UM_CRMC},
    "University Of Maryland Capital Region Health": {"largo": _UM_CAPITAL, "upper marlboro": _UM_CAPITAL},
    "University Of Maryland Shore Regional Health": {"easton": _UM_EASTON, "chestertown": _UM_CHESTERTOWN},
    "University Of Maryland Upper Chesapeake Health": {"bel air": _UM_UCMC},
}

# ── Broward Health (the list's street address of the work site) ─────────────
# The careers board's companyName filter ("BHMC", "BHN", ...) is not in the
# search list; each listing's `address` is (1600 S Andrews Avenue, 201 E
# Sample Road, 6401 N Federal Highway on 2026-10-29).
_BROWARD_ADDRESSES = {
    "1600 andrews": ("Broward Health Medical Center", "Fort Lauderdale", "FL", "cms 100039"),
    "201 sample": ("Broward Health North", "Deerfield Beach", "FL", "cms 100086"),
    "3000 coral": ("Broward Health Coral Springs", "Coral Springs", "FL", "cms 100276"),
    "6401 federal": ("Broward Health Imperial Point", "Fort Lauderdale", "FL", "cms 100200"),
}

# ── Spartanburg Regional (FacilityName) ─────────────────────────────────────
_SRHS_UNITS = {
    "Spartanburg Medical Center": {"spartanburg": ("Spartanburg Medical Center", "Spartanburg", "SC", "cms 420007")},
    "Pelham Medical Center": {"greer": ("Pelham Medical Center", "Greer", "SC", "cms 420103")},
    "Cherokee Medical Center": {"gaffney": ("Cherokee Medical Center", "Gaffney", "SC", "cms 420043")},
    "Union Medical Center": {"union": ("Union Medical Center", "Union", "SC", "cms 420108")},
    "Ellen Sagar Nursing Center": {"union": ("Ellen Sagar Nursing Center", "Union", "SC", "board")},
    "Woodruff Manor": {"woodruff": ("Woodruff Manor", "Woodruff", "SC", "board")},
}

# ── ECU Health (facility) ───────────────────────────────────────────────────
_ECU_UNITS = {
    "ECU Health Medical Center": {"greenville": ("ECU Health Medical Center", "Greenville", "NC", "cms 340040")},
    "ECU Health North": {"roanoke rapids": ("ECU Health North Hospital", "Roanoke Rapids", "NC", "cms 340151")},
    "ECUH BEA Campus of ECUH MC": {"washington": ("ECU Health Beaufort Hospital", "Washington", "NC",
                                                  "address 628 E 12th St, Washington")},
    "ECU Health Roanoke Chowan": {"ahoskie": ("ECU Health Roanoke-Chowan Hospital", "Ahoskie", "NC", "cms 340099")},
    "The Outer Banks Hospital, Inc.": {"nags head": ("The Outer Banks Hospital", "Nags Head", "NC", "cms 341324")},
    "ECU Health Duplin": {"kenansville": ("ECU Health Duplin Hospital", "Kenansville", "NC", "cms 340120")},
    "ECU Health Chowan": {"edenton": ("ECU Health Chowan Hospital", "Edenton", "NC", "cms 341318")},
    "ECU Health Edgecombe": {"tarboro": ("ECU Health Edgecombe Hospital", "Tarboro", "NC", "cms 340107")},
    "ECU Health Bertie": {"windsor": ("ECU Health Bertie Hospital", "Windsor", "NC", "cms 341304")},
    "ECU Health SurgiCenter": {"greenville": ("ECU Health SurgiCenter", "Greenville", "NC", "board")},
}
# ECU entities that are the system (physician group, integration office, a
# services company): the system label, the posting's town.
_ECU_OFFICES = ("ECU Health Phys Team Member", "ECU Health Integration", "Access East, Inc",
                "Outer Banks Medical Group", "ECU Health Ambulatory Facility")

# ── LCMC Health (facility / company) ────────────────────────────────────────
_LCMC_UMC = ("University Medical Center New Orleans", "New Orleans", "LA", "cms 190005")
_LCMC_CHNOLA = ("Children's Hospital New Orleans", "New Orleans", "LA", "cms 193300")
_LCMC_UNITS = {
    "University Medical Center": {"new orleans": _LCMC_UMC},
    "University Medical Center New Orleans": {"new orleans": _LCMC_UMC},
    "Children's Hospital": {"new orleans": _LCMC_CHNOLA},
    "Children's Hospital New Orleans": {"new orleans": _LCMC_CHNOLA},
    "East Jefferson General Hospital": {"metairie": ("East Jefferson General Hospital", "Metairie", "LA", "cms 190176")},
    "Touro Infirmary": {"new orleans": ("Touro Infirmary", "New Orleans", "LA", "cms 190046")},
    "West Jefferson": {"marrero": ("West Jefferson Medical Center", "Marrero", "LA", "cms 190039")},
    "West Jefferson Medical Center": {"marrero": ("West Jefferson Medical Center", "Marrero", "LA", "cms 190039")},
    "Lakeview": {"covington": ("Lakeview Regional Medical Center", "Covington", "LA",
                               "address 95 Judge Tanner Blvd, Covington")},
    "Lakeside": {"metairie": ("Lakeside Hospital", "Metairie", "LA", "address 4700 S I-10 Service Rd W, Metairie")},
    "New Orleans East Hospital": {"new orleans": ("New Orleans East Hospital", "New Orleans", "LA", "cms 190313")},
}
_LCMC_OFFICES = ("LCMC", "LCMC Health Physician Group (LHPG)", "Crescent City Physicians",
                 "LCMC Health Anesthesia Corporation")

# ── Franciscan Health (company; the Workday link's site) ────────────────────
def _fh(name, town, cms):
    return (name, town, "IN", f"cms {cms}")


_FH_ORTHO = _fh("Franciscan Health Indianapolis Orthopedic Hospital", "Carmel", "150193")
_FH_SEGMENTS = {"franciscanhealthindianapolisorthopedichospital": _FH_ORTHO}
# The board's company filter is not in the search list; each listing's
# `address` is the work site's street address ("10777 Illinois St, Carmel, IN
# 46032"): a hospital's own CMS street address names it.
_FH_ADDRESSES = {
    "10777 illinois": _FH_ORTHO,
    "8111 emerson": _fh("Franciscan Health Indianapolis", "Indianapolis", "150162"),
    "1701 creasy": _fh("Franciscan Health Lafayette", "Lafayette", "150109"),
    "3500 franciscan": _fh("Franciscan Health Michigan City", "Michigan City", "150015"),
    "24 joliet": _fh("Franciscan Health Dyer", "Dyer", "150090"),
    "1201 main": _fh("Franciscan Health Crown Point", "Crown Point", "150126"),
    "701 superior": _fh("Franciscan Health Munster", "Munster", "150165"),
    "1201 hadley": _fh("Franciscan Health Mooresville", "Mooresville", "150057"),
    "1710 lafayette": _fh("Franciscan Health Crawfordsville", "Crawfordsville", "150022"),
    "1104 grace": _fh("Franciscan Health Rensselaer", "Rensselaer", "151324"),
}

# ── CentraCare (workLocation: the site; companyName: the entity) ────────────
_CC_SCH = ("St. Cloud Hospital", "Saint Cloud", "MN", "cms 240036")
_CC_SITES = {
    "St Cloud Hospital": _CC_SCH,
    "CentraCare - Monticello": ("CentraCare - Monticello", "Monticello", "MN", "cms 241362"),
    "CentraCare - Sauk Centre": ("CentraCare - Sauk Centre", "Sauk Centre", "MN", "cms 241368"),
    "CentraCare - Paynesville": ("CentraCare - Paynesville", "Paynesville", "MN", "cms 241349"),
    "CentraCare - Benson": ("CentraCare - Benson", "Benson", "MN", "cms 241365"),
    "CentraCare - Long Prairie": ("CentraCare - Long Prairie", "Long Prairie", "MN", "cms 241326"),
    "CentraCare - Melrose": ("CentraCare - Melrose", "Melrose", "MN", "cms 241330"),
    "CentraCare Plaza": ("CentraCare Plaza", "Saint Cloud", "MN", "board"),
}
_CC_UNITS = {
    "St. Cloud Hospital": {"saint cloud": _CC_SCH, "st cloud": _CC_SCH},
    "CentraCare Rice Memorial Hospital": {"willmar": ("CentraCare - Rice Memorial Hospital", "Willmar", "MN",
                                                      "cms 240088")},
    "CentraCare Redwood": {"redwood falls": ("CentraCare - Redwood", "Redwood Falls", "MN", "cms 241351")},
    "CentraCare - Benson": {"benson": ("CentraCare - Benson", "Benson", "MN", "cms 241365")},
}

# ── Children's Minnesota (workLocation) ─────────────────────────────────────
_CMN_SITES = {
    "Children's - Minneapolis": ("Children's Minnesota Minneapolis Hospital", "Minneapolis", "MN", "cms 243302"),
    "Children's - St. Paul": ("Children's Minnesota St. Paul Hospital", "St. Paul", "MN",
                              "address 345 N Smith Ave, St. Paul"),
}

# ── Children's Healthcare of Atlanta (the Workday link's site) ──────────────
def _choa(name, town, source="board"):
    return (name, town, "GA", source)


_CHOA_SR = _choa("Children's Scottish Rite Hospital", "Atlanta", "cms 113301")
_CHOA_SEGMENTS = {
    "arthurmblankhospital": _choa("Arthur M. Blank Hospital", "Atlanta", "cms 113300"),
    "childrensscottishritehospital": _CHOA_SR,
    "childrensscottishritehospitaldayrehabilitationprogram": _CHOA_SR,
    "childrenshughesspaldinghospital": _choa("Children's Hughes Spalding Hospital", "Atlanta",
                                             "address 35 Jesse Hill Jr Dr SE, Atlanta"),
    "centerforadvancedpediatrics": _choa("Children's Center for Advanced Pediatrics", "Atlanta"),
    "marcusautismcenter": _choa("Marcus Autism Center", "Atlanta"),
    "zalikbehavioralandmentalhealthcenter": _choa("Zalik Behavioral and Mental Health Center", "Atlanta"),
    "childrensatmeridianmark": _choa("Children's at Meridian Mark", "Atlanta"),
    "childrensatnorthdruidhills": _choa("Children's at North Druid Hills", "Atlanta"),
    "childrensatforsyth": _choa("Children's at Forsyth", "Cumming"),
    "childrensattowncenter": _choa("Children's at Town Center", "Kennesaw"),
    "childrensattowncenteroutpatientcarecenter": _choa("Children's at Town Center Outpatient Care Center", "Kennesaw"),
    "childrensatsatelliteboulevard": _choa("Children's at Satellite Boulevard", "Duluth"),
    "childrensatsatelliteboulevardoutpatientsurgerycenter": _choa(
        "Children's at Satellite Boulevard Outpatient Surgery Center", "Duluth"),
    "childrensathudsonbridge": _choa("Children's at Hudson Bridge", "Stockbridge"),
    "childrensatnorthpoint": _choa("Children's at North Point", "Alpharetta"),
    "childrensatmountvernonhighway": _choa("Children's at Mount Vernon Highway", "Sandy Springs"),
    "childrensathamiltoncreek": _choa("Children's at Hamilton Creek", "Dacula"),
    "childrensatcherokeeurgentcare": _choa("Children's at Cherokee Urgent Care", "Canton"),
    "childrensatcherokeesportsmedicine": _choa("Children's at Cherokee Sports Medicine", "Canton"),
    "childrensatduluth": _choa("Children's at Duluth", "Duluth"),
    "childrensativywalk": _choa("Children's at Ivy Walk", "Smyrna"),
    "childrensatchambleebrookhavenurgentcare": _choa("Children's at Chamblee-Brookhaven Urgent Care", "Chamblee"),
    "childrensatoldmiltonparkway": _choa("Children's at Old Milton Parkway", "Alpharetta"),
    "childrensatwebbbridge": _choa("Children's at Webb Bridge", "Alpharetta"),
}
# Not sites of their own: the support center (offices), facility operations,
# a warehouse, remote work, the Scottish Rite medical office building, Emory
# University Hospital Midtown (another system's hospital): the system label.

# ── Kaiser Permanente (the body's "Department: <site> - <dept> - <code>") ───
def _kp(name, town, state, source="board"):
    return (name, town, state, source)


_KP_SUNSET = _kp("Kaiser Permanente Los Angeles Medical Center", "Los Angeles", "CA",
                 "address 4867 W Sunset Blvd, Los Angeles")
_KP_FONTANA = _kp("Kaiser Permanente Fontana Medical Center", "Fontana", "CA")
_KP_SSAC = _kp("Kaiser Permanente South Sacramento Medical Center", "Sacramento", "CA")
_KP_SITES = {
    # Northwest
    "Sunnyside Medical Center": _kp("Kaiser Permanente Sunnyside Medical Center", "Clackamas", "OR"),
    "Westside Medical Cntr": _kp("Kaiser Permanente Westside Medical Center", "Hillsboro", "OR"),
    # Northern California
    "Santa Clara Homestead Hospital": _kp("Kaiser Permanente Santa Clara Medical Center", "Santa Clara", "CA"),
    "Oakland Hospital": _kp("Kaiser Permanente Oakland Medical Center", "Oakland", "CA"),
    "San Francisco Hospital": _kp("Kaiser Permanente San Francisco Medical Center", "San Francisco", "CA"),
    "Santa Rosa Hospital": _kp("Kaiser Permanente Santa Rosa Medical Center", "Santa Rosa", "CA"),
    "Roseville Hospital": _kp("Kaiser Permanente Roseville Medical Center", "Roseville", "CA"),
    "Redwood City Hospital": _kp("Kaiser Permanente Redwood City Medical Center", "Redwood City", "CA"),
    "San Leandro Hospital": _kp("Kaiser Permanente San Leandro Medical Center", "San Leandro", "CA"),
    "San Jose Hospital": _kp("Kaiser Permanente San Jose Medical Center", "San Jose", "CA"),
    "So San Francisco Hospital": _kp("Kaiser Permanente South San Francisco Medical Center", "South San Francisco",
                                     "CA"),
    "Walnut Creek Hospital": _kp("Kaiser Permanente Walnut Creek Medical Center", "Walnut Creek", "CA"),
    "Vallejo Hospital": _kp("Kaiser Permanente Vallejo Medical Center", "Vallejo", "CA"),
    "So Sacramento Hospital North": _KP_SSAC,
    "So Sacramento Hospital South": _KP_SSAC,
    "Modesto Hospital": _kp("Kaiser Permanente Modesto Medical Center", "Modesto", "CA"),
    "Vacaville Hospital": _kp("Kaiser Permanente Vacaville Medical Center", "Vacaville", "CA"),
    "San Rafael Hospital": _kp("Kaiser Permanente San Rafael Medical Center", "San Rafael", "CA"),
    "Fremont Hospital": _kp("Kaiser Permanente Fremont Medical Center", "Fremont", "CA"),
    "Sacramento Hospital": _kp("Kaiser Permanente Sacramento Medical Center", "Sacramento", "CA"),
    "Richmond Hospital": _kp("Kaiser Permanente Richmond Medical Center", "Richmond", "CA"),
    "Antioch Hospital": _kp("Kaiser Permanente Antioch Medical Center", "Antioch", "CA"),
    "Fresno Sequoia Hospital": _kp("Kaiser Permanente Fresno Medical Center", "Fresno", "CA"),
    "Manteca Hospital": _kp("Kaiser Permanente Manteca Medical Center", "Manteca", "CA"),
    # Southern California
    "Downey Medical Center": _kp("Kaiser Permanente Downey Medical Center", "Downey", "CA", "cms 050139"),
    "Downey Med Center": _kp("Kaiser Permanente Downey Medical Center", "Downey", "CA", "cms 050139"),
    "Riverside Med Center": _kp("Kaiser Permanente Riverside Medical Center", "Riverside", "CA"),
    "Riverside Medical Center": _kp("Kaiser Permanente Riverside Medical Center", "Riverside", "CA"),
    "South Bay Med Center": _kp("Kaiser Permanente South Bay Medical Center", "Harbor City", "CA"),
    "Sunset Medical Center": _KP_SUNSET,
    "ZION MEDICAL CENTER": _kp("Kaiser Permanente Zion Medical Center", "San Diego", "CA"),
    "San Diego Medical Center": _kp("Kaiser Permanente San Diego Medical Center", "San Diego", "CA"),
    "West LA Medical Center": _kp("Kaiser Permanente West Los Angeles Medical Center", "Los Angeles", "CA"),
    "Woodland Hills Med Center": _kp("Kaiser Permanente Woodland Hills Medical Center", "Woodland Hills", "CA"),
    "OC Anaheim Medical Center": _kp("Kaiser Permanente Anaheim Medical Center", "Anaheim", "CA"),
    "OC Irvine Medical Center": _kp("Kaiser Permanente Irvine Medical Center", "Irvine", "CA"),
    "Fontana Medical Center": _KP_FONTANA,
    "Fontana Med Center": _KP_FONTANA,
    "Baldwin Park Med Center": _kp("Kaiser Permanente Baldwin Park Medical Center", "Baldwin Park", "CA"),
    "SAN MARCOS MEDICAL CENTER": _kp("Kaiser Permanente San Marcos Medical Center", "San Marcos", "CA"),
    "Panorama City Med Center": _kp("Kaiser Permanente Panorama City Medical Center", "Panorama City", "CA"),
    "Ontario Hospital": _kp("Kaiser Permanente Ontario Medical Center", "Ontario", "CA"),
    "Moreno Valley Hospital": _kp("Kaiser Permanente Moreno Valley Medical Center", "Moreno Valley", "CA"),
    # Hawaii (Moanalua; the Maui Health hospitals Kaiser posts for)
    "Moanalua Medical Center": _kp("Kaiser Permanente Moanalua Medical Center", "Honolulu", "HI"),
    "MAUI MEMORIAL MEDICAL CENTER": _kp("Maui Memorial Medical Center", "Wailuku", "HI"),
    "Maui Memorial Med Center": _kp("Maui Memorial Medical Center", "Wailuku", "HI"),
    "LANAI COMMUNITY HOSPITAL": _kp("Lanai Community Hospital", "Lanai", "HI"),
    "KULA HOSPITAL": _kp("Kula Hospital", "Kula", "HI"),
    # Mid-Atlantic, Georgia and Washington medical centers (named outpatient centers)
    "CATON HILL MEDICAL CENTER": _kp("Kaiser Permanente Caton Hill Medical Center", "Woodbridge", "VA"),
    "Tysons Corner Medical Center": _kp("Kaiser Permanente Tysons Corner Medical Center", "McLean", "VA"),
    "Capitol Hill Medical Center": _kp("Kaiser Permanente Capitol Hill Medical Center", "Washington D C", "DC"),
    "Largo Medical Center": _kp("Kaiser Permanente Largo Medical Center", "Largo", "MD"),
    "Gaithersburg HUB Medical Cntrt": _kp("Kaiser Permanente Gaithersburg Medical Center", "Gaithersburg", "MD"),
    "LUTHERVILLE-TIMONIUM MED CTR": _kp("Kaiser Permanente Lutherville-Timonium Medical Center",
                                        "Lutherville Timonium", "MD"),
    "S. Balt. county Med Center": _kp("Kaiser Permanente South Baltimore County Medical Center", "Halethorpe", "MD"),
    "Kensington I Medical Center": _kp("Kaiser Permanente Kensington Medical Center", "North Kensington", "MD"),
    "Shady Grove Medical Center": _kp("Kaiser Permanente Shady Grove Medical Center", "Rockville", "MD"),
    "Woodlawn Medical Center": _kp("Kaiser Permanente Woodlawn Medical Center", "Baltimore", "MD"),
    "Fair Oaks Medical Center": _kp("Kaiser Permanente Fair Oaks Medical Center", "Fairfax", "VA"),
    "Reston Medical Center": _kp("Kaiser Permanente Reston Medical Center", "Reston", "VA"),
    "Burke Medical Center": _kp("Kaiser Permanente Burke Medical Center", "Burke", "VA"),
    "Northwest DC Medical Center": _kp("Kaiser Permanente Northwest DC Medical Center", "Washington D C", "DC"),
    "Baltimore Washington Med Centr": _kp("Kaiser Permanente Baltimore Washington Medical Center", "Glen Burnie",
                                          "MD"),
    "Gwinn Comprehensive Med Center": _kp("Kaiser Permanente Gwinnett Comprehensive Medical Center", "Duluth", "GA"),
    "TP Comprehensive Med Center": _kp("Kaiser Permanente Town Park Comprehensive Medical Center", "Kennesaw", "GA"),
    "Bellevue Medical Center": _kp("Kaiser Permanente Bellevue Medical Center", "Bellevue", "WA"),
    "Olympia Medical Center": _kp("Kaiser Permanente Olympia Medical Center", "Olympia", "WA"),
    "Silverdale Medical Center": _kp("Kaiser Permanente Silverdale Medical Center", "Silverdale", "WA"),
    "Steele Street Medical Center": _kp("Kaiser Permanente Steele Street Medical Center", "Tacoma", "WA"),
    "Northgate Medical Center": _kp("Kaiser Permanente Northgate Medical Center", "Seattle", "WA"),
    "Smokey Point Medical Center": _kp("Kaiser Permanente Smokey Point Medical Center", "Marysville", "WA"),
    "Redmond Medical Center": _kp("Kaiser Permanente Redmond Medical Center", "Redmond", "WA"),
    "Burien Medical Center": _kp("Kaiser Permanente Burien Medical Center", "Burien", "WA"),
    "Federal Way Medical Center": _kp("Kaiser Permanente Federal Way Medical Center", "Federal Way", "WA"),
    "Everett Medical Center": _kp("Kaiser Permanente Everett Medical Center", "Everett", "WA"),
}
# Hospitals of other systems where Kaiser staff work ("Virginia Hospital
# Center", "Holy Cross Hospital", "Anne Arundel Medical Center", "St. Joseph
# Medical Center", "Washington Hospital Ctr", "Children's Hospital", "Doctors
# Community Hospital") and Kaiser's future Dublin hospital are not listed: the
# Kaiser label stays.
_KP_DEPT_RX = re.compile(r"(?mi)^[ \t]*Department[ \t]*:[ \t]*([^\n]+)")

# ── Body-line rules (MU Health Care, MemorialCare) ──────────────────────────
_MU_UH = ("University Hospital", "Columbia", "MO", "cms 260141")
_MU_RULES = (
    (r"^(?:MU Health Care )?University Hospital\b|^Hospital\s*[-–,]\s*1 Hospital Dr", _MU_UH),
    (r"^(?:MU Health Care )?Children\W?s Hospital\b", ("MU Health Care Children's Hospital", "Columbia", "MO",
                                                       "address 1021 Hitt St, Columbia")),
    (r"^Capital Region(?: Medical Center)?\b", ("Capital Region Medical Center", "Jefferson City", "MO",
                                                "address 1125 Madison St, Jefferson City")),
    (r"^Missouri Orthopa?edic Institute\b", ("Missouri Orthopaedic Institute", "Columbia", "MO",
                                             "address 1100 Virginia Ave, Columbia")),
    (r"^Missouri Psychiatric Center\b", ("Missouri Psychiatric Center", "Columbia", "MO",
                                         "address 3 Hospital Dr, Columbia")),
    (r"^Ellis Fischel Cancer Center\b", ("Ellis Fischel Cancer Center", "Columbia", "MO",
                                         "address 1 Hospital Dr, Columbia")),
)
_MU_LINE_RX = re.compile(r"(?mi)^[ \t]*Facility[ \t]*:[ \t]*([^\n]+)")

_MC_LBMC = ("MemorialCare Long Beach Medical Center", "Long Beach", "CA", "cms 050485")
_MC_RULES = (
    (r"Saddleback (?:Memorial )?Medical Center", ("MemorialCare Saddleback Medical Center", "Laguna Hills", "CA",
                                                  "cms 050603")),
    (r"Orange Coast (?:Memorial )?Medical Center", ("MemorialCare Orange Coast Medical Center", "Fountain Valley",
                                                    "CA", "cms 050678")),
    (r"Miller Children", ("Miller Children's & Women's Hospital Long Beach", "Long Beach", "CA", "cms 053309")),
    (r"^(?!.*Miller Children).*Long Beach (?:Memorial )?Medical Center", _MC_LBMC),
)
_MC_LINE_RX = re.compile(r"(?mi)^[ \t]*Location[ \t]*:[ \t]*([^\n]+)")

# ── The family table ────────────────────────────────────────────────────────
class Fam:
    """One system of this family. `system` is the scraper's emit name,
    `db_label` the stored hospital_system."""

    def __init__(self, system, db_label, labels=(), default=None, list_fields=(), units=None, sites=None,
                 offices=(), segments=None, text=None, codes=None, addresses=None):
        self.system = system
        self.db_label = db_label
        self.labels = frozenset(clean_text(x) for x in (labels or ())) | {system, db_label}
        self.default = default                      # (town, state) the scraper stamps when a feed gives none
        self.list_fields = tuple(list_fields)       # Phenom list keys, first non-blank wins
        self.units = {fkey(k): v for k, v in (units or {}).items()}   # entity -> {town key: site}
        self.sites = {fkey(k): v for k, v in (sites or {}).items()}   # site value -> site
        self.offices = frozenset(fkey(x) for x in offices)            # values that are the system itself
        self.segments = dict(segments or {})        # Workday link segment key -> site
        self.text = text                            # (line regex, rules or site table, kind)
        self.codes = dict(codes or {})              # Hartford Location Detail code -> site
        self.addresses = dict(addresses or {})      # street key of the list's address -> site
        self.names = {}
        for site in self._all_sites():
            self.names.setdefault(fkey(site[0]), site)

    def _all_sites(self):
        for v in self.units.values():
            yield from v.values()
        yield from self.sites.values()
        yield from self.segments.values()
        yield from self.codes.values()
        yield from self.addresses.values()
        if self.text:
            kind, table = self.text[2], self.text[1]
            if kind == "rules":
                for _rx, site in table:
                    yield site
            else:
                yield from table.values()


def _rules(rules):
    return tuple((re.compile(rx, re.I), site) for rx, site in rules)


FAMILY = (
    Fam("Hartford HealthCare", "Hartford HealthCare", list_fields=("businessUnit",), units=_HHC_UNITS,
        codes=_HHC_CODES),
    Fam("University of Maryland Medical System", "University of Maryland Medical System",
        list_fields=("memberOrganization",), units=_UMMS_UNITS),
    Fam("Broward Health", "Broward Health", addresses=_BROWARD_ADDRESSES),
    Fam("Spartanburg Regional", "Spartanburg Regional Healthcare", list_fields=("FacilityName",),
        units=_SRHS_UNITS),
    Fam("ECU Health", "ECU Health", list_fields=("facility", "company"), units=_ECU_UNITS, offices=_ECU_OFFICES),
    Fam("LCMC Health", "LCMC Health", labels=("LCMC",), default=("New Orleans", "LA"),
        list_fields=("facility", "company"), units=_LCMC_UNITS, offices=_LCMC_OFFICES),
    Fam("Franciscan Health", "Franciscan Health", segments=_FH_SEGMENTS, addresses=_FH_ADDRESSES),
    Fam("CentraCare", "CentraCare Health", list_fields=("workLocation", "companyName"), units=_CC_UNITS,
        sites=_CC_SITES),
    Fam("Children's Minnesota", "Children's Minnesota", list_fields=("workLocation",), sites=_CMN_SITES),
    Fam("Children's Healthcare ATL", "Children's Healthcare ATL", segments=_CHOA_SEGMENTS),
    Fam("Kaiser Permanente", "Kaiser Permanente", default=("Oakland", "CA"),
        text=(_KP_DEPT_RX, {fkey(k): v for k, v in _KP_SITES.items()}, "prefix")),
    Fam("MU Health Care", "MU Health Care", labels=("University of Missouri Health Care",),
        text=(_MU_LINE_RX, _rules(_MU_RULES), "rules")),
    Fam("MemorialCare", "MemorialCare", default=("Long Beach", "CA"),
        text=(_MC_LINE_RX, _rules(_MC_RULES), "rules")),
)
FAMILIES: dict[str, Fam] = {}
for _f in FAMILY:
    FAMILIES[_f.system] = _f
    FAMILIES.setdefault(_f.db_label, _f)
SYSTEMS = frozenset(f.system for f in FAMILY)
TEXT_DB_LABELS = tuple(f.db_label for f in FAMILY if f.text or f.codes)


def fam_for(system) -> Fam | None:
    return FAMILIES.get((system or "").strip())


def _town(city) -> str:
    return fkey(city)


def _blank_or_default(fam, city, state) -> bool:
    """The posting gives no town, or only the system's head-office default."""
    c = (city or "").strip()
    if not c:
        return True
    if fam.default and _town(c) == _town(fam.default[0]) and (state or "").strip().upper() in ("", fam.default[1]):
        return True
    return False


def on_label(fam, name) -> bool:
    cur = clean_text(name)
    return not cur or cur in fam.labels


# ── The four sources ────────────────────────────────────────────────────────
def unit_site(fam, raw, city, state):
    """An employing entity: (site or None, handled). handled is False when the
    value is not in the table (the adapter's own value is left alone)."""
    k = fkey(raw)
    if not k:
        return None, False
    if k in fam.offices:
        return None, True
    towns = fam.units.get(k)
    if towns is None:
        return None, False
    if _blank_or_default(fam, city, state):
        sites = {s[0]: s for s in towns.values()}
        # no town: the entity's one site, when it has one
        return (next(iter(sites.values())) if len(sites) == 1 else None), True
    site = towns.get(_town(city))
    if site and (state or "").strip().upper() not in ("", site[2]):
        site = None
    return site, True


def list_site(fam, doc, city="", state=""):
    """(site or None, kind) from one Phenom list doc. kind: "site" (a field
    that is the work site: the site's own town), "unit" (an employing entity
    in its hospital's town), "office" (a value that is the system itself, or
    a work site that is no hospital or named center), "gate" (an entity
    outside its hospital's town), "unknown" (a value the table does not
    list), "" (none of the system's fields)."""
    doc = doc or {}
    if fam.addresses:
        k = street_key(doc.get("address"))
        if k:
            site = fam.addresses.get(k)
            return (site, "site") if site else (None, "unknown")
    for f in fam.list_fields:
        raw = clean_text(doc.get(f))
        if not raw or fkey(raw) == "unspecified":
            continue
        k = fkey(raw)
        if f == "workLocation":
            if k in fam.sites:
                return fam.sites[k], "site"
            if fam.units:
                return None, "office"      # CentraCare: a clinic or a building, not one of the hospitals
            return None, "unknown"
        site, handled = unit_site(fam, raw, city, state)
        if site:
            return site, "unit"
        if handled:
            return None, ("office" if k in fam.offices else "gate")
        return None, "unknown"
    return None, ""


_DIRECTIONS = {"n", "s", "e", "w", "ne", "nw", "se", "sw", "north", "south", "east", "west"}


def street_key(address) -> str:
    """"<number> <street word>" of a street address ("1600 S Andrews Avenue"
    -> "1600 andrews"), "" when it does not start with a number."""
    toks = fkey(address).split()
    if len(toks) < 2 or not toks[0].isdigit():
        return ""
    words = [t for t in toks[1:] if t not in _DIRECTIONS]
    return f"{toks[0]} {words[0]}" if words else ""


def segment_key(url) -> str:
    """The first path segment after /job/ of a Workday link, letters and
    digits only, lower case ("" when the link is not one)."""
    m = re.search(r"\.(?:myworkdayjobs|myworkdaysite)\.com/.*?/job/([^/?#]+)/", url or "", re.I)
    return re.sub(r"[^a-z0-9]", "", m.group(1).lower()) if m else ""


def segment_site(fam, url):
    return fam.segments.get(segment_key(url)) if fam.segments else None


def text_site(fam, description):
    """The site the posting's text names, or None (no line, a line the table
    does not list, or a line that names two sites)."""
    body = description or ""
    if fam.codes:
        m = _HHC_CODE_RX.search(body)
        if m:
            return fam.codes.get(m.group(2))
        return None
    if not fam.text:
        return None
    rx, table, kind = fam.text
    m = rx.search(body)
    if not m:
        return None
    line = clean_text(m.group(1))
    if kind == "prefix":
        site_part = fkey(line.split(" - ")[0])
        best = None
        for k, site in table.items():
            if site_part == k or site_part.startswith(k + " "):
                if best is None or len(k) > len(best[0]):
                    best = (k, site)
        return best[1] if best else None
    # a line that lists several places ("University Hospital ... & Capital
    # Region", "Various options: ...", "Fountain Valley/Long Beach", "will
    # support both Dallas and Plano") names none
    if _MULTI_PLACE_RX.search(line):
        return None
    hits = {}
    for rule, site in table:
        if rule.search(line):
            hits[site[0]] = site
    return next(iter(hits.values())) if len(hits) == 1 else None


_MULTI_PLACE_RX = re.compile(r"\bvarious\b|\boptions?\b|\s&\s(?!Women)|/|\bor\b|\band\b|\bboth\b", re.I)


def site_by_name(fam, name):
    """The site a stored hospital_name is (one of this table's own names)."""
    return fam.names.get(fkey(name)) if fam else None


# ── Applying it ─────────────────────────────────────────────────────────────
def placed(fam, site, city, state, *, gate=False):
    """(name, city, state) for a site, or None. A posting with no town, or at
    the system's head-office default, takes the site's own town; a posting in
    the site's town keeps its own spelling; a posting in another town names
    no site (never guess a town: the row keeps the system label and its own
    town). `gate` (an entity) was matched on the posting's town already."""
    name, town, st = site[0], site[1], site[2]
    if _blank_or_default(fam, city, state):
        return name, town, st
    if (state or "").strip().upper() not in ("", st):
        return None
    if gate or _town(city) == _town(town):
        return name, city, st
    return None


def list_facility(system, doc, city="", state="", url=""):
    """For scrape_phenom: (hospital_name, city, state) when the list doc (or
    the row's stored link, `url`) decides the row's name ("" name = the
    system label), None when this family has nothing to say (the adapter's
    own chain runs)."""
    fam = fam_for(system)
    if fam is None or fam.system != system:
        return None
    site = segment_site(fam, url or str((doc or {}).get("applyUrl") or ""))
    if site:
        return placed(fam, site, city, state) or ("", city, state)
    site, kind = list_site(fam, doc, city, state)
    if site:
        return placed(fam, site, city, state, gate=(kind == "unit")) or ("", city, state)
    if kind in ("office", "gate"):
        return "", city, state
    return None


SEEN_ATTR = "_phenom_facility_seen"
NO_BODY_CHARS = 1000            # under this the row carries no body tonight (blank, or a list teaser)
_STORED: dict = {}               # (db_label, job_id) -> stored hospital_name that is a site of the table
STORED_PAGE = 1000


def set_stored_names(rows) -> int:
    global _STORED
    out = {}
    for r in rows or []:
        fam = fam_for(r.get("hospital_system"))
        jid = r.get("job_id")
        if fam is None or jid in (None, ""):
            continue
        site = site_by_name(fam, r.get("hospital_name"))
        if site:
            out[(fam.db_label, str(jid))] = site[0]
    _STORED = out
    return len(out)


def load_stored_names() -> int:
    """Read-only PostgREST passes over the active rows of the text-derived
    systems (Kaiser, Hartford, MU, MemorialCare),
    one system at a time, keyset-paged on id. Without credentials the map
    stays empty and nothing is kept."""
    sb_url = os.environ.get("SUPABASE_URL", "")
    sb_key = sb_keys.supabase_key()
    if not sb_url or not sb_key:
        set_stored_names([])
        return 0
    import urllib.parse as _urlparse
    import urllib.request as _urlreq
    rows = []
    for label in TEXT_DB_LABELS:
        flt = _urlparse.quote(f"eq.{label}", safe=".")
        last = 0
        try:
            while True:
                u = (f"{sb_url.rstrip('/')}/rest/v1/hospital_jobs?select=id,hospital_system,job_id,hospital_name"
                     f"&is_active=is.true&hospital_system={flt}&id=gt.{last}&order=id.asc&limit={STORED_PAGE}")
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
                last = chunk[-1]["id"]
                if len(chunk) < STORED_PAGE:
                    break
        except Exception as e:
            logger.warning(f"Stored Phenom-family names: {label} load stopped ({e}); "
                           f"its rows are named from tonight's posting only")
    try:
        n = set_stored_names(rows)
    except Exception as e:          # never let this read end the run
        logger.warning(f"Stored Phenom-family names: not installed ({e})")
        set_stored_names([])
        return 0
    logger.info(f"Stored Phenom-family names: {n:,} of {len(rows):,} active rows name a site")
    return n


def stored_name(fam, job_id) -> str:
    return _STORED.get((fam.db_label, str(job_id)), "")


def _got(where, how):
    return where + (how,) if where else None


def resolve_row(system, *, hospital_name="", url="", description="", title="", city="", state="", job_id=None,
                fallback=True):
    """(name, city, state, how) for one row whose name is the system label
    (or a raw spelling the table cleans), or None when nothing changes. how:
    "segment", "list" (a stored raw value), "text", "kept". Shared by the
    scraper (finalize) and the stored-row cleanup."""
    fam = fam_for(system)
    if fam is None:
        return None
    k = fkey(hospital_name)
    if not on_label(fam, hospital_name):
        # a raw spelling of a list value (ECU "ECUH BEA Campus of ECUH MC")
        if k in fam.sites:
            return _got(placed(fam, fam.sites[k], city, state), "list") or (fam.system, city, state, "list")
        site, handled = unit_site(fam, hospital_name, city, state)
        if site:
            return _got(placed(fam, site, city, state, gate=True), "list")
        if handled:
            return fam.system, city, state, "list"         # the system itself, or an entity outside its town
        return None
    if k in fam.offices and clean_text(hospital_name) != fam.system:
        return fam.system, city, state, "list"             # LCMC's "LCMC": the label the list now writes
    site = segment_site(fam, url)
    if site:
        return _got(placed(fam, site, city, state), "segment")
    body = description or ""
    if fam.text or fam.codes:
        if len(body.strip()) >= NO_BODY_CHARS:
            site = text_site(fam, body)
            return _got(placed(fam, site, city, state), "text") if site else None
        if fallback and job_id not in (None, ""):
            site = site_by_name(fam, stored_name(fam, job_id))
            if site:
                return _got(placed(fam, site, city, state), "kept")
    return None


def apply_facility(job) -> str:
    """Store the site on one Job of this family (finalize_jobs, after
    facility_names). Returns how ("" for another system, "none")."""
    fam = fam_for(job.hospital_system)
    if fam is None or fam.system != job.hospital_system:
        return ""
    got = resolve_row(job.hospital_system, hospital_name=job.hospital_name, url=job.url,
                      description=job.description, title=job.title, city=job.city, state=job.state,
                      job_id=job.job_id)
    if not got:
        return "none"
    name, city, state, how = got
    if how == "list" and name == fam.system:
        job.hospital_name = name
        return "office"
    job.hospital_name = name
    if (city, state) != (job.city, job.state):
        job.city, job.state = city, state
        job.location = f"{city}, {state}" if city else state
    return how


def apply_facilities(jobs) -> dict:
    """apply_facility over a run's rows ({system: {how: rows}}). The stored
    names are read first, on every call: finalize_jobs runs once a run, and
    the scheduler may run several in one process."""
    try:
        load_stored_names()
    except Exception as e:          # never let this read end the run
        logger.warning(f"Stored Phenom-family names: not loaded ({e})")
    stats: dict = {}
    for job in jobs:
        how = apply_facility(job)
        if how:
            d = stats.setdefault(job.hospital_system, {})
            d[how] = d.get(how, 0) + 1
    return stats
