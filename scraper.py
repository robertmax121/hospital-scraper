"""
Hospital Job Scraper — Maximum Coverage Build
Fixed API endpoints + verbose error logging to diagnose 0-job issues.
"""

import asyncio
import aiohttp
import json
import logging
import random
import re
import time
import os
from dataclasses import dataclass, asdict, field
from datetime import datetime
from typing import Optional

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(f"logs/run_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# ── Proxy rotation ─────────────────────────────────────────────────────────
class ProxyRotator:
    def __init__(self):
        proxy_file = os.environ.get("PROXY_FILE", "proxies.txt")
        if os.path.exists(proxy_file):
            with open(proxy_file) as f:
                self.proxies = [line.strip().rstrip(",") for line in f if line.strip().rstrip(",")]
        else:
            raw = os.environ.get("PROXY_LIST", "")
            self.proxies = [p.strip().rstrip(",") for p in re.split(r"[,\n]+", raw) if p.strip().rstrip(",")]
        self._i = 0
        if self.proxies:
            logger.info(f"  Proxies loaded: {len(self.proxies)} available")
        else:
            logger.warning("  No proxies configured — running without proxies")

    def get(self) -> Optional[str]:
        if not self.proxies:
            return None
        p = self.proxies[self._i % len(self.proxies)]
        self._i += 1
        parts = p.split(":")
        # Support both host:port:user:pass and user:pass@host:port formats
        if len(parts) == 4:
            return f"http://{parts[2]}:{parts[3]}@{parts[0]}:{parts[1]}"
        elif "@" in p:
            return f"http://{p}"
        else:
            return f"http://{p}"

proxies = ProxyRotator()

# ── Job dataclass ──────────────────────────────────────────────────────────
@dataclass
class Job:
    title: str
    hospital_system: str
    hospital_name: str
    city: str
    state: str
    location: str
    specialty: str
    job_type: str
    url: str
    job_id: str
    posted_date: str
    description: str
    ats_platform: str
    scraped_at: str = field(default_factory=lambda: datetime.now().isoformat())

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
    "Accept": "application/json, text/html, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

async def jitter(): await asyncio.sleep(random.uniform(0.8, 2.5))
def strip_html(s): return re.sub(r"<[^>]+>", "", s or "")[:500]



class _FallbackResponse:
    """Wrapper so we can use 'async with' syntax with fallback logic."""
    def __init__(self, session, method, url, proxy, kwargs):
        self._s = session
        self._method = method
        self._url = url
        self._proxy = proxy
        self._kw = kwargs
        self._ctx = None

    async def __aenter__(self):
        fn = getattr(self._s, self._method)
        try:
            self._ctx = fn(self._url, proxy=self._proxy, **self._kw)
            r = await self._ctx.__aenter__()
            if r.status in (502, 503, 407) and self._proxy:
                await self._ctx.__aexit__(None, None, None)
                self._ctx = fn(self._url, **self._kw)  # no proxy
                r = await self._ctx.__aenter__()
            return r
        except Exception as e:
            if self._proxy and ("502" in str(e) or "Bad Gateway" in str(e) or "407" in str(e)):
                fn2 = getattr(self._s, self._method)
                self._ctx = fn2(self._url, **self._kw)
                return await self._ctx.__aenter__()
            raise

    async def __aexit__(self, *args):
        if self._ctx:
            await self._ctx.__aexit__(*args)


def req(session, method, url, **kwargs):
    """Drop-in for 'async with session.get/post(...)' with proxy fallback."""
    proxy = kwargs.pop("proxy", None)
    return _FallbackResponse(session, method, url, proxy, kwargs)



# ══════════════════════════════════════════════════════════════════════════
#  WORKDAY
#  Format: "System Name": (tenant, wd_num, career_site_name)
#  Find these by visiting: https://tenant.wd5.myworkdayjobs.com/
# ══════════════════════════════════════════════════════════════════════════
WORKDAY_TENANTS = {
    "Kaiser Permanente":         ("kaiserpermanente",   "5",  "KP_External_Careers"),
    "Providence Health":         ("providence",         "5",  "Providence_External"),
    # Baylor Scott & White moved to Phenom — see PHENOM_ORGS
    "Banner Health":             ("bannerhealth",       "5",  "Banner_Health"),
    "Northwell Health":          ("northwell",          "5",  "Northwell_External"),
    "Intermountain Health":      ("intermountain",      "1",  "Careers"),
    "UC Health (Colorado)":      ("uchealth",           "1",  "UCHealth_External"),
    "Novant Health":             ("novant",             "1",  "Novant_Health_External"),
    "Prisma Health":             ("prismahealth",       "1",  "External"),
    "Geisinger":                 ("geisinger",          "1",  "Geisinger_External"),
    "Sanford Health":            ("sanfordhealth",      "5",  "Sanford_Health"),
    "SSM Health":                ("ssmhealth",          "1",  "SSM_Health_External"),
    "Mercy Health":              ("mercy",              "5",  "External"),
    "Carilion Clinic":           ("carilion",           "1",  "Carilion_External"),
    "DaVita":                    ("davita",             "1",  "DaVita_External"),
    "Henry Ford Health":         ("henryford",          "1",  "Henry_Ford_External"),
    "Houston Methodist":         ("houstonmethodist",   "1",  "HoustonMethodist_External"),
    "Indiana University Health": ("iuhealth",           "1",  "IU_Health_External"),
    "Inova Health":              ("inova",              "1",  "Inova_Careers"),
    "NewYork-Presbyterian":      ("nyp",                "1",  "NYP_External"),
    "Ochsner Health":            ("ochsner",            "5",  "Ochsner_Careers"),
    "Parkland Health":           ("parkland",           "5",  "Parkland_External"),
    "Piedmont Healthcare":       ("piedmont",           "1",  "Piedmont_Careers"),
    "RWJBarnabas Health":        ("rwjbarnabas",        "1",  "RWJBarnabas_External"),
    "Sharp HealthCare":          ("sharp",              "1",  "External"),
    "Sutter Health":             ("sutterhealth",       "1",  "Sutter_Health"),
    "UNC Health":                ("unchealth",          "1",  "UNC_Health"),
    "UnityPoint Health":         ("unitypoint",         "1",  "UnityPoint_Careers"),
    "UT Southwestern Medical":   ("utsouthwestern",     "1",  "UTSW_External"),
    "VCU Health":                ("vcuhealth",          "5",  "VCUHealth"),
    "WakeMed":                   ("wakemed",            "1",  "WakeMed_External"),
    "Wellstar Health":           ("wellstar",           "1",  "Wellstar_Health_External"),
    "Memorial Hermann":          ("memorialhermann",    "5",  "memorialhermann"),
    "OhioHealth":                ("ohiohealth",         "1",  "OhioHealth_External"),
    "WellSpan Health":           ("wellspan",           "1",  "WellSpan_Health"),
    "Hackensack Meridian":       ("hackensackmeridian", "1",  "HMH_External"),
    "MaineHealth":               ("mainehealth",        "1",  "MaineHealth_Careers"),
    "McLaren Health Care":       ("mclaren",            "1",  "McLaren_External"),
    "OSF HealthCare":            ("osf",                "1",  "OSF_HealthCare_External"),
    "Tufts Medicine":            ("tuftsmedicine",      "1",  "TuftsMedicine_External"),
    "Virtua Health":             ("virtua",             "1",  "Virtua_Careers"),
    "Adventist Health":          ("adventisthealth",    "1",  "Adventist_Health"),
    "Dignity Health":            ("dignityhealth",      "1",  "DignityHealth_External"),
    "Bon Secours":               ("bonsecours",         "1",  "BonSecours_External"),
    "Essentia Health":           ("essentiahealth",     "1",  "Essentia_Health_External"),
    "Fairview Health":           ("fairview",           "1",  "Fairview_Health_External"),
    # ── Confirmed from direct URL verification ──
    "BestCare Health":           ("bestcare",           "1",  "bestcare"),
    "Bronson Healthcare":        ("bronsonhg",          "1",  "newhires"),
    # ── Added from hospital spreadsheet ──
    "Albany Med Health System":              ("albanymed",                "5",  "Albany_Med"),
    "Allina Health":                         ("allina",                   "5",  "en-US"),
    "Avera":                                 ("avera",                    "5",  "en-US"),
    "BJC HealthCare":                        ("saintlukes",               "1",  "en-US"),
    "Baptist Health":                        ("bhs",                      "1",  "careers"),
    "Cape Fear Valley Health":               ("capefearvalley",           "1",  "CFV"),
    "Capital Health":                        ("capitalhealth",            "1",  "CapitalHealthCareers"),
    "Endeavor Health":                       ("nshs",                     "1",  "ns-eeh"),
    "Freeman Health":                        ("freemanhealth",            "1",  "jointeamfreeman"),
    "Great River Health":                    ("greatriverhealth",         "5",  "External"),
    "HSHS":                                  ("hshs",                     "1",  "hshscareers"),
    "Halifax Health":                        ("halifaxhealth",            "12", "HalifaxHealth"),
    "Healogics":                             ("healogics",                "5",  "healogics"),
    "Houston Healthcare":                    ("hhc",                      "5",  "en-US"),
    "Intermountain Healthcare":              ("imh",                      "108","IntermountainCareers"),
    "Jefferson Health":                      ("jeffersonhealth",          "5",  "ThomasJeffersonExternal"),
    "John Muir Health":                      ("jmh",                      "5",  "JohnMuirHealthCareers"),
    "Logan Health":                          ("loganhealth",              "1",  "Logan_Careers"),
    "MaineGeneral Health":                   ("mainegeneral",             "5",  "MaineGeneralCareers"),
    "Mary Washington Healthcare":            ("marywashingtonhealthcare", "5",  "en-US"),
    "Mass General Brigham":                  ("massgeneralbrigham",       "1",  "MGBExternal"),
    "Memorial Health System":                ("memorialhealthcare",       "1",  "MHS_Careers"),
    "Methodist Health System":               ("methodisthealthsystem",    "1",  "MHS_Careers"),
    "Methodist Le Bonheur":                  ("methodisthealth",          "5",  "MLH"),
    "Montefiore":                            ("montefiore",               "12", "MMC"),
    "Monument Health":                       ("monumenthealth",           "1",  "en-US"),
    "MultiCare":                             ("multicare",                "1",  "multicare"),
    "Northeast Georgia Medical Center":      ("nghs",                     "1",  "External"),
    "Phelps Health":                         ("phelpshealth",             "5",  "Phelps"),
    "Riverside Health":                      ("rivhs",                    "1",  "Non-ProviderRHS"),
    "SIH":                                   ("sih",                      "5",  "SIH_External"),
    "Saint Francis Health System":           ("saintfrancis",             "1",  "External"),
    "Tidelands Health":                      ("tidelandshealth",          "12", "Tidelands"),
    "UHS":                                   ("nyuhs",                    "12", "nyuhscareers1"),
    "UMass Memorial Health":                 ("ummh",                     "1",  "Careers"),
    "University of Rochester Medicine":      ("rochester",                "5",  "UR_Staff"),
    "UofL Health":                           ("uoflhealth",               "1",  "UofLHealthCareers"),
    "Vanderbilt Health":                     ("vumc",                     "1",  "vumccareers"),
    "Sentara Healthcare":                    ("sentara",                  "1",  "SCS"),
    "Advocate Health":                       ("advocatehealth",           "1",  "careers"),
    "West Tennessee Healthcare":             ("wth",                      "501","WTH"),
    "Bozeman Health":                        ("bozemanhealth",            "1",  "BozemanHealthCareers"),
    "Broadlawns Medical Center":             ("broadlawns",               "501","Broadlawns_Careers"),
    "Hendricks Regional Health":             ("hendricks",                "1",  "Hendricks_External_Career_Site"),
    "Harrison Health":                       ("hrhs",                     "1",  "Careers"),
    "Jupiter Medical Center":                ("jupitermed",               "1",  "External"),
    "Kaweah Health":                         ("kaweahhealth",             "1",  "Careers"),
    "Lawrence Memorial Hospital":            ("lmh",                      "1",  "LMHjobs"),
    "Owensboro Health":                      ("owensborohealth",          "1",  "owensborohealth"),
    "Salinas Valley Health":                 ("salinasvalleyhealth",      "5",  "SalinasValleyHealth"),
    "Samaritan Health":                      ("samaritanhealth",          "12", "shsny"),
    "Sarah Bush Lincoln Health":             ("sarahbush",                "1",  "en-US"),
    "Saint Francis Medical Center":          ("sfmc",                     "1",  "SFHS"),
    "Silver Cross Hospital":                 ("silvercross",              "5",  "SilverCrossCareers"),
    "Stormont Vail Health":                  ("stormontvail",             "1",  "en-US"),
    "Sturdy Memorial Hospital":              ("sturdymemorial",           "5",  "Sturdy"),
    "AdventHealth":                          ("adventhealth",             "12", "AH_External_Career_Site"),
    # ── From URL spreadsheet ──
    "WVU Medicine":                           ("wvumedicine",              "1",  "WVUH"),
    "Pullman Regional Hospital":              ("pullmanregionalhospital",  "1",  "Careers"),
    "Enloe Health":                           ("enloe",                    "12", "EnloeHealth"),
    "Banner Health (confirmed)":              ("bannerhealth",             "108","Careers"),
    "Monument Health":                        ("monumenthealth",           "1",  "Goldcareers"),
    "Saint Luke's Health System":             ("saintlukes",               "1",  "saintlukeshealthcareers"),
    "University of Washington Medicine":      ("uw",                       "5",  "UWHires"),
}

# Generic fallback site names to try when the specific one fails
CAREER_SITE_FALLBACKS = [
    "External_Career_Site",
    "External",
    "Careers",
    "careers",
    "ExternalCareers",
    "External_Careers",
]


##############################################################################
#  LOCATION LOOKUP TABLES
#  Two-tier fallback applied in normalize_job() when city/state is blank
#  or unparseable from the ATS response.
#
#  Tier 1 — FACILITY_LOCATION_MAP: specific hospital/campus name → (city, state)
#  Tier 2 — SYSTEM_LOCATION_DEFAULTS: hospital system → (city, state)
#            Used when the specific facility isn't in Tier 1.
##############################################################################

FACILITY_LOCATION_MAP: dict[str, tuple[str, str]] = {
    # ── Memorial Hermann ──────────────────────────────────────────────────
    "memorial hermann texas medical center": ("Houston", "TX"),
    "memorial hermann memorial city medical center": ("Houston", "TX"),
    "memorial hermann greater heights hospital": ("Houston", "TX"),
    "memorial hermann southwest hospital": ("Houston", "TX"),
    "memorial hermann southeast hospital": ("Houston", "TX"),
    "memorial hermann sugar land hospital": ("Sugar Land", "TX"),
    "memorial hermann pearland hospital": ("Pearland", "TX"),
    "memorial hermann katy hospital": ("Katy", "TX"),
    "memorial hermann northeast hospital": ("Humble", "TX"),
    "memorial hermann the woodlands medical center": ("The Woodlands", "TX"),
    "memorial hermann rehabilitation hospital - katy": ("Katy", "TX"),
    "memorial hermann surgical hospital": ("Houston", "TX"),
    "tirr memorial hermann": ("Houston", "TX"),
    "memorial hermann medical group": ("Houston", "TX"),
    "memorial hermann": ("Houston", "TX"),
    # ── CHRISTUS Health ───────────────────────────────────────────────────
    "christus system office": ("Irving", "TX"),
    "christus ministry system office": ("Irving", "TX"),
    "christus health ark-la-tex": ("Texarkana", "TX"),
    "christus spohn health system": ("Corpus Christi", "TX"),
    "christus spohn hospital corpus christi - shoreline": ("Corpus Christi", "TX"),
    "christus spohn hospital corpus christi - south": ("Corpus Christi", "TX"),
    "christus spohn hospital alice": ("Alice", "TX"),
    "christus spohn hospital beeville": ("Beeville", "TX"),
    "christus spohn hospital kleberg": ("Kingsville", "TX"),
    "christus spohn hospital kenedy": ("Kenedy", "TX"),
    "christus good shepherd health system": ("Longview", "TX"),
    "christus good shepherd medical center - longview": ("Longview", "TX"),
    "christus good shepherd medical center - marshall": ("Marshall", "TX"),
    "christus mother frances hospital - tyler": ("Tyler", "TX"),
    "christus mother frances hospital - jacksonville": ("Jacksonville", "TX"),
    "christus mother frances hospital - winnsboro": ("Winnsboro", "TX"),
    "christus mother frances hospital - sulphur springs": ("Sulphur Springs", "TX"),
    "christus southeast texas health system": ("Beaumont", "TX"),
    "christus southeast texas - st. elizabeth": ("Beaumont", "TX"),
    "christus southeast texas - jasper memorial": ("Jasper", "TX"),
    "christus santa rosa health system": ("San Antonio", "TX"),
    "christus santa rosa hospital - medical center": ("San Antonio", "TX"),
    "christus santa rosa hospital - alamo heights": ("San Antonio", "TX"),
    "christus santa rosa hospital - new braunfels": ("New Braunfels", "TX"),
    "christus santa rosa hospital - westover hills": ("San Antonio", "TX"),
    "christus santa rosa hospital - kyle": ("Kyle", "TX"),
    "christus trinity mother frances": ("Tyler", "TX"),
    "christus muguerza": ("Monterrey", "TX"),
    "christus health shreveport-bossier": ("Shreveport", "LA"),
    "christus health shreveport": ("Shreveport", "LA"),
    "christus schumpert health system": ("Shreveport", "LA"),
    "christus dubuis hospital": ("Houston", "TX"),
    "christus st. vincent regional medical center": ("Santa Fe", "NM"),
    "christus st. vincent": ("Santa Fe", "NM"),
    "christus highlands medical center": ("Sulphur Springs", "TX"),
    "christus continuing care": ("Irving", "TX"),
    "christus children's": ("San Antonio", "TX"),
    "christus children's hospital": ("San Antonio", "TX"),
    # ── Houston Methodist ─────────────────────────────────────────────────
    "houston methodist hospital": ("Houston", "TX"),
    "houston methodist san jacinto hospital": ("Baytown", "TX"),
    "houston methodist west hospital": ("Houston", "TX"),
    "houston methodist willowbrook hospital": ("Houston", "TX"),
    "houston methodist sugar land hospital": ("Sugar Land", "TX"),
    "houston methodist st. john hospital": ("Nassau Bay", "TX"),
    "houston methodist clear lake hospital": ("Nassau Bay", "TX"),
    "houston methodist baytown hospital": ("Baytown", "TX"),
    "houston methodist the woodlands hospital": ("The Woodlands", "TX"),
    # ── Baylor Scott & White ──────────────────────────────────────────────
    "baylor university medical center": ("Dallas", "TX"),
    "baylor scott & white medical center - temple": ("Temple", "TX"),
    "baylor scott & white medical center - waco": ("Waco", "TX"),
    "baylor scott & white medical center - round rock": ("Round Rock", "TX"),
    "baylor scott & white medical center - mckinney": ("McKinney", "TX"),
    "baylor scott & white medical center - plano": ("Plano", "TX"),
    "baylor scott & white all saints medical center": ("Fort Worth", "TX"),
    "baylor scott & white medical center - irving": ("Irving", "TX"),
    "baylor scott & white medical center - hillcrest": ("Waco", "TX"),
    # ── Cleveland Clinic ──────────────────────────────────────────────────
    "cleveland clinic main campus": ("Cleveland", "OH"),
    "cleveland clinic akron general": ("Akron", "OH"),
    "cleveland clinic florida": ("Weston", "FL"),
    "cleveland clinic abu dhabi": ("Abu Dhabi", "AE"),
    "cleveland clinic london": ("London", ""),
    "cleveland clinic avon hospital": ("Avon", "OH"),
    "cleveland clinic marymount hospital": ("Garfield Heights", "OH"),
    "cleveland clinic hillcrest hospital": ("Mayfield Heights", "OH"),
    "cleveland clinic fairview hospital": ("Cleveland", "OH"),
    "cleveland clinic medina hospital": ("Medina", "OH"),
    "cleveland clinic union hospital": ("Dover", "OH"),
    # ── Mayo Clinic ───────────────────────────────────────────────────────
    "mayo clinic - rochester": ("Rochester", "MN"),
    "mayo clinic rochester": ("Rochester", "MN"),
    "mayo clinic - phoenix": ("Phoenix", "AZ"),
    "mayo clinic - scottsdale": ("Scottsdale", "AZ"),
    "mayo clinic - jacksonville": ("Jacksonville", "FL"),
    "mayo clinic florida": ("Jacksonville", "FL"),
    "mayo clinic arizona": ("Phoenix", "AZ"),
    "mayo clinic health system": ("Rochester", "MN"),
    # ── HCA Healthcare ────────────────────────────────────────────────────
    "hca houston healthcare": ("Houston", "TX"),
    "hca florida": ("Nashville", "TN"),
    "las vegas": ("Las Vegas", "NV"),
    # ── Parkland Health ───────────────────────────────────────────────────
    "parkland memorial hospital": ("Dallas", "TX"),
    "parkland health": ("Dallas", "TX"),
    # ── UT Southwestern ───────────────────────────────────────────────────
    "ut southwestern medical center": ("Dallas", "TX"),
    "university of texas southwestern medical center": ("Dallas", "TX"),
    # ── Montefiore ────────────────────────────────────────────────────────
    "montefiore medical center": ("Bronx", "NY"),
    "montefiore einstein": ("Bronx", "NY"),
    "montefiore nyack": ("Nyack", "NY"),
    "montefiore new rochelle": ("New Rochelle", "NY"),
    "montefiore mount vernon": ("Mount Vernon", "NY"),
    # ── NewYork-Presbyterian ──────────────────────────────────────────────
    "newyork-presbyterian hospital": ("New York", "NY"),
    "newyork-presbyterian/weill cornell": ("New York", "NY"),
    "newyork-presbyterian/columbia": ("New York", "NY"),
    "newyork-presbyterian brooklyn methodist": ("Brooklyn", "NY"),
    "newyork-presbyterian queens": ("Flushing", "NY"),
    "newyork-presbyterian lower manhattan": ("New York", "NY"),
    "newyork-presbyterian hudson valley": ("Cortlandt Manor", "NY"),
    # ── Thomas Jefferson / Jefferson Health ───────────────────────────────
    "thomas jefferson university hospital": ("Philadelphia", "PA"),
    "jefferson hospital": ("Philadelphia", "PA"),
    "jefferson cherry hill hospital": ("Cherry Hill", "NJ"),
    "jefferson stratford hospital": ("Stratford", "NJ"),
    "jefferson abington hospital": ("Abington", "PA"),
    "jefferson torresdale hospital": ("Philadelphia", "PA"),
    # ── Mass General Brigham ──────────────────────────────────────────────
    "massachusetts general hospital": ("Boston", "MA"),
    "brigham and women's hospital": ("Boston", "MA"),
    "newton-wellesley hospital": ("Newton", "MA"),
    "north shore medical center": ("Salem", "MA"),
    "mclean hospital": ("Belmont", "MA"),
    "spaulding rehabilitation": ("Boston", "MA"),
    "martha's vineyard hospital": ("Oak Bluffs", "MA"),
    "nantucket cottage hospital": ("Nantucket", "MA"),
    "faulkner hospital": ("Boston", "MA"),
    # ── Vanderbilt Health ─────────────────────────────────────────────────
    "vanderbilt university medical center": ("Nashville", "TN"),
    "vanderbilt wilson county hospital": ("Lebanon", "TN"),
    "vanderbilt health one hundred oaks": ("Nashville", "TN"),
    # ── Ochsner Health ────────────────────────────────────────────────────
    "ochsner medical center": ("New Orleans", "LA"),
    "ochsner medical center - west bank": ("Gretna", "LA"),
    "ochsner medical center - kenner": ("Kenner", "LA"),
    "ochsner medical center - north shore": ("Slidell", "LA"),
    "ochsner medical center - baton rouge": ("Baton Rouge", "LA"),
    "ochsner lafayette general": ("Lafayette", "LA"),
    "ochsner medical center - shreveport": ("Shreveport", "LA"),
    # ── UNC Health ────────────────────────────────────────────────────────
    "unc hospitals": ("Chapel Hill", "NC"),
    "unc rex healthcare": ("Raleigh", "NC"),
    "unc nash health care": ("Rocky Mount", "NC"),
    "unc lenoir health care": ("Kinston", "NC"),
    "chatham hospital": ("Siler City", "NC"),
    "caldwell memorial hospital": ("Lenoir", "NC"),
    # ── Intermountain Healthcare ──────────────────────────────────────────
    "intermountain medical center": ("Murray", "UT"),
    "primary children's hospital": ("Salt Lake City", "UT"),
    "ldsh hospital": ("Salt Lake City", "UT"),
    "lds hospital": ("Salt Lake City", "UT"),
    "intermountain health": ("Salt Lake City", "UT"),
    # ── Additional single-city systems ────────────────────────────────────
    "university of texas medical branch": ("Galveston", "TX"),
    "utmb health": ("Galveston", "TX"),
    "harris health system": ("Houston", "TX"),
    "ben taub hospital": ("Houston", "TX"),
    "lww": ("Houston", "TX"),
}

# Normalize all keys to lowercase for matching
FACILITY_LOCATION_MAP = {k.lower(): v for k, v in FACILITY_LOCATION_MAP.items()}

# System-level fallback — used when facility lookup fails
# Multi-state systems use primary HQ market as default
SYSTEM_LOCATION_DEFAULTS: dict[str, tuple[str, str]] = {
    # Workday tenants
    "kaiser permanente":          ("Oakland",          "CA"),
    "providence health":          ("Renton",           "WA"),
    "banner health":              ("Phoenix",           "AZ"),
    "northwell health":           ("New Hyde Park",     "NY"),
    "intermountain health":       ("Salt Lake City",    "UT"),
    "intermountain healthcare":   ("Salt Lake City",    "UT"),
    "uc health (colorado)":       ("Aurora",            "CO"),
    "novant health":              ("Winston-Salem",     "NC"),
    "prisma health":              ("Greenville",        "SC"),
    "geisinger":                  ("Danville",          "PA"),
    "sanford health":             ("Sioux Falls",       "SD"),
    "ssm health":                 ("St. Louis",         "MO"),
    "mercy health":               ("Chesterfield",      "MO"),
    "carilion clinic":            ("Roanoke",           "VA"),
    "davita":                     ("Denver",            "CO"),
    "henry ford health":          ("Detroit",           "MI"),
    "houston methodist":          ("Houston",           "TX"),
    "indiana university health":  ("Indianapolis",      "IN"),
    "inova health":               ("Falls Church",      "VA"),
    "newyork-presbyterian":       ("New York",          "NY"),
    "ochsner health":             ("New Orleans",       "LA"),
    "parkland health":            ("Dallas",            "TX"),
    "piedmont healthcare":        ("Atlanta",           "GA"),
    "rwjbarnabas health":         ("West Orange",       "NJ"),
    "sharp healthcare":           ("San Diego",         "CA"),
    "sutter health":              ("Sacramento",        "CA"),
    "unc health":                 ("Chapel Hill",       "NC"),
    "unitypoint health":          ("West Des Moines",   "IA"),
    "ut southwestern medical":    ("Dallas",            "TX"),
    "vcu health":                 ("Richmond",          "VA"),
    "wakemed":                    ("Raleigh",           "NC"),
    "wellstar health":            ("Marietta",          "GA"),
    "memorial hermann":           ("Houston",           "TX"),
    "ohiohealth":                 ("Columbus",          "OH"),
    "wellspan health":            ("York",              "PA"),
    "hackensack meridian":        ("Edison",            "NJ"),
    "mainehealth":                ("Portland",          "ME"),
    "mclaren health care":        ("Grand Blanc",       "MI"),
    "osf healthcare":             ("Peoria",            "IL"),
    "tufts medicine":             ("Boston",            "MA"),
    "virtua health":              ("Marlton",           "NJ"),
    "adventist health":           ("Roseville",         "CA"),
    "dignity health":             ("San Francisco",     "CA"),
    "bon secours":                ("Richmond",          "VA"),
    "essentia health":            ("Duluth",            "MN"),
    "fairview health":            ("Minneapolis",       "MN"),
    "bestcare health":            ("Bend",              "OR"),
    "bronson healthcare":         ("Kalamazoo",         "MI"),
    "albany med health system":   ("Albany",            "NY"),
    "allina health":              ("Minneapolis",       "MN"),
    "avera":                      ("Sioux Falls",       "SD"),
    "bjc healthcare":             ("St. Louis",         "MO"),
    "baptist health":             ("Louisville",        "KY"),
    "cape fear valley health":    ("Fayetteville",      "NC"),
    "capital health":             ("Pennington",        "NJ"),
    "endeavor health":            ("Evanston",          "IL"),
    "freeman health":             ("Joplin",            "MO"),
    "great river health":         ("West Burlington",   "IA"),
    "hshs":                       ("Springfield",       "IL"),
    "halifax health":             ("Daytona Beach",     "FL"),
    "healogics":                  ("Jacksonville",      "FL"),
    "houston healthcare":         ("Warner Robins",     "GA"),
    "jefferson health":           ("Philadelphia",      "PA"),
    "john muir health":           ("Walnut Creek",      "CA"),
    "logan health":               ("Kalispell",         "MT"),
    "mainegeneral health":        ("Augusta",           "ME"),
    "mary washington healthcare": ("Fredericksburg",    "VA"),
    "mass general brigham":       ("Boston",            "MA"),
    "memorial health system":     ("Savannah",          "GA"),
    "methodist health system":    ("Dallas",            "TX"),
    "methodist le bonheur":       ("Memphis",           "TN"),
    "montefiore":                 ("Bronx",             "NY"),
    "monument health":            ("Rapid City",        "SD"),
    "multicare":                  ("Tacoma",            "WA"),
    "northeast georgia medical center": ("Gainesville", "GA"),
    "phelps health":              ("Rolla",             "MO"),
    "riverside health":           ("Newport News",      "VA"),
    "sih":                        ("Carbondale",        "IL"),
    "saint francis health system":("Tulsa",             "OK"),
    "tidelands health":           ("Murrells Inlet",    "SC"),
    "uhs":                        ("King of Prussia",   "PA"),
    "umass memorial health":      ("Worcester",         "MA"),
    "university of rochester medicine": ("Rochester",   "NY"),
    "uofl health":                ("Louisville",        "KY"),
    "vanderbilt health":          ("Nashville",         "TN"),
    "sentara healthcare":         ("Norfolk",           "VA"),
    "advocate health":            ("Charlotte",         "NC"),
    "west tennessee healthcare":  ("Jackson",           "TN"),
    "bozeman health":             ("Bozeman",           "MT"),
    "broadlawns medical center":  ("Des Moines",        "IA"),
    "hendricks regional health":  ("Danville",          "IN"),
    "harrison health":            ("Bremerton",         "WA"),
    "jupiter medical center":     ("Jupiter",           "FL"),
    "kaweah health":              ("Visalia",           "CA"),
    "lawrence memorial hospital": ("Lawrence",          "KS"),
    "owensboro health":           ("Owensboro",         "KY"),
    "salinas valley health":      ("Salinas",           "CA"),
    "samaritan health":           ("Watertown",         "NY"),
    "sarah bush lincoln health":  ("Mattoon",           "IL"),
    "saint francis medical center":("Cape Girardeau",   "MO"),
    "silver cross hospital":      ("New Lenox",         "IL"),
    "stormont vail health":       ("Topeka",            "KS"),
    "sturdy memorial hospital":   ("Attleboro",         "MA"),
    # SmartRecruiters
    "davita":                     ("Denver",            "CO"),
    "northwestern medicine":      ("Chicago",           "IL"),
    "healthpartners":             ("St. Paul",          "MN"),
    "envision healthcare":        ("Nashville",         "TN"),
    "amerihealth caritas":        ("Philadelphia",      "PA"),
    "chenmed":                    ("Miami",             "FL"),
    "alignment healthcare":       ("Orange",            "CA"),
    "kindred healthcare":         ("Louisville",        "KY"),
    "acadia healthcare":          ("Franklin",          "TN"),
    "surgery partners":           ("Nashville",         "TN"),
    # Playwright
    "mayo clinic":                ("Rochester",         "MN"),
    "christus health":            ("Irving",            "TX"),
    "baylor scott & white":       ("Dallas",            "TX"),
    "hca healthcare":             ("Nashville",         "TN"),
    "cleveland clinic":           ("Cleveland",         "OH"),
    "mymichigan health":          ("Midland",           "MI"),
    # CommonSpirit (TalentBrew — no state from URL)
    "commonspirit health":        ("Chicago",           "IL"),  # last resort for cities not in COMMONSPIRIT_CITY_STATE
    # Greenhouse
    "davita":                     ("Denver",            "CO"),
}

# Normalize system keys to lowercase
SYSTEM_LOCATION_DEFAULTS = {k.lower(): v for k, v in SYSTEM_LOCATION_DEFAULTS.items()}

# Systems where we ALWAYS override city/state regardless of what the ATS returns.
# Use sparingly — only when the ATS consistently returns wrong/campus-name data
# and every job is definitively in one location.
FORCE_LOCATION_OVERRIDE: dict[str, tuple[str, str]] = {
    # Systems where ALL jobs are in one metro — always override ATS data
    "memorial hermann":                    ("Houston",      "TX"),
    "methodist health system":             ("Dallas",       "TX"),
    "methodist le bonheur":                ("Memphis",      "TN"),
    "northeast georgia medical center":    ("Gainesville",  "GA"),
    "cape fear valley health":             ("Fayetteville", "NC"),
    "broadlawns medical center":           ("Des Moines",   "IA"),
    "jupiter medical center":              ("Jupiter",      "FL"),
    "phelps health":                       ("Rolla",        "MO"),
    "sturdy memorial hospital":            ("Attleboro",    "MA"),
    "freeman health":                      ("Joplin",       "MO"),
    "sih":                                 ("Carbondale",   "IL"),
    "harrison health":                     ("Bremerton",    "WA"),
    "kaweah health":                       ("Visalia",      "CA"),
    "silver cross hospital":               ("New Lenox",    "IL"),
    "tidelands health":                    ("Murrells Inlet","SC"),
    "salinas valley health":               ("Salinas",      "CA"),
    "bozeman health":                      ("Bozeman",      "MT"),
    "logan health":                        ("Kalispell",    "MT"),
    "great river health":                  ("West Burlington","IA"),
    "halifax health":                      ("Daytona Beach","FL"),
    "mary washington healthcare":          ("Fredericksburg","VA"),
    "saint francis health system":         ("Tulsa",        "OK"),
    "saint francis medical center":        ("Cape Girardeau","MO"),
    "lawrence memorial hospital":          ("Lawrence",     "KS"),
}
FORCE_LOCATION_OVERRIDE = {k.lower(): v for k, v in FORCE_LOCATION_OVERRIDE.items()}


def parse_city_state(loc_str: str) -> tuple[str, str]:
    """
    Extract (city, state) from a location string robustly.
    Handles:
      - "City, ST"
      - "City, ST, United States"
      - "ST, City"  (CHRISTUS-style reversed)
      - Full state names from SmartRecruiters ("Chicago, Illinois")
      - Single-segment strings ("Remote")
    Returns 2-char state code where possible.
    """
    STATE_ABBR = {
        "alabama":"AL","alaska":"AK","arizona":"AZ","arkansas":"AR","california":"CA",
        "colorado":"CO","connecticut":"CT","delaware":"DE","florida":"FL","georgia":"GA",
        "hawaii":"HI","idaho":"ID","illinois":"IL","indiana":"IN","iowa":"IA",
        "kansas":"KS","kentucky":"KY","louisiana":"LA","maine":"ME","maryland":"MD",
        "massachusetts":"MA","michigan":"MI","minnesota":"MN","mississippi":"MS",
        "missouri":"MO","montana":"MT","nebraska":"NE","nevada":"NV","new hampshire":"NH",
        "new jersey":"NJ","new mexico":"NM","new york":"NY","north carolina":"NC",
        "north dakota":"ND","ohio":"OH","oklahoma":"OK","oregon":"OR","pennsylvania":"PA",
        "rhode island":"RI","south carolina":"SC","south dakota":"SD","tennessee":"TN",
        "texas":"TX","utah":"UT","vermont":"VT","virginia":"VA","washington":"WA",
        "west virginia":"WV","wisconsin":"WI","wyoming":"WY","district of columbia":"DC",
        "puerto rico":"PR","guam":"GU","virgin islands":"VI",
    }
    JUNK = {"united states","us","usa","canada","remote","united kingdom","uk",""}
    if not loc_str:
        return "", ""
    parts = [p.strip() for p in str(loc_str).split(",")]
    # Strip trailing zip codes from each part (e.g. "TX  75039" → "TX", "Irving TX 75039" → "Irving TX")
    import re as _re
    parts = [_re.sub(r'\s+\d{5}(-\d{4})?$', '', p).strip() for p in parts]
    # Remove segments that are purely numeric (zip-only segments)
    parts = [p for p in parts if not p.isdigit()]
    # Mark remote explicitly before stripping junk
    is_remote = any(p.lower() == "remote" for p in parts)
    parts = [p for p in parts if p.lower() not in JUNK]
    if not parts:
        return ("Remote", "") if is_remote else ("", "")

    # Find 2-char alpha state code anywhere in parts
    state = next((p for p in parts if len(p) == 2 and p.isalpha()), "")

    # If no 2-char code, check for full state name
    if not state:
        for p in parts:
            abbr = STATE_ABBR.get(p.lower(), "")
            if abbr:
                state = abbr
                break

    # Determine city: if first part IS the state code → reversed format
    if parts and len(parts[0]) == 2 and parts[0].isalpha() and parts[0].upper() == state:
        city = parts[1] if len(parts) > 1 else ""
    else:
        # Remove state/country parts to get city
        city = next((p for p in parts
                     if p != state
                     and p.lower() not in JUNK
                     and not STATE_ABBR.get(p.lower(), "")), parts[0])

    return city.strip(), state.upper() if state else ""

async def scrape_workday(session: aiohttp.ClientSession, system: str, tenant_data: tuple) -> list[Job]:
    tenant, wd_num, primary_site = tenant_data
    jobs = []

    # Try primary site name first, then fallbacks, across multiple wd numbers
    site_names = [primary_site] + [s for s in CAREER_SITE_FALLBACKS if s != primary_site]
    wd_nums = [wd_num] + [n for n in ["5","1","3","10"] if n != wd_num]

    working_url = None
    # Try both myworkdayjobs.com (standard) and myworkdaysite.com (used by some universities)
    domains = ["myworkdayjobs.com", "myworkdaysite.com"]
    for domain in domains:
        for wdn in wd_nums:
            for sn in site_names:
                if domain == "myworkdaysite.com":
                    # myworkdaysite uses different URL pattern
                    url = f"https://{domain}/recruiting/{tenant}/{sn}"
                    # Convert to API endpoint
                    api_url = f"https://{domain}/wday/cxs/{tenant}/{sn}/jobs"
                else:
                    url = f"https://{tenant}.wd{wdn}.{domain}/wday/cxs/{tenant}/{sn}/jobs"
                    api_url = url
                try:
                    async with req(session, "post", api_url,
                        json={"limit": 1, "offset": 0, "searchText": "", "locations": [], "categories": []},
                        headers={**HEADERS, "Content-Type": "application/json"}, ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=10)) as r:
                        if r.status == 200:
                            working_url = api_url
                            break
                except:
                    continue
            if working_url:
                break
        if working_url:
            break

    if not working_url:
        logger.info(f"Workday {system}: no working URL found for tenant '{tenant}'")
        return []

    logger.info(f"Workday {system}: found at {working_url}")
    offset = 0
    while True:
        try:
            async with req(session, "post", working_url,
                json={"limit": 20, "offset": offset, "searchText": "", "locations": [], "categories": []},
                headers={**HEADERS, "Content-Type": "application/json"}, ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=25)) as r:
                if r.status != 200:
                    break
                data = await r.json()
            listings = data.get("jobPostings", [])
            if not listings: break
            for j in listings:
                loc = j.get("locationsText", "")
                _city, _state = parse_city_state(loc)
                jobs.append(Job(
                    title=j.get("title", ""),
                    hospital_system=system,
                    hospital_name=system,
                    city=_city,
                    state=_state,
                    location=loc,
                    specialty=(j.get("categories") or [{}])[0].get("name", ""),
                    job_type=j.get("timeType", ""),
                    url=working_url.replace("/wday/cxs/"+tenant+"/","/" ).replace("/jobs","") + "/job/" + j.get("externalPath",""),
                    job_id=str(j.get("bulletFields", [""])[0] or j.get("title", "") + loc),
                    posted_date=j.get("postedOn", ""),
                    description=strip_html(str(j.get("jobDescription", ""))),
                    ats_platform="Workday",
                ))
            offset += 20
            if offset >= data.get("total", 0): break
            await jitter()
        except Exception as e:
            logger.info(f"Workday {system}: {e}")
            break
    return jobs

async def run_workday(session) -> list[Job]:
    logger.info(f"Workday: scraping {len(WORKDAY_TENANTS)} systems...")
    results = await asyncio.gather(
        *[scrape_workday(session, s, t) for s, t in WORKDAY_TENANTS.items()],
        return_exceptions=True
    )
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  Workday: {len(jobs):,} jobs")
    return jobs


# ══════════════════════════════════════════════════════════════════════════
#  TALEO — Fixed endpoint
# ══════════════════════════════════════════════════════════════════════════
# Removed DNS-dead orgs: hcahealthcare, tenethealth, lifepointhealth, chscare,
#   christushealth, selectmedical, shrinershospitals, nhccare, teamhealth, encompasshealth
# Adding orgs with confirmed *.taleo.net DNS resolution:
TALEO_ORGS = {
    "HCA Healthcare":              "hcahealthcare",
    "Tenet Health":                "tenethealth",
    "LifePoint Health":            "lifepointhealth",
    "Community Health Systems":    "chscare",
    "Select Medical":              "selectmedical",
    "Shriners Hospitals":          "shrinershospitals",
    "Encompass Health":            "encompasshealth",
    "National Healthcare Corp":    "nhccare",
    "TeamHealth":                  "teamhealth",
}

async def scrape_taleo(session: aiohttp.ClientSession, system: str, org: str) -> list[Job]:
    jobs = []
    # Updated Taleo endpoint pattern
    base_url = f"https://{org}.taleo.net"
    try:
        # First get the company code
        async with session.get(
            f"{base_url}/careersection/rest/jobboard/renderRequisitionList",
            params={"lang": "en", "organization": org, "pageNo": 1, "pageSize": 25,
                    "sortField": "POSTING_DATE", "sortDirection": "DESC"},
            headers=HEADERS,
            timeout=aiohttp.ClientTimeout(total=25)) as r:
            if r.status != 200:
                logger.info(f"Taleo {system}: HTTP {r.status}")
                return []
            data = await r.json(content_type=None)
    except Exception as e:
        logger.info(f"Taleo {system}: {e}")
        return []

    page = 1
    while True:
        try:
            async with session.get(
                f"{base_url}/careersection/rest/jobboard/renderRequisitionList",
                params={"lang": "en", "organization": org, "pageNo": page, "pageSize": 25,
                        "sortField": "POSTING_DATE", "sortDirection": "DESC"},
                headers=HEADERS,
                timeout=aiohttp.ClientTimeout(total=25)) as r:
                if r.status != 200: break
                data = await r.json(content_type=None)
            reqs = data.get("requisitionList", [])
            if not reqs: break
            for j in reqs:
                _tcity = j.get("city", "")
                _tstate = j.get("state", "")
                # Taleo state can be full name ("Texas") — normalize to 2-char
                _, state = parse_city_state(f"{_tcity}, {_tstate}")
                city  = _tcity
                state = state or _tstate
                jobs.append(Job(
                    title=j.get("title", ""),
                    hospital_system=system,
                    hospital_name=j.get("organizationName", system),
                    city=city, state=state, location=f"{city}, {state}",
                    specialty=j.get("jobField", ""),
                    job_type=j.get("jobType", ""),
                    url=f"{base_url}/careersection/2/jobdetail.ftl?job={j.get('contestNo','')}",
                    job_id=str(j.get("contestNo", "")),
                    posted_date=j.get("postingDate", ""),
                    description=strip_html(j.get("jobDescription", "")),
                    ats_platform="Taleo",
                ))
            if len(reqs) < 25: break
            page += 1
            await jitter()
        except Exception as e:
            logger.info(f"Taleo {system} page {page}: {e}")
            break
    return jobs

async def run_taleo(session) -> list[Job]:
    logger.info(f"Taleo: scraping {len(TALEO_ORGS)} systems...")
    results = await asyncio.gather(
        *[scrape_taleo(session, s, o) for s, o in TALEO_ORGS.items()],
        return_exceptions=True
    )
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  Taleo: {len(jobs):,} jobs")
    return jobs


# ══════════════════════════════════════════════════════════════════════════
#  iCIMS — Correct per-org subdomain API
#  Each org has its own career portal domain backed by iCIMS.
#  The JSON search endpoint: GET /jobs/search?mode=json returns structured data.
#  org_data = full domain of the career portal (no protocol)
# ══════════════════════════════════════════════════════════════════════════
ICIMS_ORGS = {
    # Format: "System": "subdomain.icims.com"
    # All domains verified to use .icims.com subdomain format for JSON API access
    # REMOVED (wrong platform): UPMC (Taleo), Sentara (Workday), Advocate Aurora (Workday),
    #   Northwestern Medicine (SmartRecruiters), HealthPartners (SmartRecruiters)
    "MedStar Health":         "careers.medstarhealth.org",
    "Kettering Health":       "careers-ketteringhealth.icims.com",
    "Loma Linda University":  "careers-lluh.icims.com",
    "Texas Health Resources": "careers-texashealth.icims.com",
    "Cone Health":            "careers-conehealth.icims.com",
    "Monument Health":        "careers-monument.icims.com",
    "Owensboro Health":       "careers-owensborohealth.icims.com",
    "Stormont Vail":          "careers-stormontvail.icims.com",
    # ── From URL spreadsheet ──
    "Appalachian Regional Healthcare":  "careers-arh.icims.com",
    "Prime Healthcare":                 "careers-primehealthcare.icims.com",
    "Midland Health":                   "hospital-midlandhealth.icims.com",
    "Covenant Health":                  "careers-covenanthealth.icims.com",
    "Providence Health & Services":     "careers-hub-phs.icims.com",
    "Tri-City Medical Center":          "careers-tricitymed.icims.com",
    "Emory Healthcare":                 "ehccareers-emory.icims.com",
    "St. Luke's Health System":         "careers-slhs.icims.com",
    "Methodist Hospitals":              "careers-methodisthospitals.icims.com",
    "Central Maine Healthcare":         "careers-centralmainehealthcare-ph.icims.com",
    "Tuality Healthcare":               "careers-tuality.icims.com",
    "Legacy Health":                    "careers-lhs.icims.com",
    "OHSU":                             "careersat-ohsu.icims.com",
}


# ── TALENTBREW ─────────────────────────────────────────────────────────────────
# TalentBrew career sites — HTML results endpoint, paginated
TALENTBREW_ORGS = {
    # Format: "System": ("base_url", records_per_page)
    "CommonSpirit Health": ("https://www.commonspirit.careers/search-jobs", 100),
}


##############################################################################
#  COMMONSPIRIT HEALTH — city slug → state lookup
#  CommonSpirit operates in 21 states. The TalentBrew URL contains city but
#  no state. This map resolves the city slug to a state code.
#  Source: CommonSpirit facility directory (commonspirit.org/locations)
##############################################################################
COMMONSPIRIT_CITY_STATE: dict[str, str] = {
    # Arizona
    "phoenix": "AZ", "chandler": "AZ", "mesa": "AZ", "tempe": "AZ",
    "scottsdale": "AZ", "flagstaff": "AZ", "prescott": "AZ",
    "prescott-valley": "AZ", "sun-city": "AZ", "casa-grande": "AZ",
    "globe": "AZ", "show-low": "AZ", "sierra-vista": "AZ",
    "bullhead-city": "AZ", "lake-havasu-city": "AZ", "kingman": "AZ",
    "parker": "AZ", "wickenburg": "AZ", "yuma": "AZ", "nogales": "AZ",
    "tucson": "AZ", "laveen": "AZ", "gilbert": "AZ", "peoria": "AZ",
    "surprise": "AZ", "glendale": "AZ", "goodyear": "AZ",
    # California
    "bakersfield": "CA", "fresno": "CA", "stockton": "CA",
    "modesto": "CA", "sacramento": "CA", "santa-rosa": "CA",
    "san-jose": "CA", "san-francisco": "CA", "oakland": "CA",
    "redding": "CA", "eureka": "CA", "gilroy": "CA", "hollister": "CA",
    "morgan-hill": "CA", "merced": "CA", "turlock": "CA",
    "los-gatos": "CA", "santa-cruz": "CA", "watsonville": "CA",
    "monterey": "CA", "san-luis-obispo": "CA", "santa-barbara": "CA",
    "ventura": "CA", "oxnard": "CA", "long-beach": "CA",
    "los-angeles": "CA", "burlingame": "CA", "daly-city": "CA",
    "hayward": "CA", "fremont": "CA", "san-leandro": "CA",
    "castro-valley": "CA", "livermore": "CA", "pleasanton": "CA",
    "walnut-creek": "CA", "concord": "CA", "antioch": "CA",
    "pittsburg": "CA", "vallejo": "CA", "napa": "CA", "petaluma": "CA",
    "santa-monica": "CA", "torrance": "CA", "garden-grove": "CA",
    "anaheim": "CA", "corona": "CA", "riverside": "CA",
    "san-bernardino": "CA", "fontana": "CA", "ontario": "CA",
    "rancho-cucamonga": "CA", "palm-springs": "CA", "visalia": "CA",
    "porterville": "CA", "hanford": "CA", "tulare": "CA",
    "woodland": "CA", "chico": "CA", "marysville": "CA",
    # Colorado
    "colorado-springs": "CO", "pueblo": "CO", "denver": "CO",
    "canon-city": "CO", "woodland-park": "CO", "aurora": "CO",
    "colorado-city": "CO",
    # Illinois
    "chicago": "IL", "joliet": "IL", "aurora": "IL", "bolingbrook": "IL",
    "romeoville": "IL", "channahon": "IL", "waukegan": "IL", "elgin": "IL",
    "urbana": "IL", "champaign": "IL", "danville": "IL", "kankakee": "IL",
    "pontiac": "IL", "springfield": "IL", "decatur": "IL",
    "peoria": "IL", "bloomington": "IL", "rockford": "IL",
    "ottawa": "IL", "streator": "IL", "peru": "IL",
    # Indiana
    "hammond": "IN", "munster": "IN", "dyer": "IN", "valparaiso": "IN",
    "crown-point": "IN", "merrillville": "IN", "michigan-city": "IN",
    "la-porte": "IN", "hobart": "IN", "portage": "IN",
    "east-chicago": "IN", "gary": "IN",
    # Iowa
    "iowa-city": "IA", "cedar-rapids": "IA", "davenport": "IA",
    "dubuque": "IA", "waterloo": "IA",
    # Kansas
    "wichita": "KS", "chanute": "KS", "pittsburg": "KS",
    # Kentucky
    "lexington": "KY", "corbin": "KY",
    # Minnesota
    "saint-paul": "MN", "st-paul": "MN", "crookston": "MN",
    "minneapolis": "MN",
    # Montana
    "missoula": "MT", "helena": "MT", "great-falls": "MT",
    "butte": "MT", "billings": "MT", "kalispell": "MT",
    "bozeman": "MT", "miles-city": "MT", "glendive": "MT",
    "havre": "MT", "polson": "MT",
    # Nebraska
    "omaha": "NE", "lincoln": "NE", "hastings": "NE", "kearney": "NE",
    "norfolk": "NE", "mccook": "NE", "alliance": "NE",
    "papillion": "NE", "bellevue": "NE", "grand-island": "NE",
    "north-platte": "NE", "columbus": "NE", "fremont": "NE",
    "york": "NE", "beatrice": "NE",
    # Nevada
    "las-vegas": "NV", "henderson": "NV", "north-las-vegas": "NV",
    "reno": "NV",
    # North Dakota
    "bismarck": "ND", "fargo": "ND", "grand-forks": "ND",
    "minot": "ND", "jamestown": "ND", "devils-lake": "ND",
    "dickinson": "ND", "williston": "ND",
    # Oregon
    "portland": "OR", "eugene": "OR", "bend": "OR", "salem": "OR",
    "corvallis": "OR", "grants-pass": "OR", "medford": "OR",
    "roseburg": "OR", "coos-bay": "OR", "north-bend": "OR",
    "ashland": "OR", "klamath-falls": "OR", "la-grande": "OR",
    "pendleton": "OR", "the-dalles": "OR", "hood-river": "OR",
    # South Dakota
    "sioux-falls": "SD", "aberdeen": "SD", "huron": "SD",
    "watertown": "SD", "mitchell": "SD", "pierre": "SD",
    "yankton": "SD", "vermillion": "SD", "rapid-city": "SD",
    # Tennessee
    "memphis": "TN",
    # Texas
    "houston": "TX", "san-antonio": "TX", "corpus-christi": "TX",
    "victoria": "TX", "laredo": "TX", "waco": "TX",
    # Washington
    "yakima": "WA", "kennewick": "WA", "spokane": "WA",
    "richland": "WA", "walla-walla": "WA", "colville": "WA",
    "omak": "WA", "bridgeport": "WA", "brewster": "WA",
    "prosser": "WA", "sunnyside": "WA", "grandview": "WA",
    "othello": "WA", "pasco": "WA", "moses-lake": "WA",
    "wenatchee": "WA", "ellensburg": "WA",
    # Wisconsin
    "la-crosse": "WA", "neillsville": "WI", "monroe": "WI",
    "sparta": "WI", "onalaska": "WI",
    # Arkansas
    "harrison": "AR",
}
# Also accept the title-cased city name (from .replace("-"," ").title())
_cs_extra = {}
for k, v in COMMONSPIRIT_CITY_STATE.items():
    _cs_extra[k.replace("-", " ").title().lower()] = v
COMMONSPIRIT_CITY_STATE.update(_cs_extra)

async def scrape_talentbrew(session: aiohttp.ClientSession, system: str, base_url: str, rpp: int = 100) -> list[Job]:
    """Scrape a TalentBrew career site via their paginated results endpoint.
    Includes robust retry logic with exponential backoff + proxy rotation for
    connection drops (the CommonSpirit server intermittently drops the TCP
    connection mid-session, typically around page 14 of 48).
    """
    jobs = []
    page = 1
    results_url = base_url.rstrip("/") + "/results"
    MAX_RETRIES = 10         # max retries per page before giving up on that page
    BASE_BACKOFF = 3.0       # seconds — doubles each retry

    while True:
        params = {
            "ActiveFacetID": "0",
            "CurrentPage": str(page),
            "RecordsPerPage": str(rpp),
            "TotalContentResults": "",
            "Distance": "50",
            "RadiusUnitType": "0",
            "Keywords": "",
            "Location": "",
            "ShowRadius": "False",
            "IsPagination": "True" if page > 1 else "False",
            "CustomFacetName": "",
            "FacetTerm": "",
            "FacetType": "0",
            "SearchResultsModuleName": "Section 6 - Search Results List",
            "SearchFiltersModuleName": "Section 6 - Search Filters",
            "SortCriteria": "0",
            "SortDirection": "0",
            "PostalCode": "",
            "TotalContentPages": "0",
            "SearchType": "5",
            "ResultsType": "0",
            "fc": "", "fl": "", "fcf": "", "afc": "", "afl": "", "afcf": "",
        }

        attempt = 0
        page_succeeded = False

        while attempt <= MAX_RETRIES:
            try:
                async with req(session, "get", results_url, params=params,
                               headers={**HEADERS, "X-Requested-With": "XMLHttpRequest",
                                        "Accept": "text/html,*/*"},
                               proxy=proxies.get(),
                               timeout=aiohttp.ClientTimeout(total=90)) as r:
                    if r.status != 200:
                        logger.info(f"TalentBrew {system}: HTTP {r.status} on page {page}, retry {attempt}/{MAX_RETRIES}")
                        raise Exception(f"HTTP {r.status}")  # trigger retry logic
                    html = await r.text()

                # Parse response — JSON envelope wrapping HTML fragment
                try:
                    data = json.loads(html)
                    results_html = data.get("results", "")
                    has_jobs = data.get("hasJobs", False)
                except Exception:
                    results_html = html
                    has_jobs = True

                if not has_jobs or not results_html:
                    logger.info(f"TalentBrew {system}: hasJobs={has_jobs}, empty on page {page} — done")
                    return jobs

                # Extract job URLs — pattern: /job/{city}/{title-slug}/35300/{job-id}
                job_matches = re.findall(
                    r'href="(?:https?://[^"]*)?(/job/([^/]+)/([^/]+)/\d+/(\d+))"',
                    results_html
                )

                if not job_matches:
                    logger.info(f"TalentBrew {system}: no job links on page {page} — done")
                    return jobs

                seen = set()
                for url_path, city, title_slug, job_id in job_matches:
                    if job_id in seen:
                        continue
                    seen.add(job_id)

                    title = title_slug.replace("-", " ").title()
                    city_name = city.replace("-", " ").title()
                    # Resolve state from city slug using CommonSpirit location map
                    city_state = COMMONSPIRIT_CITY_STATE.get(city.lower(), "") or                                  COMMONSPIRIT_CITY_STATE.get(city_name.lower(), "")

                    # Extract actual title from adjacent heading in HTML
                    title_match = re.search(
                        rf'href="[^"]*{re.escape(job_id)}"[^>]*>\s*([^<]+)<',
                        results_html
                    )
                    if title_match:
                        title = title_match.group(1).strip()

                    jobs.append(Job(
                        title=title,
                        hospital_system=system,
                        hospital_name=system,
                        city=city_name,
                        state=city_state,
                        location=f"{city_name}, {city_state}" if city_state else city_name,
                        specialty="",
                        job_type="",
                        url=f"https://www.commonspirit.careers{url_path}",
                        job_id=job_id,
                        posted_date="",
                        description="",
                        ats_platform="TalentBrew",
                    ))

                logger.info(f"TalentBrew {system}: page {page} → {len(seen)} jobs (total so far: {len(jobs)})")
                page_succeeded = True

                if len(seen) < rpp:
                    return jobs  # last page — we're done
                break  # success — move to next page

            except Exception as e:
                err_str = str(e).lower()
                attempt += 1
                # Retryable: any TCP/SSL connection failure, timeout, or incomplete read
                is_retryable = any(kw in err_str for kw in [
                    "connect", "timeout", "payload", "incomplete",
                    "reset", "broken pipe", "eof", "ssl", "timed out"
                ])
                if is_retryable and attempt <= MAX_RETRIES:
                    backoff = BASE_BACKOFF * (2 ** (attempt - 1))  # 2, 4, 8, 16, 32, 64s
                    logger.info(f"TalentBrew {system}: page {page} connection error ({e}) — retry {attempt}/{MAX_RETRIES} in {backoff:.0f}s (new proxy)")
                    await asyncio.sleep(backoff)
                    # proxies.get() will automatically rotate to next proxy on next call
                else:
                    logger.info(f"TalentBrew {system}: page {page} failed after {MAX_RETRIES} retries — stopping at {len(jobs)} jobs")
                    return jobs  # give up on this system

        if not page_succeeded:
            return jobs

        page += 1
        await jitter()

    return jobs


async def run_talentbrew(session: aiohttp.ClientSession) -> list[Job]:
    logger.info(f"TalentBrew: scraping {len(TALENTBREW_ORGS)} systems...")
    tasks = [scrape_talentbrew(session, sys, url, rpp) for sys, (url, rpp) in TALENTBREW_ORGS.items()]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    all_jobs = []
    total = 0
    for (sys, _), result in zip(TALENTBREW_ORGS.items(), results):
        if isinstance(result, Exception):
            logger.info(f"  TalentBrew {sys}: ERROR {result}")
        else:
            logger.info(f"  TalentBrew {sys}: {len(result)} jobs")
            total += len(result)
            all_jobs.extend(result)
    logger.info(f"  TalentBrew total: {total} jobs")
    return all_jobs

async def _scrape_icims_modern(session: aiohttp.ClientSession, system: str, domain: str) -> list[Job]:
    """Handles newer iCIMS portals that use JavaScript-rendered search pages.
    Fetches the search results page and extracts job data from embedded JSON
    or structured HTML attributes."""
    import json as _json
    jobs = []
    base_url = f"https://{domain}"
    # Modern iCIMS search URL — pr=1 triggers paginated results
    url = f"{base_url}/jobs/search"
    page = 1
    while True:
        try:
            async with req(session, "get", url,
                params={"ss": "1", "pr": str(page), "searchCategory": "", "searchLocation": "", "searchKeyword": ""},
                headers={**HEADERS, "Accept": "text/html,application/xhtml+xml"},
                proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status != 200:
                    logger.info(f"iCIMS modern {system}: HTTP {r.status}")
                    break
                text = await r.text()

            # Pattern 1: JSON blob embedded in page
            m = re.search(r'icims\.data\s*=\s*(\{.*?"jobs"\s*:\s*\[.*?\].*?\});', text, re.DOTALL)
            if not m:
                m = re.search(r'window\.__ICIMS_DATA__\s*=\s*(\{.*?\});', text, re.DOTALL)
            if m:
                try:
                    data = _json.loads(m.group(1))
                    raw = data.get("jobs", data.get("searchResults", []))
                    if not raw:
                        break
                    for j in raw:
                        loc = j.get("joblocation", j.get("location", ""))
                        _city, _state = parse_city_state(str(loc))
                        jid = str(j.get("jobid", j.get("id", "")))
                        jobs.append(Job(
                            title=j.get("jobtitle", j.get("title", "")),
                            hospital_system=system, hospital_name=j.get("jobcompany", system),
                            city=_city, state=_state, location=str(loc),
                            specialty=j.get("jobcategory", ""), job_type=j.get("jobtype", ""),
                            url=j.get("detailUrl", f"{base_url}/jobs/{jid}/job"),
                            job_id=jid,
                            posted_date=str(j.get("postdate", ""))[:10],
                            description=strip_html(j.get("jobdescription", "")),
                            ats_platform="iCIMS",
                        ))
                    if len(raw) < 25:
                        break
                    page += 1
                    await jitter()
                    continue
                except Exception as e:
                    logger.info(f"iCIMS modern {system}: JSON parse error {e}")

            # Pattern 2: HTML data attributes
            found = re.findall(
                r'data-id="(\d+)"[^>]*data-title="([^"]+)"[^>]*data-location="([^"]*)"',
                text
            )
            if found:
                for jid, title, loc in found:
                    _city, _state = parse_city_state(loc)
                    jobs.append(Job(
                        title=title, hospital_system=system, hospital_name=system,
                        city=_city, state=_state, location=loc,
                        specialty="", job_type="",
                        url=f"{base_url}/jobs/{jid}/job",
                        job_id=jid, posted_date="", description="", ats_platform="iCIMS",
                    ))
                # HTML results are not paginated — check for next page link
                if 'class="iCIMS_Pager"' in text and f'pr={page+1}' in text:
                    page += 1
                    await jitter()
                    continue
            break
        except Exception as e:
            logger.info(f"iCIMS modern {system}: {e}")
            break
    logger.info(f"iCIMS modern {system}: {len(jobs)} jobs")
    return jobs


async def scrape_icims(session: aiohttp.ClientSession, system: str, domain: str) -> list[Job]:
    jobs = []
    base_url = f"https://{domain}"

    # iCIMS has two JSON API patterns depending on portal version:
    # 1. Classic: /jobs/search?mode=json&ss=1&p_startrow=N  (older portals)
    # 2. Modern:  /jobs/search?ss=1&pr=1&searchCategory=&searchLocation=&searchKeyword=  (newer, returns HTML with embedded JSON)
    # Try classic JSON first, fall through to HTML parsing if it fails.

    url = f"{base_url}/jobs/search"
    offset = 0
    while True:
        try:
            async with req(session, "get", 
                url,
                params={
                    "ss": "1",
                    "searchKeyword": "",
                    "searchLocation": "",
                    "mode": "json",
                    "iis": "Job+Board",
                    "in_iframe": "1",
                    "p_startrow": offset,
                },
                headers={**HEADERS, "Accept": "application/json, text/html"}, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status == 404:
                    # Classic JSON API not available — try modern HTML+embedded JSON endpoint
                    logger.info(f"iCIMS {system}: classic API 404, trying modern endpoint")
                    jobs = await _scrape_icims_modern(session, system, domain)
                    return jobs
                if r.status != 200:
                    logger.info(f"iCIMS {system}: HTTP {r.status}")
                    break
                ct = r.headers.get("content-type", "")
                if "json" in ct:
                    data = await r.json(content_type=None)
                    listings = data.get("jobs", data.get("searchResults", []))
                    if not listings:
                        break
                    for j in listings:
                        loc = j.get("joblocation", "") or j.get("location", "")
                        _city, _state = parse_city_state(str(loc))
                        jid = str(j.get("jobid", j.get("id", "")))
                        jobs.append(Job(
                            title=j.get("jobtitle", j.get("title", "")),
                            hospital_system=system,
                            hospital_name=j.get("jobcompany", system),
                            city=_city, state=_state,
                            location=str(loc),
                            specialty=j.get("jobcategory", ""),
                            job_type=j.get("jobtype", ""),
                            url=j.get("detailUrl", f"https://{domain}/jobs/{jid}/job"),
                            job_id=jid,
                            posted_date=str(j.get("postdate", ""))[:10],
                            description=strip_html(j.get("jobdescription", "")),
                            ats_platform="iCIMS",
                        ))
                    if len(listings) < 25:
                        break
                    offset += 25
                else:
                    # HTML fallback — parse structured data from page
                    text = await r.text()
                    found = re.findall(
                        r'data-id="(\d+)"[^>]*data-title="([^"]+)"[^>]*data-location="([^"]*)"',
                        text
                    )
                    if not found:
                        # Try JSON embedded in page
                        m = re.search(r'window\.__ICIMS_DATA__\s*=\s*(\{.*?\});', text, re.DOTALL)
                        if m:
                            try:
                                import json
                                page_data = json.loads(m.group(1))
                                found_json = page_data.get("jobs", [])
                                for j in found_json:
                                    loc = j.get("location", "")
                                    _city, _state = parse_city_state(loc)
                                    jid = str(j.get("id", ""))
                                    jobs.append(Job(
                                        title=j.get("title", ""),
                                        hospital_system=system, hospital_name=system,
                                        city=_city, state=_state,
                                        location=loc, specialty="", job_type="",
                                        url=f"https://{domain}/jobs/{jid}/job",
                                        job_id=jid, posted_date="", description="",
                                        ats_platform="iCIMS",
                                    ))
                            except: pass
                        break
                    for jid, title, loc in found:
                        _city, _state = parse_city_state(loc)
                        jobs.append(Job(
                            title=title, hospital_system=system, hospital_name=system,
                            city=_city, state=_state,
                            location=loc, specialty="", job_type="",
                            url=f"https://{domain}/jobs/{jid}/job",
                            job_id=jid, posted_date="", description="", ats_platform="iCIMS",
                        ))
                    break
                await jitter()
        except Exception as e:
            logger.info(f"iCIMS {system}: {e}")
            break
    return jobs

async def run_icims(session) -> list[Job]:
    logger.info(f"iCIMS: scraping {len(ICIMS_ORGS)} systems...")
    results = await asyncio.gather(
        *[scrape_icims(session, s, o) for s, o in ICIMS_ORGS.items()],
        return_exceptions=True
    )
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  iCIMS: {len(jobs):,} jobs")
    return jobs


# ══════════════════════════════════════════════════════════════════════════
#  MAJOR HEALTH SYSTEM CAREER PORTALS (formerly "SuccessFactors")
#  These orgs use custom career portals — scraped via Playwright
#  They are added to CUSTOM_SITES in run_playwright_scrapers()
# ══════════════════════════════════════════════════════════════════════════
SUCCESSFACTORS_ORGS: dict = {}  # Handled via Playwright — see CUSTOM_SITES

async def scrape_successfactors(session, system, org_data) -> list[Job]:
    return []  # These orgs scraped via Playwright

async def run_successfactors(session) -> list[Job]:
    return []  # No-op — these orgs handled by Playwright




# ══════════════════════════════════════════════════════════════════════════
#  GREENHOUSE — Public API (no proxy needed, very reliable)
# ══════════════════════════════════════════════════════════════════════════
GREENHOUSE_ORGS = {
    "One Medical":                 "onemedical",
    "Carbon Health":               "carbonhealth",
    "Included Health":             "includedhealth",
    "Osmind":                      "osmind",
    "Alto Pharmacy":               "alto",
    "Brightspring Health":         "brightspringhealth",
    "Aveanna Healthcare":          "aveanna",
    "BrightSpring":                "brightspring",
    "Pediatrix Medical Group":     "pediatrix",
    "RadNet":                      "radnet",
}

async def scrape_greenhouse(session: aiohttp.ClientSession, system: str, org: str) -> list[Job]:
    try:
        async with req(session, "get", 
            f"https://boards-api.greenhouse.io/v1/boards/{org}/jobs?content=true",
            headers=HEADERS, ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=25)) as r:
            if r.status != 200:
                logger.info(f"Greenhouse {system}: HTTP {r.status}")
                return []
            data = await r.json()
        jobs = []
        for j in data.get("jobs", []):
            loc = j.get("location", {}).get("name", "")
            _city, _state = parse_city_state(loc)
            jobs.append(Job(
                title=j.get("title", ""),
                hospital_system=system,
                hospital_name=system,
                city=_city,
                state=_state,
                location=loc,
                specialty=next((d["name"] for d in j.get("departments", []) if d.get("name")), ""),
                job_type="Full-time",
                url=j.get("absolute_url", ""),
                job_id=str(j.get("id", "")),
                posted_date=j.get("updated_at", "")[:10],
                description=strip_html(j.get("content", "")),
                ats_platform="Greenhouse",
            ))
        return jobs
    except Exception as e:
        logger.info(f"Greenhouse {system}: {e}")
        return []

async def run_greenhouse(session) -> list[Job]:
    logger.info(f"Greenhouse: scraping {len(GREENHOUSE_ORGS)} orgs...")
    results = await asyncio.gather(
        *[scrape_greenhouse(session, s, o) for s, o in GREENHOUSE_ORGS.items()],
        return_exceptions=True
    )
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  Greenhouse: {len(jobs):,} jobs")
    return jobs


# ══════════════════════════════════════════════════════════════════════════
#  SMARTRECRUITERS
# ══════════════════════════════════════════════════════════════════════════
SMARTRECRUITERS_ORGS = {
    # IDs = company slug from jobs.smartrecruiters.com/{slug}
    "DaVita":               "DaVita",
    "Northwestern Medicine": "northwesternmedicine",
    "HealthPartners":       "HealthPartners1",
    "Envision Healthcare":  "EnvisionHealthcare",
    "AmeriHealth Caritas":  "AmeriHealthCaritas",
    "ChenMed":              "ChenMed",
    "Alignment Healthcare": "AlignmentHealthcare",
    # Added verified SR orgs:
    "Kindred Healthcare":   "KindredatHome",
    "Acadia Healthcare":    "AcadiaHealthcare",
    "Surgery Partners":     "SurgeryPartners",
    # IORA Health removed — acquired by One Medical (Amazon)
}

async def scrape_smartrecruiters(session: aiohttp.ClientSession, system: str, org: str) -> list[Job]:
    jobs, offset = [], 0
    while True:
        try:
            async with req(session, "get", 
                f"https://api.smartrecruiters.com/v1/companies/{org}/postings",
                params={"limit": 100, "offset": offset},
                headers=HEADERS, ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=25)) as r:
                if r.status != 200:
                    logger.info(f"SmartRecruiters {system}: HTTP {r.status}")
                    break
                data = await r.json()
            listings = data.get("content", [])
            if not listings: break
            for j in listings:
                loc_d  = j.get("location", {})
                city   = loc_d.get("city", "")
                # region is often a full state name ("Illinois") — normalize it
                _, _state = parse_city_state(f"{city}, {loc_d.get('region','')}")
                state  = _state or loc_d.get("region", "")
                jobs.append(Job(
                    title=j.get("name", ""),
                    hospital_system=system,
                    hospital_name=system,
                    city=city, state=state, location=f"{city}, {state}",
                    specialty=j.get("department", {}).get("label", ""),
                    job_type=j.get("typeOfEmployment", {}).get("label", ""),
                    url=f"https://jobs.smartrecruiters.com/{org}/{j.get('id','')}",
                    job_id=str(j.get("id", "")),
                    posted_date=j.get("releasedDate", "")[:10],
                    description="",
                    ats_platform="SmartRecruiters",
                ))
            offset += 100
            if offset >= data.get("totalFound", 0): break
            await jitter()
        except Exception as e:
            logger.info(f"SmartRecruiters {system}: {e}")
            break
    return jobs

async def run_smartrecruiters(session) -> list[Job]:
    logger.info(f"SmartRecruiters: scraping {len(SMARTRECRUITERS_ORGS)} orgs...")
    results = await asyncio.gather(
        *[scrape_smartrecruiters(session, s, o) for s, o in SMARTRECRUITERS_ORGS.items()],
        return_exceptions=True
    )
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  SmartRecruiters: {len(jobs):,} jobs")
    return jobs


# ══════════════════════════════════════════════════════════════════════════
#  LEVER
# ══════════════════════════════════════════════════════════════════════════
LEVER_ORGS = {
    # Verified working Lever org IDs (slug from jobs.lever.co/{slug})
    "Brightside Health":    "brightside",
    "Tempus AI":            "tempus-ai",
    "Hims & Hers":          "hims-hers-1",
    "SonderMind":           "SonderMind",
    "Nuvation Bio":         "nuvation-bio",
    # Removed (404): cityblock-health, nomi-health, calibrate
}

async def scrape_lever(session: aiohttp.ClientSession, system: str, org: str) -> list[Job]:
    try:
        async with req(session, "get", 
            f"https://api.lever.co/v0/postings/{org}?mode=json",
            headers=HEADERS, ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=25)) as r:
            if r.status != 200:
                logger.info(f"Lever {system}: HTTP {r.status}")
                return []
            listings = await r.json()
        jobs = []
        for j in (listings if isinstance(listings, list) else []):
            loc = j.get("categories", {}).get("location", "")
            _city, _state = parse_city_state(loc)
            jobs.append(Job(
                title=j.get("text", ""),
                hospital_system=system,
                hospital_name=system,
                city=_city,
                state=_state,
                location=loc,
                specialty=j.get("categories", {}).get("department", ""),
                job_type=j.get("categories", {}).get("commitment", ""),
                url=j.get("hostedUrl", ""),
                job_id=j.get("id", ""),
                posted_date=str(j.get("createdAt", ""))[:10],
                description=strip_html(j.get("descriptionPlain", "")),
                ats_platform="Lever",
            ))
        return jobs
    except Exception as e:
        logger.info(f"Lever {system}: {e}")
        return []

async def run_lever(session) -> list[Job]:
    logger.info(f"Lever: scraping {len(LEVER_ORGS)} orgs...")
    results = await asyncio.gather(
        *[scrape_lever(session, s, o) for s, o in LEVER_ORGS.items()],
        return_exceptions=True
    )
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  Lever: {len(jobs):,} jobs")
    return jobs


# ══════════════════════════════════════════════════════════════════════════
#  USAJOBS — Free public API
# ══════════════════════════════════════════════════════════════════════════
async def run_usajobs(session) -> list[Job]:
    logger.info("USAJOBS: scraping VA + federal hospitals...")
    jobs = []
    MEDICAL_SERIES = "0600;0601;0602;0610;0620;0630;0640;0645;0646;0647;0648;0649;0660;0670;0675"
    ORGS = [
        ("VA Hospitals",            "VATA"),
        ("Indian Health Service",   "HE38"),
        ("Military Health System",  "DD"),
        ("NIH Clinical Center",     "HE06"),
    ]
    usajobs_key = os.environ.get("USAJOBS_API_KEY", "")
    usajobs_email = os.environ.get("USAJOBS_EMAIL", "")
    usajobs_headers = {
        **HEADERS,
        "Host": "data.usajobs.gov",
        "User-Agent": usajobs_email or "hospitalJobScraper@example.com",
        "Authorization-Key": usajobs_key,
    }
    for system_name, org_code in ORGS:
        try:
            async with session.get(
                "https://data.usajobs.gov/api/search",
                params={"Organization": org_code, "ResultsPerPage": 500, "JobCategoryCode": MEDICAL_SERIES},
                headers=usajobs_headers,
                timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status == 401:
                    logger.info(f"USAJOBS: 401 — set USAJOBS_API_KEY and USAJOBS_EMAIL env vars (free at usajobs.gov/Applicant/ProfileDashboard/Home)")
                    break
                if r.status != 200:
                    logger.info(f"USAJOBS {system_name}: HTTP {r.status}")
                    continue
                data = await r.json()
            for item in data.get("SearchResult", {}).get("SearchResultItems", []):
                m = item.get("MatchedObjectDescriptor", {})
                loc = (m.get("PositionLocation") or [{}])[0]
                city  = loc.get("CityName", "")
                state = loc.get("CountrySubDivisionCode", "")
                jobs.append(Job(
                    title=m.get("PositionTitle", ""),
                    hospital_system=system_name,
                    hospital_name=m.get("OrganizationName", system_name),
                    city=city, state=state, location=f"{city}, {state}",
                    specialty=(m.get("JobCategory") or [{}])[0].get("Name", ""),
                    job_type=(m.get("PositionSchedule") or [{}])[0].get("Name", ""),
                    url=m.get("PositionURI", ""),
                    job_id=m.get("PositionID", ""),
                    posted_date=m.get("PublicationStartDate", "")[:10],
                    description=m.get("QualificationSummary", "")[:500],
                    ats_platform="USAJOBS",
                ))
            await jitter()
        except Exception as e:
            logger.info(f"USAJOBS {system_name}: {e}")

    logger.info(f"  USAJOBS: {len(jobs):,} jobs")
    return jobs


# ══════════════════════════════════════════════════════════════════════════
#  PLAYWRIGHT
# ══════════════════════════════════════════════════════════════════════════
##############################################################################
#  PHENOM PEOPLE — CommonSpirit, Baptist Health, Corewell, etc.
#  Phenom renders jobs via JS — no public REST API accessible without auth.
#  These orgs are scraped via Playwright (see CUSTOM_SITES below).
##############################################################################
# Phenom org codes from CDN URLs (cdn.phenompeople.com/CareerConnectResources/{ORG_CODE}/...)
# Used to build the direct Phenom backend API URL as first probe attempt.
PHENOM_ORG_CODES = {
    "Ascension Health":  "AHEAHUUS",   # confirmed from cdn.phenompeople.com/CareerConnectResources/AHEAHUUS/
    "Corewell Health":   "SPHEUS",      # confirmed from cdn.phenompeople.com/CareerConnectResources/SPHEUS/
    "Temple Health":     "TUHTUHUS",   # confirmed from widgets intercept refNum
}

PHENOM_ORGS = {
    # CommonSpirit moved to TalentBrew — see run_talentbrew
    # Baylor Scott & White moved to Playwright — session-based Phenom
    "Baptist Health":               "https://jobs.baptisthealthcareers.com",
    "Munson Healthcare":            "https://careers.munsonhealthcare.org",
    "Bryan Health":                 "https://careers.bryanhealth.com",
    "PeaceHealth":                  "https://careers.peacehealth.org",
    "Roper St. Francis Healthcare": "https://careers.rsfh.com",
    "ScionHealth":                  "https://jobs.scionhealth.com",
    "Temple Health":                "https://careers.templehealth.org",
    "Atrium Health":                "https://careers.atriumhealth.org",
    "ECU Health":                   "https://careers.ecuhealth.org",
    "Penn Medicine":                "https://careers.pennmedicine.org",
    "UPMC":                         "https://careers.upmc.com",
}

async def scrape_phenom(session: aiohttp.ClientSession, system: str, base_url: str) -> list[Job]:
    jobs = []
    # Build endpoint list — prepend direct Phenom People backend if org code known
    org_code = PHENOM_ORG_CODES.get(system, "")
    endpoints = []
    if org_code:
        endpoints.append(f"https://api.phenompeople.com/CareerConnectResources/{org_code}/jobs/search")
    endpoints += [
        f"{base_url}/api/jobs",
        f"{base_url}/api/search/jobs",
        f"{base_url}/search/jobs",
        f"{base_url}/en/search-results",
    ]
    api_url = None
    use_post = False  # track whether the working endpoint needs POST
    probe_headers = {
        **HEADERS,
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Origin": base_url,
        "Referer": f"{base_url}/us/en/search-results",
    }

    for ep in endpoints:
        is_cdn = "api.phenompeople.com" in ep

        # Try POST first (modern Phenom portals), then GET fallback
        for method in ("post", "get"):
            try:
                if method == "post":
                    post_body = {"from": 0, "size": 1, "language": "en_US",
                                 "query": "", "location": ""}
                    req_kwargs = {"json": post_body}
                else:
                    get_params = {"from": 0, "size": 1, "language": "en_US"} if is_cdn else {"start": 0, "num": 1, "from": 0, "size": 1, "language": "en_US"}
                    req_kwargs = {"params": get_params}

                async with getattr(session, method)(
                    ep,
                    **req_kwargs,
                    headers=probe_headers,
                    proxy=proxies.get(), ssl=False, timeout=aiohttp.ClientTimeout(total=15)
                ) as r:
                    if r.status == 200:
                        ct = r.headers.get("content-type", "")
                        if "json" in ct:
                            try:
                                probe_data = await r.json(content_type=None)
                                if probe_data.get("errorCode") or probe_data.get("error"):
                                    logger.info(f"Phenom {system}: {ep} [{method}] → errorCode={probe_data.get('errorCode')} msg={str(probe_data.get('errorMsg',''))[:60]}")
                                    continue  # try next method
                            except Exception:
                                pass
                            api_url = ep
                            use_post = (method == "post")
                            break
            except Exception as probe_err:
                logger.info(f"Phenom {system}: probe {ep} [{method}] → {probe_err}")
                continue
        if api_url:
            break

    if not api_url:
        logger.info(f"Phenom {system}: no API endpoint found")
        return []

    logger.info(f"Phenom {system}: using {api_url} [{'POST' if use_post else 'GET'}]")
    offset = 0
    fetch_headers = {
        **HEADERS,
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Origin": base_url,
        "Referer": f"{base_url}/us/en/search-results",
    }
    while True:
        try:
            is_cdn = "api.phenompeople.com" in api_url
            if use_post:
                fetch_kwargs = {"json": {"from": offset, "size": 50, "language": "en_US", "query": "", "location": ""}}
                http_method = session.post
            else:
                fetch_params = {"from": offset, "size": 50, "language": "en_US"} if is_cdn else {"start": offset, "num": 50, "size": 50, "from": offset, "language": "en_US"}
                fetch_kwargs = {"params": fetch_params}
                http_method = session.get
            async with http_method(
                api_url,
                **fetch_kwargs,
                headers=fetch_headers,
                proxy=proxies.get(), ssl=False, timeout=aiohttp.ClientTimeout(total=25)
            ) as r:
                if r.status != 200:
                    break
                data = await r.json(content_type=None)

            # Phenom career sites return various structures. Log the raw shape on first page
            # to diagnose extraction issues.
            if offset == 0:
                top_keys = list(data.keys())[:8]
                logger.info(f"Phenom {system}: response keys={top_keys}")

            # --- Unwrap hits / jobs list ---
            # Phenom sites return various structures — check each key in priority order.
            # Previous ternary chain had Python precedence bugs; this is explicit and safe.
            def _extract_listings(d):
                # Standard keys
                for key in ("jobs", "requisitions", "results", "entries"):
                    v = d.get(key)
                    if isinstance(v, list) and v:
                        return v
                # Elasticsearch hits.hits
                hits = d.get("hits")
                if isinstance(hits, dict):
                    inner = hits.get("hits")
                    if isinstance(inner, list) and inner:
                        return inner
                # data.jobs (nested dict)
                data_val = d.get("data")
                if isinstance(data_val, dict):
                    sub = data_val.get("jobs") or data_val.get("entries") or data_val.get("results")
                    if isinstance(sub, list) and sub:
                        return sub
                # data is itself the list
                if isinstance(data_val, list) and data_val:
                    return data_val
                return []

            raw = _extract_listings(data)
            listings = [j for j in raw if isinstance(j, dict)]
            if not listings:
                logger.info(f"Phenom {system}: no listings at offset {offset} — keys={list(data.keys())[:8]}, data_val={str(data.get('data', ''))[:150]}")
                break

            for j in listings:
                # Elasticsearch wraps the real document under _source
                doc = j.get("_source", j)
                loc = doc.get("city", "") or doc.get("location", "") or doc.get("locations", "")
                if isinstance(loc, list):
                    loc = ", ".join(str(x) for x in loc)
                _raw_city  = doc.get("city", "")
                _raw_state = doc.get("state", "") or doc.get("stateCode", "")
                city, state = parse_city_state(f"{_raw_city}, {_raw_state}") if (_raw_city or _raw_state) else (_raw_city, _raw_state)
                city  = city  or _raw_city
                state = state or _raw_state
                title = doc.get("title", "") or doc.get("jobTitle", "") or doc.get("name", "")
                job_id = str(
                    doc.get("id", "") or doc.get("jobId", "") or doc.get("requisitionId", "") or
                    j.get("_id", "")  # ES outer doc id as fallback
                )
                url = doc.get("applyUrl", "") or doc.get("jobUrl", "") or doc.get("url", "") or f"{base_url}/job/{job_id}"
                if title and job_id:
                    jobs.append(Job(
                        title=title,
                        hospital_system=system,
                        hospital_name=doc.get("facility", "") or doc.get("company", "") or system,
                        city=city, state=state,
                        location=loc or f"{city}, {state}",
                        specialty=doc.get("category", "") or doc.get("jobCategory", "") or doc.get("department", ""),
                        job_type=doc.get("employmentType", "") or doc.get("jobType", "") or doc.get("type", ""),
                        url=url,
                        job_id=job_id,
                        posted_date=str(doc.get("postedDate", "") or doc.get("datePosted", "") or doc.get("postDate", ""))[:10],
                        description=strip_html(str(doc.get("description", "") or doc.get("shortDescription", ""))),
                        ats_platform="Phenom",
                    ))

            # Various total count field names across Phenom implementations
            total = (
                data.get("total") or
                data.get("count") or
                data.get("total_entries") or
                data.get("totalCount") or
                (data.get("hits", {}) or {}).get("total", {}).get("value") or
                len(listings)
            )
            if isinstance(total, dict):  # ES total: {"value": N, "relation": "eq"}
                total = total.get("value", len(listings))
            offset += 50
            if offset >= int(total) or len(listings) < 50:
                break
            await jitter()
        except Exception as e:
            logger.info(f"Phenom {system}: {e}")
            break

    logger.info(f"  Phenom {system}: {len(jobs)} jobs")
    return jobs

async def run_phenom(session) -> list[Job]:
    logger.info(f"Phenom: scraping {len(PHENOM_ORGS)} systems...")
    results = await asyncio.gather(
        *[scrape_phenom(session, s, u) for s, u in PHENOM_ORGS.items()],
        return_exceptions=True
    )
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  Phenom total: {len(jobs):,} jobs")
    return jobs





##############################################################################
#  ADP WORKFORCE NOW — public job listings via ADP's embed API
#  Each org has a unique `cid` (company ID) visible in the iframe URL
##############################################################################
ADP_ORGS = {
    # cid values from the career page iframe URLs
    # System names TBD — will show in logs once jobs come back
    "ADP Health System 1": "152f13f3-9efa-4e16-9a69-bb7500136904",
    "ADP Health System 2": "542f7b59-1156-4a17-a729-f8cd9337acf6",
    "ADP Health System 3": "af93ba9c-e8c7-4a6f-ade3-711614110405",
}

async def scrape_adp(session: aiohttp.ClientSession, system: str, cid: str) -> list[Job]:
    jobs = []
    # ADP WFN public job board backing endpoint — confirmed from browser network tab
    # The iframe loads this URL to fetch job listings as JSON
    base_portal = f"https://workforcenow.adp.com/mascsr/default/mdf/recruitment/recruitment.html?cid={cid}&ccId=19000101_000001&type=MP&lang=en_US"
    api_url = "https://workforcenow.adp.com/mascsr/default/mdf/recruitment/recruitment.html"
    # ADP's actual JSON endpoint for job listings
    json_url = f"https://workforcenow.adp.com/mascsr/default/mdf/recruitment/json/jobPosting"
    offset = 0
    while True:
        try:
            async with req(session, "get", 
                json_url,
                params={
                    "cid": cid,
                    "ccId": "19000101_000001",
                    "type": "MP",
                    "lang": "en_US",
                    "start": offset,
                    "limit": 25,
                    "jobType": "all",
                },
                headers={
                    **HEADERS,
                    "Referer": base_portal,
                    "Accept": "application/json, text/javascript, */*; q=0.01",
                    "X-Requested-With": "XMLHttpRequest",
                }, ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=30)
            ) as r:
                if r.status != 200:
                    logger.info(f"ADP {system}: HTTP {r.status} at {json_url}")
                    break
                data = await r.json(content_type=None)

            listings = (
                data.get("jobPostings") or
                data.get("jobRequisitions") or
                data.get("jobs") or
                []
            )
            if not listings:
                # Try alternate key structure
                if isinstance(data, dict) and data.get("totalCount", 0) > 0:
                    logger.info(f"ADP {system}: got data but unknown structure: {list(data.keys())}")
                break

            for j in listings:
                loc_obj = j.get("location") or j.get("primaryLocation") or {}
                if isinstance(loc_obj, str):
                    loc = loc_obj
                    city, state = parse_city_state(loc)
                else:
                    city  = loc_obj.get("city", "")
                    raw_st = loc_obj.get("stateCode", "") or loc_obj.get("countrySubdivisionCode", "")
                    _, state = parse_city_state(f"{city}, {raw_st}")
                    state = state or raw_st
                    loc   = f"{city}, {state}"
                title  = j.get("jobTitle", j.get("title", ""))
                job_id = str(j.get("requisitionId", j.get("id", j.get("jobPostingId", ""))))
                jobs.append(Job(
                    title=title,
                    hospital_system=system,
                    hospital_name=j.get("organizationName", j.get("company", system)),
                    city=city, state=state, location=loc,
                    specialty=j.get("jobCategory", ""),
                    job_type=j.get("jobType", j.get("employmentType", "")),
                    url=base_portal,
                    job_id=job_id,
                    posted_date=str(j.get("postingDate", j.get("postedDate", "")))[:10],
                    description=strip_html(j.get("jobDescription", j.get("description", ""))),
                    ats_platform="ADP",
                ))

            if len(listings) < 25:
                break
            offset += 25
            await jitter()
        except Exception as e:
            logger.info(f"ADP {system}: {e}")
            break

    logger.info(f"  ADP {system}: {len(jobs)} jobs")
    return jobs

async def run_adp(session) -> list[Job]:
    logger.info(f"ADP: scraping {len(ADP_ORGS)} systems...")
    results = await asyncio.gather(
        *[scrape_adp(session, s, c) for s, c in ADP_ORGS.items()],
        return_exceptions=True
    )
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  ADP total: {len(jobs):,} jobs")
    return jobs


##############################################################################
#  SELECTMINDS / ORACLE RECRUITING — used by Henry Ford Health
#  SelectMinds exposes a public JSON search API
##############################################################################
SELECTMINDS_ORGS = {
    "Henry Ford Health": "henryford",
}

async def scrape_selectminds(session: aiohttp.ClientSession, system: str, org: str) -> list[Job]:
    jobs = []
    # SelectMinds public API endpoint pattern
    base = f"https://{org}.referrals.selectminds.com"
    api_url = f"{base}/api/jobs/search"
    page = 1
    while True:
        try:
            async with req(session, "get", 
                api_url,
                params={"page": page, "per_page": 25, "keywords": ""},
                headers={**HEADERS, "X-Requested-With": "XMLHttpRequest"}, ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=25)
            ) as r:
                if r.status != 200:
                    # Try alternate endpoint
                    async with req(session, "get", 
                        f"{base}/jobs/search",
                        params={"page": page, "per_page": 25},
                        headers=HEADERS, ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=25)
                    ) as r2:
                        if r2.status != 200:
                            logger.info(f"SelectMinds {system}: HTTP {r.status}")
                            break
                        data = await r2.json(content_type=None)
                else:
                    data = await r.json(content_type=None)

            listings = data.get("jobs", data.get("results", []))
            if not listings:
                break

            for j in listings:
                loc = j.get("location", "")
                _city, _state = parse_city_state(loc)
                jobs.append(Job(
                    title=j.get("title", ""),
                    hospital_system=system,
                    hospital_name=j.get("department", system),
                    city=_city, state=_state,
                    location=loc,
                    specialty=j.get("category", ""),
                    job_type=j.get("employment_type", ""),
                    url=j.get("url", f"{base}/jobs/{j.get('id','')}"),
                    job_id=str(j.get("id", "")),
                    posted_date=str(j.get("created_at", ""))[:10],
                    description=strip_html(j.get("description", "")),
                    ats_platform="SelectMinds",
                ))

            if len(listings) < 25:
                break
            page += 1
            await jitter()
        except Exception as e:
            logger.info(f"SelectMinds {system}: {e}")
            break

    logger.info(f"  SelectMinds {system}: {len(jobs)} jobs")
    return jobs

async def run_selectminds(session) -> list[Job]:
    logger.info(f"SelectMinds: scraping {len(SELECTMINDS_ORGS)} systems...")
    results = await asyncio.gather(
        *[scrape_selectminds(session, s, o) for s, o in SELECTMINDS_ORGS.items()],
        return_exceptions=True
    )
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  SelectMinds total: {len(jobs):,} jobs")
    return jobs


##############################################################################
#  RECRUITING.COM — used by STB Careers
#  Has a simple public JSON API
##############################################################################
RECRUITINGCOM_ORGS = {
    "STB Careers": "stbcareers",
}

async def scrape_recruitingcom(session: aiohttp.ClientSession, system: str, org: str) -> list[Job]:
    jobs = []
    api_url = f"https://{org}.recruiting.com/api/v1/jobs"
    page = 1
    while True:
        try:
            async with req(session, "get", 
                api_url,
                params={"page": page, "per_page": 50},
                headers={
                    **HEADERS,
                    "Referer": f"https://{org}.recruiting.com/",
                    "Origin": f"https://{org}.recruiting.com",
                    "Accept": "application/json",
                }, ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=25)
            ) as r:
                if r.status != 200:
                    logger.info(f"Recruiting.com {system}: HTTP {r.status}")
                    break
                data = await r.json(content_type=None)

            listings = data if isinstance(data, list) else data.get("jobs", data.get("data", []))
            if not listings:
                break

            for j in listings:
                loc = j.get("location", "") or j.get("city", "")
                _city, _state = parse_city_state(str(loc))
                jobs.append(Job(
                    title=j.get("title", ""),
                    hospital_system=system,
                    hospital_name=system,
                    city=_city, state=_state,
                    location=str(loc),
                    specialty=j.get("department", "") or j.get("category", ""),
                    job_type=j.get("employment_type", "") or j.get("type", ""),
                    url=j.get("url", f"https://{org}.recruiting.com/jobs/{j.get('id','')}"),
                    job_id=str(j.get("id", "")),
                    posted_date=str(j.get("posted_at", j.get("created_at", "")))[:10],
                    description=strip_html(j.get("description", "")),
                    ats_platform="Recruiting.com",
                ))

            if len(listings) < 50:
                break
            page += 1
            await jitter()
        except Exception as e:
            logger.info(f"Recruiting.com {system}: {e}")
            break

    logger.info(f"  Recruiting.com {system}: {len(jobs)} jobs")
    return jobs

async def run_recruitingcom(session) -> list[Job]:
    logger.info(f"Recruiting.com: scraping {len(RECRUITINGCOM_ORGS)} systems...")
    results = await asyncio.gather(
        *[scrape_recruitingcom(session, s, o) for s, o in RECRUITINGCOM_ORGS.items()],
        return_exceptions=True
    )
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  Recruiting.com total: {len(jobs):,} jobs")
    return jobs


##############################################################################
#  INFOR CLOUDSUITE HCM — used by Faith Regional Health Services
#  Public job board with REST API
##############################################################################
INFOR_ORGS = {
    # Format: "System": ("css-subdomain", "hr_org")
    "Faith Regional Health":       ("css-faithregional-prd",             "100"),
    "CAMC":                        ("css-camc-prd",                      "CAMC"),
    "Vandalia Health":             ("css-camc-prd",                      "CAMC"),   # Vandalia rebranded from CAMC — same system
    "Ballad Health":               ("css-balladhealth-prd",              "1"),
    "PH Healthcare":               ("css-phhealthcare-prd",              "1"),
    "Carson Tahoe Health":         ("css-carsontahoehs-prd",             "1"),
    "Middlesex Health":            ("css-middlesex-prd",                 "1"),
    "Bay Health":                  ("css-bayhealth-prd",                 "1"),
    "BayCare Health System":       ("css-baycarehs-prd",                 "1"),
    "Lakeland Regional Health":    ("css-lakelandrmc-prd",               "LRH"),
    "Tift Regional Health":        ("css-tiftregional-prd",              "1"),
    "Eastern Maine Health":        ("css-emh-prd",                       "1"),
    "Maury Regional Health":       ("css-mauryregionalhos-prd",          "MR"),
    "Skagit Regional Health":      ("css-mnc4u622l854lnnt-prd",          "1"),
    "DHR Health":                  ("css-pf7dmpe5vb7ydcw4-prd",          "1"),
}

async def scrape_infor(session: aiohttp.ClientSession, system: str, org_data: tuple) -> list[Job]:
    org_id, hr_org = org_data
    jobs = []

    # Infor CloudSuite HCM — Lawson CandidateSelfService JSON API
    base = f"https://{org_id}.inforcloudsuite.com"
    
    # Try multiple endpoint patterns
    endpoints = [
        # Newer OData v1
        (f"{base}/hcm/v1/Jobs", {
            "csk.JobBoard": "EXTERNAL",
            "csk.HROrganization": hr_org,
            "$format": "json",
            "$top": 100,
        }),
        # Lawson CandidateSelfService with JSON output
        (f"{base}/hcm/CandidateSelfService/controller.servlet", {
            "context.session.key.HROrganization": hr_org,
            "context.session.key.JobBoard": "EXTERNAL",
            "context.dataarea": "hcm",
            "dataarea": "lmghr",
            "JobPost": "1",
            "format": "json",
        }),
        # Alternative OData path
        (f"{base}/hcm/Jobs/page/JobsSearchPage", {
            "csk.JobBoard": "EXTERNAL",
            "csk.HROrganization": hr_org,
            "$format": "json",
            "$top": 100,
        }),
    ]

    data = None
    working_url = None
    for json_api, params in endpoints:
        try:
            async with req(session, "get",
                json_api,
                params=params,
                headers={**HEADERS, "Accept": "application/json"},
                ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=25)
            ) as r:
                if r.status == 200:
                    ct = r.headers.get("content-type", "")
                    if "json" in ct:
                        data = await r.json(content_type=None)
                        working_url = json_api
                        break
                    else:
                        # Got HTML back — this endpoint doesn't return JSON
                        continue
                else:
                    logger.info(f"Infor {system}: HTTP {r.status} at {json_api}")
        except Exception as e:
            logger.info(f"Infor {system}: {e}")
            continue

    if not data:
        logger.info(f"Infor {system}: no JSON endpoint found — may need Playwright")
        return []

    logger.info(f"Infor {system}: using {working_url}")
    try:
        listings = data.get("value", data.get("d", {}).get("results", data.get("jobs", [])))
        for j in listings:
            _icity = j.get("City", "")
            _istate = j.get("State", "") or j.get("StateProvince", "")
            _, state = parse_city_state(f"{_icity}, {_istate}")
            city  = _icity
            state = state or _istate
            jobs.append(Job(
                title=j.get("JobTitle", j.get("Title", "")),
                hospital_system=system,
                hospital_name=j.get("Organization", system),
                city=city, state=state,
                location=f"{city}, {state}",
                specialty=j.get("JobCategory", ""),
                job_type=j.get("EmploymentType", ""),
                url=base_url,
                job_id=str(j.get("RequisitionId", j.get("JobId", ""))),
                posted_date=str(j.get("PostingDate", ""))[:10],
                description=strip_html(j.get("JobDescription", "")),
                ats_platform="Infor",
            ))
    except Exception as e:
        logger.info(f"Infor {system}: {e}")

    logger.info(f"  Infor {system}: {len(jobs)} jobs")
    return jobs

async def run_infor(session) -> list[Job]:
    logger.info(f"Infor: scraping {len(INFOR_ORGS)} systems...")
    results = await asyncio.gather(
        *[scrape_infor(session, s, o) for s, o in INFOR_ORGS.items()],
        return_exceptions=True
    )
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  Infor total: {len(jobs):,} jobs")
    return jobs



##############################################################################
#  UKG PRO / ULTIPRO — GUID-based job board API
#  Format: ("base_url", "guid")
##############################################################################
UKG_ORGS = {
    "Catawba Valley Medical":       ("https://cchsconnect.rec.pro.ukg.net/COL1053CCHD",  "c6df4630-7da9-4627-af22-819e939d86fa"),
    "Augusta University Health":    ("https://recruiting.ultipro.com/AUG1000AUG",        "02a29cd6-e7aa-4501-96be-6336647e3184"),
    "Cape Regional Health":         ("https://crhukg.rec.pro.ukg.net/CHE1503CHPE",       "09584b08-b32f-4882-8c7b-223bbd8e3851"),
    "Northwest Medical Center":     ("https://nwmedicalctr.rec.pro.ukg.net/NOR1080NWMC", "f22ba272-5440-48f3-9f0f-84f6f384d461"),
    "Guadalupe Regional Medical":   ("https://grmedcenter.rec.pro.ukg.net/GUA1500GDRM",  "42079bd4-4198-48a9-b64a-26c8b01496d6"),
    "Quorum Health":                ("https://recruiting2.ultipro.com/QHC1000QHCS",      "c304f8f7-4638-4bc5-8567-18580345a749"),
    "Granite Hills Medical":        ("https://recruiting.ultipro.com/GRE1050GNHP",       "2b67ecb4-00fb-4863-931a-7bf0ebcb493a"),
    "Medical Associates":           ("https://recruiting.ultipro.com/MEA1004MEVM",       "d561e1d3-aa5e-4c1b-bcf5-5319c6abdcac"),
    "Excela Health":                ("https://recruiting.ultipro.com/EXC1005EXCEH",      "a00363e2-39d4-4408-a790-fbd62f4846d8"),
    "Heritage Health":              ("https://recruiting.ultipro.com/HER1004HERIT",      "68189271-8c8d-4634-bb50-bd2edf375278"),
    "Alliant Health":               ("https://recruiting.ultipro.com/ALL1034ABHC",       "ad28382f-2fcd-4cbb-bb18-24dd71b05bce"),
    "Erie County Medical Center":   ("https://ecmc462.rec.pro.ukg.net/ERI1003ECMC",      "4d1858fb-5b2a-499b-a320-4f1f4e5bcb06"),
    "Wyoming County Community":     ("https://recruiting.ultipro.com/WYC1000WHMC",       "5e6bf310-55e9-45dd-8252-85e4c670f433"),
    "Deaconess Health":             ("https://deaconess.rec.pro.ukg.net/DEA1005DEAC",    "a1f943e7-8d4d-4348-bf5e-4664f78d3abb"),
    "North Mississippi Medical":    ("https://recruiting.ultipro.com/NOR1041NAHO",       "84528182-2cf7-4f42-b7ca-dbb54c6f1c10"),
    "Kern Medical":                 ("https://recruiting.ultipro.com/KER1002KERN",       "e74fb506-5af0-e4c1-999e-64d5e8414cb0"),
    "Grinnell Regional Medical":    ("https://recruiting.ultipro.com/GRI1004GHSC",       "f5d979ef-386f-4469-8178-a3801183d063"),
    "Columbia Regional Medical":    ("https://recruiting.ultipro.com/COL1042CRME",       "5ac3f35f-7e01-49ff-ad53-0acc27b4cee7"),
    "Crisp Regional Health":        ("https://recruiting.ultipro.com/CRI1005CRISP",      "c74342f0-7984-4858-8545-16e720353c82"),
    "South Georgia Health":         ("https://sghsukg.rec.pro.ukg.net/SOU1076SOUG",      "2de20fad-cb3f-4525-87cd-7bd1d3c2a720"),
    "Murray-Calloway County":       ("https://murray.rec.pro.ukg.net/MUR1004MCCH",       "78a4032f-cda1-471d-86f1-9e64991ed7d2"),
    "TJ Regional Health":           ("https://tjregional.rec.pro.ukg.net/TJS1500TJSC",   "a4b9e606-5dc1-4c8c-ba68-83fd41e97ade"),
    "Lakewood Health":              ("https://recruiting2.ultipro.com/SKY1006LAKES",     "9dcd58e9-9155-4226-9b21-f476fcd1d29b"),
}

async def scrape_ukg(session: aiohttp.ClientSession, system: str, org_data: tuple) -> list[Job]:
    base_url, guid = org_data
    jobs = []
    # Confirmed endpoint from network intercept on Deaconess
    api = f"{base_url}/JobBoard/{guid}/JobBoardView/LoadSearchResults"
    offset = 0
    limit = 25
    while True:
        try:
            payload = {
                "opportunitySearch": {
                    "Top": limit,
                    "Skip": offset,
                    "QueryString": "",
                    "OrderBy": [{"Value": "postedDateDesc", "PropertyName": "PostedDate", "Ascending": False}],
                    "Filters": [],
                },
                "deviceType": "desktop",
                "recommendationSettings": {},
            }
            async with req(session, "post", api,
                json=payload,
                headers={**HEADERS, "Accept": "application/json", "Content-Type": "application/json"},
                ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=25)) as r:
                if r.status != 200:
                    logger.info(f"UKG {system}: HTTP {r.status}")
                    break
                data = await r.json(content_type=None)
            # Response: {"opportunities": [...], "total": N}
            items = data.get("opportunities", data.get("Opportunities", []))
            if not items:
                break
            for j in items:
                city  = j.get("city",  j.get("City",  ""))
                state = j.get("state", j.get("State", ""))
                if not city and not state:
                    loc_raw = j.get("location", j.get("Location", j.get("formattedLocation", "")))
                    city, state = parse_city_state(str(loc_raw))
                jobs.append(Job(
                    title=j.get("title", j.get("Title", "")),
                    hospital_system=system,
                    hospital_name=j.get("company", {}).get("name", system) if isinstance(j.get("company"), dict) else system,
                    city=city, state=state,
                    location=f"{city}, {state}".strip(", "),
                    specialty=j.get("jobCategory", j.get("category", "")),
                    job_type=j.get("employmentType", j.get("workHours", "")),
                    url=f"{base_url}/JobBoard/{guid}/?detail={j.get('opportunityId', j.get('id', ''))}",
                    job_id=str(j.get("opportunityId", j.get("id", j.get("jobId", "")))),
                    posted_date=str(j.get("postedDate", j.get("PostedDate", "")))[:10],
                    description=strip_html(j.get("shortDescription", j.get("description", ""))),
                    ats_platform="UKG",
                ))
            total = data.get("total", data.get("Total", data.get("totalCount", 0)))
            offset += limit
            if offset >= total:
                break
            await jitter()
        except Exception as e:
            logger.info(f"UKG {system}: {e}")
            break
    logger.info(f"  UKG {system}: {len(jobs)} jobs")
    return jobs

async def run_ukg(session) -> list[Job]:
    logger.info(f"UKG: scraping {len(UKG_ORGS)} systems...")
    results = await asyncio.gather(
        *[scrape_ukg(session, s, o) for s, o in UKG_ORGS.items()],
        return_exceptions=True
    )
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  UKG total: {len(jobs):,} jobs")
    return jobs


##############################################################################
#  ORACLE HCM CLOUD — REST search endpoint
#  Format: ("base_url",)  — base includes full path up to /sites/{site}
##############################################################################
ORACLE_ORGS = {
    # Format: "System": ("https://{oracle-subdomain}.oraclecloud.com", "siteNumber")
    # siteNumber extracted from original career site URLs (/sites/{siteNumber})
    # API: GET {base}/hcmRestApi/resources/latest/recruitingCEJobRequisitions
    #      with finder=findReqs;siteNumber={siteNumber},limit=N,offset=N
    "Jackson Hospital":          ("https://ejid.fa.us6.oraclecloud.com",                      "CX_1001"),
    "Erlanger Health System":    ("https://elar.fa.us2.oraclecloud.com",                      "CX_1"),
    "EvergreenHealth":           ("https://erym.fa.us6.oraclecloud.com",                      "CX_1"),
    "Valley Health (NV)":        ("https://fa-eveq-saasfaprod1.fa.ocs.oraclecloud.com",       "CX_1"),
    "Mount Nittany Health":      ("https://mnh-ibosjb.fa.ocs.oraclecloud.com",               "MountNittanyHealthCareers"),
    "Trinity Health (Oregon)":   ("https://ertr.fa.us2.oraclecloud.com",                      "CX_3001"),
    "Memorial Hospital":         ("https://wearememorial-ibrkjb.fa.ocs.oraclecloud.com",      "Careers"),
    "Cape Cod Healthcare":       ("https://ecvz.fa.us2.oraclecloud.com",                      "CX_1"),
    "Flagler Health":            ("https://erou.fa.us2.oraclecloud.com",                      "CX_1"),
    "Eastern Connecticut Health":("https://eglz.fa.us2.oraclecloud.com",                      "CX"),
    "Guthrie Health":            ("https://elfw.fa.us2.oraclecloud.com",                      "CX_1001"),  # confirmed
    "Valley Children's":         ("https://epyz.fa.us2.oraclecloud.com",                      "CX_1"),
    "Southwest Health":          ("https://fa-exgl-saasfaprod1.fa.ocs.oraclecloud.com",       "JoinOurTeam"),
    "HealthPartners":            ("https://fa-etnv-saasfaprod1.fa.ocs.oraclecloud.com",       "healthpartners"),
    "United Regional":           ("https://erqh.fa.us2.oraclecloud.com",                      "CX_1001"),
    "Unknown (fa-eyip)":         ("https://fa-eyip-saasfaprod1.fa.ocs.oraclecloud.com",       "CX_4001"),
}

async def scrape_oracle(session: aiohttp.ClientSession, system: str, org_data: tuple) -> list[Job]:
    base_url, site_number = org_data
    jobs = []
    # Confirmed from Guthrie network intercept.
    # IMPORTANT: offset/limit are top-level params, NOT inside the finder string.
    # onlyData=true strips pagination metadata → totalResults=0 → never paginates.
    api = f"{base_url}/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
    offset = 0
    limit  = 25
    while True:
        try:
            params = {
                "finder":  f"findReqs;siteNumber={site_number},sortBy=POSTING_DATES_DESC",
                "expand":  "requisitionList.workLocation,requisitionList.secondaryLocations",
                "limit":   limit,
                "offset":  offset,
            }
            async with req(session, "get", api, params=params,
                headers={**HEADERS,
                         "Accept": "application/vnd.oracle.adf.resourcecollection+json",
                         "REST-Framework-Version": "4"},
                ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status != 200:
                    logger.info(f"Oracle {system}: HTTP {r.status}")
                    break
                data = await r.json(content_type=None)
            items = data.get("items", [])
            if not items:
                break
            for j in items:
                loc = j.get("PrimaryLocation", j.get("primaryLocation", ""))
                if isinstance(loc, dict):
                    loc = loc.get("Name", loc.get("name", ""))
                _city, _state = parse_city_state(str(loc))
                func = j.get("JobFunction", j.get("jobFunction", ""))
                if isinstance(func, dict):
                    func = func.get("Name", func.get("name", ""))
                jobs.append(Job(
                    title=j.get("Title", j.get("title", "")),
                    hospital_system=system,
                    hospital_name=system,
                    city=_city, state=_state, location=str(loc),
                    specialty=str(func),
                    job_type=j.get("WorkHours", j.get("workHours", "")),
                    url=f"{base_url}/hcmUI/CandidateExperience/en/sites/{site_number}/jobs/{j.get('Id', j.get('id', ''))}",
                    job_id=str(j.get("Id", j.get("id", j.get("RequisitionNumber", "")))),
                    posted_date=str(j.get("PostedDate", j.get("postedDate", "")))[:10],
                    description="",
                    ats_platform="Oracle HCM",
                ))
            # Use hasMore if present; fall back to totalResults; fall back to item count < limit
            has_more    = data.get("hasMore", None)
            total       = data.get("totalResults", data.get("count", 0))
            offset += limit
            if has_more is False:
                break
            if has_more is None and total and offset >= total:
                break
            if has_more is None and not total and len(items) < limit:
                break
            await jitter()
        except Exception as e:
            logger.info(f"Oracle {system}: {e}")
            break
    logger.info(f"  Oracle {system}: {len(jobs)} jobs")
    return jobs

async def run_oracle(session) -> list[Job]:
    logger.info(f"Oracle HCM: scraping {len(ORACLE_ORGS)} systems...")
    results = await asyncio.gather(
        *[scrape_oracle(session, s, o) for s, o in ORACLE_ORGS.items()],
        return_exceptions=True
    )
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  Oracle HCM total: {len(jobs):,} jobs")
    return jobs


##############################################################################
#  HEALTHCARESOURCE PM — hospital-specific tenant API
#  Format: ("tenant_slug",)
##############################################################################
HEALTHCARESOURCE_ORGS = {
    "Central Valley Medical":   "centralvalleymedicalcenter",
    "RMCM":                     "rmcm",
    "CRMC Health":              "crmchealth",
    "AnMed Health":             "anmed",
    "CaroMont Health":          "caromont",
    "Randolph Health":          "randolph",
    "Scotland Health":          "scotland",
    "Carteret Health Care":     "carteret",
    "Stillwater Medical":       "stillwater",
    "Crouse Health":            "crouse",
    "York Hospital":            "yorkhospital",
    "Lake Regional Health":     "lakeregional",
    "Liberty Hospital":         "liberty",
    "Forrest Health":           "forresthealth",
    "MRHC":                     "mrhc",
    "Brattleboro Memorial":     "bch",
    "Waterbury Hospital":       "waterbury",
    "ECHN":                     "echn",
    "Archbold Medical":         "archbold",
    "Kootenai Health":          "kootenai",
    "Community Memorial":       "comhs",
    "Union Hospital":           "unionhospital",
    "Hays Medical Center":      "haysmed",
    "CHC Healthcare":           "chc",
    "Lawrence General":         "lawrence",
    "Holyoke Health":           "Holyokehealth",
    "Sarasota Memorial":        "smh",
}

async def scrape_healthcaresource(session: aiohttp.ClientSession, system: str, tenant: str) -> list[Job]:
    jobs = []
    # Try GET first (simpler), then POST if that fails.
    # Endpoint confirmed: /JobseekerSearchAPI/{tenant}/api/Search
    api    = f"https://pm.healthcaresource.com/JobseekerSearchAPI/{tenant}/api/Search"
    offset = 0
    size   = 25
    # Determine method — try GET with query params first
    method = "get"
    while True:
        try:
            if method == "get":
                async with req(session, "get", api,
                    params={"size": size, "from": offset},
                    headers={**HEADERS, "Accept": "application/json"},
                    ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=25)) as r:
                    status = r.status
                    if status == 405:
                        method = "post"   # Switch to POST and retry
                        logger.info(f"HealthcareSource {system}: GET 405, switching to POST")
                        break
                    if status != 200:
                        logger.info(f"HealthcareSource {system}: HTTP {status}")
                        return jobs
                    data = await r.json(content_type=None)
            else:
                # Minimal POST body — avoid Elasticsearch syntax that causes 500
                async with req(session, "post", api,
                    json={"size": size, "from": offset},
                    headers={**HEADERS, "Accept": "application/json",
                             "Content-Type": "application/json"},
                    ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=25)) as r:
                    status = r.status
                    if status != 200:
                        logger.info(f"HealthcareSource {system}: HTTP {status}")
                        return jobs
                    data = await r.json(content_type=None)

            # Response shape: {"hits": {"hits": [...], "total": N or {"value": N}}}
            hits  = data.get("hits", {})
            items = hits.get("hits", [])
            if not items:
                break
            for j in items:
                src    = j.get("_source", j)
                city   = src.get("city",  src.get("City",  ""))
                state  = src.get("state", src.get("State", ""))
                job_id = str(src.get("requisitionId", src.get("jobId",
                             src.get("id", j.get("_id", "")))))
                jobs.append(Job(
                    title=src.get("title", src.get("jobTitle", "")),
                    hospital_system=system,
                    hospital_name=src.get("facilityName", src.get("facility", system)),
                    city=city, state=state,
                    location=f"{city}, {state}".strip(", "),
                    specialty=src.get("category", src.get("jobCategory", "")),
                    job_type=src.get("employmentType", src.get("jobType", "")),
                    url=f"https://pm.healthcaresource.com/cs/{tenant}/#/job/{job_id}",
                    job_id=job_id,
                    posted_date=str(src.get("postedDate", src.get("datePosted", "")))[:10],
                    description="",
                    ats_platform="HealthcareSource",
                ))
            total = (hits.get("total", {}).get("value", 0)
                     if isinstance(hits.get("total"), dict)
                     else hits.get("total", 0))
            offset += size
            if offset >= total or len(items) < size:
                break
            await jitter()
        except Exception as e:
            logger.info(f"HealthcareSource {system}: {e}")
            break

    # If we switched method mid-loop, restart with POST
    if method == "post" and not jobs:
        offset = 0
        method = "post_active"   # prevent infinite loop
        while True:
            try:
                async with req(session, "post", api,
                    json={"size": size, "from": offset},
                    headers={**HEADERS, "Accept": "application/json",
                             "Content-Type": "application/json"},
                    ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=25)) as r:
                    if r.status != 200:
                        logger.info(f"HealthcareSource {system}: POST HTTP {r.status}")
                        break
                    data = await r.json(content_type=None)
                hits  = data.get("hits", {})
                items = hits.get("hits", [])
                if not items:
                    break
                for j in items:
                    src    = j.get("_source", j)
                    city   = src.get("city",  src.get("City",  ""))
                    state  = src.get("state", src.get("State", ""))
                    job_id = str(src.get("requisitionId", src.get("jobId",
                                 src.get("id", j.get("_id", "")))))
                    jobs.append(Job(
                        title=src.get("title", src.get("jobTitle", "")),
                        hospital_system=system,
                        hospital_name=src.get("facilityName", src.get("facility", system)),
                        city=city, state=state,
                        location=f"{city}, {state}".strip(", "),
                        specialty=src.get("category", src.get("jobCategory", "")),
                        job_type=src.get("employmentType", src.get("jobType", "")),
                        url=f"https://pm.healthcaresource.com/cs/{tenant}/#/job/{job_id}",
                        job_id=job_id,
                        posted_date=str(src.get("postedDate", src.get("datePosted", "")))[:10],
                        description="",
                        ats_platform="HealthcareSource",
                    ))
                total = (hits.get("total", {}).get("value", 0)
                         if isinstance(hits.get("total"), dict)
                         else hits.get("total", 0))
                offset += size
                if offset >= total or len(items) < size:
                    break
                await jitter()
            except Exception as e:
                logger.info(f"HealthcareSource {system}: {e}")
                break

    logger.info(f"  HealthcareSource {system}: {len(jobs)} jobs")
    return jobs

async def run_healthcaresource(session) -> list[Job]:
    logger.info(f"HealthcareSource: scraping {len(HEALTHCARESOURCE_ORGS)} systems...")
    results = await asyncio.gather(
        *[scrape_healthcaresource(session, s, o) for s, o in HEALTHCARESOURCE_ORGS.items()],
        return_exceptions=True
    )
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  HealthcareSource total: {len(jobs):,} jobs")
    return jobs


##############################################################################
#  TENET HEALTH — custom career site with JSON search API
##############################################################################
TENET_BRANDS = {
    "Baptist Health System (TX)":   "Baptist Health System",
    "Valley Baptist Health System": "Valley Baptist Health System",
    "The hospitals of Providence":  "The hospitals of Providence",
    "Pittsburgh-area facilities":   "Pittsburgh",
    "Detroit Medical Center":       "Detroit Medical Center",
}

async def scrape_tenet(session: aiohttp.ClientSession, system: str, brand: str) -> list[Job]:
    jobs = []
    # Must use POST — the URL-encoded JSON filter exceeds aiohttp's 8190-byte header limit as GET params
    api = "https://jobs.tenethealth.com/search-jobs/results"
    offset = 0
    while True:
        try:
            payload = {
                "orgIds": "30315",
                "ascf": [{"key": "custom_fields.CustomBrand", "value": brand}],
                "from": offset, "num": 25,
            }
            async with req(session, "post", api,
                json=payload,
                headers={**HEADERS, "Accept": "application/json", "Content-Type": "application/json"},
                ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=25)) as r:
                if r.status != 200:
                    logger.info(f"Tenet {system}: HTTP {r.status}")
                    break
                data = await r.json(content_type=None)
            items = data.get("eagerLoadRefineSearch", {}).get("data", {}).get("jobs", [])
            if not items:
                break
            for j in items:
                loc = j.get("jobLocation", j.get("Location", ""))
                _city, _state = parse_city_state(loc)
                jobs.append(Job(
                    title=j.get("title", ""),
                    hospital_system="Tenet Health",
                    hospital_name=system,
                    city=_city, state=_state, location=loc,
                    specialty=j.get("industry", ""),
                    job_type=j.get("jobType", ""),
                    url=f"https://jobs.tenethealth.com/{j.get('canonicalPositionUrl','')}",
                    job_id=str(j.get("jobId", "")),
                    posted_date=str(j.get("postedDate", ""))[:10],
                    description="",
                    ats_platform="Tenet",
                ))
            total = data.get("eagerLoadRefineSearch", {}).get("data", {}).get("totalJobsCount", 0)
            offset += 25
            if offset >= total:
                break
            await jitter()
        except Exception as e:
            logger.info(f"Tenet {system}: {e}")
            break
    logger.info(f"  Tenet {system}: {len(jobs)} jobs")
    return jobs

async def run_tenet(session) -> list[Job]:
    logger.info(f"Tenet: scraping {len(TENET_BRANDS)} brands...")
    results = await asyncio.gather(
        *[scrape_tenet(session, s, b) for s, b in TENET_BRANDS.items()],
        return_exceptions=True
    )
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  Tenet total: {len(jobs):,} jobs")
    return jobs


##############################################################################
#  TRINITY HEALTH — Jibe-based career portal with JSON search API
##############################################################################
TRINITY_ORGS = {
    "St. Peter's Health Partners":  "https://jobs.trinity-health.org/stpetershealthpartners",
    "Loyola Medicine":              "https://jobs.trinity-health.org/loyolamedicine",
    "Saint Alphonsus":              "https://jobs.trinity-health.org/saintalphonsus",
    "MercyOne":                     "https://jobs.trinity-health.org/mercyone",
    "Holy Cross Health":            "https://jobs.trinity-health.org/holycrosshealth",
}

async def scrape_trinity(session: aiohttp.ClientSession, system: str, base_url: str) -> list[Job]:
    jobs = []
    # Trinity/Jibe career portals use GET /search-results?m=3&pg=N&pgcnt=N
    # The ?m=3 parameter appears to be required (sort mode).
    # Add Accept: application/json to request JSON response instead of HTML.
    api  = f"{base_url}/search-results"
    page = 1
    while True:
        try:
            params = {"m": "3", "pg": page, "pgcnt": 25}
            async with req(session, "get", api, params=params,
                headers={**HEADERS, "Accept": "application/json, text/javascript, */*"},
                ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=25)) as r:
                if r.status != 200:
                    logger.info(f"Trinity {system}: HTTP {r.status}")
                    break
                ct = r.headers.get("content-type", "")
                if "json" not in ct:
                    # Some Jibe sites return HTML — check for JSON in body anyway
                    text = await r.text()
                    try:
                        import json as _json
                        data = _json.loads(text)
                    except Exception:
                        logger.info(f"Trinity {system}: non-JSON response (HTML page)")
                        break
                else:
                    data = await r.json(content_type=None)

            # Jibe response keys vary by version
            items = (data.get("jobs") or
                     data.get("requisitionList") or
                     data.get("results") or [])
            if not items:
                logger.info(f"Trinity {system}: empty — keys={list(data.keys())[:8]}")
                break
            for j in items:
                loc = j.get("location", j.get("primaryLocation",
                            j.get("jobLocation", "")))
                if isinstance(loc, dict):
                    loc = loc.get("name", loc.get("Name", ""))
                _city, _state = parse_city_state(str(loc))
                jobs.append(Job(
                    title=j.get("title", j.get("Title", "")),
                    hospital_system="Trinity Health",
                    hospital_name=system,
                    city=_city, state=_state, location=str(loc),
                    specialty=j.get("category", j.get("jobFunction", "")),
                    job_type=j.get("type", j.get("workHours", "")),
                    url=j.get("applyUrl", j.get("detailUrl",
                              f"{base_url}/jobs/{j.get('id', j.get('jobId', ''))}")),
                    job_id=str(j.get("id", j.get("jobId", j.get("Id", "")))),
                    posted_date=str(j.get("postedDate", j.get("PostedDate", "")))[:10],
                    description="",
                    ats_platform="Jibe",
                ))
            total = (data.get("totalJobsCount") or
                     data.get("total") or
                     data.get("count") or 0)
            if page * 25 >= total or len(items) < 25:
                break
            page += 1
            await jitter()
        except Exception as e:
            logger.info(f"Trinity {system}: {e}")
            break
    logger.info(f"  Trinity {system}: {len(jobs)} jobs")
    return jobs

async def run_trinity(session) -> list[Job]:
    logger.info(f"Trinity Health: scraping {len(TRINITY_ORGS)} orgs...")
    results = await asyncio.gather(
        *[scrape_trinity(session, s, u) for s, u in TRINITY_ORGS.items()],
        return_exceptions=True
    )
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  Trinity total: {len(jobs):,} jobs")
    return jobs


##############################################################################
#  UHS INC — custom career site, brand-filtered
##############################################################################
UHS_BRANDS = {
    "South Texas Health System":       "south-texas-health-system",
    "Texoma Medical Center":           "texoma-medical-center",
    "Aiken Regional Medical":          "aiken-regional-medical-centers",
    "St. Mary's Regional Medical":     "st-marys-regional-medical-center",
    "Northern Nevada Health System":   "the-northern-nevada-health-system",
    "Valley Health System (NV)":       "the-valley-health-system",
    "Southwest Healthcare":            "southwest-healthcare",
    "Wellington Regional Medical":     "wellington-regional-medical-center",
}

async def scrape_uhs(session: aiohttp.ClientSession, system: str, brand: str) -> list[Job]:
    jobs = []
    api = f"https://jobs.uhsinc.com/{brand}/jobs-data"
    page = 1
    while True:
        try:
            params = {"page": page, "pageSize": 25}
            async with req(session, "get", api, params=params,
                headers={**HEADERS, "Accept": "application/json"},
                ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=25)) as r:
                if r.status != 200:
                    break
                data = await r.json(content_type=None)
            items = data.get("jobs", data.get("results", []))
            if not items:
                break
            for j in items:
                loc = j.get("location", j.get("jobLocation", ""))
                _city, _state = parse_city_state(loc)
                jobs.append(Job(
                    title=j.get("title", ""),
                    hospital_system="UHS",
                    hospital_name=system,
                    city=_city, state=_state, location=loc,
                    specialty=j.get("category", ""),
                    job_type=j.get("employmentType", ""),
                    url=f"https://jobs.uhsinc.com/{brand}/jobs/{j.get('id','')}",
                    job_id=str(j.get("id", j.get("jobId", ""))),
                    posted_date=str(j.get("postedDate", ""))[:10],
                    description="",
                    ats_platform="UHS",
                ))
            total = data.get("total", data.get("totalCount", 0))
            if page * 25 >= total:
                break
            page += 1
            await jitter()
        except Exception as e:
            logger.info(f"UHS {system}: {e}")
            break
    logger.info(f"  UHS {system}: {len(jobs)} jobs")
    return jobs

async def run_uhs(session) -> list[Job]:
    logger.info(f"UHS: scraping {len(UHS_BRANDS)} brands...")
    results = await asyncio.gather(
        *[scrape_uhs(session, s, b) for s, b in UHS_BRANDS.items()],
        return_exceptions=True
    )
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  UHS total: {len(jobs):,} jobs")
    return jobs


##############################################################################
#  LIFEPOINT HEALTH — brand-filtered subdomain job listings
##############################################################################
##############################################################################
#  LIFEPOINT HEALTH — moved to Playwright (site rebuilt on WordPress 2025)
#  Old /brand/jobs-data API is dead. Now scraped via run_playwright_scrapers.
##############################################################################
async def run_lifepoint(session) -> list[Job]:
    # LifePoint is now handled by Playwright — this stub keeps run_all() intact
    return []


##############################################################################
#  KRONOS (Legacy Workforce Ready) — mykronos.com career portal
#  Format: "System": ("subdomain", "company_id")
#  API: GET /ta/rest/ui/recruitment/companies/|{id}/job-requisitions
##############################################################################
KRONOS_ORGS = {
    "Astria Health":    ("prd01-hcm01.prd", "6110092"),
    "ArnotHealth":      ("prd01-hcm01.npr", "6012355"),
    "Ridgeview":        ("prd01-hcm01.prd", "6104389"),
}

async def scrape_kronos(session: aiohttp.ClientSession, system: str, org_data: tuple) -> list[Job]:
    subdomain, company_id = org_data
    jobs  = []
    base  = f"https://{subdomain}.mykronos.com"
    api   = f"{base}/ta/rest/ui/recruitment/companies/%7C{company_id}/job-requisitions"
    offset = 1
    size   = 20
    while True:
        try:
            params = {"offset": offset, "size": size, "sort": "desc",
                      "ein_id": "", "lang": "en-US", "_": int(time.time()*1000)}
            async with req(session, "get", api, params=params,
                headers={**HEADERS, "Accept": "application/json"},
                ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=25)) as r:
                if r.status != 200:
                    logger.info(f"Kronos {system}: HTTP {r.status}")
                    break
                data = await r.json(content_type=None)
            items = data if isinstance(data, list) else data.get("requisitions", data.get("jobs", []))
            if not items:
                break
            for j in items:
                loc  = j.get("location", {})
                city  = loc.get("city", "")
                state = loc.get("state", "")
                cats  = j.get("job_categories", [])
                jobs.append(Job(
                    title=j.get("job_title", ""),
                    hospital_system=system,
                    hospital_name=system,
                    city=city, state=state,
                    location=f"{city}, {state}".strip(", "),
                    specialty=cats[0] if cats else "",
                    job_type=j.get("base_pay_frequency", ""),
                    url=f"{base}/ta/{company_id}.careers?CareersSearch=&lang=en-US",
                    job_id=str(j.get("id", "")),
                    posted_date="",
                    description="",
                    ats_platform="Kronos",
                ))
            offset += size
            if len(items) < size:
                break
            await jitter()
        except Exception as e:
            logger.info(f"Kronos {system}: {e}")
            break
    logger.info(f"  Kronos {system}: {len(jobs)} jobs")
    return jobs

async def run_kronos(session) -> list[Job]:
    logger.info(f"Kronos: scraping {len(KRONOS_ORGS)} systems...")
    results = await asyncio.gather(
        *[scrape_kronos(session, s, o) for s, o in KRONOS_ORGS.items()],
        return_exceptions=True
    )
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  Kronos total: {len(jobs):,} jobs")
    return jobs


##############################################################################
#  APPLICANTPRO — applicantpro.com career portal
#  API: GET https://{subdomain}.applicantpro.com/core/jobs/{site_id}
#  Returns JSON array of job objects
##############################################################################
APPLICANTPRO_ORGS = {
    "Cayuga Health":        ("cayugahealthsystem", "17888"),
    "Cascade Medical":      ("cascademedicalcenter", ""),
    "Jefferson Healthcare": ("jeffersonhealthcare", ""),
}

async def scrape_applicantpro(session: aiohttp.ClientSession, system: str, org_data: tuple) -> list[Job]:
    subdomain, site_id = org_data
    jobs = []
    # If site_id known, use direct endpoint; otherwise fetch from /jobs to get site_id
    if not site_id:
        # Try to find site_id from the jobs listing page
        try:
            async with req(session, "get",
                f"https://{subdomain}.applicantpro.com/jobs/",
                headers=HEADERS, ssl=False, proxy=proxies.get(),
                timeout=aiohttp.ClientTimeout(total=20)) as r:
                if r.status == 200:
                    text = await r.text()
                    m = re.search(r'/core/jobs/(\d+)', text)
                    if m:
                        site_id = m.group(1)
        except Exception as e:
            logger.info(f"ApplicantPro {system}: site_id discovery failed: {e}")
            return []

    if not site_id:
        logger.info(f"ApplicantPro {system}: could not determine site_id")
        return []

    try:
        api = f"https://{subdomain}.applicantpro.com/core/jobs/{site_id}"
        async with req(session, "get", api,
            headers={**HEADERS, "Accept": "application/json"},
            ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=25)) as r:
            if r.status != 200:
                logger.info(f"ApplicantPro {system}: HTTP {r.status}")
                return []
            data = await r.json(content_type=None)
        items = data if isinstance(data, list) else data.get("jobs", [])
        for j in items:
            city  = j.get("city", "")
            state = j.get("abbreviation", j.get("state", ""))
            jobs.append(Job(
                title=j.get("title", ""),
                hospital_system=system,
                hospital_name=j.get("subdomain", system),
                city=city, state=state,
                location=f"{city}, {state}".strip(", "),
                specialty=j.get("classification", j.get("jobCategory", "")),
                job_type=j.get("employmentType", ""),
                url=f"https://{subdomain}.applicantpro.com/jobs/{j.get('id', '')}.html",
                job_id=str(j.get("id", "")),
                posted_date=str(j.get("startDateRef", ""))[:10],
                description="",
                ats_platform="ApplicantPro",
            ))
    except Exception as e:
        logger.info(f"ApplicantPro {system}: {e}")

    logger.info(f"  ApplicantPro {system}: {len(jobs)} jobs")
    return jobs

async def run_applicantpro(session) -> list[Job]:
    logger.info(f"ApplicantPro: scraping {len(APPLICANTPRO_ORGS)} systems...")
    results = await asyncio.gather(
        *[scrape_applicantpro(session, s, o) for s, o in APPLICANTPRO_ORGS.items()],
        return_exceptions=True
    )
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  ApplicantPro total: {len(jobs):,} jobs")
    return jobs


# ══════════════════════════════════════════════════════════════════════════
#  PHENOM DOM SCRAPER  (BSW / HCA / Ascension)
#  Reads rendered job cards directly from the DOM + paginates via Next button.
#  More reliable than intercepting /widgets API whose response key varies
#  per tenant and per page state.
# ══════════════════════════════════════════════════════════════════════════
PHENOM_DOM_SITES = [
    # BSW: plain search-results — sortBy param broke pagination, removed
    ("Baylor Scott & White", "https://jobs.bswhealth.com/us/en/search-results"),
    ("HCA Healthcare",       "https://careers.hcahealthcare.com/us/en/search-results"),
    ("Ascension Health",     "https://jobs.ascension.org/us/en/search-results"),
]

async def scrape_phenom_dom(browser, system_name: str, base_url: str) -> list:
    """
    DOM-based scraper for Phenom People career sites.
    Handles BSW, HCA, and Ascension — all on Phenom but with slight
    rendering differences per tenant.
    """
    jobs   = []
    domain = base_url.split("/")[2]
    is_hca = "hcahealthcare.com" in domain

    try:
        ctx = await browser.new_context(
            viewport={"width": 1440, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            locale="en-US",
        )
        await ctx.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>false})")
        page = await ctx.new_page()

        await page.goto(base_url, wait_until="domcontentloaded", timeout=45000)
        # HCA needs a longer initial hydration wait
        await asyncio.sleep(6 if is_hca else 4)
        await page.evaluate("window.scrollBy(0, 400)")
        await asyncio.sleep(1.5)

        page_num  = 0
        max_pages = 500
        seen_ids  = set()

        # Job link selector — Phenom standard + HCA fallbacks
        JOB_LINK_SELECTORS = [
            "a[href*='/job/']",                        # BSW, Ascension
            "a[data-ph-at-job-title-link-text]",       # Phenom data attribute (HCA)
            "[data-ph-at-id='job-title-link']",        # alternate Phenom attr
            "a[href*='/jobs/']",                       # some Phenom tenants
        ]

        while page_num < max_pages:
            # Wait for ANY job link variant to appear
            found_sel = None
            for sel in JOB_LINK_SELECTORS:
                try:
                    await page.wait_for_selector(sel, timeout=12000)
                    found_sel = sel
                    break
                except Exception:
                    continue

            if not found_sel:
                # HCA debug: log all anchor hrefs to see what URL patterns exist
                if is_hca and page_num == 0:
                    try:
                        all_anchors = await page.query_selector_all("a[href]")
                        sample_hrefs = []
                        for a in all_anchors[:30]:
                            h = await a.get_attribute("href") or ""
                            if h and h not in sample_hrefs:
                                sample_hrefs.append(h)
                        logger.info(f"  [HCA debug] sample hrefs: {sample_hrefs[:20]}")
                    except Exception:
                        pass
                logger.info(f"  [{system_name}] no job links on page {page_num+1}, stopping")
                break

            # Collect all matching anchors across all selector variants
            anchors = []
            for sel in JOB_LINK_SELECTORS:
                try:
                    els = await page.query_selector_all(sel)
                    anchors.extend(els)
                except Exception:
                    continue

            if not anchors:
                break

            page_jobs = 0
            for anchor in anchors:
                try:
                    href = await anchor.get_attribute("href") or ""

                    # Extract numeric job ID — Phenom always embeds it in the path
                    m = re.search(r"/job[s]?/(\d+)", href)
                    if not m:
                        # Try data attribute as fallback (HCA)
                        job_id = await anchor.get_attribute("data-ph-at-job-id") or ""
                        if not job_id:
                            continue
                    else:
                        job_id = m.group(1)

                    if job_id in seen_ids:
                        continue
                    seen_ids.add(job_id)

                    if href:
                        job_url = f"https://{domain}{href}" if href.startswith("/") else href
                    else:
                        job_url = f"https://{domain}/us/en/job/{job_id}/"

                    # ── Title ─────────────────────────────────────────────
                    title = await anchor.get_attribute("data-ph-at-job-title-text") or ""
                    if not title:
                        t_el = await anchor.query_selector(
                            "[data-ph-at-job-title-text], h2, h3, h4, "
                            "[class*='title'], [class*='Title']"
                        )
                        title = (await t_el.inner_text()).strip() if t_el else ""
                    if not title:
                        title = (await anchor.inner_text()).strip().split("\n")[0]
                    if not title:
                        continue

                    # ── Location ──────────────────────────────────────────
                    loc_el = await anchor.query_selector(
                        "[data-ph-at-job-location-text], "
                        "[class*='location'], [class*='Location'], "
                        "[class*='city'], [class*='address']"
                    )
                    loc = (await loc_el.inner_text()).strip() if loc_el else ""
                    loc = re.sub(r"^\s*[\u2605\u2022\uf041\uf3c5]\s*", "", loc).strip()
                    _city, _state = parse_city_state(loc)

                    # ── Category ──────────────────────────────────────────
                    cat_el = await anchor.query_selector(
                        "[class*='category'], [class*='Category'], "
                        "[class*='department'], [class*='Department']"
                    )
                    category = (await cat_el.inner_text()).strip() if cat_el else ""

                    # ── Job type ──────────────────────────────────────────
                    type_el = await anchor.query_selector(
                        "[class*='job-type'], [class*='jobType'], "
                        "[class*='employment'], [class*='Employment']"
                    )
                    job_type = (await type_el.inner_text()).strip() if type_el else ""

                    jobs.append(Job(
                        title=title, hospital_system=system_name,
                        hospital_name=system_name,
                        city=_city, state=_state, location=loc,
                        specialty=category, job_type=job_type,
                        url=job_url, job_id=job_id,
                        posted_date="", description="",
                        ats_platform="Phenom",
                    ))
                    page_jobs += 1
                except Exception:
                    continue

            logger.info(f"  [{system_name}] page {page_num+1}: {page_jobs} new jobs (running total {len(jobs)})")
            page_num += 1

            # ── Next page button ──────────────────────────────────────────
            next_btn = None
            for next_sel in [
                "[data-ph-at-pagination-next-btn]",
                "[aria-label='Next page']",
                "[aria-label='next page']",
                "button[class*='next']:not([disabled])",
                "a[class*='next']:not([disabled])",
                "[class*='pagination-next']:not([disabled])",
                "[class*='nextPage']:not([disabled])",
            ]:
                try:
                    el = await page.query_selector(next_sel)
                    if el:
                        disabled      = await el.get_attribute("disabled")
                        aria_disabled = await el.get_attribute("aria-disabled")
                        if disabled is None and aria_disabled != "true":
                            next_btn = el
                            break
                except Exception:
                    continue

            if not next_btn:
                logger.info(f"  [{system_name}] no next button — done at page {page_num}")
                break

            try:
                # Capture one current job href to detect when page actually changes
                first_href_before = ""
                try:
                    cur = await page.query_selector("a[href*='/job/']")
                    if cur:
                        first_href_before = await cur.get_attribute("href") or ""
                except Exception:
                    pass

                await next_btn.scroll_into_view_if_needed()
                await next_btn.click()

                # Wait for page content to visibly change — up to 30s
                # Strategy: poll until first job href differs from before OR 30s pass
                waited = 0
                while waited < 30:
                    await asyncio.sleep(2)
                    waited += 2
                    try:
                        cur = await page.query_selector("a[href*='/job/']")
                        if cur:
                            new_href = await cur.get_attribute("href") or ""
                            if new_href and new_href != first_href_before:
                                break  # Content has changed — new page loaded
                    except Exception:
                        pass
                await asyncio.sleep(1)  # Brief settle

            except Exception as e:
                logger.info(f"  [{system_name}] pagination click failed: {e}")
                break

        await ctx.close()
    except Exception as e:
        logger.error(f"Phenom DOM {system_name}: {e}")

    logger.info(f"  {system_name}: {len(jobs)} jobs")
    return jobs


async def run_playwright_scrapers() -> list[Job]:
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        logger.warning("Playwright not installed — skipping custom sites")
        return []

    logger.info("Playwright: scraping JS-heavy custom sites...")
    jobs = []

    CUSTOM_SITES = [
        # PRODUCING JOBS — keep these
        ("Mayo Clinic",                   "https://jobs.mayoclinic.org/search-jobs"),
        ("CHRISTUS Health",               "https://careers.christushealth.org/job-search"),
        ("MyMichigan Health",             "https://careers.mymichigan.org/jobs"),
        # BSW, HCA, Ascension → moved to scrape_phenom_dom() below
        ("Cleveland Clinic",              "https://jobs.clevelandclinic.org/search/"),
        # HCA AFFILIATES
        ("Methodist Healthcare",          "https://www.joinmethodist.com/search/jobs"),
        # LIFEPOINT — rebuilt on WordPress 2025
        ("LifePoint Health",              "https://jobs.lifepointhealth.net/jobs/"),
        # CUSTOM ATS
        ("MUSC Health",                   "https://musc.career-pages.com/jobs/search"),
        ("University of Vermont Health",  "https://www.uvmhealthnetworkcareers.org/jobs/"),
    ]

    # Deduplicate by system name
    seen_systems = set()
    CUSTOM_SITES = [(name, url) for name, url in CUSTOM_SITES
                    if name not in seen_systems and not seen_systems.add(name)]

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=[
            "--no-sandbox", "--disable-setuid-sandbox",
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
        ])

        # ── Dedicated Phenom DOM scrapers (BSW, HCA, Ascension) ──────────
        for _name, _url in PHENOM_DOM_SITES:
            _phenom_jobs = await scrape_phenom_dom(browser, _name, _url)
            jobs.extend(_phenom_jobs)
            await asyncio.sleep(random.uniform(3, 5))

        for system_name, url in CUSTOM_SITES:
            try:
                ctx = await browser.new_context(
                    viewport={"width": 1440, "height": 900},
                    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
                    locale="en-US",
                )
                await ctx.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>false})")
                page = await ctx.new_page()

                captured = []
                async def capture(response, _sn=system_name):
                    url_l = response.url.lower()
                    if any(x in url_l for x in [
                        "api/jobs", "search-jobs", "careers/search", "job-search",
                        "jobpostings", "/jobs?", "requisitions", "positions",
                        "api/search", "job_search", "jobsearch", "joblist",
                        "/search/", "apply/v2", "talentbrew", "tb_ajax",
                        "findly", "job-search-results/results",
                        "career-pages.com", "uvmhealthnetwork", "widgets",
                        "wp-json", "lifepointhealth.net/jobs",
                    ]):
                        try:
                            ct = response.headers.get("content-type", "")
                            if "json" in ct:
                                d = await response.json()
                                if isinstance(d, dict):
                                    # Handle Elasticsearch nested hits: {"hits": {"hits": [...], "total": N}}
                                    if isinstance(d.get("hits"), dict) and isinstance(d["hits"].get("hits"), list):
                                        raw_hits = d["hits"]["hits"]
                                        unwrapped = [h.get("_source", h) for h in raw_hits if isinstance(h, dict)]
                                        captured.extend(unwrapped)
                                    else:
                                        # Handle Phenom widgets nested response:
                                        # {"data": {"jobs": [...], "count": N}, "reqData": {...}}
                                        # or {"data": {"requisitions": [...]}}
                                        data_val = d.get("data")
                                        if isinstance(data_val, dict):
                                            for inner_key in ("jobs", "requisitions", "results", "postings", "items"):
                                                inner = data_val.get(inner_key)
                                                if isinstance(inner, list) and inner:
                                                    captured.extend(inner)
                                                    break
                                        for key in ("jobs", "jobPostings", "results", "requisitions",
                                                    "postings", "items", "hits"):
                                            val = d.get(key)
                                            if isinstance(val, list) and val:
                                                unwrapped = [j.get("_source", j) if isinstance(j, dict) and "_source" in j else j for j in val]
                                                captured.extend(unwrapped)
                                                break
                                        # data is a flat list
                                        if isinstance(data_val, list) and data_val:
                                            captured.extend(data_val)
                                elif isinstance(d, list) and d:
                                    # Unwrap _source if ES-style hits
                                    unwrapped = [j.get("_source", j) if isinstance(j, dict) and "_source" in j else j for j in d]
                                    captured.extend(unwrapped)
                        except: pass
                page.on("response", capture)

                advent_site = "adventhealth.com" in url
                _wait = "domcontentloaded" if advent_site else "networkidle"
                await page.goto(url, wait_until=_wait, timeout=30000)
                await asyncio.sleep(random.uniform(2, 4))

                # AdventHealth (Findly) — click Search jobs button then paginate through results
                if advent_site:
                    try:
                        # Click the Search jobs button to trigger initial API call
                        btn = await page.query_selector("a[href='#'][class*='search'], button[class*='search'], a:has-text('Search jobs'), button:has-text('Search jobs')")
                        if not btn:
                            # Try injecting a direct fetch of the results API (Findly pattern)
                            logger.info("AdventHealth: trying direct results API fetch")
                        else:
                            await btn.click()
                            await asyncio.sleep(5)
                            logger.info("AdventHealth: clicked search button")

                        # Findly paginates via results endpoint — fetch all pages directly
                        base_results = "https://jobs.adventhealth.com/job-search-results/results"
                        page_num = 1
                        rpp = 100
                        while True:
                            try:
                                resp = await page.evaluate(f"""async () => {{
                                    const params = new URLSearchParams({{
                                        ActiveFacetID: '0',
                                        CurrentPage: '{page_num}',
                                        RecordsPerPage: '{rpp}',
                                        TotalContentResults: '',
                                        Distance: '50',
                                        RadiusUnitType: '0',
                                        Keywords: '',
                                        Location: '',
                                        ShowRadius: 'False',
                                        IsPagination: '{str(page_num > 1).lower()}',
                                        SortCriteria: '0',
                                        SortDirection: '0',
                                        SearchType: '5',
                                        ResultsType: '0',
                                        fc: '', fl: '', fcf: '', afc: '', afl: '', afcf: ''
                                    }});
                                    const r = await fetch('{base_results}?' + params.toString(), {{
                                        headers: {{'X-Requested-With': 'XMLHttpRequest', 'Accept': 'application/json, text/javascript, */*'}}
                                    }});
                                    return await r.text();
                                }}""")
                                import json as _json
                                try:
                                    data = _json.loads(resp)
                                except:
                                    logger.info(f"AdventHealth page {page_num}: non-JSON response")
                                    break
                                has_jobs = data.get("hasJobs", True)
                                results_html = data.get("results", "")
                                if not has_jobs or not results_html:
                                    logger.info(f"AdventHealth: no more jobs at page {page_num}")
                                    break
                                # Parse job cards from results HTML
                                import re as _re
                                job_links = _re.findall(
                                    r'href="(/job/([^/]+)/([^/]+)/\d+/(\d+))"',
                                    results_html
                                )
                                title_matches = dict(_re.findall(
                                    r'data-job-id="(\d+)"[^>]*>\s*<[^>]+>([^<]+)<',
                                    results_html
                                ))
                                seen_ids = set()
                                page_count = 0
                                for url_path, city_slug, title_slug, job_id in job_links:
                                    if job_id in seen_ids:
                                        continue
                                    seen_ids.add(job_id)
                                    page_count += 1
                                    city_name = city_slug.replace("-", " ").title()
                                    city_state = COMMONSPIRIT_CITY_STATE.get(city_slug.lower(), "") or                                                  COMMONSPIRIT_CITY_STATE.get(city_name.lower(), "")
                                    title = title_slug.replace("-", " ").title()
                                    # Try to get the real title from HTML
                                    title_m = _re.search(
                                        rf'href="[^"]*{_re.escape(job_id)}"[^>]*>\s*([^<]+)<',
                                        results_html
                                    )
                                    if title_m:
                                        title = title_m.group(1).strip()
                                    jobs.append(Job(
                                        title=title,
                                        hospital_system=system_name,
                                        hospital_name=system_name,
                                        city=city_name,
                                        state=city_state,
                                        location=f"{city_name}, {city_state}" if city_state else city_name,
                                        specialty="", job_type="",
                                        url=f"https://jobs.adventhealth.com{url_path}",
                                        job_id=job_id,
                                        posted_date="", description="",
                                        ats_platform="Findly",
                                    ))
                                logger.info(f"AdventHealth: page {page_num} → {page_count} jobs (total: {len([j for j in jobs if j.hospital_system == system_name])})")
                                if page_count < rpp:
                                    break
                                page_num += 1
                                await asyncio.sleep(random.uniform(1, 2))
                            except Exception as e:
                                logger.info(f"AdventHealth pagination error: {e}")
                                break
                    except Exception as e:
                        logger.info(f"AdventHealth search trigger: {e}")

                for _ in range(4):
                    await page.evaluate("window.scrollBy(0, 600)")
                    await asyncio.sleep(0.8)

                # Debug: log first raw job for CHRISTUS to inspect field names
                if system_name == "CHRISTUS Health" and captured:
                    sample = {k: v for k, v in list(captured[0].items())[:20]}
                    logger.info(f"CHRISTUS sample job fields: {sample}")

                for j in captured:
                    if not isinstance(j, dict):
                        continue
                    title = j.get("title", j.get("jobTitle", j.get("name", j.get("positionTitle", ""))))
                    # Try many possible location field names; CHRISTUS uses various structures
                    loc = (j.get("location") or j.get("city") or j.get("locationsText") or
                           j.get("primaryLocation") or j.get("address") or
                           j.get("locationName") or j.get("jobLocation") or "")
                    if isinstance(loc, dict):
                        # Handle many possible dict shapes from different ATS platforms
                        loc_city  = (loc.get("city") or loc.get("cityName") or loc.get("municipality") or
                                     loc.get("addressLocality") or loc.get("name") or "")
                        loc_state = (loc.get("stateCode") or loc.get("state") or loc.get("region") or
                                     loc.get("countrySubdivisionCode") or loc.get("addressRegion") or "")
                        loc = f"{loc_city}, {loc_state}" if loc_city or loc_state else ""
                    elif isinstance(loc, list):
                        loc = ", ".join(str(x) for x in loc[:2])
                    _city, _state = parse_city_state(str(loc))
                    job_id = str(j.get("id", j.get("jobId", j.get("requisitionId", j.get("externalId", "")))))
                    if not job_id:
                        job_id = f"{system_name}_{title}_{loc}"[:80]
                    # hospital_name: prefer bu (CHRISTUS business unit) > facility > department > system
                    _hosp_name = (j.get("bu") or j.get("facility") or j.get("department") or system_name)
                    if title:
                        jobs.append(Job(
                            title=str(title), hospital_system=system_name,
                            hospital_name=_hosp_name,
                            city=_city,
                            state=_state,
                            location=str(loc), specialty=j.get("category", j.get("jobCategory", "")),
                            job_type=j.get("employmentType", j.get("jobType", "")),
                            url=str(j.get("url", j.get("applyUrl", j.get("canonicalPositionUrl", url)))),
                            job_id=job_id,
                            posted_date=str(j.get("datePosted", j.get("postedOn", j.get("postingDate", ""))))[:10],
                            description=strip_html(str(j.get("description", j.get("shortDescription", "")))),
                            ats_platform="Custom",
                        ))

                # DOM fallback if no API responses captured
                if not [j for j in jobs if j.hospital_system == system_name]:
                    cards = await page.query_selector_all(
                        "[data-job-id],[data-testid='job-card'],.job-card,.job-listing,"
                        ".search-result-item,li.job,.job-result,article.job,[class*='JobCard'],"
                        "[class*='job-item'],[class*='career-item']"
                    )
                    for card in cards[:200]:
                        try:
                            t = await card.query_selector(
                                "h2,h3,h4,.job-title,[data-testid='job-title'],"
                                "[class*='title'],[class*='Title']"
                            )
                            a = await card.query_selector("a[href]")
                            l = await card.query_selector(
                                ".location,.job-location,[data-testid='location'],"
                                "[class*='location'],[class*='Location']"
                            )
                            title_txt = (await t.inner_text()).strip() if t else ""
                            href      = await a.get_attribute("href") if a else ""
                            loc_txt   = (await l.inner_text()).strip() if l else ""
                            if title_txt:
                                p = [x.strip() for x in loc_txt.split(",")]
                                jobs.append(Job(
                                    title=title_txt, hospital_system=system_name,
                                    hospital_name=system_name,
                                    city=p[0] if p else "", state=p[-1] if len(p)>1 else "",
                                    location=loc_txt, specialty="", job_type="",
                                    url=f"{url.rstrip('/')}{href}" if href and href.startswith("/") else href or url,
                                    job_id=href.split("/")[-1] if href else title_txt[:60],
                                    posted_date="", description="", ats_platform="Custom",
                                ))
                        except: continue

                await ctx.close()
                count = len([j for j in jobs if j.hospital_system == system_name])
                logger.info(f"  {system_name}: {count} jobs")
                await asyncio.sleep(random.uniform(3, 5))

            except Exception as e:
                logger.error(f"Playwright {system_name}: {e}")

        await browser.close()

    logger.info(f"  Playwright total: {len(jobs):,} jobs")
    return jobs


# ══════════════════════════════════════════════════════════════════════════
#  MASTER RUNNER
# ══════════════════════════════════════════════════════════════════════════
async def run_all() -> list[dict]:
    start = datetime.now()
    # Two sessions: one with ssl=False for proxy-routed scrapers,
    # one with normal SSL for scrapers that connect directly (Taleo, SF, etc.)
    proxy_connector  = aiohttp.TCPConnector(limit=30, ssl=False)
    direct_connector = aiohttp.TCPConnector(limit=30)

    # max_line_size raised to 64 KB — Tenet's Set-Cookie headers exceed the 8 KB default
    async with aiohttp.ClientSession(connector=proxy_connector,  headers=HEADERS,
                                      max_line_size=65536, max_field_size=65536) as proxy_session, \
               aiohttp.ClientSession(connector=direct_connector, headers=HEADERS,
                                      max_line_size=65536, max_field_size=65536) as direct_session:
        ats_results = await asyncio.gather(
            run_workday(proxy_session),
            run_taleo(direct_session),       # direct — no ssl=False
            run_icims(proxy_session),
            run_greenhouse(proxy_session),
            run_smartrecruiters(proxy_session),
            run_lever(proxy_session),
            run_usajobs(direct_session),
            run_adp(proxy_session),
            run_selectminds(proxy_session),
            run_recruitingcom(proxy_session),
            run_infor(proxy_session),
            run_phenom(proxy_session),
            run_talentbrew(proxy_session),
            # ── New platforms from URL spreadsheet ──
            run_ukg(proxy_session),
            run_oracle(proxy_session),
            run_healthcaresource(proxy_session),
            run_tenet(proxy_session),
            run_trinity(proxy_session),
            run_uhs(proxy_session),
            run_lifepoint(proxy_session),
            run_kronos(proxy_session),
            run_applicantpro(proxy_session),
            return_exceptions=True,
        )

    pw_jobs = await run_playwright_scrapers()

    all_jobs: list[Job] = pw_jobs[:]
    for r in ats_results:
        if isinstance(r, list):
            all_jobs.extend(r)

    seen, unique = set(), []

    def normalize_job(j: Job) -> dict:
        """Standardize location fields before writing to Supabase.
        - city/state cleaned and trimmed
        - If city matches hospital_name (or hospital_system), blank the city
        - location always built as 'City, ST' from clean city + state
        """
        d = asdict(j)

        city  = (d.get("city")  or "").strip().strip(",").strip()
        state = (d.get("state") or "").strip().upper()

        # Force override — always wins regardless of scraped data
        _sys_key = (d.get("hospital_system") or "").strip().lower()
        if _sys_key in FORCE_LOCATION_OVERRIDE:
            city, state = FORCE_LOCATION_OVERRIDE[_sys_key]

        # Keep only the 2-char state code if state is noisy (e.g. "TX, United States" or "United States")
        COUNTRY_JUNK = {"united states", "us", "usa", "canada", "united kingdom", "uk"}
        if state and (len(state) > 2 or state.lower() in COUNTRY_JUNK):
            parts = [p.strip() for p in state.split(",")]
            state = next((p for p in parts if len(p) == 2 and p.isalpha()), "")
            if not state:
                # Try pulling state from raw location string instead
                raw_loc = (d.get("location") or "").upper()
                loc_parts = [p.strip() for p in raw_loc.split(",")]
                state = next((p for p in loc_parts if len(p) == 2 and p.isalpha()), "")

        # Blank city only if it is an exact match for the hospital/system name
        # (Workday previously put loc string as hospital_name — now fixed upstream)
        hosp_name   = (d.get("hospital_name")   or "").strip().lower()
        hosp_system = (d.get("hospital_system") or "").strip().lower()
        city_lower  = city.lower()
        if city_lower and (city_lower == hosp_name or city_lower == hosp_system):
            city = ""

        # Location lookup fallback — fires when city or state still missing
        if not city or not state:
            lookup = FACILITY_LOCATION_MAP.get(hosp_name) or SYSTEM_LOCATION_DEFAULTS.get(hosp_system)
            if lookup:
                fallback_city, fallback_state = lookup
                if not city:
                    city = fallback_city
                if not state:
                    state = fallback_state

        # Build canonical location: "City, ST" — blank if both missing
        if city and state:
            location = f"{city}, {state}"
        elif state:
            location = state
        elif city:
            location = city
        else:
            location = ""

        d["city"]     = city
        d["state"]    = state
        d["location"] = location
        return d

    for job in all_jobs:
        key = f"{job.ats_platform}::{job.hospital_system}::{job.job_id}"
        if key not in seen and job.job_id and job.title:
            seen.add(key)
            unique.append(normalize_job(job))

    elapsed = (datetime.now() - start).seconds
    logger.info("=" * 55)
    logger.info(f"  TOTAL UNIQUE JOBS:  {len(unique):,}")
    logger.info(f"  SYSTEMS COVERED:    {len({j['hospital_system'] for j in unique})}")
    logger.info(f"  STATES COVERED:     {len({j['state'] for j in unique if j['state']})}")
    logger.info(f"  RUNTIME:            {elapsed}s")
    logger.info("=" * 55)
    return unique


def scrape() -> list[dict]:
    os.makedirs("logs", exist_ok=True)
    return asyncio.run(run_all())


if __name__ == "__main__":
    jobs = scrape()
    with open("jobs_latest.json", "w") as f:
        json.dump(jobs, f, indent=2)
    print(f"Saved {len(jobs):,} jobs to jobs_latest.json")
