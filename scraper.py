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
    "CommonSpirit Health":       ("commonspirit",       "1",  "CommonSpirit_Health_External"),
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

async def scrape_workday(session: aiohttp.ClientSession, system: str, tenant_data: tuple) -> list[Job]:
    tenant, wd_num, primary_site = tenant_data
    jobs = []

    # Try primary site name first, then fallbacks, across multiple wd numbers
    site_names = [primary_site] + [s for s in CAREER_SITE_FALLBACKS if s != primary_site]
    wd_nums = [wd_num] + [n for n in ["5","1","3","10"] if n != wd_num]

    working_url = None
    for wdn in wd_nums:
        for sn in site_names:
            url = f"https://{tenant}.wd{wdn}.myworkdayjobs.com/wday/cxs/{tenant}/{sn}/jobs"
            try:
                async with req(session, "post", url,
                    json={"limit": 1, "offset": 0, "searchText": "", "locations": [], "categories": []},
                    headers={**HEADERS, "Content-Type": "application/json"}, ssl=False, proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=10)) as r:
                    if r.status == 200:
                        working_url = url
                        break
            except:
                continue
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
                parts = [p.strip() for p in loc.split(",")]
                jobs.append(Job(
                    title=j.get("title", ""),
                    hospital_system=system,
                    hospital_name=loc or system,
                    city=parts[0] if parts else "",
                    state=parts[1] if len(parts) > 1 else "",
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
                city, state = j.get("city", ""), j.get("state", "")
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
    # Format: "System": "full.career.domain"
    # Domains verified from each org's public career page
    "CommonSpirit Health":    "careers-commonspirit.icims.com",
    "Ascension Health":       "jobs.ascension.org",
    "Advocate Aurora Health": "jobs.advocateaurorahealth.org",
    "AdventHealth":           "jobs.adventhealth.com",
    "Sentara Healthcare":     "careers.sentara.com",
    "MedStar Health":         "careers.medstarhealth.org",
    "Texas Health Resources": "jobs.texashealth.org",
    "UPMC":                   "careers.upmc.com",
    "Cone Health":            "careers.conehealth.com",
    "HealthPartners":         "careers.healthpartners.com",
    "Northwestern Medicine":  "careers.nm.org",
    "Loma Linda University":  "jobs.lomalindahealth.org",
    "Kettering Health":       "careers.ketteringhealth.org",
    "Monument Health":        "jobs.monument.health",
    "Owensboro Health":       "careers.owensborohealth.org",
    "Stormont Vail":          "careers.stormontvail.org",
}

async def scrape_icims(session: aiohttp.ClientSession, system: str, domain: str) -> list[Job]:
    jobs = []
    # iCIMS JSON API — returns structured job data when mode=json
    url = f"https://{domain}/jobs/search"
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
                        parts = [p.strip() for p in str(loc).split(",")]
                        jid = str(j.get("jobid", j.get("id", "")))
                        jobs.append(Job(
                            title=j.get("jobtitle", j.get("title", "")),
                            hospital_system=system,
                            hospital_name=j.get("jobcompany", system),
                            city=parts[0] if parts else "",
                            state=parts[-1] if len(parts) > 1 else "",
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
                                    parts = [p.strip() for p in loc.split(",")]
                                    jid = str(j.get("id", ""))
                                    jobs.append(Job(
                                        title=j.get("title", ""),
                                        hospital_system=system, hospital_name=system,
                                        city=parts[0] if parts else "", state=parts[-1] if len(parts)>1 else "",
                                        location=loc, specialty="", job_type="",
                                        url=f"https://{domain}/jobs/{jid}/job",
                                        job_id=jid, posted_date="", description="",
                                        ats_platform="iCIMS",
                                    ))
                            except: pass
                        break
                    for jid, title, loc in found:
                        parts = [p.strip() for p in loc.split(",")]
                        jobs.append(Job(
                            title=title, hospital_system=system, hospital_name=system,
                            city=parts[0] if parts else "", state=parts[-1] if len(parts)>1 else "",
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
            parts = [p.strip() for p in loc.split(",")]
            jobs.append(Job(
                title=j.get("title", ""),
                hospital_system=system,
                hospital_name=system,
                city=parts[0] if parts else "",
                state=parts[-1] if len(parts) > 1 else "",
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
                loc   = j.get("location", {})
                city  = loc.get("city", "")
                state = loc.get("region", "")
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
            parts = [p.strip() for p in loc.split(",")]
            jobs.append(Job(
                title=j.get("text", ""),
                hospital_system=system,
                hospital_name=system,
                city=parts[0] if parts else "",
                state=parts[-1] if len(parts) > 1 else "",
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
PHENOM_ORGS = {
    # CommonSpirit moved to iCIMS — see ICIMS_ORGS
    "Baptist Health":        "https://jobs.baptisthealthcareers.com",
    "Corewell Health":       "https://careers.corewellhealth.org",
    "Munson Healthcare":     "https://careers.munsonhealthcare.org",
    "Bryan Health":          "https://careers.bryanhealth.com",
    "Baylor Scott & White":  "https://jobs.bswhealth.com",
}

async def scrape_phenom(session: aiohttp.ClientSession, system: str, base_url: str) -> list[Job]:
    jobs = []
    endpoints = [
        f"{base_url}/api/jobs",
        f"{base_url}/api/search/jobs",
        f"{base_url}/search/jobs",
        f"{base_url}/en/search-results",
    ]
    api_url = None
    for ep in endpoints:
        try:
            async with session.get(
                ep,
                params={"start": 0, "num": 1},
                headers={**HEADERS, "Accept": "application/json"},
                proxy=proxies.get(), ssl=False, timeout=aiohttp.ClientTimeout(total=15)
            ) as r:
                if r.status == 200:
                    ct = r.headers.get("content-type", "")
                    if "json" in ct:
                        api_url = ep
                        break
        except:
            continue

    if not api_url:
        logger.info(f"Phenom {system}: no API endpoint found")
        return []

    logger.info(f"Phenom {system}: using {api_url}")
    offset = 0
    while True:
        try:
            async with session.get(
                api_url,
                params={"start": offset, "num": 20, "size": 20, "from": offset},
                headers={**HEADERS, "Accept": "application/json"},
                proxy=proxies.get(), ssl=False, timeout=aiohttp.ClientTimeout(total=25)
            ) as r:
                if r.status != 200:
                    break
                data = await r.json(content_type=None)

            listings = (
                data.get("jobs") or
                data.get("requisitions") or
                data.get("results") or
                data.get("data", {}).get("jobs") or
                []
            )
            if not listings:
                break

            for j in listings:
                loc = j.get("city", "") or j.get("location", "") or j.get("locations", "")
                if isinstance(loc, list):
                    loc = ", ".join(loc)
                city = j.get("city", "")
                state = j.get("state", "") or j.get("stateCode", "")
                title = j.get("title", "") or j.get("jobTitle", "")
                job_id = str(j.get("id", "") or j.get("jobId", "") or j.get("requisitionId", ""))
                url = j.get("applyUrl", "") or j.get("jobUrl", "") or f"{base_url}/job/{job_id}"
                if title and job_id:
                    jobs.append(Job(
                        title=title,
                        hospital_system=system,
                        hospital_name=j.get("facility", system) or system,
                        city=city, state=state,
                        location=loc or f"{city}, {state}",
                        specialty=j.get("category", "") or j.get("jobCategory", ""),
                        job_type=j.get("employmentType", "") or j.get("jobType", ""),
                        url=url,
                        job_id=job_id,
                        posted_date=str(j.get("postedDate", "") or j.get("datePosted", ""))[:10],
                        description=strip_html(str(j.get("description", "") or j.get("shortDescription", ""))),
                        ats_platform="Phenom",
                    ))

            total = data.get("total", data.get("count", len(listings)))
            offset += 20
            if offset >= total or len(listings) < 20:
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
                    parts = [p.strip() for p in loc.split(",")]
                    city = parts[0] if parts else ""
                    state = parts[-1] if len(parts) > 1 else ""
                else:
                    city  = loc_obj.get("city", "")
                    state = loc_obj.get("stateCode", "") or loc_obj.get("countrySubdivisionCode", "")
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
                parts = [p.strip() for p in loc.split(",")]
                jobs.append(Job(
                    title=j.get("title", ""),
                    hospital_system=system,
                    hospital_name=j.get("department", system),
                    city=parts[0] if parts else "",
                    state=parts[-1] if len(parts) > 1 else "",
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
                parts = [p.strip() for p in str(loc).split(",")]
                jobs.append(Job(
                    title=j.get("title", ""),
                    hospital_system=system,
                    hospital_name=system,
                    city=parts[0] if parts else "",
                    state=parts[-1] if len(parts) > 1 else "",
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
    "Faith Regional Health": (
        "css-faithregional-prd",
        "https://css-faithregional-prd.inforcloudsuite.com/hcm/Jobs/page/JobsHomePage",
        "100",
    ),
}

async def scrape_infor(session: aiohttp.ClientSession, system: str, org_data: tuple) -> list[Job]:
    org_id, base_url, hr_org = org_data
    jobs = []

    # Infor CloudSuite HCM — Lawson CandidateSelfService JSON API
    # This is the older Lawson HCM pattern used by CHRISTUS, Faith Regional, etc.
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
            city  = j.get("City", "")
            state = j.get("State", "") or j.get("StateProvince", "")
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


async def run_playwright_scrapers() -> list[Job]:
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        logger.warning("Playwright not installed — skipping custom sites")
        return []

    logger.info("Playwright: scraping JS-heavy custom sites...")
    jobs = []

    CUSTOM_SITES = [
        # Original high-volume systems
        ("Mayo Clinic",          "https://jobs.mayoclinic.org/search-jobs"),
        ("Cleveland Clinic",     "https://jobs.clevelandclinic.org/search/"),
        ("Mass General Brigham", "https://www.massgeneralbrigham.org/en/careers/search-jobs"),
        ("HCA Healthcare",       "https://careers.hcahealthcare.com/jobs"),
        ("Ascension Health",     "https://ascension.org/careers"),
        # Infor CloudSuite — session-based, requires browser
        ("CHRISTUS Health",      "https://careers.christushealth.org/job-search"),
        # Phenom-powered sites (JS-rendered, no public REST API)
        ("CommonSpirit Health",  "https://www.commonspirit.careers"),
        ("Baptist Health",       "https://jobs.baptisthealthcareers.com"),
        ("Corewell Health",      "https://careers.corewellhealth.org"),
        ("Munson Healthcare",    "https://careers.munsonhealthcare.org"),
        ("Bryan Health",         "https://careers.bryanhealth.com"),
        # SF-backed major health systems (custom career portals)
        ("Cleveland Clinic",     "https://jobs.clevelandclinic.org/search/"),  # also SF
        ("NYU Langone Health",   "https://jobs.nyulangone.org/search/"),
        ("Duke Health",          "https://careers.dukehealth.org/jobs"),
        ("Johns Hopkins",        "https://jobs.hopkinsmedicine.org/search-jobs/"),
        ("Cedars-Sinai",         "https://jobs.cedars-sinai.edu/search/"),
        ("Penn Medicine",        "https://careers.pennmedicine.org/search-jobs"),
        # From URL list
        ("McLaren Health Care",  "https://careers.mclaren.org/jobs/search/3767858"),
        ("MyMichigan Health",    "https://careers.mymichigan.org/jobs"),
        ("U of Michigan Health", "https://careers.umich.edu/search-jobs"),
        ("Aspirus Health",       "https://careers.aspirus.org/job-search/"),
        ("White River Health",   "https://whiteriverhealth.org/online-job-board"),
        ("SARH Care",            "https://sarhcare.org/careers/"),
        ("W Regional Medical",   "https://www.wregional.com/main/open-positions"),
        ("AMMC Nursing",         "https://www.myammc.org/joblistings/category/nursing_positions"),
    ]

    # Deduplicate by system name (Cleveland Clinic listed twice above)
    seen_systems = set()
    CUSTOM_SITES = [(name, url) for name, url in CUSTOM_SITES
                    if name not in seen_systems and not seen_systems.add(name)]

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=[
            "--no-sandbox", "--disable-setuid-sandbox",
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
        ])

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
                        "/search/", "apply/v2",
                    ]):
                        try:
                            ct = response.headers.get("content-type", "")
                            if "json" in ct:
                                d = await response.json()
                                if isinstance(d, dict):
                                    for key in ("jobs", "jobPostings", "results", "requisitions",
                                                "postings", "data", "items", "hits"):
                                        val = d.get(key)
                                        if isinstance(val, list) and val:
                                            captured.extend(val)
                                            break
                                    else:
                                        # Maybe the dict itself is a wrapper
                                        if d.get("total") and isinstance(d.get("jobs"), list):
                                            captured.extend(d["jobs"])
                                elif isinstance(d, list) and d:
                                    captured.extend(d)
                        except: pass
                page.on("response", capture)

                await page.goto(url, wait_until="networkidle", timeout=45000)
                await asyncio.sleep(random.uniform(2, 4))
                for _ in range(4):
                    await page.evaluate("window.scrollBy(0, 600)")
                    await asyncio.sleep(0.8)

                for j in captured:
                    if not isinstance(j, dict):
                        continue
                    title = j.get("title", j.get("jobTitle", j.get("name", j.get("positionTitle", ""))))
                    loc   = j.get("location", j.get("city", j.get("locationsText", j.get("primaryLocation", ""))))
                    if isinstance(loc, dict):
                        loc = f"{loc.get('city','')}, {loc.get('stateCode', loc.get('state',''))}"
                    elif isinstance(loc, list):
                        loc = ", ".join(str(x) for x in loc[:2])
                    parts = [p.strip() for p in str(loc).split(",")]
                    job_id = str(j.get("id", j.get("jobId", j.get("requisitionId", j.get("externalId", "")))))
                    if not job_id:
                        job_id = f"{system_name}_{title}_{loc}"[:80]
                    if title:
                        jobs.append(Job(
                            title=str(title), hospital_system=system_name,
                            hospital_name=j.get("facility", j.get("department", system_name)),
                            city=parts[0] if parts else "",
                            state=parts[-1] if len(parts) > 1 else "",
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
    proxy_connector = aiohttp.TCPConnector(limit=30, ssl=False)
    direct_connector = aiohttp.TCPConnector(limit=30)

    async with aiohttp.ClientSession(connector=proxy_connector, headers=HEADERS) as proxy_session, \
               aiohttp.ClientSession(connector=direct_connector, headers=HEADERS) as direct_session:
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
            return_exceptions=True,
        )

    pw_jobs = await run_playwright_scrapers()

    all_jobs: list[Job] = pw_jobs[:]
    for r in ats_results:
        if isinstance(r, list):
            all_jobs.extend(r)

    seen, unique = set(), []
    for job in all_jobs:
        key = f"{job.ats_platform}::{job.hospital_system}::{job.job_id}"
        if key not in seen and job.job_id and job.title:
            seen.add(key)
            unique.append(asdict(job))

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
