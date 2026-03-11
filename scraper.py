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
        raw = os.environ.get("PROXY_LIST", "")
        self.proxies = [p.strip() for p in raw.split(",") if p.strip()]
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


# ══════════════════════════════════════════════════════════════════════════
#  WORKDAY
#  Format: "System Name": (tenant, wd_num, career_site_name)
#  Find these by visiting: https://tenant.wd5.myworkdayjobs.com/
# ══════════════════════════════════════════════════════════════════════════
WORKDAY_TENANTS = {
    "Kaiser Permanente":         ("kaiserpermanente",   "5",  "Kaiser_Permanente_External"),
    "Providence Health":         ("providence",         "5",  "External"),
    "Baylor Scott & White":      ("bswhealth",          "5",  "BSWHealth"),
    "Banner Health":             ("bannerhealth",       "5",  "Banner_Health_External"),
    "Northwell Health":          ("northwell",          "5",  "External"),
    "Intermountain Health":      ("intermountain",      "5",  "CareersatIntermountainHealth"),
    "UC Health (Colorado)":      ("uchealth",           "5",  "UCHEALTH_External"),
    "Novant Health":             ("novant",             "5",  "Novant_Health"),
    "Prisma Health":             ("prismahealth",       "5",  "Prisma_Health_External"),
    "Geisinger":                 ("geisinger",          "5",  "External"),
    "Sanford Health":            ("sanfordhealth",      "5",  "Sanford_Health_External"),
    "SSM Health":                ("ssmhealth",          "5",  "SSM_Health_External"),
    "Mercy Health":              ("mercy",              "5",  "Mercy_External"),
    "Carilion Clinic":           ("carilion",           "5",  "Carilion_External"),
    "DaVita":                    ("davita",             "5",  "External"),
    "Henry Ford Health":         ("henryford",          "5",  "Henry_Ford_Health_External"),
    "Houston Methodist":         ("houstonmethodist",   "5",  "HM_External"),
    "Indiana University Health": ("iuhealth",           "5",  "IUH_External"),
    "Inova Health":              ("inova",              "5",  "Inova_External"),
    "NewYork-Presbyterian":      ("nyp",                "5",  "NYP_External"),
    "Ochsner Health":            ("ochsner",            "5",  "Ochsner_Careers"),
    "Parkland Health":           ("parkland",           "5",  "Parkland_External"),
    "Piedmont Healthcare":       ("piedmont",           "5",  "Piedmont_External"),
    "RWJBarnabas Health":        ("rwjbarnabas",        "5",  "External"),
    "Sharp HealthCare":          ("sharp",              "5",  "Sharp_External"),
    "Sutter Health":             ("sutterhealth",       "5",  "Sutter_Health_External"),
    "UNC Health":                ("unchealth",          "5",  "UNC_Health_External"),
    "UnityPoint Health":         ("unitypoint",         "5",  "External"),
    "UT Southwestern Medical":   ("utsouthwestern",     "5",  "UTSW_External"),
    "VCU Health":                ("vcuhealth",          "5",  "VCU_Health_External"),
    "WakeMed":                   ("wakemed",            "5",  "WakeMed_Careers"),
    "Wellstar Health":           ("wellstar",           "5",  "Wellstar_External"),
    "Memorial Hermann":          ("memorialhermann",    "5",  "External"),
    "OhioHealth":                ("ohiohealth",         "5",  "OhioHealth_External"),
    "WellSpan Health":           ("wellspan",           "5",  "WellSpan_External"),
    "Hackensack Meridian":       ("hackensackmeridian", "5",  "HMH_External"),
    "MaineHealth":               ("mainehealth",        "5",  "MaineHealth_External"),
    "McLaren Health Care":       ("mclaren",            "5",  "McLaren_External"),
    "OSF HealthCare":            ("osf",                "5",  "OSF_External"),
    "Tufts Medicine":            ("tuftsmedicine",      "5",  "Tufts_Medicine_External"),
    "Virtua Health":             ("virtua",             "5",  "Virtua_External"),
    "Adventist Health":          ("adventisthealth",    "5",  "Adventist_Health_External"),
    "CommonSpirit Health":       ("commonspirit",       "5",  "CommonSpirit_External"),
    "Dignity Health":            ("dignityhealth",      "5",  "DignityHealth_External"),
    "Bon Secours":               ("bonsecours",         "5",  "Bon_Secours_External"),
    "Essentia Health":           ("essentiahealth",     "5",  "Essentia_External"),
    "Fairview Health":           ("fairview",           "5",  "Fairview_External"),
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
                async with session.post(url,
                    json={"limit": 1, "offset": 0, "searchText": "", "locations": [], "categories": []},
                    headers={**HEADERS, "Content-Type": "application/json"},
                    proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=10)) as r:
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
            async with session.post(working_url,
                json={"limit": 20, "offset": offset, "searchText": "", "locations": [], "categories": []},
                headers={**HEADERS, "Content-Type": "application/json"},
                proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=25)) as r:
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
TALEO_ORGS = {
    "HCA Healthcare":              "hcahealthcare",
    "Tenet Health":                "tenethealth",
    "LifePoint Health":            "lifepointhealth",
    "Community Health Systems":    "chscare",
    "CHRISTUS Health":             "christushealth",
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
            proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=25)) as r:
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
                proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=25)) as r:
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
#  iCIMS — Fixed endpoint
# ══════════════════════════════════════════════════════════════════════════
ICIMS_ORGS = {
    "Ascension Health":            "4647",
    "Advocate Aurora Health":      "4002",
    "AdventHealth":                "4206",
    "Sentara Healthcare":          "1837",
    "MedStar Health":              "2550",
    "Texas Health Resources":      "1556",
    "UPMC":                        "3879",
    "Cone Health":                 "2210",
    "HealthPartners":              "2245",
    "Northwestern Medicine":       "2893",
    "Loma Linda University":       "1598",
    "Kettering Health":            "2346",
    "Monument Health":             "2587",
    "Owensboro Health":            "1822",
    "Stormont Vail":               "2156",
}

async def scrape_icims(session: aiohttp.ClientSession, system: str, org_id: str) -> list[Job]:
    jobs, start = [], 1
    while True:
        try:
            async with session.get(
                f"https://careers.icims.com/jobs/search",
                params={
                    "ss": "1",
                    "in_iframe": "1",
                    "sortby": "posting_date",
                    "sortorder": "desc",
                    "p_startrow": start,
                    "customerId": org_id,
                    "hashed": "-625862532",
                },
                headers=HEADERS,
                proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=25)) as r:
                if r.status != 200:
                    logger.info(f"iCIMS {system}: HTTP {r.status}")
                    break
                text = await r.text()
                # iCIMS returns HTML — parse job links
                job_ids = re.findall(r'data-job-id="(\d+)"', text)
                titles  = re.findall(r'class="title"[^>]*>([^<]+)<', text)
                locs    = re.findall(r'class="location"[^>]*>([^<]+)<', text)
                if not job_ids: break
                for i, jid in enumerate(job_ids):
                    title = titles[i].strip() if i < len(titles) else ""
                    loc   = locs[i].strip() if i < len(locs) else ""
                    parts = [p.strip() for p in loc.split(",")]
                    jobs.append(Job(
                        title=title,
                        hospital_system=system,
                        hospital_name=system,
                        city=parts[0] if parts else "",
                        state=parts[-1] if len(parts) > 1 else "",
                        location=loc,
                        specialty="",
                        job_type="",
                        url=f"https://careers.icims.com/jobs/{jid}/job",
                        job_id=jid,
                        posted_date="",
                        description="",
                        ats_platform="iCIMS",
                    ))
                if len(job_ids) < 25: break
                start += 25
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
#  SUCCESSFACTORS — Fixed with correct SAP SF API
# ══════════════════════════════════════════════════════════════════════════
SUCCESSFACTORS_ORGS = {
    "Cleveland Clinic":            ("clevelandclinic",    "ClevelandClinic_External"),
    "Johns Hopkins Medicine":      ("johnshopkins",       "JohnsHopkins_External"),
    "Cedars-Sinai":                ("cedarssinai",        "CedarsSinai_External"),
    "NYU Langone Health":          ("nyulangone",         "NYULangone_External"),
    "Penn Medicine":               ("upenn",              "PennMedicine_External"),
    "Duke Health":                 ("dukehealth",         "DukeHealth_External"),
    "Emory Healthcare":            ("emoryhealthcare",    "EmoryHealthcare_External"),
    "Hartford HealthCare":         ("hartfordhealthcare", "HartfordHealthCare_External"),
}

async def scrape_successfactors(session: aiohttp.ClientSession, system: str, org_data: tuple) -> list[Job]:
    subdomain, site_id = org_data
    jobs, start = [], 0
    # SAP SuccessFactors public job search API
    url = f"https://{subdomain}.jobs.com/api/apply/v2/jobs"
    while True:
        try:
            async with session.get(url,
                params={"domain": f"{subdomain}.jobs.com", "start": start, "num": 25},
                headers=HEADERS,
                proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=25)) as r:
                if r.status != 200:
                    logger.info(f"SuccessFactors {system}: HTTP {r.status} at {url}")
                    break
                data = await r.json(content_type=None)
            listings = data.get("jobs", [])
            if not listings: break
            for j in listings:
                city  = j.get("city", "")
                state = j.get("state", "")
                jobs.append(Job(
                    title=j.get("title", ""),
                    hospital_system=system,
                    hospital_name=j.get("location", system),
                    city=city, state=state, location=f"{city}, {state}",
                    specialty=j.get("department", ""),
                    job_type=j.get("type", ""),
                    url=j.get("canonicalPositionUrl", ""),
                    job_id=str(j.get("jobId", "")),
                    posted_date=j.get("postedDate", ""),
                    description=strip_html(j.get("shortDescription", "")),
                    ats_platform="SuccessFactors",
                ))
            start += 25
            if start >= data.get("total", 0): break
            await jitter()
        except Exception as e:
            logger.info(f"SuccessFactors {system}: {e}")
            break
    return jobs

async def run_successfactors(session) -> list[Job]:
    logger.info(f"SuccessFactors: scraping {len(SUCCESSFACTORS_ORGS)} systems...")
    results = await asyncio.gather(
        *[scrape_successfactors(session, s, o) for s, o in SUCCESSFACTORS_ORGS.items()],
        return_exceptions=True
    )
    jobs = [j for r in results if isinstance(r, list) for j in r]
    logger.info(f"  SuccessFactors: {len(jobs):,} jobs")
    return jobs


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
        async with session.get(
            f"https://boards-api.greenhouse.io/v1/boards/{org}/jobs?content=true",
            headers=HEADERS,
            proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=25)) as r:
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
    "DaVita":                      "DaVita",
    "Envision Healthcare":         "EnvisionHealthcare",
    "AmeriHealth Caritas":         "AmeriHealthCaritas",
    "ChenMed":                     "ChenMed",
    "IORA Health":                 "IORAHealth",
    "Alignment Healthcare":        "AlignmentHealthcare",
}

async def scrape_smartrecruiters(session: aiohttp.ClientSession, system: str, org: str) -> list[Job]:
    jobs, offset = [], 0
    while True:
        try:
            async with session.get(
                f"https://api.smartrecruiters.com/v1/companies/{org}/postings",
                params={"limit": 100, "offset": offset},
                headers=HEADERS,
                proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=25)) as r:
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
    "Cityblock Health":            "cityblock-health",
    "Nomi Health":                 "nomi-health",
    "Calibrate":                   "calibrate",
    "Brightside Health":           "brightside",
    "Tempus AI":                   "tempus-ai",
    "Hims & Hers":                 "hims-hers-1",
}

async def scrape_lever(session: aiohttp.ClientSession, system: str, org: str) -> list[Job]:
    try:
        async with session.get(
            f"https://api.lever.co/v0/postings/{org}?mode=json",
            headers=HEADERS,
            proxy=proxies.get(), timeout=aiohttp.ClientTimeout(total=25)) as r:
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
async def run_playwright_scrapers() -> list[Job]:
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        logger.warning("Playwright not installed — skipping custom sites")
        return []

    logger.info("Playwright: scraping JS-heavy custom sites...")
    jobs = []

    CUSTOM_SITES = [
        ("Mayo Clinic",          "https://jobs.mayoclinic.org/search-jobs"),
        ("Cleveland Clinic",     "https://jobs.clevelandclinic.org/search/"),
        ("Mass General Brigham", "https://www.massgeneralbrigham.org/en/careers/search-jobs"),
        ("HCA Healthcare",       "https://careers.hcahealthcare.com/jobs"),
        ("Ascension Health",     "https://ascension.org/careers"),
    ]

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
                async def capture(response):
                    if any(x in response.url for x in ["api/jobs","search-jobs","careers/search","job-search","jobPostings","/jobs?"]):
                        try:
                            d = await response.json()
                            if isinstance(d, dict):
                                captured.extend(d.get("jobs", d.get("jobPostings", d.get("results", []))))
                            elif isinstance(d, list):
                                captured.extend(d)
                        except: pass
                page.on("response", capture)

                await page.goto(url, wait_until="networkidle", timeout=45000)
                await asyncio.sleep(random.uniform(2, 4))
                for _ in range(4):
                    await page.evaluate("window.scrollBy(0, 600)")
                    await asyncio.sleep(0.8)

                for j in captured:
                    title = j.get("title", j.get("jobTitle", j.get("name", "")))
                    loc   = j.get("location", j.get("city", j.get("locationsText", "")))
                    if isinstance(loc, dict):
                        loc = f"{loc.get('city','')}, {loc.get('state','')}"
                    parts = [p.strip() for p in str(loc).split(",")]
                    if title:
                        jobs.append(Job(
                            title=str(title), hospital_system=system_name,
                            hospital_name=system_name,
                            city=parts[0] if parts else "",
                            state=parts[-1] if len(parts) > 1 else "",
                            location=str(loc), specialty="", job_type="",
                            url=str(j.get("url", j.get("applyUrl", url))),
                            job_id=str(j.get("id", j.get("jobId", title + str(loc)))),
                            posted_date=str(j.get("datePosted", j.get("postedOn", "")))[:10],
                            description=strip_html(str(j.get("description", ""))),
                            ats_platform="Custom",
                        ))

                # DOM fallback
                if not [j for j in jobs if j.hospital_system == system_name]:
                    cards = await page.query_selector_all(
                        "[data-job-id],[data-testid='job-card'],.job-card,.job-listing,.search-result-item,li.job"
                    )
                    for card in cards[:200]:
                        try:
                            t = await card.query_selector("h2,h3,.job-title,[data-testid='job-title']")
                            a = await card.query_selector("a")
                            l = await card.query_selector(".location,.job-location,[data-testid='location']")
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
                                    job_id=href.split("/")[-1] if href else title_txt,
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
    connector = aiohttp.TCPConnector(limit=30, ssl=False)

    async with aiohttp.ClientSession(connector=connector, headers=HEADERS) as session:
        ats_results = await asyncio.gather(
            run_workday(session),
            run_taleo(session),
            run_icims(session),
            run_successfactors(session),
            run_greenhouse(session),
            run_smartrecruiters(session),
            run_lever(session),
            run_usajobs(session),
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
