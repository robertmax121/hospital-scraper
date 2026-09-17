"""Coverage lever 4: find each uncovered hospital's careers site and name its ATS.

Input: a JSON list of hospitals ({ccn, name, city, state, type, sys, beds}),
typically the uncovered rows of cms_coverage_detail. For each hospital:

  1. website   : OpenStreetMap (Overpass, one query per state, cached) matched
                 by normalized name + city; Brave search as the fallback.
  2. careers   : the site's careers/jobs/employment pages (links from the
                 homepage plus the usual paths).
  3. ats       : the applicant-tracking system referenced by those pages, with
                 the config tuple our adapters need (Workday tenant/wd/site,
                 iCIMS domain, Oracle base + site, Jibe base, HealthcareSource
                 slug, Greenhouse/Lever/SmartRecruiters org, ...).
  4. count     : a live job count for the platforms with a cheap API, so an
                 entry is only proposed when it returns rows.

Output: data/fingerprints/<stamp>.json (one record per hospital) and
data/fingerprints/<stamp>.md (config lines grouped by platform, plus the
platforms we do not parse yet, by hospital count).

    python fingerprint_hospitals.py uncovered.json --states TX,CA,FL,NY --max 600

Read-only against the web; writes nothing to the database.
"""
from __future__ import annotations

import argparse
import html as _html
import io
import json
import os
import re
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse, unquote

import requests as plain_requests
from curl_cffi import requests

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36"
OVERPASS = "https://overpass-api.de/api/interpreter"
OSM_CACHE = "data/osm_hospitals"
OUT_DIR = "data/fingerprints"
STOP = {"hospital", "hospitals", "medical", "center", "centre", "regional", "memorial", "health", "healthcare",
        "the", "of", "and", "inc", "llc", "lp", "system", "services", "community", "general", "county",
        "university", "campus", "clinic", "at", "for", "a", "an", "&"}
AGGREGATORS = ("indeed", "glassdoor", "linkedin", "facebook", "yelp", "healthgrades", "cms.gov", "medicare.gov",
               "wikipedia", "ziprecruiter", "simplyhired", "google.", "bing.com", "duckduckgo", "yellowpages",
               "mapquest", "usnews", "definitivehc", "ahd.com", "hospitalsafetygrade", "leapfrog", "npidb",
               "npino", "hipaaspace", "zocdoc", "vitals.com", "webmd", "caredash", "opencorporates", "bbb.org",
               "manta", "dnb.com", "zoominfo", "instagram", "twitter", "x.com", "youtube", "tiktok", "monster",
               "careerbuilder", "jobs2careers", "talent.com", "jooble", "snagajob", "zippia", "comparably",
               "brave.com", "apple.com", "microsoft.com", "mozilla", "w3.org", "nursingjobs", "nurse.com",
               "hospitaljobsonline", "healthecareers", "practicelink", "doximity", "beckershospitalreview",
               "openstreetmap", "cdc.gov", "hhs.gov", "medicaid", "ahrq", "jointcommission", "qualitycheck")
CAREER_WORDS = re.compile(r"career|job|employment|work[- ]with[- ]us|join[- ]our[- ]team|opportunit|recruit|hiring", re.I)
CAREER_PATHS = ("/careers", "/careers/", "/jobs", "/jobs/", "/employment", "/careers/job-openings", "/about/careers",
                "/about-us/careers", "/join-our-team", "/work-with-us", "/career-opportunities", "/human-resources")

# ATS fingerprints: name -> (regex over page text + link targets, config builder)
ATS_PATTERNS = [
    ("Workday", re.compile(r"([a-z0-9-]+)\.(wd\d+)\.myworkdayjobs\.com/(?:[a-z]{2}-[A-Za-z]{2}/)?([A-Za-z0-9_-]{2,})", re.I)),
    ("Oracle HCM", re.compile(r"https?://([a-z0-9-]+\.fa\.(?:[a-z0-9]+\.)?oraclecloud\.com)(?:/hcmUI/CandidateExperience/[a-z-]+/sites/(CX_[A-Za-z0-9]+))?", re.I)),
    ("iCIMS", re.compile(r"https?://([a-z0-9-]+\.icims\.com)", re.I)),
    ("HealthcareSource", re.compile(r"pm\.healthcaresource\.com/cs/([a-z0-9_-]+)", re.I)),
    ("UKG", re.compile(r"recruiting\d?\.ultipro\.com/([A-Za-z0-9]+)/JobBoard/([0-9a-f-]{36})", re.I)),
    ("ADP", re.compile(r"workforcenow\.adp\.com/[^\"'\s]*?cid=([0-9a-f-]{36})", re.I)),
    ("Paycom", re.compile(r"paycomonline\.net/v4/ats/web\.php/jobs\?clientkey=([0-9A-Fa-f]+)", re.I)),
    ("Paylocity", re.compile(r"recruiting\.paylocity\.com/recruiting/jobs/All/([0-9a-f-]{36})/([A-Za-z0-9-]+)", re.I)),
    ("Greenhouse", re.compile(r"boards\.greenhouse\.io/([a-z0-9-]+)", re.I)),
    ("Lever", re.compile(r"jobs\.lever\.co/([a-z0-9-]+)", re.I)),
    ("SmartRecruiters", re.compile(r"(?:jobs|careers)\.smartrecruiters\.com/([A-Za-z0-9-]+)", re.I)),
    ("CSOD", re.compile(r"https?://([a-z0-9-]+)\.csod\.com", re.I)),
    ("ApplicantPro", re.compile(r"https?://([a-z0-9-]+)\.applicantpro\.com", re.I)),
    ("NeoGov", re.compile(r"governmentjobs\.com/careers/([a-z0-9-]+)", re.I)),
    ("TAM", re.compile(r"theapplicantmanager\.com/careers\?co=([a-z0-9]+)", re.I)),
    ("Workable", re.compile(r"apply\.workable\.com/([a-z0-9-]+)", re.I)),
    ("Ashby", re.compile(r"jobs\.ashbyhq\.com/([a-z0-9-]+)", re.I)),
    ("Paycor", re.compile(r"recruitingbypaycor\.com/career/CareerHome\.action\?clientId=([0-9a-f-]{36})", re.I)),
    ("Kronos", re.compile(r"https?://([a-z0-9-]+\.[a-z0-9-]+\.mykronos\.com)", re.I)),
    ("Phenom", re.compile(r"phenompeople\.com|phenompro\.com", re.I)),
    ("TalentBrew", re.compile(r"tbcdn\.talentbrew\.com|talentbrew\.com", re.I)),
    ("Jobvite", re.compile(r"jobs\.jobvite\.com/([a-z0-9-]+)", re.I)),
    ("SuccessFactors", re.compile(r"career\d*\.successfactors\.com", re.I)),
    ("Taleo", re.compile(r"https?://([a-z0-9-]+)\.taleo\.net", re.I)),
    ("Avature", re.compile(r"https?://([a-z0-9-]+)\.avature\.net", re.I)),
    ("JazzHR", re.compile(r"https?://([a-z0-9-]+)\.applytojob\.com", re.I)),
    ("BambooHR", re.compile(r"https?://([a-z0-9-]+)\.bamboohr\.com/careers", re.I)),
    ("Dayforce", re.compile(r"jobs\.dayforcehcm\.com/[a-z-]+/([a-z0-9-]+)", re.I)),
    ("Infor", re.compile(r"(css-[a-z0-9-]+-prd)\.inforcloudsuite\.com", re.I)),
    ("Symplr", re.compile(r"symplr\.com|hctsportal|hctslive", re.I)),
    ("Breezy", re.compile(r"https?://([a-z0-9-]+)\.breezy\.hr", re.I)),
    ("Recruitee", re.compile(r"https?://([a-z0-9-]+)\.recruitee\.com", re.I)),
    ("Hirebridge", re.compile(r"hirebridge\.com", re.I)),
    ("PeopleAdmin", re.compile(r"peopleadmin\.com|hr\.[a-z0-9-]+\.edu/postings", re.I)),
    ("Frontline/AppliTrack", re.compile(r"applitrack\.com", re.I)),
    ("iSolved", re.compile(r"isolvedhire\.com|myisolved\.com", re.I)),
    ("Jibe", re.compile(r"data-jibe|jibeapply\.com", re.I)),
]
SUPPORTED = {"Workday", "Oracle HCM", "iCIMS", "HealthcareSource", "UKG", "ADP", "Paycom", "Paylocity", "Greenhouse",
             "Lever", "SmartRecruiters", "CSOD", "ApplicantPro", "NeoGov", "TAM", "Workable", "Ashby", "Paycor",
             "Kronos", "Phenom", "TalentBrew", "Jibe"}


def log(msg: str) -> None:
    print(f"{datetime.now().strftime('%H:%M:%S')} {msg}", flush=True)


def norm(s: str) -> str:
    s = (s or "").lower().replace("&", " and ").replace("'", "")
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    return " ".join(s.split())


def toks(s: str) -> set:
    return {t for t in norm(s).split() if t not in STOP and len(t) > 1}


def jaccard(a: set, b: set) -> float:
    return len(a & b) / len(a | b) if a and b else 0.0


def host_of(u: str) -> str:
    try:
        return (urlparse(u).hostname or "").lower()
    except Exception:
        return ""


# ── 1. websites ─────────────────────────────────────────────────────────────
def osm_hospitals(state: str) -> list[dict]:
    os.makedirs(OSM_CACHE, exist_ok=True)
    path = f"{OSM_CACHE}/{state}.json"
    if os.path.exists(path) and time.time() - os.path.getmtime(path) < 30 * 86400:
        return json.load(io.open(path, encoding="utf-8"))
    ql = (f'[out:json][timeout:90];area["ISO3166-2"="US-{state}"]->.a;'
          f'(nwr["amenity"="hospital"](area.a);nwr["healthcare"="hospital"](area.a););out tags center 5000;')
    for attempt in range(3):
        try:
            r = plain_requests.get(OVERPASS, params={"data": ql}, timeout=150,
                                   headers={"User-Agent": "waypoint-coverage/1.0 (robert@waypointrecruit.com)"})
            if r.status_code == 200:
                els = r.json().get("elements", [])
                out = [{"name": e["tags"].get("name", ""), "website": e["tags"].get("website") or e["tags"].get("contact:website") or "",
                        "city": e["tags"].get("addr:city", "")} for e in els if e.get("tags", {}).get("name")]
                json.dump(out, io.open(path, "w", encoding="utf-8"))
                log(f"OSM {state}: {len(out)} named hospitals, {sum(1 for o in out if o['website'])} with a website")
                return out
            log(f"OSM {state}: HTTP {r.status_code}, retrying")
        except Exception as e:
            log(f"OSM {state}: {e}")
        time.sleep(15 * (attempt + 1))
    return []


def match_osm(h: dict, osm: list[dict]) -> str:
    ht, hc = toks(h["name"]), norm(h["city"])
    best, best_score = "", 0.0
    for o in osm:
        if not o["website"]:
            continue
        ot = toks(o["name"])
        j = jaccard(ht, ot)
        same_city = hc and norm(o["city"]) == hc
        exact = norm(h["name"]) == norm(o["name"])
        score = j + (0.35 if same_city else 0) + (0.5 if exact else 0)
        ok = exact or (j >= 0.5 and same_city) or j >= 0.8
        if ok and score > best_score:
            best, best_score = o["website"], score
    return best


def brave_site(h: dict) -> str:
    q = f"{h['name'].title()} {h['city']} {h['state']}"
    try:
        r = requests.get("https://search.brave.com/search", params={"q": q, "source": "web"},
                         impersonate="chrome", timeout=30)
    except Exception as e:
        log(f"brave {h['name'][:30]}: {e}")
        return ""
    links = re.findall(r'href="(https?://[^"]+)"', r.text)
    name_toks = toks(h["name"])
    seen, cands = set(), []
    for l in links:
        hst = host_of(l)
        if not hst or hst in seen or any(b in l.lower() for b in AGGREGATORS):
            continue
        seen.add(hst)
        cands.append(l)
    if not cands:
        return ""
    # prefer a domain that carries a name token
    for l in cands[:8]:
        if any(t in host_of(l).replace("-", "") for t in name_toks if len(t) >= 4):
            return f"{urlparse(l).scheme}://{host_of(l)}/"
    return f"{urlparse(cands[0]).scheme}://{host_of(cands[0])}/"


# ── 2 + 3. careers pages and ATS ────────────────────────────────────────────
def fetch(u: str) -> str:
    try:
        r = requests.get(u, impersonate="chrome", timeout=25, headers={"User-Agent": UA})
        if r.status_code == 200 and "text/html" in r.headers.get("content-type", "") and len(r.text) > 500:
            return r.text
        if r.status_code == 200 and len(r.text) > 500:
            return r.text
    except Exception:
        pass
    return ""


def careers_pages(site: str) -> tuple[list[str], str]:
    """(fetched career page URLs, concatenated HTML of home + career pages)."""
    home = fetch(site)
    texts = [home]
    links = []
    if home:
        for m in re.finditer(r'<a[^>]+href="([^"#]+)"[^>]*>(.*?)</a>', home, re.I | re.DOTALL):
            href, text = m.group(1), re.sub(r"<[^>]+>", " ", m.group(2))
            if CAREER_WORDS.search(href) or CAREER_WORDS.search(text):
                links.append(urljoin(site, _html.unescape(href)))
    seen, cands = set(), []
    for l in links + [urljoin(site, p) for p in CAREER_PATHS]:
        if l in seen or host_of(l) in ("", "facebook.com", "www.facebook.com"):
            continue
        seen.add(l)
        cands.append(l)
    fetched = []
    for l in cands[:6]:
        t = fetch(l)
        if t:
            fetched.append(l)
            texts.append(t)
            # a careers page that immediately hands off to an ATS domain
            m = re.search(r'(?:http-equiv="refresh"[^>]+url=|window\.location(?:\.href)?\s*=\s*["\'])([^"\'>]+)', t, re.I)
            if m:
                target = urljoin(l, _html.unescape(m.group(1)))
                if host_of(target) != host_of(l):
                    tt = fetch(target)
                    if tt:
                        fetched.append(target)
                        texts.append(tt)
        if len(fetched) >= 3:
            break
    return fetched, "\n".join(texts) + "\n" + "\n".join(cands)


def detect_ats(blob: str, pages: list[str]) -> list[tuple[str, tuple]]:
    found = []
    for name, rx in ATS_PATTERNS:
        for m in rx.finditer(blob):
            groups = tuple(g for g in m.groups() if g) if m.groups() else ()
            if name == "Workday" and groups and groups[-1].lower() in ("en-us", "en-gb", "fr-ca", "es"):
                continue
            key = (name, groups)
            if key not in found:
                found.append(key)
            if len(found) > 12:
                break
    if any(n == "Jibe" for n, _ in found) or any(n == "Phenom" for n, _ in found) or any(n == "TalentBrew" for n, _ in found):
        # these fronts live on the careers host itself
        for n in ("Jibe", "Phenom", "TalentBrew"):
            hosts = sorted({host_of(p) for p in pages if host_of(p)})
            if any(x == n for x, _ in found) and hosts:
                found = [(x, g if x != n else (hosts[0],)) for x, g in found]
    return found


# ── 4. live counts for the cheap APIs ───────────────────────────────────────
def count_for(ats: str, cfg: tuple) -> tuple[int | None, tuple]:
    H = {"User-Agent": UA, "Accept": "application/json"}
    try:
        if ats == "Workday" and len(cfg) == 3:
            tenant, wd, site = cfg
            r = plain_requests.post(f"https://{tenant}.{wd}.myworkdayjobs.com/wday/cxs/{tenant}/{site}/jobs",
                                    json={"appliedFacets": {}, "limit": 1, "offset": 0, "searchText": ""},
                                    headers={**H, "Content-Type": "application/json"}, timeout=25)
            return (r.json().get("total") if r.status_code == 200 else None), cfg
        if ats == "Oracle HCM":
            base = f"https://{cfg[0]}"
            sites = [cfg[1]] if len(cfg) > 1 else []
            sites += [s for s in ("CX_1", "CX_2", "CX_3", "CX_1001", "CX_1002", "CX_2001") if s not in sites]
            for site in sites:
                r = plain_requests.get(f"{base}/hcmRestApi/resources/latest/recruitingCEJobRequisitions",
                                       params={"onlyData": "true", "finder": f"findReqs;siteNumber={site},limit=1,offset=0"},
                                       headers=H, timeout=25)
                if r.status_code == 200:
                    tot = r.json()["items"][0].get("TotalJobsCount")
                    if tot:
                        return tot, (base, site)
            return None, (base,)
        if ats == "Jibe":
            r = requests.get(f"https://{cfg[0]}/api/jobs", params={"page": "1", "limit": "1"}, impersonate="chrome",
                             timeout=25, headers={"Accept": "application/json"})
            if "json" in r.headers.get("content-type", ""):
                return r.json().get("totalCount"), (f"https://{cfg[0]}",)
            return None, cfg
        if ats == "iCIMS":
            dom = cfg[0]
            r = requests.get(f"https://{dom}/jobs/search", params={"ss": "1", "searchKeyword": "", "mode": "json", "iis": "Job+Board", "in_iframe": "1", "p_startrow": 0},
                             impersonate="chrome", timeout=25)
            ct = r.headers.get("content-type", "")
            if "json" in ct:
                d = r.json()
                return len(d.get("jobs", d.get("searchResults", []))), cfg
            if "iCIMS_JobCardItem" in r.text:
                return r.text.count("iCIMS_JobCardItem"), cfg
            if 'data-id="' in r.text:
                return r.text.count('data-id="'), cfg
            return 0, cfg
        if ats == "Greenhouse":
            r = plain_requests.get(f"https://boards-api.greenhouse.io/v1/boards/{cfg[0]}/jobs", headers=H, timeout=25)
            return (len(r.json().get("jobs", [])) if r.status_code == 200 else None), cfg
        if ats == "Lever":
            r = plain_requests.get(f"https://api.lever.co/v0/postings/{cfg[0]}?mode=json", headers=H, timeout=25)
            return (len(r.json()) if r.status_code == 200 else None), cfg
        if ats == "SmartRecruiters":
            r = plain_requests.get(f"https://api.smartrecruiters.com/v1/companies/{cfg[0]}/postings", headers=H, timeout=25)
            return (r.json().get("totalFound") if r.status_code == 200 else None), cfg
    except Exception:
        return None, cfg
    return None, cfg


# ── driver ──────────────────────────────────────────────────────────────────
def process(h: dict, osm: list[dict], brave_budget: list) -> dict:
    rec = {**h, "website": "", "source": "", "careers": [], "ats": [], "best": None}
    site = match_osm(h, osm)
    if site:
        rec["source"] = "osm"
    elif brave_budget[0] > 0:
        brave_budget[0] -= 1
        site = brave_site(h)
        rec["source"] = "brave" if site else ""
        time.sleep(2.5)
    if not site:
        return rec
    if not site.startswith("http"):
        site = "https://" + site
    # OSM often stores a deep page ("/locations/lumberton"); start at the root.
    site = f"{urlparse(site).scheme}://{host_of(site)}/"
    rec["website"] = site
    pages, blob = careers_pages(site)
    rec["careers"] = pages
    found = detect_ats(blob, pages)
    results = []
    for ats, cfg in found:
        n, cfg2 = (count_for(ats, cfg) if ats in ("Workday", "Oracle HCM", "Jibe", "iCIMS", "Greenhouse", "Lever", "SmartRecruiters") else (None, cfg))
        results.append({"ats": ats, "config": list(cfg2), "count": n, "supported": ats in SUPPORTED})
    rec["ats"] = results
    ranked = sorted(results, key=lambda x: (x["supported"], (x["count"] or 0) > 0, x["count"] or 0), reverse=True)
    rec["best"] = ranked[0] if ranked else None
    return rec


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("hospitals", help="JSON list of uncovered hospitals")
    ap.add_argument("--states", default="", help="comma-separated state codes, in priority order")
    ap.add_argument("--max", type=int, default=400)
    ap.add_argument("--brave", type=int, default=250, help="max Brave searches this run")
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--only-independent", action="store_true", help="skip hospitals that AHRQ assigns to a system")
    args = ap.parse_args()
    hospitals = json.load(io.open(args.hospitals, encoding="utf-8"))
    states = [s.strip().upper() for s in args.states.split(",") if s.strip()]
    if states:
        order = {s: i for i, s in enumerate(states)}
        hospitals = sorted([h for h in hospitals if h["state"] in order], key=lambda h: order[h["state"]])
    if args.only_independent:
        hospitals = [h for h in hospitals if not h.get("sys") or norm(h["sys"]) == norm(h["name"])]
    hospitals = hospitals[: args.max]
    log(f"{len(hospitals)} hospitals to fingerprint")
    osm_by_state = {s: osm_hospitals(s) for s in sorted({h["state"] for h in hospitals})}
    brave_budget = [args.brave]
    # OSM matching is local and fast; the site fetches run in threads, Brave stays sequential.
    with_site, without = [], []
    for h in hospitals:
        (with_site if match_osm(h, osm_by_state[h["state"]]) else without).append(h)
    log(f"OSM matched {len(with_site)}; {len(without)} go to Brave (budget {args.brave})")
    records = []
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        for rec in ex.map(lambda h: process(h, osm_by_state[h["state"]], [0]), with_site):
            records.append(rec)
            if len(records) % 25 == 0:
                log(f"  {len(records)} done")
    for h in without:
        records.append(process(h, [], brave_budget))
        if brave_budget[0] <= 0:
            log("Brave budget exhausted")
            break
    os.makedirs(OUT_DIR, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M")
    json.dump(records, io.open(f"{OUT_DIR}/{stamp}.json", "w", encoding="utf-8"), indent=1)
    # summary
    by_ats = Counter()
    lines = [f"# Hospital fingerprints {stamp}", "", f"{len(records)} hospitals; website found for {sum(1 for r in records if r['website'])}; "
             f"ATS named for {sum(1 for r in records if r['best'])}; supported with rows for "
             f"{sum(1 for r in records if r['best'] and r['best']['supported'] and (r['best']['count'] or 0) > 0)}", ""]
    groups: dict[str, list[str]] = {}
    for r in records:
        b = r["best"]
        if not b:
            continue
        by_ats[b["ats"]] += 1
        line = f"- {r['name'].title()} ({r['city']}, {r['state']}; ccn {r['ccn']}): {b['config']} count={b['count']} via {r['website']}"
        groups.setdefault(b["ats"], []).append(line)
    lines.append("## ATS tally")
    lines += [f"- {a}: {n}{'' if a in SUPPORTED else '  (no adapter)'}" for a, n in by_ats.most_common()]
    for a, ls in sorted(groups.items(), key=lambda kv: -len(kv[1])):
        lines += ["", f"## {a}{'' if a in SUPPORTED else ' (no adapter)'}"] + ls
    no_site = [r for r in records if not r["website"]]
    no_ats = [r for r in records if r["website"] and not r["best"]]
    lines += ["", f"## No website found ({len(no_site)})"] + [f"- {r['name'].title()} ({r['city']}, {r['state']})" for r in no_site[:200]]
    lines += ["", f"## Website but no ATS recognised ({len(no_ats)})"] + [f"- {r['name'].title()} ({r['city']}, {r['state']}): {r['website']} pages={len(r['careers'])}" for r in no_ats[:200]]
    io.open(f"{OUT_DIR}/{stamp}.md", "w", encoding="utf-8").write("\n".join(lines))
    log(f"wrote {OUT_DIR}/{stamp}.json and .md | ATS tally: {by_ats.most_common(12)}")


if __name__ == "__main__":
    main()
