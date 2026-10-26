"""Coverage lever 4, v2 (2026-10-25): find each uncovered hospital's careers
board, name its platform and tenant, and prove the tenant lists THAT
hospital's jobs before anything becomes a config line.

Why v2: the 2026-09-17 pass (v1) was about 19% junk. Its failure modes, all
fixed here:
  * wrong website: OSM's website tag was matched by name alone and trimmed to
    the domain root, so a system's or a county's site stood in for the
    hospital (San Antonio Behavioral -> christushealth.org, Community
    Memorial Ventura -> the county's NEOGOV board); Brave's first non-
    aggregator result was taken unverified. v2 verifies every candidate site
    against the CMS record: the hospital's phone number, its street number +
    street name, or its ZIP plus its name tokens must appear on the page.
  * vendor and partner boards: any ATS link anywhere on the pages counted, so
    a revenue-cycle vendor (R1 RCM Workday on Big Bend's site), a staffing
    partner or the parent system's board became the hospital's tenant. v2
    keeps where each link was seen (careers page anchor vs. homepage footer)
    and, above all, checks the tenant's own job list: a tenant only counts
    when its postings sit in the hospital's town or the board's company name
    is the hospital's.
  * generic hosts: "prd01-hcm01.prd.mykronos.com" or "paylocity.com" with no
    company id were reported as tenants. v2 only reports a tenant when the
    company key is present (Kronos company id, Paylocity guid, Paycom client
    key, UKG code + board guid, ...).
  * no proof of jobs: v1 counted only seven platforms and never checked
    locations. v2 has a live list check for every platform with a public
    list endpoint (counts and the postings' towns).

Pipeline per hospital (read-only on the web, nothing written to any database):
  1. website   : candidates from OSM (phone match first, then name + town),
                 Wikidata (name + town), domain guesses (DNS-checked), and a
                 DuckDuckGo HTML search as the budgeted fallback; the first
                 candidate that passes verify_site() wins.
  2. careers   : careers/jobs/employment links from the homepage (anchor text
                 and path), the usual paths when none, then one hop to a
                 "view openings" link.
  3. platform  : ATS_PATTERNS over the careers pages' HTML and links, with the
                 tenant key each adapter needs.
  4. verify    : VERIFIERS[platform](tenant) -> job count, postings' towns,
                 board company name; score_tenant() turns that into a
                 confidence (high / medium / low).

    python fingerprint_hospitals.py targets.json --out data/fingerprints/fp.json \
        --osm-dir data/osm_hospitals --wikidata wikidata.json --workers 10 --search 400

Politeness: about one request per second per host (Pacer), a normal browser
user agent, no logins, no browser automation.
"""
from __future__ import annotations

import argparse
import html as _html
import io
import json
import os
import re
import socket
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from urllib.parse import parse_qs, unquote, urljoin, urlparse

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/124.0.0.0 Safari/537.36")

STOP = {"hospital", "hospitals", "medical", "center", "centre", "regional", "memorial", "health", "healthcare",
        "the", "of", "and", "inc", "llc", "lp", "system", "services", "community", "general", "county", "campus",
        "clinic", "clinics", "at", "for", "a", "an", "district", "care", "cah", "corp", "corporation", "co",
        "authority", "hosp", "med", "ctr", "mem", "reg", "hlth", "svcs", "facility", "institute", "behavioral",
        "psychiatric", "rehabilitation", "rehab", "specialty", "surgical", "acute", "critical", "access", "dba",
        "saint", "st", "new", "dept", "department", "s", "hd", "rmc", "mc", "ltd", "pllc", "pc", "rural",
        "emergency", "area", "valley", "city"}

AGGREGATORS = ("indeed.", "glassdoor.", "linkedin.", "facebook.", "yelp.", "healthgrades.", "ziprecruiter.",
               "simplyhired.", "monster.", "careerbuilder.", "talent.com", "jooble.", "snagajob.", "zippia.",
               "comparably.", "wikipedia.", "usnews.", "nurse.com", "vivian.com", "salary.com", "payscale.",
               "builtin.", "joblist.", "lensa.", "jobleads.", "careerjet.", "adzuna.", "jora.", "hospitalcareers.",
               "healthecareers.", "practicelink.", "doximity.", "beckershospitalreview.", "mapquest.", "bbb.org",
               "instagram.", "twitter.", "x.com", "youtube.", "tiktok.", "medicare.gov", "cms.gov", "npino",
               "npidb", "zoominfo", "dnb.com", "manta.", "opencorporates", "ahd.com", "leapfrog",
               "hospitalsafetygrade", "vitals.com", "webmd.", "zocdoc.", "duckduckgo.", "bing.com", "google.",
               "ladders.", "dice.com", "recruit.net", "whatjobs", "ihiremedical", "nexxt.", "jobcase.",
               "careerarc", "hiringcafe", "getwork", "jobright", "reddit.", "mapcarta", "pinterest", "niche.com",
               "chamberofcommerce", "birdeye", "superpages", "yellowpages", "apple.com", "microsoft.com",
               "definitivehc", "hipaaspace", "caredash", "carelistings", "medicarelist", "hospitalview",
               "amazon.", "ebay.", "wikidata.", "openstreetmap.", "usajobs.gov", "hhs.gov", "cdc.gov",
               "americanhospitaldirectory", "hospital-data", "allhospitals", "ushospitalfinder", "zillow.",
               "realtor.", "tripadvisor.", "waze.", "foursquare.", "nextdoor.", "newsbreak.", "patch.com",
               "giveffect", "guidestar", "causeiq", "propublica", "charitynavigator", "bizapedia", "buzzfile",
               "rocketreach", "signalhire", "crunchbase", "pitchbook", "owler", "dandb", "medicaid.gov",
               "hrsa.gov", "ruralhealthinfo", "flexmonitoring", "aha.org", "jointcommission", "qualitycheck",
               "ratemds", "sharecare", "castleconnolly", "md.com", "wellness.com", "hospitalpricing",
               "turquoise.health", "clearhealthcosts", "fairhealth", "goodrx", "medicalnewstoday", "healthline",
               "hospitalsineachstate", "hospitallists", "medicalcenters.", "carecompare", "health.usnews",
               "findhospital", "hospitalfinder", "nursinghomes", "countyoffice", "allbiz", "cylex", "hotfrog",
               "brownbook", "merchantcircle", "showmelocal", "ezlocal", "mapquest", "local.yahoo", "city-data",
               "uscity.net", "usa-people-search", "medicaredirectory", "hospital.directory", "healthcarefacilities",
               "medicalfacilities", "doctor.com", "docspot", "wellness.com", "providerdata", "opennpi", "npiprofile",
               "npilookup", "hipaa", "nppes", "medicare.org", "eldercare", "seniorly", "caring.com", "aplaceformom",
               "healthcare4ppl", "hospitalprices", "medicalproviders", "healthgrove", "hospitalinspections",
               "data.medicare", "kff.org", "hospitalreviews", "carelist", "medicinenet", "everyhospital",
               "hospitalsandclinics", "hospitalnavigator", "healthcarecompass", "hospitalcompare",
               "localoffices", "emergencyroomnearme", "hospitalsnearme", "officialusa", "chamberofcommerce",
               "allbusiness", "bizapedia", "opengovus", "govtribe", "healthcarefacilitiesus", "hospitalcaredata",
               "safehospitalsusa", "hospitalpedia", "healthcaredive", "fiercehealthcare", "modernhealthcare",
               "hospitaldatacenter", "ourhealthnetwork", "hospitalinfo", "hospitaldirectory", "findahospital",
               "medicalcenterlist", "careratings", "hospitalratings")
_FACILITY_WORDS = re.compile(r"(?i)\b(hospital|medical center|health center|regional medical|clinic)\b")
_DIRECTORY_TITLE = re.compile(r"\b(directory|reviews?|ratings?|near me|list of|hospitals in|compare|top \d+|npi\b|"
                              r"profile|phone number|address|hours|locations? near|map|wiki)\b", re.I)

SOCIAL = ("facebook.", "twitter.", "instagram.", "linkedin.", "youtube.", "tiktok.", "x.com", "pinterest.")


def log(msg: str) -> None:
    print(f"{datetime.now().strftime('%H:%M:%S')} {msg}", flush=True)


# -- text helpers ---------------------------------------------------------------
def norm(s: str) -> str:
    s = (s or "").lower().replace("&", " and ").replace("'", "").replace("’", "")
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    return " ".join(s.split())


def core_tokens(name: str) -> list[str]:
    return [t for t in norm(name).split() if t not in STOP and len(t) >= 3]


def distinct_tokens(h: dict) -> list[str]:
    """The hospital's name tokens that are not its town's words (may be empty)."""
    town = set(norm(h.get("city", "")).split())
    return [t for t in core_tokens(h.get("name", "")) if t not in town]


def host_of(u: str) -> str:
    try:
        return (urlparse(u).hostname or "").lower()
    except Exception:
        return ""


def reg_domain(h: str) -> str:
    parts = (h or "").lower().split(".")
    if len(parts) >= 3 and parts[-2] in ("co", "com", "org", "net", "gov", "k12") and len(parts[-1]) == 2:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


def phone10(s: str) -> str:
    d = re.sub(r"\D", "", s or "")
    return d[-10:] if len(d) >= 10 else ""


PHONE_RX = re.compile(r"(?<![\d.])\(?([2-9]\d{2})\)?[\s.\-– ]{0,3}([2-9]\d{2})[\s.\-– ]{0,3}(\d{4})(?!\d)")


def page_phones(html: str) -> set:
    out = {"".join(m.groups()) for m in PHONE_RX.finditer((html or "")[:MAX_PAGE])}
    out |= {phone10(m) for m in re.findall(r"tel:\+?1?([\d\-\.\s()]{10,20})", html or "", re.I)}
    out.discard("")
    return out


MAX_PAGE = 1_500_000      # characters of a page that are analysed (big pages are mostly inline scripts)
_BLOCK_TAG = re.compile(r"<(/?)(script|style|noscript|svg)\b[^>]*>", re.I)


def _strip_blocks(html: str) -> str:
    """Drop script/style/noscript/svg blocks and comments in one linear pass
    (a lazy `.*?</\\1>` regex goes quadratic on pages with unclosed blocks)."""
    out, pos, skip = [], 0, None
    for m in _BLOCK_TAG.finditer(html):
        close, tag = m.group(1), m.group(2).lower()
        if skip is None and not close:
            out.append(html[pos:m.start()])
            skip = tag
        elif skip is not None and close and tag == skip:
            pos, skip = m.end(), None
    if skip is None:
        out.append(html[pos:])
    t = "".join(out)
    res, i = [], 0
    while True:
        j = t.find("<!--", i)
        if j < 0:
            res.append(t[i:])
            break
        res.append(t[i:j])
        k = t.find("-->", j + 4)
        if k < 0:
            break
        i = k + 3
    return " ".join(res)


def visible_text(html: str) -> str:
    t = _strip_blocks((html or "")[:MAX_PAGE])
    t = re.sub(r"<[^>]{0,2000}>", " ", t)
    return " ".join(_html.unescape(t).split())


def title_of(html: str) -> str:
    m = re.search(r"(?is)<title[^>]{0,200}>(.{0,600}?)</title>", (html or "")[:MAX_PAGE])
    return " ".join(_html.unescape(m.group(1)).split()) if m else ""


_ORD = {"first": "1st", "second": "2nd", "third": "3rd", "fourth": "4th", "fifth": "5th"}
_STREET_SKIP = {"n", "s", "e", "w", "ne", "nw", "se", "sw", "north", "south", "east", "west", "state", "us",
                "highway", "hwy", "hiway", "route", "rt", "rte", "fm", "farm", "road", "rd", "county", "cr",
                "street", "st", "avenue", "ave", "box", "po", "p", "o", "drive", "dr", "boulevard", "blvd", "the",
                "old", "loop", "parkway", "pkwy", "lane", "ln", "suite", "ste", "sh", "i", "interstate"}


def street_parts(addr: str) -> tuple:
    """('2100', 'piper') from '2100 PIPER STREET'; ('', '') when unusable."""
    toks = norm(addr).split()
    if not toks or not re.fullmatch(r"\d{1,6}[a-z]?", toks[0]):
        return "", ""
    num = re.sub(r"[a-z]$", "", toks[0])
    for t in toks[1:]:
        t = _ORD.get(t, t)
        if re.fullmatch(r"\d+", t):
            return num, t        # 'US Highway 59' -> '59'
        if t in _STREET_SKIP or (len(t) < 3 and not re.fullmatch(r"\d+(st|nd|rd|th)", t)):
            continue
        return num, t
    return num, ""


# -- polite HTTP ----------------------------------------------------------------
class Pacer:
    """At most one request per `gap` seconds per host, across threads."""

    def __init__(self, gap: float = 1.05, per_host: dict | None = None):
        self.gap = gap
        self.per_host = per_host or {}
        self.lock = threading.Lock()
        self.next: dict = {}

    def wait(self, host: str) -> None:
        g = self.per_host.get(host, self.gap)
        with self.lock:
            now = time.time()
            t = max(now, self.next.get(host, 0.0))
            self.next[host] = t + g
        if t > now:
            time.sleep(t - now)


PACE = Pacer(1.05, {"recruiting.paylocity.com": 2.5, "www.paycomonline.net": 1.5, "html.duckduckgo.com": 12.0})
_TL = threading.local()


def _session():
    s = getattr(_TL, "s", None)
    if s is None:
        from curl_cffi import requests as creq
        s = creq.Session(impersonate="chrome")
        _TL.s = s
    return s


def http(url: str, method: str = "GET", timeout: int = 25, **kw):
    """(status, final_url, text, content_type); status None on a network error."""
    PACE.wait(host_of(url))
    # The impersonated browser's own User-Agent: overriding it while keeping
    # Chrome's TLS fingerprint gets HTTP/2 resets from common hospital WAFs.
    headers = kw.pop("headers", {})
    try:
        r = _session().request(method, url, headers=headers, timeout=timeout, allow_redirects=True, **kw)
        raw = r.content or b""
        text = r.text if len(raw) < 4_000_000 else raw[:4_000_000].decode("utf-8", "replace")
        return r.status_code, str(r.url), text, r.headers.get("content-type", "")
    except Exception as e:  # noqa: BLE001 - any network failure is just "no page"
        return None, url, "", str(e)[:120]


def http_json(url: str, method: str = "GET", **kw):
    st, final, text, ct = http(url, method, headers={"Accept": "application/json, text/plain, */*",
                                                       **kw.pop("headers", {})}, **kw)
    if st != 200:
        return st, None
    try:
        return st, json.loads(text)
    except Exception:
        return st, None


def dns_ok(host: str) -> bool:
    try:
        socket.getaddrinfo(host, 443)
        return True
    except Exception:
        return False


# -- 1. websites ----------------------------------------------------------------
def verify_site(h: dict, url: str, html: str | None = None, final: str | None = None) -> dict | None:
    """Evidence that `url` is this hospital's own site. None when unreachable."""
    if html is None:
        st, final, html, _ = http(url)
        if st != 200 or len(html) < 300:
            return None
    final = final or url
    text = visible_text(html)
    nt = norm(text)
    ev = {"url": url, "final": final}
    ph = phone10(h.get("phone", ""))
    ev["phone"] = bool(ph) and ph in page_phones(html)
    num, street = street_parts(h.get("address", ""))
    ev["street"] = bool(num and street) and bool(re.search(rf"\b{num}\b(?:\s+\S+){{0,4}}?\s+{re.escape(street)}\b", nt))
    z = (h.get("zip") or "")[:5]
    ev["zip"] = bool(z) and re.search(rf"\b{z}\b", text) is not None
    city = norm(h.get("city", ""))
    ev["city"] = bool(city) and f" {city} " in f" {nt} "
    # name tokens that are not the town: "GREAT FALLS CLINIC HOSPITAL" has none, so any Great Falls
    # page would otherwise "name" it (a guessed greatfallshospital.com redirected to benefis.org)
    toks = distinct_tokens(h)
    tit = norm(title_of(html) + " " + host_of(final).replace("-", "").replace(".", " "))
    hostcat = host_of(final).replace("-", "")
    ev["name_title"] = round(sum(1 for t in toks if f" {t} " in f" {tit} " or t in hostcat) / len(toks), 2) if toks else 0.0
    ev["name_text"] = round(sum(1 for t in toks if f" {t} " in f" {nt} ") / len(toks), 2) if toks else 0.0
    ev["score"] = (3 * ev["phone"] + 3 * ev["street"] + ev["zip"] + ev["city"] + 2 * (ev["name_title"] >= 0.5)
                   + (ev["name_text"] >= 0.75))
    ev["ok"] = bool(ev["phone"] or ev["street"] or (ev["zip"] and ev["name_text"] >= 0.5)
                    or (ev["city"] and ev["name_title"] >= 0.99 and ev["name_text"] >= 0.99))
    # directory pages carry the hospital's phone and street too: they are titled like a listing,
    # or link to many other hospitals on their own domain
    others = sum(1 for u, txt in anchors(html, final)
                 if _FACILITY_WORDS.search(txt) and reg_domain(host_of(u)) == reg_domain(host_of(final)))
    ev["directory"] = (bool(_DIRECTORY_TITLE.search(title_of(html))) and len(page_phones(html)) >= 6) or others >= 8
    ev["contact"] = ""
    if not ev["ok"]:
        for u, txt in anchors(html, final):
            if re.search(r"\b(contact|location|directions|find us|about us)\b", txt, re.I)                     and reg_domain(host_of(u)) == reg_domain(host_of(final)) and u.rstrip("/") != final.rstrip("/"):
                ev["contact"] = u
                break
    return ev


def _jac(a: set, b: set) -> float:
    return len(a & b) / len(a | b) if a and b else 0.0


def osm_candidates(h: dict, osm: list) -> list:
    out = []
    ph = phone10(h.get("phone", ""))
    ht, hc, hz = set(core_tokens(h["name"])), norm(h.get("city", "")), (h.get("zip") or "")[:5]
    scored = []
    for o in osm:
        w = o.get("website") or ""
        if not w:
            continue
        if ph and phone10(o.get("phone", "")) == ph:
            out.append((w, "osm-phone"))
            continue
        names = [o.get("name", "")] + list(o.get("alt") or [])
        j = max(_jac(ht, set(core_tokens(n))) for n in names)
        same = bool((hc and norm(o.get("city", "")) == hc) or (hz and (o.get("zip") or "")[:5] == hz))
        exact = any(norm(n) == norm(h["name"]) for n in names)
        if (exact and same) or (j >= 0.5 and same) or (j >= 0.99 and len(ht) >= 2):
            scored.append((j + same + exact, w))
    out += [(w, "osm-name") for _, w in sorted(scored, reverse=True)[:2]]
    return out


def wikidata_candidates(h: dict, wd: list) -> list:
    ht, hc = set(core_tokens(h["name"])), norm(h.get("city", ""))
    scored = []
    for w in wd:
        j = _jac(ht, set(core_tokens(w.get("itemLabel", ""))))
        same = bool(hc and norm(w.get("locLabel", "")) == hc)
        if (j >= 0.5 and same) or (j >= 0.99 and len(ht) >= 2):
            scored.append((j + same, w["website"]))
    return [(w, "wikidata") for _, w in sorted(scored, reverse=True)[:2]]


def guess_domains(h: dict) -> list:
    """Domains a small hospital plausibly uses, most likely first (DNS-checked before any request)."""
    words = norm(h["name"]).split()
    core = core_tokens(h["name"])
    city = norm(h.get("city", "")).replace(" ", "")
    st = (h.get("state") or "").lower()
    stname = norm(STATE_NAMES.get(st.upper(), "")).replace(" ", "")
    initials = "".join(w[0] for w in words if w not in ("the", "of", "and", "at", "inc", "llc"))
    c = "".join(core)
    stems = ["".join(words)]
    if c:
        stems += [c + "hospital", c + "health", c + "medical", c + "healthcare", c + "memorial",
                  c + "medicalcenter", c + "regional", c + "countyhospital", c + "mc", c + "rmc",
                  c + "hospitaldistrict", c + "hd", c + "health" + st, c + "hospital" + st]
        if len(c) >= 8 or len(core) >= 2:
            stems.append(c)
    if len(initials) >= 3:
        stems += [initials + st, initials + stname, initials + "hospital", initials + "health", initials + "online"]
    if city:
        stems += [city + "hospital", city + "health", city + "medical", city + "healthcare"]
    seen, first, second = set(), [], []
    for s_ in stems:
        s_ = re.sub(r"[^a-z0-9]", "", s_)
        if len(s_) < 5 or s_ in seen:
            continue
        seen.add(s_)
        first += [s_ + ".org", s_ + ".com"]
        second.append(s_ + ".net")
        if len(s_) <= 6:
            second.append(s_ + ".us")
    return (first + second)[:48]


_SEARCH_LOCK = threading.Lock()
_SEARCH_STATE = {"blocked_until": 0.0, "used": 0}


def ddg_search(q: str):
    """[(url, title)] from DuckDuckGo's HTML endpoint, None when blocked."""
    with _SEARCH_LOCK:
        if time.time() < _SEARCH_STATE["blocked_until"]:
            return None
        _SEARCH_STATE["used"] += 1
        st, _, text, _ = http("https://html.duckduckgo.com/html/", "POST", data={"q": q}, timeout=30)
        time.sleep(1.0)
        if st != 200 or "result__a" not in text:
            if st in (202, 403, 429) or (st == 200 and "anomaly" in text.lower()):
                _SEARCH_STATE["blocked_until"] = time.time() + 900     # back off 15 minutes
            return None if st != 200 else []
    out = []
    for m in re.finditer(r'class="result__a"[^>]*href="([^"]+)"[^>]*>(.*?)</a>', text, re.S):
        u = _html.unescape(m.group(1))
        if "duckduckgo.com/l/?" in u:
            u = unquote(parse_qs(urlparse(u).query).get("uddg", [u])[0])
        out.append((u, re.sub(r"<[^>]+>", "", _html.unescape(m.group(2))).strip()))
    return out


def is_aggregator(u: str) -> bool:
    h = host_of(u)
    return not h or any(a in h for a in AGGREGATORS)


def find_site(h: dict, osm: list, wd: list, search_budget: list, log_lines: list):
    """(verified site evidence | None, search hits that look like ATS links)."""
    tried, ats_hits = set(), []
    cands = osm_candidates(h, osm) + wikidata_candidates(h, wd)

    def try_url(u: str, source: str):
        if not u.startswith("http"):
            u = "https://" + u.lstrip("/")
        key = reg_domain(host_of(u)) + urlparse(u).path.rstrip("/")
        if key in tried or is_aggregator(u):
            return None
        tried.add(key)
        ev = verify_site(h, u)
        if (ev is None or not ev["ok"]) and urlparse(u).path not in ("", "/"):
            root = f"{urlparse(u).scheme}://{host_of(u)}/"
            ev2 = verify_site(h, root)
            ev = ev2 if ev2 and (ev is None or ev2["ok"]) else ev
        if ev and not ev["ok"] and ev.get("contact"):
            # second chance: the contact / location page usually carries the phone and street
            ev2 = verify_site(h, ev["contact"])
            if ev2 and ev2["ok"]:
                ev2["final"] = ev["final"]
                ev = ev2
        if ev:
            ev["source"] = source
            in_host = any(t in host_of(ev["final"]).replace("-", "") for t in distinct_tokens(h))
            strong = ev["phone"] or ev["street"]
            if not ev["ok"] and source == "osm-phone" and (ev["name_title"] >= 0.5 or ev["name_text"] >= 0.5):
                ev["ok"] = True          # OSM's own phone tag matched the CMS phone; the page names the hospital
            if not ev["ok"] and source in ("osm-name", "wikidata", "guess") and in_host \
                    and ev["name_title"] >= 0.99 and (ev["city"] or ev["zip"]):
                ev["ok"] = True          # the name is the domain and the title, and the page names the town
            if source == "guess" and ev["ok"] and not strong and \
                    (reg_domain(host_of(ev["final"])) != reg_domain(host_of(u)) or not in_host):
                ev["ok"] = False         # a guessed domain that redirects elsewhere, or does not carry the name
            if source == "search" and ev["ok"]:
                # a search hit must carry the hospital's name in its title or host, and must not be a
                # directory page (those list the phone and address too)
                ev["ok"] = (ev["name_title"] >= 0.5 and not ev.get("directory")
                            and (in_host or ((ev["phone"] or ev["street"]) and ev["name_title"] >= 0.99)))
            log_lines.append(f"{source} {u} score={ev['score']} ok={ev['ok']}")
            if ev["ok"]:
                return ev
        return None

    for u, src in cands[:4]:
        ev = try_url(u, src)
        if ev:
            return ev, ats_hits
    n_fetch = 0
    for d in guess_domains(h):          # DNS first: no request to a host that does not exist
        if n_fetch >= 5:
            break
        bare, www = dns_ok(d), False
        if not bare:
            www = dns_ok("www." + d)
            if not www:
                continue
        n_fetch += 1
        ev = try_url("https://" + (d if bare else "www." + d) + "/", "guess")
        if ev:
            return ev, ats_hits
    if search_budget[0] <= 0:
        log_lines.append("search budget spent")
        return None, ats_hits
    search_budget[0] -= 1
    res = ddg_search(f"{h['name'].title()} {h.get('city', '').title()} {h['state']}")
    if res is None:
        log_lines.append("search blocked")
    if not res:
        return None, ats_hits
    toks = core_tokens(h["name"])
    ranked = []
    for u, title in res[:10]:
        if detect_ats(u):
            ats_hits.append((u, title))
            continue
        if is_aggregator(u):
            continue
        hc = host_of(u).replace("-", "")
        bonus = sum(1 for t in toks if t in hc) + (1 if any(t in norm(title) for t in toks) else 0)
        ranked.append((-bonus, len(ranked), u))
    for _, _, u in sorted(ranked)[:3]:
        ev = try_url(u, "search")
        if ev:
            return ev, ats_hits
    return None, ats_hits


# -- 2. careers pages -------------------------------------------------------------
_CAREER_TEXT = re.compile(
    r"\b(careers?|employment|jobs?|join\s+(?:our|the)\s+team|work\s+(?:with|for|at)\s+us|job\s+(?:openings|"
    r"opportunities|listings|postings)|now\s+hiring|open\s+positions|current\s+openings|we.re\s+hiring|"
    r"career\s+opportunities|employment\s+opportunities|human\s+resources|join\s+us|apply\s+(?:now|online|today)|"
    r"open\s+jobs|job\s+board)\b", re.I)
_CAREER_PATH = re.compile(r"career|employ|/jobs?\b|/job-|join-?(?:our-)?team|work-?(?:with|for)-?us|hiring|"
                          r"opportunit|human-?resources|/hr\b|openings|positions", re.I)
_OPENINGS_TEXT = re.compile(r"(view|see|search|browse|current|open|all)\s+(?:all\s+|our\s+)?(?:job|position|opening|"
                            r"opportunit|career)|apply\s+(?:now|online|here|today)|job\s+(?:board|openings|listings|"
                            r"search)|openings|vacancies|available\s+positions", re.I)
_NEG_LINK = re.compile(r"volunteer|auxiliary|donat|foundation|student|shadow|physician\s+recruit|provider\s+recruit|"
                       r"bill|pay\s+(?:my|your)|portal|patient|news|event|blog|login|sign\s?in", re.I)
COMMON_PATHS = ("/careers", "/employment", "/jobs")


_A_OPEN = re.compile(r"(?i)<a\b([^>]{0,2000})>")
_A_CLOSE = re.compile(r"(?i)</a\s*>")


def anchors(html: str, base: str) -> list:
    """[(absolute url, anchor text)]. Linear: each <a> runs to the next </a>
    (found by bisecting the close-tag positions), at most 3,000 characters."""
    import bisect
    html = (html or "")[:MAX_PAGE]
    closes = [m.start() for m in _A_CLOSE.finditer(html)]
    out = []
    for m in _A_OPEN.finditer(html):
        k = bisect.bisect_left(closes, m.end())
        if k >= len(closes) or closes[k] - m.end() > 3000:
            continue
        attrs, inner = m.group(1), html[m.end():closes[k]]
        hm = re.search(r"""href\s*=\s*["']([^"'#]+)""", attrs)
        if not hm:
            continue
        href = _html.unescape(hm.group(1)).strip()
        if href.lower().startswith(("mailto:", "tel:", "javascript:", "data:")):
            continue
        txt = " ".join(_html.unescape(re.sub(r"<[^>]+>", " ", inner)).split())
        tm = re.search(r"""(?:title|aria-label)\s*=\s*["']([^"']+)""", attrs)
        if tm and len(txt) < 3:
            txt = tm.group(1)
        try:
            out.append((urljoin(base, href), txt[:120]))
        except Exception:
            continue
    return out


def career_links(html: str, base: str) -> list:
    """[(score, url, anchor text)] best first."""
    site = reg_domain(host_of(base))
    scored = {}
    for u, txt in anchors(html, base):
        h = host_of(u)
        ats = bool(detect_ats(u))
        if not h or any(s in h for s in SOCIAL) or (is_aggregator(u) and not ats):
            continue
        s = 0
        if _CAREER_TEXT.search(txt):
            s += 3
        if _CAREER_PATH.search(urlparse(u).path):
            s += 2
        if ats:
            s += 4
        if _NEG_LINK.search(txt) and not _CAREER_TEXT.search(txt):
            s -= 3
        if s <= 0:
            continue
        if reg_domain(h) != site and not ats:
            s -= 1
        if u not in scored or scored[u][0] < s:
            scored[u] = (s, txt)
    return sorted(((s, u, t) for u, (s, t) in scored.items()), key=lambda x: -x[0])


def openings_links(html: str, base: str) -> list:
    out = []
    for u, txt in anchors(html, base):
        if detect_ats(u):
            out.insert(0, u)
        elif _OPENINGS_TEXT.search(txt) and not _NEG_LINK.search(txt) and not is_aggregator(u) and u.rstrip("/") != base.rstrip("/"):
            out.append(u)
    seen, res = set(), []
    for u in out:
        if u not in seen:
            seen.add(u)
            res.append(u)
    return res


# -- 3. platforms -------------------------------------------------------------------
# (platform, regex, tenant builder). Tenants carry the key each adapter needs.
def _wd(m):
    site = m.group(3)
    if site.lower() in ("en-us", "job", "login", "details", "wday", "jobs") or re.fullmatch(r"[a-z]{2}-[A-Z]{2}", site):
        return None
    return {"tenant": m.group(1).lower(), "wd": m.group(2), "site": site}


def _qs(m, *keys):
    q = parse_qs(urlparse("http://x/?" + _html.unescape(m.group(0)).split("?", 1)[-1]).query)
    return {k: (q.get(k) or [""])[0] for k in keys}


def _adp_wfn(m):
    t = _qs(m, "cid", "ccId")
    return {"cid": t["cid"].lower(), "ccId": t["ccId"]} if re.fullmatch(r"[0-9a-f-]{36}", t["cid"].lower()) else None


def _tbe(m):
    t = _qs(m, "org", "cws")
    if not t["org"]:
        return None
    return {"base": f"https://{m.group(1).lower()}.tbe.taleo.net/{m.group(2).lower()}", "org": t["org"],
            "cws": t["cws"] or "1"}


def _sub(m, bad=("www", "app", "api", "cdn", "static", "assets", "help")):
    s = m.group(1).lower()
    return None if s in bad else {"sub": s}


ATS_PATTERNS: list = [
    ("Workday", re.compile(r"(?<![a-z0-9-])([a-z0-9-]{1,63})\.wd(\d+)\.myworkdayjobs\.com/(?:[a-z]{2}-[A-Za-z]{2}/)?([A-Za-z0-9_-]{2,100})", re.I), _wd),
    ("Workday", re.compile(r"wd(\d+)\.myworkdaysite\.com/(?:[a-z]{2}-[A-Za-z]{2}/)?recruiting/([a-z0-9_-]{1,100})/([A-Za-z0-9_-]{1,100})", re.I),
     lambda m: {"tenant": m.group(2).lower(), "wd": m.group(1), "site": m.group(3)}),
    ("Oracle", re.compile(r"(?<![a-z0-9-])([a-z0-9]{1,63}\.fa(?:\.[a-z0-9]{1,63})?\.oraclecloud\.com)/hcmUI/CandidateExperience/[A-Za-z-]{1,40}/sites/([A-Za-z0-9_]{1,100})", re.I),
     lambda m: {"host": m.group(1).lower(), "site": m.group(2)}),
    ("iCIMS", re.compile(r"(?<![a-z0-9.-])((?!www\.|cdn|static|assets|developer|community|careers\.icims)[a-z0-9-]{1,63}\.icims\.com)", re.I),
     lambda m: {"domain": m.group(1).lower()}),
    ("UKG Pro", re.compile(r"(recruiting2?\.ultipro\.com|[a-z0-9]{1,63}\.rec\.pro\.ukg\.net)/([A-Za-z0-9]{1,100})/JobBoard/([0-9a-f-]{36})", re.I),
     lambda m: {"base": f"https://{m.group(1).lower()}/{m.group(2)}", "guid": m.group(3).lower()}),
    ("UKG Ready", re.compile(r"(?<![a-z0-9.-])([a-z0-9-]{1,63}\.[a-z0-9-]{1,63})\.mykronos\.com/ta/(\d{1,12})\.careers", re.I),
     lambda m: {"sub": m.group(1).lower(), "company": m.group(2)}),
    # UKG Ready also runs on white-label hosts (secure*.entertimeonline.com, ...); the
    # /ta/<company id>.careers path is the signature
    ("UKG Ready", re.compile(r"(?<![a-z0-9.-])((?:secure\d{0,3}|[a-z0-9-]{1,63})\.(?:saashr|entertimeonline|[a-z0-9-]{0,30}time[a-z0-9-]{0,30})\.com)/ta/(\d{5,12})\.careers", re.I),
     lambda m: {"sub": m.group(1).lower(), "company": m.group(2)}),
    ("Paycom", re.compile(r"paycomonline\.net/v4/ats/web\.php/(?:jobs|portal)[^\"'\s<>]{0,400}?clientkey=([0-9A-Fa-f]{32})", re.I),
     lambda m: {"clientkey": m.group(1).upper()}),
    ("Paycom", re.compile(r"paycomonline\.net/v4/ats/web\.php/portal/([0-9A-Fa-f]{32})", re.I),
     lambda m: {"clientkey": m.group(1).upper()}),
    ("Paylocity", re.compile(r"recruiting\.paylocity\.com/recruiting/jobs/(?:All|List|Details)/([0-9a-f-]{36})(?:/([A-Za-z0-9_-]{1,100}))?", re.I),
     lambda m: {"guid": m.group(1).lower(), "slug": m.group(2) or ""}),
    ("ADP", re.compile(r"workforcenow\.adp\.com/mascsr/default/mdf/recruitment/recruitment\.html\?[^\"'\s<>]{1,400}", re.I), _adp_wfn),
    ("ADP", re.compile(r"myjobs\.adp\.com/([a-z0-9-]{1,63})(?:/cx)?", re.I), lambda m: {"myjobs": m.group(1).lower()}),
    ("ADP", re.compile(r"recruiting\.adp\.com/srccar/public/RTI\.home\?[^\"'\s<>]{0,400}c=(\d+)", re.I), lambda m: {"rm": m.group(1)}),
    ("ApplicantPro", re.compile(r"(?<![a-z0-9-])([a-z0-9-]{1,63})\.applicantpro\.com", re.I), _sub),
    ("HealthcareSource", re.compile(r"(?:pm|jobs)\.healthcaresource\.com/(?:cs|CS)/([A-Za-z0-9_-]{1,100})", re.I),
     lambda m: {"slug": m.group(1).lower()}),
    ("Symplr/HCTS", re.compile(r"(?<![a-z0-9-])([a-z0-9-]{1,63})\.hctsportals\.com", re.I), _sub),
    ("Paycor", re.compile(r"(?:recruitingbypaycor\.com|newton\.newtonsoftware\.com)/career/[A-Za-z]{1,60}\.action\?[^\"'\s<>]{0,400}clientId=([0-9a-f]{32})", re.I),
     lambda m: {"clientId": m.group(1).lower()}),
    ("NEOGOV", re.compile(r"(?:governmentjobs|schooljobs)\.com/(?:careers|jobs)/([a-z0-9-]{1,63})", re.I),
     lambda m: {"agency": m.group(1).lower()} if m.group(1).lower() not in ("home", "search", "index") else None),
    ("Cornerstone", re.compile(r"(?<![a-z0-9-])([a-z0-9-]{1,63})\.csod\.com/ux/ats/careersite/(\d+)", re.I),
     lambda m: {"host": f"https://{m.group(1).lower()}.csod.com", "site": m.group(2)}),
    ("Taleo BE", re.compile(r"(?<![a-z0-9-])([a-z0-9]{1,63})\.tbe\.taleo\.net/([a-z0-9]{1,63})/ats/careers/[^\"'\s<>]{1,400}", re.I), _tbe),
    ("Taleo", re.compile(r"(?<![a-z0-9-])([a-z0-9-]{1,63})\.taleo\.net/careersection/([a-z0-9_.-]{1,100})", re.I),
     lambda m: {"org": m.group(1).lower(), "section": m.group(2)} if "tbe" not in m.group(1).lower() else None),
    ("SuccessFactors", re.compile(r"(career\d*\.successfactors\.(?:com|eu))/(?:career|portalcareer)\?[^\"'\s<>]{0,400}company=([A-Za-z0-9_]{1,100})", re.I),
     lambda m: {"origin": f"https://{m.group(1).lower()}", "company": m.group(2)}),
    ("Workable", re.compile(r"apply\.workable\.com/([a-z0-9-]{1,63})", re.I),
     lambda m: {"account": m.group(1).lower()} if m.group(1).lower() not in ("api", "j") else None),
    ("Greenhouse", re.compile(r"(?:boards|job-boards)\.greenhouse\.io/(?:embed/job_board\?for=)?([a-z0-9-]{1,63})", re.I),
     lambda m: {"org": m.group(1).lower()} if m.group(1).lower() not in ("embed",) else None),
    ("Lever", re.compile(r"jobs\.lever\.co/([a-z0-9-]{1,63})", re.I), lambda m: {"org": m.group(1).lower()}),
    ("SmartRecruiters", re.compile(r"(?:jobs|careers)\.smartrecruiters\.com/([A-Za-z0-9-]+)", re.I),
     lambda m: {"org": m.group(1)}),
    ("CareerPlug", re.compile(r"(?<![a-z0-9-])([a-z0-9-]{1,63})\.careerplug\.com", re.I),
     lambda m: {"slug": m.group(1).lower()} if m.group(1).lower() not in ("www", "app", "assets") else None),
    ("TAM", re.compile(r"theapplicantmanager\.com/(?:careers|jobs)\?co=([a-z0-9]{1,63})", re.I), lambda m: {"co": m.group(1).lower()}),
    ("Infor", re.compile(r"(css-[a-z0-9]{1,63}-prd)\.inforcloudsuite\.com", re.I), lambda m: {"host": m.group(1).lower()}),
    ("PeopleSoft", re.compile(r"(https?://[a-z0-9.-]{1,200})/psc/([A-Za-z0-9_]{1,100})/[A-Z_]{1,60}/HRMS/c/HRS_HRAM", re.I),
     lambda m: {"origin": m.group(1).lower(), "node": m.group(2)}),
    # platforms with no adapter (tenant still recorded for the new-adapter tally)
    ("CATS", re.compile(r"(?<![a-z0-9-])([a-z0-9-]{1,63})\.catsone\.com", re.I), _sub),
    ("Cadient", re.compile(r"cadienttalent\.com/index\.jsp\?[^\"'\s<>]{0,400}applicationName=([A-Za-z0-9_]{1,100})", re.I),
     lambda m: {"app": m.group(1)}),
    ("Form only", re.compile(r"(?:docs\.google\.com/forms|forms\.gle|goo\.gl/forms|form\.jotform\.com|[a-z0-9-]{1,63}\.formstack\.com/forms|forms\.office\.com|cognitoforms\.com)/?[^\"'\s<>]{0,400}", re.I),
     lambda m: {}),
    ("Indeed only", re.compile(r"indeed\.com/(?:cmp/([A-Za-z0-9_-]{1,100})/jobs|jobs\?q=([^\"'&\s<>]+)|viewjob\?jk=([0-9a-f]+))", re.I),
     lambda m: {"indeed": unquote(m.group(1) or m.group(2) or "").replace("+", " ")}),
    ("FastHealth", re.compile(r"/jobs/show_job\.php\?id=\d+|/jobs/jobs\.xml", re.I), lambda m: {}),
    ("Netchex", re.compile(r"(?<![a-z0-9-])([a-z0-9-]{1,63})\.netchexonline\.(?:net|com)|netchexonline\.(?:net|com)/[^\"'\s<>]{0,400}(?:career|job)", re.I),
     lambda m: {"sub": (m.group(1) or "").lower()}),
    ("BirdDogHR", re.compile(r"(?<![a-z0-9-])([a-z0-9-]{1,63})\.birddoghr\.com", re.I), _sub),
    ("TriNet Hire", re.compile(r"app\.trinethire\.com/companies/(\d+)", re.I), lambda m: {"company": m.group(1)}),
    ("Zoho Recruit", re.compile(r"(?<![a-z0-9-])([a-z0-9-]{1,63})\.zohorecruit\.com", re.I), _sub),
    ("HireCentric", re.compile(r"(?<![a-z0-9-])([a-z0-9-]{1,63})\.hirecentric\.com", re.I), _sub),
    ("Recruiterbox", re.compile(r"(?<![a-z0-9-])([a-z0-9-]{1,63})\.recruiterbox\.com", re.I), _sub),
    ("Ascentis", re.compile(r"(?:selfservice|secure)\d*\.ascentis\.com/([A-Za-z0-9_-]{1,100})/", re.I), lambda m: {"org": m.group(1)}),
    ("ApplicantPool", re.compile(r"(?<![a-z0-9-])([a-z0-9-]{1,63})\.applicantpool\.com", re.I), _sub),
    ("PrismHR", re.compile(r"(?<![a-z0-9-])([a-z0-9-]{1,63})\.prismhr\.com", re.I), _sub),
    ("BambooHR", re.compile(r"(?<![a-z0-9-])([a-z0-9-]{1,63})\.bamboohr\.com", re.I),
     lambda m: _sub(m, ("www", "api", "help", "marketplace", "resources", "app", "static"))),
    ("JazzHR", re.compile(r"(?<![a-z0-9-])([a-z0-9-]{1,63})\.applytojob\.com", re.I), _sub),
    ("Breezy", re.compile(r"(?<![a-z0-9-])([a-z0-9-]{1,63})\.breezy\.hr", re.I), lambda m: _sub(m, ("www", "app", "assets", "attachments", "api"))),
    ("Dayforce", re.compile(r"jobs\.dayforcehcm\.com/(?:[a-z]{2}-[A-Za-z]{2}/)?([A-Za-z0-9_-]{1,100})(?:/([A-Za-z0-9_-]{1,100}))?", re.I),
     lambda m: {"tenant": m.group(1).lower(), "board": m.group(2) or ""} if m.group(1).lower() not in ("en-us",) else None),
    ("Dayforce", re.compile(r"(?<![a-z0-9-])([a-z0-9]{1,63})\.dayforcehcm\.com/CandidatePortal/[a-z-]{1,40}/([A-Za-z0-9_]{1,100})", re.I),
     lambda m: {"tenant": m.group(2).lower(), "board": ""}),
    ("iSolved", re.compile(r"(?<![a-z0-9-])([a-z0-9-]{1,63})\.isolvedhire\.com", re.I), _sub),
    ("ApplicantStack", re.compile(r"(?<![a-z0-9-])([a-z0-9-]{1,63})\.applicantstack\.com", re.I), _sub),
    ("JobAps", re.compile(r"(?:www\.)?jobaps\.com/([A-Za-z0-9]{1,100})", re.I), lambda m: {"org": m.group(1)}),
    ("Hireology", re.compile(r"careers\.hireology\.com/([a-z0-9-]{1,63})", re.I), lambda m: {"org": m.group(1).lower()}),
    ("ClearCompany", re.compile(r"(?<![a-z0-9-])([a-z0-9-]{1,63})\.(?:clearcompany|hrmdirect)\.com", re.I), _sub),
    ("Apploi", re.compile(r"apploi\.com/(?:view-company|company|jobs)/([A-Za-z0-9_-]{1,100})", re.I), lambda m: {"org": m.group(1)}),
    ("Paychex", re.compile(r"(?:paychex\.recruiting\.com|recruiting\.paychex\.com)/([A-Za-z0-9_-]{1,100})", re.I), lambda m: {"org": m.group(1)}),
    ("Jobvite", re.compile(r"jobs\.jobvite\.com/([a-z0-9-]{1,63})", re.I), lambda m: {"org": m.group(1).lower()}),
    ("Avature", re.compile(r"(?<![a-z0-9-])([a-z0-9-]{1,63})\.avature\.net", re.I), _sub),
    ("Hirebridge", re.compile(r"hirebridge\.com/v3/(?:jobs|careercenter)/[^\"'\s<>]{0,400}cid=(\d+)", re.I), lambda m: {"cid": m.group(1)}),
    ("PeopleAdmin", re.compile(r"(?<![a-z0-9-])([a-z0-9-]{1,63})\.peopleadmin\.com", re.I), _sub),
    ("HiringThing", re.compile(r"(?<![a-z0-9-])([a-z0-9-]{1,63})\.hiringthing\.com", re.I), _sub),
    ("Rippling", re.compile(r"ats\.rippling\.com/([a-z0-9-]{1,63})", re.I), lambda m: {"org": m.group(1).lower()}),
    ("Gusto", re.compile(r"jobs\.gusto\.com/boards/([a-z0-9-]{1,63})", re.I), lambda m: {"org": m.group(1).lower()}),
    ("Viventium", re.compile(r"(?<![a-z0-9-])([a-z0-9-]{1,63})\.viventium\.com", re.I), _sub),
    ("Recruitee", re.compile(r"(?<![a-z0-9-])([a-z0-9-]{1,63})\.recruitee\.com", re.I), _sub),
    ("Teamtailor", re.compile(r"(?<![a-z0-9-])([a-z0-9-]{1,63})\.teamtailor\.com", re.I), _sub),
    ("Eightfold", re.compile(r"(?<![a-z0-9-])([a-z0-9-]{1,63})\.eightfold\.ai", re.I), _sub),
    ("Phenom", re.compile(r"cdn\.phenompeople\.com|phenompro\.com", re.I), lambda m: {}),
    ("TalentBrew", re.compile(r"tbcdn\.talentbrew\.com", re.I), lambda m: {}),
    ("Jibe", re.compile(r"jibeapply\.com|jibecdn\.com", re.I), lambda m: {}),
    ("USAJOBS", re.compile(r"usajobs\.gov/(?:job|Search|search)", re.I), lambda m: {}),
]

def _needles(pattern: str) -> list:
    """Literal fragments (5+ chars) of a pattern; a page holding none of them cannot match it."""
    frags = re.findall(r"(?:[A-Za-z0-9_/=-]|\\[.?])+", pattern)
    out = []
    for f in frags:
        lit = f.replace("\\.", ".").replace("\\?", "?")
        if len(re.sub(r"[^a-z0-9]", "", lit.lower())) >= 5 and not re.search(r"[A-Z]-|a-z|0-9", lit):
            out.append(lit.lower())
    return out or [""]


_NEEDLES = {rx.pattern: _needles(rx.pattern) for _, rx, _ in ATS_PATTERNS}


# Platforms whose adapters exist in scraper.py today (the config tables named).
SUPPORTED = {
    "Workday": "WORKDAY_TENANTS", "Oracle": "ORACLE_ORGS", "iCIMS": "ICIMS_ORGS", "UKG Pro": "UKG_ORGS",
    "UKG Ready": "KRONOS_ORGS", "Paycom": "PAYCOM_ORGS", "Paylocity": "PAYLOCITY_ORGS", "ADP": "ADP_ORGS / ADPCX_ORGS",
    "ApplicantPro": "APPLICANTPRO_ORGS", "HealthcareSource": "HEALTHCARESOURCE_ORGS", "Symplr/HCTS": "HCTS_PORTALS",
    "Paycor": "PAYCOR_ORGS", "NEOGOV": "NEOGOV_AGENCIES", "Cornerstone": "CSOD_ORGS", "Taleo BE": "TALEO_BE_ORGS",
    "SuccessFactors": "SUCCESSFACTORS_ORGS", "Workable": "WORKABLE_ORGS", "Greenhouse": "GREENHOUSE_ORGS",
    "Lever": "LEVER_ORGS", "SmartRecruiters": "SMARTRECRUITERS_ORGS", "CareerPlug": "CAREERPLUG_ORGS",
    "TAM": "TAM_ORGS", "Infor": "INFOR_ORGS", "PeopleSoft": "PEOPLESOFT_SITES", "Eightfold": "EIGHTFOLD_ORGS",
    "Phenom": "PHENOM_ORGS", "TalentBrew": "TALENTBREW_ORGS", "Jibe": "JIBE_SITES", "USAJOBS": "USAJOBS_ORGS",
}


def detect_ats(text: str) -> list:
    """[(platform, tenant, matched text)] in order of first appearance, deduped."""
    found, seen = [], set()
    text = (text or "")[:MAX_PAGE]
    low = text.lower()
    for name, rx, build in ATS_PATTERNS:
        if not any(n in low for n in _NEEDLES[rx.pattern]):
            continue
        for m in rx.finditer(text):
            try:
                t = build(m)
            except Exception:
                t = None
            if t is None:
                continue
            key = (name, json.dumps(t, sort_keys=True))
            if key in seen:
                continue
            seen.add(key)
            found.append((m.start(), name, t, m.group(0)[:200]))
            if len(found) > 40:
                break
    return [(n, t, s) for _, n, t, s in sorted(found, key=lambda x: x[0])]


# -- 4. live tenant checks ------------------------------------------------------------
def _locs_from(items, *paths) -> list:
    out = []
    for it in items or []:
        parts = []
        for p in paths:
            v = it
            for k in p.split("."):
                v = v.get(k) if isinstance(v, dict) else None
            if isinstance(v, (str, int)) and str(v).strip():
                parts.append(str(v).strip())
        if parts:
            out.append(", ".join(parts))
    return out


def v_workday(t):
    url = f"https://{t['tenant']}.wd{t['wd']}.myworkdayjobs.com/wday/cxs/{t['tenant']}/{t['site']}/jobs"
    st, d = http_json(url, "POST", json={"appliedFacets": {}, "limit": 20, "offset": 0, "searchText": ""},
                      headers={"Content-Type": "application/json"})
    if not isinstance(d, dict):
        return {"count": None, "note": f"HTTP {st}"}
    return {"count": d.get("total"), "locs": [p.get("locationsText", "") for p in d.get("jobPostings") or []]}


def v_oracle(t):
    url = f"https://{t['host']}/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
    st, d = http_json(url, params={"onlyData": "true", "expand": "requisitionList.secondaryLocations",
                                   "finder": f"findReqs;siteNumber={t['site']},limit=25,offset=0"})
    try:
        it = d["items"][0]
        reqs = it.get("requisitionList") or []
        return {"count": it.get("TotalJobsCount"), "locs": [r.get("PrimaryLocation", "") for r in reqs]}
    except Exception:
        return {"count": None, "note": f"HTTP {st}"}


def v_icims(t):
    st, final, text, _ = http(f"https://{t['domain']}/jobs/search", params={"ss": "1", "in_iframe": "1", "pr": "0"})
    if st != 200:
        return {"count": None, "note": f"HTTP {st}"}
    if len(text) < 400 or ("iCIMS_JobsTable" not in text and "iCIMS_JobCardItem" not in text):
        return {"count": None, "note": "no job table (Jibe front or empty)"}
    locs = re.findall(r"(?is)<span[^>]*>\s*(?:Job\s+)?Locations?\s*</span>\s*<span[^>]*>(.*?)</span>", text)
    locs = [" ".join(_html.unescape(re.sub("<[^>]+>", " ", x)).split()) for x in locs]
    ids = set(re.findall(r'href="[^"]*/jobs/(\d+)/[^"]*/job', text))
    m = re.search(r"(\d+)\s+(?:jobs?|results?|positions?)\s+found|of\s+(\d+)\s+(?:jobs?|results?)", text, re.I)
    total = int(m.group(1) or m.group(2)) if m else len(ids)
    return {"count": total, "locs": locs[:50]}


def v_ukg_pro(t):
    api = f"{t['base']}/JobBoard/{t['guid']}/JobBoardView/LoadSearchResults"
    body = {"opportunitySearch": {"Top": 50, "Skip": 0, "QueryString": "", "Filters": [],
                                  "OrderBy": [{"Value": "postedDateDesc", "PropertyName": "PostedDate", "Ascending": False}]},
            "deviceType": "desktop", "recommendationSettings": {}}
    st, d = http_json(api, "POST", json=body, headers={"Content-Type": "application/json",
                                                         "Referer": f"{t['base']}/JobBoard/{t['guid']}/",
                                                         "X-Requested-With": "XMLHttpRequest"})
    if not isinstance(d, dict):
        return {"count": None, "note": f"HTTP {st}"}
    locs = []
    for o in d.get("opportunities") or []:
        for l in o.get("Locations") or []:
            a = l.get("Address") or {}
            locs.append(", ".join(x for x in ((a.get("City") or ""), ((a.get("State") or {}).get("Code") or "")) if x)
                        or (l.get("LocalizedDescription") or ""))
    return {"count": d.get("totalCount"), "locs": locs}


def v_ukg_ready(t):
    base = f"https://{t['sub']}" if t["sub"].endswith((".com", ".net")) else f"https://{t['sub']}.mykronos.com"
    st, d = http_json(f"{base}/ta/rest/ui/recruitment/companies/%7C{t['company']}/job-requisitions",
                      params={"offset": 1, "size": 100, "sort": "desc", "ein_id": "", "lang": "en-US"})
    if not isinstance(d, (dict, list)):
        return {"count": None, "note": f"HTTP {st}"}
    items = d if isinstance(d, list) else (d.get("job_requisitions") or [])
    return {"count": len(items), "locs": _locs_from(items, "location.city", "location.state")}


_PAYCOM_FILTERS = {"distanceFrom": 0, "workEnvironments": [], "positionTypes": [], "educationLevels": [],
                   "categories": [], "travelTypes": [], "shiftTypes": [], "otherFilters": [],
                   "keywordSearchText": "", "location": "", "sortOption": "N"}


def v_paycom(t):
    lst = "https://www.paycomonline.net/v4/ats/web.php/jobs"
    st, cfg = http_json(lst, params={"clientkey": t["clientkey"]}, headers={"Accept": "application/json"})
    if not isinstance(cfg, dict):
        return {"count": None, "note": f"no session ({st})"}
    lib = cfg.get("libConfig") or {}
    jwt = cfg.get("sessionJWT")
    base = str(lib.get("atsPortalMantleServiceUrl") or "").rstrip("/")
    if not jwt or not base:
        return {"count": None, "note": "no session"}
    company = ""
    for k in ("companyName", "clientName", "portalTitle", "careerPageTitle"):
        v = lib.get(k) or cfg.get(k)
        if isinstance(v, str) and v:
            company = v
            break
    st, d = http_json(f"{base}/api/ats/job-posting-previews/search", "POST",
                      json={"skip": 0, "take": 50, "filtersForQuery": dict(_PAYCOM_FILTERS)},
                      headers={"Content-Type": "application/json", "Authorization": f"Bearer {jwt}",
                               "Locale": "en-US", "Translation-Highlights": "false",
                               "Origin": "https://www.paycomonline.net",
                               "Referer": f"{lst}?clientkey={t['clientkey']}"})
    if not isinstance(d, dict):
        return {"count": None, "note": f"search HTTP {st}", "company": company}
    items = d.get("jobPostingPreviews") or []
    locs = []
    for x in items:
        l = x.get("locations") or x.get("location") or x.get("jobLocation") or ""
        locs.append(", ".join(map(str, l)) if isinstance(l, list) else str(l))
    return {"count": d.get("jobPostingPreviewsCount", len(items)), "locs": locs, "company": company}


def v_paylocity(t):
    url = f"https://recruiting.paylocity.com/recruiting/jobs/All/{t['guid']}" + (f"/{t['slug']}" if t.get("slug") else "")
    st, final, text, _ = http(url)
    if st != 200:
        return {"count": None, "note": f"HTTP {st}"}
    m = re.search(r"window\.pageData\s*=\s*(\{.*?\});\s*(?:</script>|window\.)", text, re.S)
    if not m:
        return {"count": None, "note": "no pageData"}
    try:
        d = json.loads(m.group(1))
    except Exception:
        return {"count": None, "note": "pageData parse"}
    jobs = d.get("Jobs") or []
    locs = _locs_from(jobs, "JobLocation.City", "JobLocation.State") or [j.get("LocationName", "") for j in jobs]
    slug = urlparse(final).path.rstrip("/").split("/")[-1]
    return {"count": len(jobs), "locs": locs, "company": d.get("CompanyName") or slug.replace("-", " "),
            "slug": slug if slug.lower() != t["guid"] else t.get("slug", "")}


def v_adp(t):
    if t.get("cid"):
        params = {"cid": t["cid"], "ccId": t.get("ccId") or "19000101_000001", "lang": "en_US", "locale": "en_US",
                  "$top": "20", "$skip": "0", "timeStamp": str(int(time.time() * 1000))}
        st, d = http_json("https://workforcenow.adp.com/mascsr/default/careercenter/public/events/staffing/v1/job-requisitions",
                          params=params, headers={"Referer": "https://workforcenow.adp.com/mascsr/default/mdf/recruitment/recruitment.html"})
        if not isinstance(d, dict):
            return {"count": None, "note": f"HTTP {st}"}
        items = d.get("jobRequisitions") or []
        locs = []
        for j in items:
            for l in j.get("requisitionLocations") or []:
                a = l.get("address") or {}
                locs.append(", ".join(x for x in (a.get("cityName") or "", (a.get("countrySubdivisionLevel1") or {}).get("codeValue") or "") if x))
        return {"count": int((d.get("meta") or {}).get("totalNumber") or len(items)), "locs": locs}
    return {"count": None, "note": "ADP MyJobs/RM: counted by the adapter dry run"}


def v_applicantpro(t):
    st, final, text, _ = http(f"https://{t['sub']}.applicantpro.com/jobs/")
    if st != 200:
        return {"count": None, "note": f"HTTP {st}"}
    m = re.search(r"/core/jobs/(\d+)", text) or re.search(r"domainId\D{0,4}(\d+)", text)
    company = re.sub(r"(?i)\b(jobs?\s+at|careers?|employment|opportunities)\b", " ", title_of(text).split("|")[0]).strip(" -|")
    if not m:
        return {"count": None, "note": "no domain id", "company": company}
    st, d = http_json(f"https://{t['sub']}.applicantpro.com/core/jobs/{m.group(1)}",
                      params={"getParams": json.dumps({"cityUrl": "", "countryAbbreviation": "", "stateAbbreviation": "", "isInternal": 0})})
    items = []
    if isinstance(d, list):
        items = d
    elif isinstance(d, dict):
        items = (d.get("data") or {}).get("jobs") if isinstance(d.get("data"), dict) else d.get("jobs")
    items = items or []
    return {"count": len(items), "locs": _locs_from(items, "city", "abbreviation"), "company": company,
            "domain_id": m.group(1)}


def v_hcs(t):
    st, d = http_json(f"https://pm.healthcaresource.com/JobseekerSearchAPI/{t['slug']}/api/Search", "POST",
                      params={"size": 50, "from": 0}, json={"query": {"bool": {"must": {"match_all": {}}}}},
                      headers={"Content-Type": "application/json; charset=utf-8",
                               "Referer": f"https://pm.healthcaresource.com/cs/{t['slug']}"})
    if not isinstance(d, dict):
        return {"count": None, "note": f"HTTP {st}"}
    hits = d.get("hits") or {}
    tot = hits.get("total")
    tot = tot.get("value") if isinstance(tot, dict) else tot
    locs = []
    for hh in hits.get("hits") or []:
        s = hh.get("_source") or {}
        jl = s.get("jobLocation")
        jl = jl[0] if isinstance(jl, list) and jl else jl
        a = (jl or {}).get("address") or {} if isinstance(jl, dict) else {}
        bits = [str(x) for x in (a.get("addressLocality"), a.get("addressRegion")) if x]
        locs.append(", ".join(bits) or str(s.get("facilityName") or ""))
    return {"count": tot, "locs": locs}


def v_hcts(t):
    st, final, text, _ = http(f"https://{t['sub']}.hctsportals.com/jobs/search", params={"page": 1})
    if st != 200:
        return {"count": None, "note": f"HTTP {st}"}
    cards = re.findall(r"jobs-section__item", text)
    locs = re.findall(r"(?is)Location:?\s*</[^>]+>\s*(?:<[^>]+>\s*)*([^<]{2,80})", text)
    return {"count": len(cards), "locs": [l.strip() for l in locs], "note": "first page only"}


def v_paycor(t):
    st, final, text, _ = http("https://recruitingbypaycor.com/career/CareerHome.action", params={"clientId": t["clientId"]})
    if st != 200:
        return {"count": None, "note": f"HTTP {st}"}
    ids = set(re.findall(r"JobIntroduction\.action\?[^\"']*?id=([0-9a-f]+)", text))
    locs = [" ".join(_html.unescape(re.sub("<[^>]+>", " ", x)).split())
            for x in re.findall(r"(?is)gnewtonCareerGroupJobDescriptionClass[^>]*>(.*?)</div>", text)]
    return {"count": len(ids), "locs": locs, "company": title_of(text)}


def v_taleo_be(t):
    st, final, text, _ = http(f"{t['base']}/ats/servlet/Rss", params={"org": t["org"], "cws": t["cws"], "WebPage": "SRCHR_V2",
                                                                       "WebVersion": "0", "_rss_version": "2"}, timeout=60)
    if st != 200:
        return {"count": None, "note": f"HTTP {st}"}
    items = re.findall(r"(?s)<item>(.*?)</item>", text)
    locs = []
    for it in items:
        c = re.search(r"<taleo:locationCity>(.*?)</taleo:locationCity>", it)
        s = re.search(r"<taleo:locationState>(.*?)</taleo:locationState>", it)
        locs.append(", ".join(x.group(1) for x in (c, s) if x))
    return {"count": len(items), "locs": locs}


def v_workable(t):
    st, d = http_json(f"https://apply.workable.com/api/v3/accounts/{t['account']}/jobs", "POST",
                      json={"query": "", "location": [], "department": [], "worktype": [], "remote": []},
                      headers={"Content-Type": "application/json"})
    if not isinstance(d, dict):
        return {"count": None, "note": f"HTTP {st}"}
    items = d.get("results") or []
    return {"count": d.get("total", len(items)), "locs": _locs_from(items, "location.city", "location.region")}


def v_greenhouse(t):
    st, d = http_json(f"https://boards-api.greenhouse.io/v1/boards/{t['org']}/jobs")
    if not isinstance(d, dict):
        return {"count": None, "note": f"HTTP {st}"}
    items = d.get("jobs") or []
    return {"count": len(items), "locs": [(j.get("location") or {}).get("name", "") for j in items]}


def v_lever(t):
    st, d = http_json(f"https://api.lever.co/v0/postings/{t['org']}", params={"mode": "json"})
    if not isinstance(d, list):
        return {"count": None, "note": f"HTTP {st}"}
    return {"count": len(d), "locs": [(j.get("categories") or {}).get("location", "") for j in d]}


def v_smartrecruiters(t):
    st, d = http_json(f"https://api.smartrecruiters.com/v1/companies/{t['org']}/postings", params={"limit": 100})
    if not isinstance(d, dict):
        return {"count": None, "note": f"HTTP {st}"}
    items = d.get("content") or []
    return {"count": d.get("totalFound", len(items)), "locs": _locs_from(items, "location.city", "location.region")}


def v_neogov(t):
    st, final, text, _ = http(f"https://www.governmentjobs.com/careers/{t['agency']}")
    if st != 200:
        return {"count": None, "note": f"HTTP {st}"}
    m = re.search(r"(\d[\d,]*)\s+jobs?\s+found", text, re.I)
    return {"count": int(m.group(1).replace(",", "")) if m else None, "locs": [],
            "note": "agency-wide board (county or state); the hospital's share needs a department filter",
            "company": title_of(text)}


def v_bamboohr(t):
    st, d = http_json(f"https://{t['sub']}.bamboohr.com/careers/list")
    if not isinstance(d, dict):
        return {"count": None, "note": f"HTTP {st}"}
    items = d.get("result") or []
    return {"count": len(items), "locs": _locs_from(items, "location.city", "location.state")}


def v_breezy(t):
    st, d = http_json(f"https://{t['sub']}.breezy.hr/json")
    if not isinstance(d, list):
        return {"count": None, "note": f"HTTP {st}"}
    return {"count": len(d), "locs": _locs_from(d, "location.city", "location.state.id")}


def v_jazzhr(t):
    st, final, text, _ = http(f"https://{t['sub']}.applytojob.com/apply")
    if st != 200:
        return {"count": None, "note": f"HTTP {st}"}
    ids = set(re.findall(r'href="[^"]*/apply/([A-Za-z0-9]{6,})/', text))
    locs = [" ".join(_html.unescape(re.sub("<[^>]+>", " ", x)).split())
            for x in re.findall(r"(?is)<li[^>]*>\s*<i[^>]+fa-map-marker[^>]*></i>(.*?)</li>", text)]
    return {"count": len(ids), "locs": locs, "company": title_of(text)}


def v_isolved(t):
    st, final, text, _ = http(f"https://{t['sub']}.isolvedhire.com/jobs/")
    if st != 200:
        return {"count": None, "note": f"HTTP {st}"}
    n = len(set(re.findall(r'href="[^"]*/jobs/(\d+)', text)))
    return {"count": n, "locs": [], "company": title_of(text)}


def v_applicantstack(t):
    st, final, text, _ = http(f"https://{t['sub']}.applicantstack.com/x/openings")
    if st != 200:
        return {"count": None, "note": f"HTTP {st}"}
    n = len(set(re.findall(r'href="[^"]*/x/detail/([a-z0-9]+)', text)))
    locs = re.findall(r"(?is)<td[^>]*>\s*([A-Za-z .'-]+,\s*[A-Z]{2})\s*</td>", text)
    return {"count": n, "locs": locs, "company": title_of(text)}


def v_careerplug(t):
    st, final, text, _ = http(f"https://{t['slug']}.careerplug.com/jobs")
    if st != 200:
        return {"count": None, "note": f"HTTP {st}"}
    n = len(set(re.findall(r'href="/jobs/(\d+)', text)))
    return {"count": n, "locs": [], "company": title_of(text)}


VERIFIERS = {"Workday": v_workday, "Oracle": v_oracle, "iCIMS": v_icims, "UKG Pro": v_ukg_pro, "UKG Ready": v_ukg_ready,
             "Paycom": v_paycom, "Paylocity": v_paylocity, "ADP": v_adp, "ApplicantPro": v_applicantpro,
             "HealthcareSource": v_hcs, "Symplr/HCTS": v_hcts, "Paycor": v_paycor, "Taleo BE": v_taleo_be,
             "Workable": v_workable, "Greenhouse": v_greenhouse, "Lever": v_lever, "SmartRecruiters": v_smartrecruiters,
             "NEOGOV": v_neogov, "BambooHR": v_bamboohr, "Breezy": v_breezy, "JazzHR": v_jazzhr, "iSolved": v_isolved,
             "ApplicantStack": v_applicantstack, "CareerPlug": v_careerplug}

STATE_NAMES = {"AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California", "CO": "Colorado",
               "CT": "Connecticut", "DE": "Delaware", "DC": "District of Columbia", "FL": "Florida", "GA": "Georgia",
               "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
               "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland", "MA": "Massachusetts",
               "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi", "MO": "Missouri", "MT": "Montana",
               "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico",
               "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
               "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina", "SD": "South Dakota",
               "TN": "Tennessee", "TX": "Texas", "UT": "Utah", "VT": "Vermont", "VA": "Virginia", "WA": "Washington",
               "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming", "PR": "Puerto Rico", "GU": "Guam",
               "VI": "Virgin Islands", "AS": "American Samoa", "MP": "Northern Mariana Islands"}


_TOWN_ABBR = [(r"\bsaint\b", "st"), (r"\bsainte\b", "ste"), (r"\bmount\b", "mt"), (r"\bfort\b", "ft")]



def town_norm(s: str) -> str:
    """norm() with Saint/St, Mount/Mt, Fort/Ft folded, so 'Saint Peter, MN' matches CMS 'St Peter'."""
    t = norm(s)
    for rx, rep in _TOWN_ABBR:
        t = re.sub(rx, rep, t)
    return t


def score_tenant(h: dict, platform: str, tenant: dict, res: dict, context: str) -> dict:
    """Confidence that this tenant lists THIS hospital's jobs.

    high   : seen on the hospital's own careers pages, the board lists jobs,
             and half or more of the postings sit in the hospital's town (or
             the board's company name is the hospital's and no posting lies
             outside its state)
    medium : jobs listed and some evidence (a posting in town, or the name)
    low    : no live evidence (count unknown, empty board, or postings elsewhere)
    """
    city = town_norm(h.get("city", ""))
    st = (h.get("state") or "").upper()
    locs = [l for l in (res.get("locs") or []) if l]
    nl = [town_norm(l) for l in locs]
    in_city = sum(1 for l in nl if city and f" {city} " in f" {l} ")
    sname = norm(STATE_NAMES.get(st, ""))
    in_state = sum(1 for l, n in zip(locs, nl) if re.search(rf"(?:^|[,\s-]){st}(?:$|[,\s-])", l) or (sname and sname in n))
    # the town's own words do not name a hospital ("daytonchildrens" is not Access Hospital Dayton)
    toks = set(distinct_tokens(h))
    comp = set(core_tokens(res.get("company") or "")) | set(core_tokens(" ".join(str(v) for v in tenant.values())))
    comp |= {t for v in tenant.values() for t in toks if t in str(v).lower().replace("-", "")}
    name_match = bool(toks) and len(toks & comp) / len(toks) >= 0.5
    n = res.get("count")
    share = in_city / len(locs) if locs else None
    conf = "low"
    if isinstance(n, int) and n > 0:
        if context == "careers" and ((share is not None and share >= 0.5) or (name_match and (not locs or in_state == len(locs)))):
            conf = "high"
        elif in_city > 0 or name_match:
            conf = "medium"
    return {"in_city": in_city, "locs_seen": len(locs), "in_state": in_state, "name_match": name_match,
            "city_share": None if share is None else round(share, 2), "confidence": conf}


# -- driver ---------------------------------------------------------------------------
_NONE_SIGNS = re.compile(r"no (?:current )?(?:job )?(?:openings|positions|vacancies)|not currently hiring|"
                         r"there are (?:currently )?no (?:open|available)", re.I)
_JOB_WORDS = re.compile(r"\b(?:RN|LPN|LVN|CNA|Registered Nurse|Technician|Technologist|Therapist|Coordinator|"
                        r"Pharmacist|Aide|Housekeep\w*|Dietary|Phlebotom\w*|Radiolog\w*|Paramedic)\b")


def careers_scan(h: dict, site: dict, ats_hits: list) -> dict:
    """Careers pages, platform detections with context, and no-board signals."""
    base = site["final"]
    st, final, home, _ = http(base)
    pages, detections = [], []
    links = career_links(home, final) if st == 200 else []
    career_urls = {u for _, u, _ in links}
    if st == 200:
        for name, t, s in detect_ats(home):
            ctx = "careers" if any(s[:60].lower() in u.lower() for u in career_urls) else "home"
            detections.append((name, t, ctx, final))
    home_dom = reg_domain(host_of(final))
    same = [u for s, u, _ in links if reg_domain(host_of(u)) == home_dom and s >= 2]
    for s, u, _ in links:
        if reg_domain(host_of(u)) != home_dom:
            for name, t, m in detect_ats(u):
                detections.append((name, t, "careers", u))
    queue = same[:2] or [urljoin(final, p) for p in COMMON_PATHS]
    blob_pages = []
    fetched = 0
    for u in queue:
        if fetched >= 3:
            break
        st2, f2, t2, _ = http(u)
        fetched += 1
        if st2 != 200 or len(t2) < 300:
            continue
        if not same and not (_CAREER_TEXT.search(title_of(t2)) or _CAREER_PATH.search(urlparse(f2).path)):
            continue
        if reg_domain(host_of(f2)) != home_dom and not detect_ats(f2):
            continue
        pages.append(f2)
        blob_pages.append(t2)
        for name, t, s in detect_ats(t2 + "\n" + f2):
            detections.append((name, t, "careers", f2))
        if any(d[2] == "careers" and d[0] in VERIFIERS for d in detections):
            break
        for nxt in openings_links(t2, f2)[:2]:      # one hop: a "view openings" link
            if detect_ats(nxt):
                for name, t, s in detect_ats(nxt):
                    detections.append((name, t, "careers", nxt))
                break
            if fetched >= 4:
                break
            st3, f3, t3, _ = http(nxt)
            fetched += 1
            if st3 == 200:
                pages.append(f3)
                blob_pages.append(t3)
                for name, t, s in detect_ats(t3 + "\n" + f3):
                    detections.append((name, t, "careers", f3))
            break
        if any(d[2] == "careers" for d in detections):
            break
    for u, title in ats_hits:
        for name, t, s in detect_ats(u):
            detections.append((name, t, "search", u))
    blob = "\n".join(blob_pages)
    vt = visible_text(blob)
    signals = {
        "careers_page": bool(pages),
        "home_blocked": st != 200 or len(home or "") < 1000,
        "jsonld_jobposting": '"JobPosting"' in blob,
        "pdf_application": bool(re.search(r"href=\"[^\"]+(?:applic|employ)[^\"]*\.pdf\"", blob, re.I)),
        "mailto_hr": bool(re.search(r"mailto:[^\"']*(?:hr|career|job|recruit|employ|resume|human)", blob, re.I)),
        "no_openings_text": bool(_NONE_SIGNS.search(vt)) if blob else False,
        "wp_job_manager": bool(re.search(r"job_listing|wp-job-manager|/job/[a-z0-9-]+/?\"", blob)),
        "job_words": len(_JOB_WORDS.findall(vt)),
    }
    return {"careers_urls": pages, "detections": detections, "signals": signals}


def classify_none(sig: dict) -> str:
    if not sig.get("careers_page"):
        return "site refused automated fetch" if sig.get("home_blocked") else "no careers page found"
    if sig.get("wp_job_manager"):
        return "custom HTML (WordPress job posts)"
    if sig.get("jsonld_jobposting") or sig.get("job_words", 0) >= 8:
        return "custom HTML"
    if sig.get("pdf_application") or sig.get("mailto_hr"):
        return "none (PDF / email application)"
    if sig.get("no_openings_text"):
        return "none (no openings listed)"
    return "none found on careers page"


_RANK = {"careers": 0, "search": 1, "home": 2}
_CONF = {"high": 0, "medium": 1, "low": 2}


def pick_best(checks: list):
    if not checks:
        return None
    return sorted(checks, key=lambda c: (_CONF[c["score"]["confidence"]], _RANK[c["context"]],
                                         c["platform"] not in SUPPORTED, -(c["res"].get("count") or 0)))[0]


def run_checks(h: dict, detections: list) -> list:
    uniq = {}
    for name, t, ctx, where in detections:
        key = (name, json.dumps(t, sort_keys=True))
        if key not in uniq or _RANK[ctx] < _RANK[uniq[key][2]]:
            uniq[key] = (name, t, ctx, where)
    checks = []
    for name, t, ctx, where in sorted(uniq.values(), key=lambda x: _RANK[x[2]])[:6]:
        if name not in VERIFIERS:
            checks.append({"platform": name, "tenant": t, "context": ctx, "where": where,
                           "res": {"count": None, "note": "no live check"}, "score": {"confidence": "low"}})
            continue
        res = VERIFIERS[name](t)
        if name == "Paylocity" and res.get("slug"):
            t = {**t, "slug": res["slug"]}
        if name == "ApplicantPro" and res.get("domain_id"):
            t = {**t, "domain_id": res["domain_id"]}
        checks.append({"platform": name, "tenant": t, "context": ctx, "where": where,
                       "res": {k: v for k, v in res.items() if k != "locs"},
                       "locs_sample": (res.get("locs") or [])[:8], "score": score_tenant(h, name, t, res, ctx)})
    return checks


def process(h: dict, osm: list, wd: list, search_budget: list) -> dict:
    lines: list = []
    rec = {k: h.get(k) for k in ("ccn", "name", "city", "state", "zip", "type", "own", "phone")}
    rec.update({"website": "", "site_source": "", "site_evidence": {}, "careers_urls": [], "platform": "",
                "tenant": {}, "verified_count": None, "check": {}, "confidence": "", "detections": [], "signals": {}})
    try:
        site, ats_hits = find_site(h, osm, wd, search_budget, lines)
        if site:
            rec["website"] = site["final"]
            rec["site_source"] = site.get("source", "")
            rec["site_evidence"] = {k: site[k] for k in ("phone", "street", "zip", "city", "name_title", "name_text", "score")}
            sc = careers_scan(h, site, ats_hits)
        else:
            sc = {"careers_urls": [], "signals": {},
                  "detections": [(n, t, "search", u) for u, _ in ats_hits for n, t, _ in detect_ats(u)]}
        rec["careers_urls"] = sc["careers_urls"]
        rec["signals"] = sc["signals"]
        checks = run_checks(h, sc["detections"])
        rec["detections"] = checks
        b = pick_best(checks)
        if b and b["platform"] in ("Form only", "Indeed only") and sc["signals"].get("job_words", 0) >= 8:
            b = None                     # a list of openings on the hospital's own pages, applied for by form
        if b and (site or b["score"]["confidence"] != "low"):
            rec["platform"] = b["platform"]
            rec["tenant"] = b["tenant"]
            rec["verified_count"] = b["res"].get("count")
            rec["check"] = b["score"]
            rec["confidence"] = b["score"]["confidence"]
            rec["careers_board_url"] = b["where"]
        elif site:
            rec["platform"] = classify_none(sc["signals"])
        else:
            rec["platform"] = "website not found"
    except Exception as e:  # noqa: BLE001
        rec["error"] = repr(e)[:300]
        rec["platform"] = rec["platform"] or "error"
    rec["log"] = lines
    return rec


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("hospitals", help="JSON list: ccn, name, city, state, zip, address, phone, type")
    ap.add_argument("--out", required=True, help="output JSON (resumable: done ccns are skipped)")
    ap.add_argument("--osm-dir", default="data/osm_hospitals")
    ap.add_argument("--wikidata", default="")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--search", type=int, default=300, help="max DuckDuckGo searches this run")
    ap.add_argument("--max", type=int, default=100000)
    args = ap.parse_args()
    H = json.load(io.open(args.hospitals, encoding="utf-8"))[: args.max]
    done = {}
    if os.path.exists(args.out):
        done = {r["ccn"]: r for r in json.load(io.open(args.out, encoding="utf-8"))}
    todo = [h for h in H if h["ccn"] not in done]
    osm = {}
    for st in sorted({h["state"] for h in todo}):
        p = os.path.join(args.osm_dir, f"{st}.json")
        osm[st] = json.load(io.open(p, encoding="utf-8")) if os.path.exists(p) else []
    wd_all = json.load(io.open(args.wikidata, encoding="utf-8")) if args.wikidata else []
    budget = [args.search]
    log(f"{len(todo)} to fingerprint ({len(done)} already done); search budget {args.search}")
    lock = threading.Lock()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(process, h, osm.get(h["state"], []), wd_all, budget): h for h in todo}
        for i, f in enumerate(as_completed(futs), 1):
            r = f.result()
            with lock:
                done[r["ccn"]] = r
                if i % 10 == 0 or i == len(todo):
                    tmp = args.out + ".tmp"
                    json.dump(list(done.values()), io.open(tmp, "w", encoding="utf-8"), indent=0, default=str)
                    os.replace(tmp, args.out)
            log(f"{i}/{len(todo)} {r['ccn']} {r['name'][:34]:34} {r['state']} | {r['site_source'] or '-':9} "
                f"{(r['website'] or '')[:40]:40} | {r['platform'][:22]:22} n={r['verified_count']} {r['confidence']}")
    json.dump(list(done.values()), io.open(args.out, "w", encoding="utf-8"), indent=0, default=str)
    c = Counter(r["platform"] for r in done.values())
    log(f"done: {len(done)}; searches used {_SEARCH_STATE['used']}; platforms {c.most_common(20)}")


if __name__ == "__main__":
    main()
