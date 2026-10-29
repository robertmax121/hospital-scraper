"""2026-10-29 (facility scale, dialysis chains): DaVita, Fresenius Medical Care
and US Renal Care rows name their clinic (facilities_dialysis; the owner: "i
thought you were gonna edit all the large multihospital systems").

Fixtures (tests/fixtures/dialysis1029/), real data read 2026-10-29, a few
seconds apart, no login:
  davita_refine.json  careers.davita.com refineSearch, the first 20 listings
                      (Phenom; applyUrl is the Workday apply step whose first
                      path segment is the clinic)
  fme_lists.json      two Fresenius CXS list pages (US facet), 20 postings
                      each, cut to the fields scrape_workday reads
  usrc_jobs.json      24 US Renal Care /api/jobs rows, one of each
                      location_name shape (clinic, corporate, work at home,
                      remote, New York "<town> Inc" entities)
  stored_rows.json    hospital_jobs rows (DATA project, read-only): one of
                      each DaVita and Fresenius site shape
No network, no database."""
import asyncio
import json
import os

import pytest

import facilities_dialysis as D
import scraper
from conftest import FIXTURES
from test_wd_tenant_facets import _fake_req

FIX = os.path.join(FIXTURES, "dialysis1029")


def _load(name):
    with open(os.path.join(FIX, name), encoding="utf-8") as f:
        return json.load(f)


DAVITA = _load("davita_refine.json")["jobs"]
FME = _load("fme_lists.json")["pages"]
USRC = _load("usrc_jobs.json")["jobs"]
STORED = {r["url"].split("/job/")[1].split("/")[0]: r for r in _load("stored_rows.json")["rows"]}


def _stored(seg_tail):
    for seg, r in STORED.items():
        if seg == seg_tail or seg.endswith("---" + seg_tail):
            return r
    raise KeyError(seg_tail)


def _resolve(r):
    return D.resolve_row(r["hospital_system"], hospital_name=r["hospital_name"], url=r["url"], city=r["city"],
                         state=r["state"], description=r.get("description_head", ""))


def _davita_job(doc):
    """The Job scrape_phenom builds from one list doc (its URL and town
    steps), still on the label as the adapter leaves it."""
    city, state = scraper.parse_city_state(f"{doc['city']}, {doc['state']}", town_first=True)
    url = scraper._workday_job_url(doc["applyUrl"])
    return scraper.Job(title=doc["title"], hospital_system="DaVita", hospital_name="DaVita", city=city or doc["city"],
                       state=state or doc["state"], location=doc["cityState"], specialty="", job_type="", url=url,
                       job_id=str(doc["jobId"]), posted_date="", description="", ats_platform="Phenom")


# ── DaVita ──────────────────────────────────────────────────────────────────

def test_davita_list_docs_name_the_clinic_from_the_workday_link():
    jobs = [_davita_job(d) for d in DAVITA]
    stats = D.apply_jobs(jobs)
    by = {j.url.split("/job/")[1].split("/")[0]: j for j in jobs}
    assert by["12530---Renal-Center-of-Lewisville"].hospital_name == "DaVita Renal Center of Lewisville"
    assert by["12143---Wise-County-Dialysis"].hospital_name == "DaVita Wise County Dialysis"
    assert by["05009---McKinney-Dialysis"].hospital_name == "DaVita McKinney Dialysis"
    assert by["00936---Heb-Dialysis-Center"].hospital_name == "DaVita Heb Dialysis Center"
    assert by["12527---Renal-Center-of-the-Hills"].hospital_name == "DaVita Renal Center of the Hills"
    # a clinic named by its town alone takes the brand's "<name> Dialysis"
    assert by["00923---Sherman"].hospital_name == "DaVita Sherman Dialysis"
    # a cut short Workday name is written out
    assert by["03209---UT-Southwstrn-Dallas-D"].hospital_name == "DaVita UT Southwestern Dallas Dialysis"
    # a regional float pool is no site: the label, its own town
    pool = by["19083---Western-Stars-R02-Labor-Float-Pool"]
    assert pool.hospital_name == "DaVita" and (pool.city, pool.state) == ("Coppell", "TX")
    # the posting's own town is never moved
    for d in DAVITA:
        j = by[scraper._workday_job_url(d["applyUrl"]).split("/job/")[1].split("/")[0]]
        assert j.state == "TX" and j.city.lower() == scraper.parse_city_state(
            f"{d['city']}, {d['state']}", town_first=True)[0].lower()
    assert stats["DaVita"]["clinic"] >= 17 and stats["DaVita"]["office"] >= 1


def test_davita_names_survive_finalize_and_normalize():
    rows = {r["job_id"]: r for r in scraper.finalize_jobs([_davita_job(d) for d in DAVITA])}
    lew = next(r for r in rows.values() if "Renal-Center-of-Lewisville" in r["url"])
    assert (lew["hospital_name"], lew["hospital_system"], lew["state"]) ==         ("DaVita Renal Center of Lewisville", "DaVita", "TX")
    pool = next(r for r in rows.values() if "Labor-Float-Pool" in r["url"])
    assert pool["hospital_name"] == "DaVita" and pool["city"] == "Coppell"


def test_davita_stored_shapes():
    assert _resolve(_stored("05107---Fort-Wayne-West"))[:3] == ("DaVita Fort Wayne West Dialysis", "Fort Wayne", "IN")
    assert _resolve(_stored("06243---Hunters-Creek-At-Home"))[0] == "DaVita Hunters Creek at Home"
    assert _resolve(_stored("06015---Lowry-at-Home"))[0] == "DaVita Lowry at Home"
    assert _resolve(_stored("00868---Leesburg-Dialysis-Cente"))[0] == "DaVita Leesburg Dialysis Center"
    assert _resolve(_stored("11430---INWOOD-DIALYSIS"))[0] == "DaVita Inwood Dialysis"
    assert _resolve(_stored("12511---Renal-Center-of-Newton"))[0] == "DaVita Renal Center of Newton"
    assert _resolve(_stored("02329---Emerald-Coast-FL"))[0] == "DaVita Emerald Coast Dialysis"
    assert _resolve(_stored("03202---Hospital-Hill-Dialysis"))[0] == "DaVita Hospital Hill Dialysis"
    assert _resolve(_stored("11008---Jersey-Village-Dialysis"))[0] == "DaVita Jersey Village Dialysis"


@pytest.mark.parametrize("seg,kind", [
    ("01076---HSHS-St-Elizabeths-Acute", "acute"),        # inside another employer's hospital
    ("03404H1---Yale-Acutes-Hospital-1", "acute"),
    ("09396---SNF-Dialysis-At-Parkville-Bloomfield", "snf"),
    ("04725---IKC-Local-MO-AR-TN-TN", "office"),          # Integrated Kidney Care, hybrid remote
    ("05555---Casa-del-Mundo-Office-Denver-HQ", "office"),
    ("02049---Reidsville-DeNovo", "office"),              # a clinic not open yet
])
def test_davita_non_clinics_keep_the_label_and_town(seg, kind):
    r = _stored(seg)
    name, city, state, how = _resolve(r)
    assert name is None and how == kind
    assert (city, state) == (r["city"], r["state"])


def test_davita_blank_town_takes_the_postings_street_address():
    r = dict(_stored("05107---Fort-Wayne-West"), city="", state="")
    assert _resolve(r)[1:3] == ("Fort Wayne", "IN")
    assert D.davita_address_town("Posting Date\n09/03/2026\n\n1 St Elizabeth Blvd, O Fallon, Illinois, "
                                 "62269-1099, United States of America\n\nMake") == ("O Fallon", "IL")


# ── Fresenius ───────────────────────────────────────────────────────────────

def _fme_crawl(monkeypatch, page):
    _fake_req(monkeypatch, lambda b: page if (b.get("offset") or 0) == 0 and b.get("limit") != 1 else
              {"total": len(page["jobPostings"]), "jobPostings": []})
    return asyncio.run(scraper.scrape_workday(None, D.FRESENIUS, scraper.WORKDAY_TENANTS[D.FRESENIUS]))


def test_fresenius_crawl_then_finalize_names_clinic_code_sites(monkeypatch):
    jobs = _fme_crawl(monkeypatch, FME["list_clinic"]) + _fme_crawl(monkeypatch, FME["list_first"])
    assert len(jobs) == 40
    locs = {j.job_id: j.location for j in jobs}          # the board's locationsText, before finalize
    # tonight's list parse reads "Clinic" for the town of a clinic-code site
    assert any(j.city == "Clinic" for j in jobs if D.fresenius_code(j.location))
    rows = {r["job_id"]: r for r in scraper.finalize_jobs(jobs)}
    by_loc = {}
    for jid, loc in locs.items():
        by_loc.setdefault(loc, []).append(rows[jid])
    peralta = by_loc["CA277 FMCNA Peralta RAI - Clinic"][0]
    assert peralta["hospital_name"] == "Fresenius Kidney Care Peralta" and peralta["state"] == "CA"
    assert not peralta["city"]                       # the name names no town: never guessed
    avondale = by_loc["AZ062 Avondale JV - Clinic"][0]
    assert (avondale["hospital_name"], avondale["city"], avondale["state"]) == \
        ("Fresenius Kidney Care Avondale", "Avondale", "AZ")
    pueblo = by_loc["CO019 FMCNA Pueblo West - Clinic"][0]
    assert pueblo["hospital_name"] == "Fresenius Kidney Care Pueblo West" and pueblo["state"] == "CO"
    # a town site names no clinic: the label and the board's own town
    for loc, town, st in (("Greensboro, NC", "Greensboro", "NC"), ("Sellersville, PA", "Sellersville", "PA"),
                          ("Lubbock, TX, USA", "Lubbock", "TX")):
        row = by_loc[loc][0]
        assert row["hospital_name"] == "Fresenius Medical Care" and (row["city"], row["state"]) == (town, st)
        assert row["hospital_system"] == "Fresenius Medical Care"


def test_fresenius_stored_shapes():
    # the stored "city" was the descriptor itself: blanked, the code's state
    assert _resolve(_stored("DE006-Central-Delaware---Clinic"))[:3] == \
        ("Fresenius Kidney Care Central Delaware", "", "DE")
    assert _resolve(_stored("DC002-Columbia-Heights---Clinic"))[:3] == \
        ("Fresenius Kidney Care Columbia Heights", "Washington", "DC")
    assert _resolve(_stored("IL295-FMCNA-Grayslake---Clinic"))[:3] == \
        ("Fresenius Kidney Care Grayslake", "Grayslake", "IL")
    # a street ("Webster Avenue"), a bay ("Marina Bay") and "Western Hartford"
    # (West Hartford is a town of its own) give no town
    assert _resolve(_stored("NY310-FMCNA-Webster-Avenue---Clinic"))[:3] == \
        ("Fresenius Kidney Care Webster Avenue", "", "NY")
    assert _resolve(_stored("CA155-FMCNA-Marina-Bay---Clinic"))[1] == ""
    assert _resolve(_stored("CT014-FMCNA-Western-Hartford---Clinic"))[1] == ""
    # an Oxnard clinic on a posting in Long Beach names no site (never guess)
    name, city, state, how = _resolve(_stored("CA019-Oxnard---Clinic"))
    assert name is None and (city, state) == ("Long Beach", "CA") and how == "clinic other town"
    # "Vascular Care LA": the label; the code's state replaces the parse's Louisiana
    assert _resolve(_stored("CA331-FMCNA-Vascular-Care-LA---Clinic"))[:3] == (None, "", "CA")
    # an acute service inside a hospital: the label, the hospital's town
    assert _resolve(_stored("PA336-Thomas-Jefferson-University-Hospital---Hospital"))[:3] == \
        (None, "Philadelphia", "PA")
    # a town site with no stored town takes the link's town; one with a town is left alone
    assert _resolve(_stored("Spokane-WA-USA"))[:3] == (None, "Spokane", "WA")
    assert _resolve(_stored("Greenville-NC")) is None


# ── US Renal Care ───────────────────────────────────────────────────────────

class _R:
    status = 200

    def __init__(self, data):
        self._d = data

    async def json(self, content_type=None):
        return self._d


class _Ctx:
    def __init__(self, r):
        self.r = r

    async def __aenter__(self):
        return self.r

    async def __aexit__(self, *a):
        return False


async def _no_wait():
    return None


def test_scrape_jibe_names_the_us_renal_care_clinic(monkeypatch):
    payload = {"jobs": [{"data": r} for r in USRC], "totalCount": len(USRC)}
    monkeypatch.setattr(scraper, "req", lambda session, method, url, **kw: _Ctx(_R(payload)))
    monkeypatch.setattr(scraper, "jitter", _no_wait)
    jobs = asyncio.run(scraper.scrape_jibe(None, D.USRC, scraper.JIBE_SITES[D.USRC]))
    by = {j.job_id: j for j in jobs}
    assert by["8425"].hospital_name == "U.S. Renal Care Oceanside Dialysis"
    assert (by["8425"].city, by["8425"].state) == ("Oceanside", "CA")
    assert by["8527"].hospital_name == "U.S. Renal Care East Fort Worth Dialysis"
    assert by["8542"].hospital_name == "U.S. Renal Care Norwood Dialysis" and by["8542"].city == "Cincinnati"
    # offices, work at home, remote and New York "<place> Inc" entities keep the label
    for rid in ("7995", "7484", "7392", "7853", "7680", "8356"):
        assert by[rid].hospital_name == D.USRC, rid
    assert all(j.hospital_system == D.USRC for j in jobs)


@pytest.mark.parametrize("raw,name", [
    ("1207 - Gulfgate Kidney Center TX", "U.S. Renal Care Gulfgate Kidney Center"),
    ("0012 - Plano Dialysis TX", "U.S. Renal Care Plano Dialysis"),
    ("0021 - McAllen TX", "U.S. Renal Care McAllen Dialysis"),
    ("TX00 - Dallas Corporate TX", None),
    ("9999 - Work @ Home ZZ", None),
    ("remote", None),
    ("", None),
])
def test_usrc_location_names(raw, name):
    assert D.usrc_facility(raw) == name


# ── scope ───────────────────────────────────────────────────────────────────

def test_only_rows_on_the_label_of_the_chains_change():
    r = _stored("05107---Fort-Wayne-West")
    assert D.resolve_row("DaVita", hospital_name="DaVita Fort Wayne West Dialysis", url=r["url"],
                         city="Fort Wayne", state="IN") is None
    other = scraper.Job(title="RN", hospital_system="Advocate Health", hospital_name="Advocate Health", city="", state="",
                        location="", specialty="", job_type="", url=r["url"], job_id="1", posted_date="",
                        description="", ats_platform="Workday")
    assert D.apply_jobs([other]) == {} and other.hospital_name == "Advocate Health"
    assert D.FRESENIUS not in scraper.WD_SITE_FACILITY       # placed in finalize, not by a site table
    assert scraper.JIBE_FACILITY_RESOLVER[D.USRC] is D.usrc_facility


def test_every_fixture_clinic_name_is_clean():
    names = set()
    for d in DAVITA:
        kind, name = D.davita_site(scraper._workday_job_url(d["applyUrl"]))
        if name:
            names.add(name)
    for page in FME.values():
        for p in page["jobPostings"]:
            got = D.fresenius_site(p["locationsText"])
            if got and got[1]:
                names.add(got[1])
    for r in USRC:
        n = D.usrc_facility(r["location_name"])
        if n:
            names.add(n)
    assert len(names) >= 25
    for n in names:
        assert n == n.strip() and "  " not in n and "-" not in n.split()[-1], n
        assert not n.endswith((" LLC", " Inc", " JV", " RAI", " TRC")), n
        assert n.split()[-1].upper() not in ("TX", "CA", "AZ", "CO", "NY", "IN"), n
