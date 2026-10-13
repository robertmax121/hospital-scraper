"""2026-10-13 (fix/locations-other): hidden locations on non-Workday boards,
the McLeod Health mislabel, and postings outside the US.

Oracle: a PrimaryLocation without a city ("SC, United States") takes the town
from the requisition's work location (fixture: one live page of McLeod's
erym/CX_1 site, trimmed). UKG: a posting with no Address.City whose location
name is the town (Excela, Griffin) or the one hospital (Wyckoff) gets that
town. Non-US: Fresenius Medical Care and Sunrise Senior Living location
strings and Workday paths read off their live boards on 2026-10-13."""
import asyncio
import json
import os

import aiohttp
import pytest
from aiohttp import web

import scraper
from scraper import Job
from test_list_resilience import _Redirect, _fast, _no_proxies, _serve

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "locations_other")


def _req(primary, work=(), country="US", **kw):
    j = {"Id": kw.get("id", "1"), "Title": kw.get("title", "RN"), "PrimaryLocation": primary,
         "workLocation": {"items": [dict(zip(("TownOrCity", "Region2", "Country"), w)) for w in work]}}
    if country is not None:
        j["PrimaryLocationCountry"] = country
    return j


# ── Oracle: work-location fallback ─────────────────────────────────────────

def test_oracle_state_only_primary_takes_the_work_location_town():
    assert scraper._oracle_location(_req("SC, United States", [("Loris", "SC", "US")])) == \
        ("SC, United States", "Loris", "SC")


def test_oracle_located_primary_is_kept_as_it_was():
    assert scraper._oracle_location(_req("Florence, SC, United States", [("Myrtle Beach", "SC", "US")]))[1:] == \
        ("Florence", "SC")


def test_oracle_country_only_primary_takes_town_and_state():
    assert scraper._oracle_location(_req("United States", [("Dillon", "South Carolina", "US")]))[1:] == \
        ("Dillon", "SC")


def test_oracle_work_location_in_another_state_is_not_used():
    assert scraper._oracle_location(_req("SC, United States", [("Charlotte", "NC", "US")]))[1:] == ("", "SC")


def test_oracle_two_towns_give_no_city_but_one_state():
    two = [("Florence", "SC", "US"), ("Loris", "SC", "US")]
    assert scraper._oracle_location(_req("SC, United States", two))[1:] == ("", "SC")
    assert scraper._oracle_location(_req("United States", two))[1:] == ("", "SC")
    split = [("Florence", "SC", "US"), ("Charlotte", "NC", "US")]
    assert scraper._oracle_location(_req("United States", split))[1:] == ("", "")


def test_oracle_non_us_work_location_is_ignored():
    assert scraper._oracle_location(_req("United States", [("Toronto", "ON", "CA")]))[1:] == ("", "")


def test_oracle_country_field():
    assert scraper._oracle_non_us(_req("Toronto, ON, Canada", country="CA"))
    assert not scraper._oracle_non_us(_req("Dillon, SC, United States", country="US"))
    assert not scraper._oracle_non_us(_req("San Juan, PR", country="PR"))           # a US territory
    assert scraper._oracle_non_us(_req("", [("Manila", "", "PH")], country=None))
    assert not scraper._oracle_non_us(_req("", [("Dillon", "SC", "US")], country=None))
    assert not scraper._oracle_non_us(_req("", country=None))


def test_scrape_oracle_fills_hidden_rows_from_the_fixture(monkeypatch):
    """The real adapter against a localhost copy of McLeod's page."""
    _no_proxies(monkeypatch)
    _fast(monkeypatch)
    page = json.load(open(os.path.join(FIX, "oracle_mcleod_page.json"), encoding="utf-8"))
    page["items"][0]["requisitionList"]["items"].append(
        {"Id": "999", "Title": "Nurse (Canada)", "PrimaryLocation": "Toronto, ON, Canada",
         "PrimaryLocationCountry": "CA", "workLocation": {"items": []}})

    async def reqs(request):
        return web.json_response(page)

    async def go():
        runner, base = await _serve([("GET", "/hcmRestApi/resources/latest/recruitingCEJobRequisitions", reqs)])
        try:
            async with aiohttp.ClientSession() as s:
                return await scraper.scrape_oracle(_Redirect(s, base), "McLeod Health",
                                                   scraper.ORACLE_ORGS["McLeod Health"])
        finally:
            await runner.cleanup()
    rows = asyncio.run(go())
    assert len(rows) == 8                                    # the Canadian posting is not listed
    assert all(r.state == "SC" and r.city for r in rows)
    by_id = {r.job_id: (r.city, r.location) for r in rows}
    assert by_id["27906"] == ("Florence", "SC, United States")   # was ("", "SC")
    assert by_id["27912"][0] == "Florence"                     # no work location: PrimaryLocation as before
    assert {r.city for r in rows} == {"Florence", "Loris", "Little River"}
    assert all(r.hospital_system == "McLeod Health" for r in rows)


# ── McLeod relabel ─────────────────────────────────────────────────────────

def test_erym_is_mcleod_health():
    assert scraper.ORACLE_ORGS["McLeod Health"] == ("https://erym.fa.us6.oraclecloud.com", "CX_1")
    assert "EvergreenHealth" not in scraper.ORACLE_ORGS
    assert "EvergreenHealth" not in scraper.configured_feed_map()
    assert "EvergreenHealth" not in scraper.HOSPITAL_SYSTEM_ALIASES
    assert "EvergreenHealth" not in scraper.HOSPITAL_SYSTEM_ALIASES.values()
    hosts = [v[0] for v in scraper.ORACLE_ORGS.values()]
    assert hosts.count("https://erym.fa.us6.oraclecloud.com") == 1


# ── UKG: location name as the town ─────────────────────────────────────────

EXCELA = scraper.UKG_ORGS["Excela Health"]
GRIFFIN = scraper.UKG_ORGS["Griffin Health"]                  # GRI1004GHSC
WYCKOFF = scraper.UKG_ORGS["Wyckoff Heights Medical Center"]  # WYC1000WHMC


def _opp(name=None, desc=None, city=None, state="PA", country="USA", oid="o1"):
    addr = {"City": city, "State": {"Code": state} if state else None, "Country": {"Code": country} if country else None}
    return {"Id": oid, "Title": "RN", "Locations": [{"Address": addr, "LocalizedName": name,
                                                       "LocalizedDescription": desc}]}


def _ukg(org, system, opp):
    return scraper._ukg_job(opp, system, org[0], org[1], org[2] if len(org) > 2 else "")


def test_ukg_excela_town_names():
    for name, city in (("Latrobe", "Latrobe"), ("Butler", "Butler"), ("Delmont", "Delmont"),
                       ("Butler City", "Butler"), ("East Butler", "East Butler"),
                       ("Slippery Rock Boro", "Slippery Rock"), ("Seven Fields Boro", "Seven Fields")):
        j = _ukg(EXCELA, "Excela Health", _opp(desc=name))
        assert (j.city, j.state) == (city, "PA"), name
        assert j.hospital_name == "Excela Health", name          # a town is not a facility
        assert scraper.normalize_job(j)["city"] == city, name     # and is not blanked as one


def test_ukg_griffin_and_wyckoff():
    j = _ukg(GRIFFIN, "Griffin Health", _opp(desc="Shelton", state="CT"))
    assert (j.city, j.state, j.hospital_name) == ("Shelton", "CT", "Griffin Health")
    j = _ukg(GRIFFIN, "Griffin Health", _opp(desc="Shelton", state=None))
    assert (j.city, j.state) == ("Shelton", "CT")
    j = _ukg(WYCKOFF, "Wyckoff Heights Medical Center",
             _opp(name="Wyckoff Heights Medical Center", desc="Wyckoff Heights Medical C", state="NY"))
    assert (j.city, j.state, j.hospital_name) == ("Brooklyn", "NY", "Wyckoff Heights Medical Center")
    row = scraper.normalize_job(j)
    assert (row["city"], row["state"], row["location"]) == ("Brooklyn", "NY", "Brooklyn, NY")


def test_ukg_griffin_located_town_postings_stay_listed():
    # Live Shelton postings carry Address.City "Shelton" and LocalizedName
    # "Shelton": the town name is not the facility, so normalize_job keeps the
    # city instead of blanking it as equal to hospital_name.
    for town in ("Shelton", "Derby"):
        for opp in (_opp(name=town, desc=town, city=town, state="CT"),
                    _opp(desc=town, city=town, state="CT"),
                    _opp(name=town, city=town, state="CT")):
            j = _ukg(GRIFFIN, "Griffin Health", opp)
            assert (j.city, j.state, j.hospital_name) == (town, "CT", "Griffin Health"), (town, opp)
            row = scraper.normalize_job(j)
            assert (row["city"], row["state"], row["hospital_system"]) == (town, "CT", "Griffin Health")
            assert row["hospital_name"] == "Griffin Health"
    # Before the fix the row came out with no city (hidden on the site).
    raw = scraper._ukg_job(_opp(name="Shelton", desc="Shelton", city="Shelton", state="CT"),
                           "Griffin Health", "https://recruiting.ultipro.com/NOT_A_TABLE_BOARD", "g")
    assert raw.hospital_name == "Shelton" and scraper.normalize_job(raw)["city"] == ""


def test_ukg_located_swap_stays_narrow():
    # A facility name that is not exactly one of the board's towns stays.
    j = _ukg(EXCELA, "Excela Health", _opp(desc="Butler Hospital", city="Butler"))
    assert (j.city, j.hospital_name) == ("Butler", "Butler Hospital")
    # Another board's "Shelton" location keeps its name (no table for it).
    j = _ukg(scraper.UKG_ORGS["Deaconess Health"], "Deaconess Health", _opp(desc="Shelton", city="Shelton", state="IN"))
    assert j.hospital_name == "Shelton"
    # Wyckoff's table entry is the hospital itself, not a town: its name stays.
    j = _ukg(WYCKOFF, "Wyckoff Heights Medical Center",
             _opp(name="Wyckoff Heights Medical Center", city="Brooklyn", state="NY"))
    assert (j.city, j.hospital_name) == ("Brooklyn", "Wyckoff Heights Medical Center")


def test_ukg_boards_carry_their_real_systems():
    # WYC1000WHMC is Wyckoff Heights (Brooklyn NY), GRI1004GHSC is Griffin
    # Health (Derby / Shelton CT): neither board is Wyoming County Community
    # (Warsaw NY) nor Grinnell Regional (IA).
    assert scraper.UKG_ORGS["Wyckoff Heights Medical Center"][0] == "https://recruiting.ultipro.com/WYC1000WHMC"
    assert scraper.UKG_ORGS["Griffin Health"][0] == "https://recruiting.ultipro.com/GRI1004GHSC"
    feeds = scraper.configured_feed_map()
    for old in ("Wyoming County Community", "Grinnell Regional Medical"):
        assert old not in scraper.UKG_ORGS
        assert old not in feeds
        assert old not in scraper.HOSPITAL_SYSTEM_ALIASES
        assert old not in scraper.HOSPITAL_SYSTEM_ALIASES.values()
    bases = [v[0].rstrip("/").rsplit("/", 1)[-1] for v in scraper.UKG_ORGS.values()]
    assert bases.count("WYC1000WHMC") == 1 and bases.count("GRI1004GHSC") == 1
    # The town tables are keyed by board code, so they follow the relabel.
    assert set(scraper.UKG_LOCATION_TOWNS) <= set(bases)


def test_ukg_town_rules_stay_narrow():
    # A located posting keeps its own city; a town-name location is still not
    # a facility.
    j = _ukg(EXCELA, "Excela Health", _opp(desc="Latrobe", city="Greensburg"))
    assert j.city == "Greensburg" and j.hospital_name == "Excela Health"
    # Another board's "Latrobe" is not Excela's.
    j = _ukg(scraper.UKG_ORGS["Deaconess Health"], "Deaconess Health", _opp(desc="Latrobe"))
    assert j.city == ""
    # A state that disagrees with the table is left alone.
    j = _ukg(EXCELA, "Excela Health", _opp(desc="Latrobe", state="OH"))
    assert (j.city, j.state) == ("", "OH")
    # Only exact names: "Butler Hospital" is not the town.
    j = _ukg(EXCELA, "Excela Health", _opp(desc="Butler Hospital"))
    assert j.city == ""


def test_ukg_country_field():
    assert scraper._ukg_non_us(_opp(desc="Toronto", state="ON", country="CAN"))
    assert not scraper._ukg_non_us(_opp(desc="Latrobe"))
    assert not scraper._ukg_non_us(_opp(desc="Latrobe", country=None))
    assert not scraper._ukg_non_us({"Id": "x", "Locations": []})


# ── Postings outside the US ────────────────────────────────────────────────

FME = "https://freseniusmedicalcare.wd3.myworkdayjobs.com/fme/job/"
SUN = "https://sunriseseniorliving.wd12.myworkdayjobs.com/SUNRISE_EXT_CAREERS/job/"

NON_US = [
    ("Bonifacio Global City, PHL", FME + "Bonifacio-Global-City-PHL/Senior-Accountant_R0270899"),
    ("Krosno, Poland", FME + "Krosno-Poland/Pielegniarka_R1"),
    ("Bengaluru, KA", FME + "Bengaluru-KA/Analyst_R2"),
    ("Chennai, TN", FME + "Chennai-TN/Analyst_R3"),                       # TN is Tamil Nadu here
    ("Delhi, DL", FME + "Delhi-DL/Analyst_R4"),
    ("Baja California, Tijuana, MEX", FME + "Baja-California-Tijuana-MEX/Operador_R5"),
    ("Rio de Janeiro, RJ, BR", FME + "Rio-de-Janeiro-RJ-BR/Enfermeiro_R6"),
    ("3 Locations", FME + "Maca-RJ-BR/Tecnico_R7"),
    ("2 Locations", FME + "Heilbronn-Germany/Pflege_R8"),
    ("6 Locations", FME + "Gurugram-HR/Manager_R9"),
    ("4 Locations", FME + "Porto---Sede-Portugal/Enfermeiro_R10"),
    ("GBR Hereford – Clinic", FME + "GBR-Hereford--Clinic/Nurse_R11"),
    ("2 Locations", FME + "CHN-FCH-Guangzhou-Office---Office/Specialist_R12"),
    ("Pathumwan, BKK", FME + "Pathumwan-BKK/Nurse_R13"),
    ("2 Locations", FME + "Watthana-BKK/Renal-Nurse_R0271462"),
    ("Amphoe Chom Thong, CMI", FME + "Amphoe-Chom-Thong-CMI/Nurse_R14"),
    ("Quanzhou,Fujian", FME + "QuanzhouFujian/Nurse_R15"),
    ("Rochdale, United Kingdom", FME + "Rochdale-United-Kingdom/Nurse_R16"),
    ("Richmond Hill, ON - Sunrise of Richmond Hill", SUN + "Richmond-Hill-ON---Sunrise-of-Richmond-Hill/Care-Manager_JR1"),
    ("North Vancouver, BC - Sunrise at Lonsdale Square", SUN + "North-Vancouver-BC---Sunrise-at-Lonsdale-Square/Cook_JR2"),
    ("Dollard - Des Ormeaux, QC - Sunrise of Dollard", SUN + "Dollard---Des-Ormeaux-QC---Sunrise-of-Dollard/Cook_JR3"),
    ("Sunrise of Oakville", SUN + "Oakville-ON---Sunrise-of-Oakville/Cook_JR4"),
]

US = [
    ("Eunice, LA", FME + "Eunice-LA/Biomedical-Technician_R0268922"),
    ("Kennebunk, ME - Huntington Common", SUN + "Kennebunk-ME---Huntington-Common/Care-Manager_JR122360"),
    ("Sunrise of Fair Oaks", SUN + "Sunrise-of-Fair-Oaks/Care-Manager_JR128667"),
    ("Mexico, MO", ""), ("Poland, OH", ""), ("Delhi, NY", ""), ("Toronto, OH", ""),
    ("London, KY", ""), ("Dublin, OH", ""), ("Brazil, IN", ""), ("Vancouver, WA", ""),
    ("Albuquerque, New Mexico", "https://x.wd5.myworkdayjobs.com/s/job/Albuquerque-New-Mexico/RN_1"),
    ("Remote", "https://x.wd5.myworkdayjobs.com/s/job/Remote---Philadelphia-PHL/RN_1"),
    ("2 Locations", "https://x.wd5.myworkdayjobs.com/s/job/IND-Indianapolis-Campus/RN_1"),
    ("San Juan, PR", ""), ("Lexington, KY", "https://x.wd5.myworkdayjobs.com/s/job/UK-Chandler-Hospital/RN_1"),
]


@pytest.mark.parametrize("loc,url", NON_US)
def test_non_us_postings_are_named(loc, url):
    city, state = scraper.parse_city_state(loc)
    assert scraper.non_us_reason(loc, city, state, url), (loc, city, state)


@pytest.mark.parametrize("loc,url", US)
def test_us_postings_pass(loc, url):
    city, state = scraper.parse_city_state(loc)
    assert scraper.non_us_reason(loc, city, state, url) is None, (loc, city, state)


def _wd(system, loc, url, jid):
    city, state = scraper.parse_city_state(loc)
    return Job(title="Registered Nurse", hospital_system=system, hospital_name=system, city=city, state=state,
               location=loc, specialty="", job_type="", url=url, job_id=jid, posted_date="", description="",
               ats_platform="Workday")


def test_finalize_drops_non_us_rows_only():
    jobs = [_wd("Fresenius Medical Care", loc, url, f"N{i}") for i, (loc, url) in enumerate(NON_US)]
    jobs += [_wd("Fresenius Medical Care", "Eunice, LA", FME + "Eunice-LA/RN_R1", "U1"),
             _wd("Sunrise Senior Living", "Kennebunk, ME - Huntington Common",
                 SUN + "Kennebunk-ME---Huntington-Common/Care-Manager_JR122360", "U2")]
    rows = scraper.finalize_jobs(jobs)
    assert sorted(r["job_id"] for r in rows) == ["U1", "U2"]
    # The dropped rows still count as emitted: the system returned rows tonight.
    assert "Fresenius Medical Care" in scraper.EMITTED_SYSTEMS
