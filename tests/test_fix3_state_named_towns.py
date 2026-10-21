"""Fix 3 (2026-10-21, owner): towns named like a state.

Mercy's Jibe feed hands over city "Washington", state "Missouri" for Mercy
Hospital Washington (Washington MO); parse_city_state read the first state
name it met, so 97 active rows were filed in WA. The same rule filed
Fairview's Wyoming MN as WY, OSF's Washington IL as WA, MedStar's
California MD as CA, Bon Secours Mercy's Oregon OH as OR (Phenom), and so
on. BAYADA's Greenhouse "Washington, NJ 07882 | lat | lon" and ProMedica's
bare "Oregon" (Bay Park Hospital, Oregon OH) were the same mistake by other
roads; OhioHealth's Delaware OH bullet lost its town to the default.
"""
import asyncio

import pytest

import scraper
from scraper import Job, parse_city_state


@pytest.mark.parametrize("loc, want", [
    # the owner's examples, as the Jibe / Phenom feeds write them
    ("Washington, Missouri", ("Washington", "MO")),
    ("Washington, Missouri, United States", ("Washington", "MO")),
    ("Nevada, Missouri", ("Nevada", "MO")),
    ("NEVADA, Missouri", ("NEVADA", "MO")),
    ("Indiana, Pennsylvania", ("Indiana", "PA")),
    ("Delaware, Ohio", ("Delaware", "OH")),
    ("Wyoming, Michigan", ("Wyoming", "MI")),
    ("Wyoming, Minnesota", ("Wyoming", "MN")),
    ("Oregon, Ohio", ("Oregon", "OH")),
    ("WASHINGTON, Illinois", ("WASHINGTON", "IL")),
    ("Washington, Indiana", ("Washington", "IN")),
    ("Washington, Pennsylvania", ("Washington", "PA")),
    ("Washington, Georgia", ("Washington", "GA")),
    ("Virginia, Minnesota", ("Virginia", "MN")),
    # the District, every way the boards write it
    ("Washington, District of Columbia", ("Washington", "DC")),
    ("Washington, District of Columbia ", ("Washington", "DC")),
    ("Washington, D.C.", ("Washington", "DC")),
    ("Washington, DC", ("Washington", "DC")),
    # with the code, as before
    ("Washington, MO", ("Washington", "MO")),
    ("Nevada, MO", ("Nevada", "MO")),
    ("Indiana, PA", ("Indiana", "PA")),
    ("Delaware, OH", ("Delaware", "OH")),
    ("Wyoming, MI", ("Wyoming", "MI")),
    # a site name before a state-named town of the coded state
    ("Indiana Regional Medical Center, Indiana, PA", ("Indiana", "PA")),
])
def test_town_named_like_a_state(loc, want):
    assert parse_city_state(loc) == want


@pytest.mark.parametrize("loc, want", [
    # towns on no town list: only a caller that joined a city field and a
    # state field (Jibe, Phenom) says the first name is the town
    ("California, Maryland", ("California", "MD")),     # MedStar St. Mary's area
    ("Nevada, Texas", ("Nevada", "TX")),                # AccentCare
    ("Oregon, Illinois", ("Oregon", "IL")),             # OSF
    ("Washington, Missouri", ("Washington", "MO")),
    (", Missouri", ("Missouri", "MO")),                 # as before; normalize_job blanks it
    ("Washington, ", ("Washington", "WA")),
])
def test_city_and_state_fields(loc, want):
    assert parse_city_state(loc, town_first=True) == want


def test_free_text_list_of_states_reads_as_before():
    # Charlie Health's Greenhouse location for a two-state licence role
    assert parse_city_state("Texas, Louisiana ") == ("Texas", "TX")
    assert parse_city_state("California, Maryland") == ("California", "CA")


@pytest.mark.parametrize("loc, want", [
    ("Chicago, Illinois", ("Chicago", "IL")),
    ("New York, New York", ("New York", "NY")),
    ("Kansas City, Missouri", ("Kansas City", "MO")),
    ("Texas", ("Texas", "TX")),
    ("Washington", ("Washington", "WA")),
    ("West Virginia", ("West Virginia", "WV")),
    # a list of states is not a town and its state: read as before
    ("Kentucky, Ohio, Indiana", ("Kentucky", "KY")),
    ("Remote, Washington, Oregon", ("Washington", "WA")),
    ("Sicklerville, NJ 08081", ("Sicklerville", "NJ")),
    ("Algiers - New Orleans Region - Louisiana", ("Algiers", "LA")),
])
def test_other_locations_unchanged(loc, want):
    assert parse_city_state(loc) == want


def test_normalize_keeps_the_town_and_its_state():
    j = Job(title="RN", hospital_system="Mercy", hospital_name="Mercy Hospital Washington",
            city="Washington", state="MO", location="Washington, MO", specialty="", job_type="",
            url="https://careers.mercy.net/jobs/JR269146", job_id="JR269146", posted_date="",
            description="", ats_platform="iCIMS")
    d = scraper.normalize_job(j)
    assert (d["city"], d["state"], d["location"]) == ("Washington", "MO", "Washington, MO")


class _R:
    status = 200
    headers = {"content-type": "application/json"}

    def __init__(self, body):
        self._body = body

    async def json(self, content_type=None):
        return self._body

    async def text(self):
        return ""

    async def read(self):
        return b""


class _Ctx:
    def __init__(self, r):
        self.r = r

    async def __aenter__(self):
        return self.r

    async def __aexit__(self, *a):
        return False


def test_mercy_jibe_row_is_filed_in_missouri(monkeypatch):
    feed = {"totalCount": 2, "jobs": [
        {"data": {"req_id": "JR269146", "slug": "JR269146", "title": "Registered Nurse - ICU",
                  "city": "Washington", "state": "Missouri", "country": "United States",
                  "location_name": "Mercy Hospital Washington", "description": "<p>x</p>",
                  "meta_data": {"canonical_url": "https://careers.mercy.net/jobs/JR269146?lang=en-us"}}},
        {"data": {"req_id": "JR272739", "slug": "JR272739", "title": "Medical Assistant",
                  "city": "Washington", "state": "Missouri", "country": "United States",
                  "location_name": "Mercy Clinic Washington", "description": "<p>x</p>"}},
    ]}

    def fake_req(session, method, url, **kw):
        return _Ctx(_R(feed))

    async def no_wait():
        return None
    monkeypatch.setattr(scraper, "req", fake_req)
    monkeypatch.setattr(scraper, "jitter", no_wait)
    jobs = asyncio.run(scraper.scrape_jibe(None, "Mercy", "https://careers.mercy.net"))
    assert [(j.hospital_name, j.city, j.state) for j in jobs] == [
        ("Mercy Hospital Washington", "Washington", "MO"),
        ("Mercy Clinic Washington", "Washington", "MO")]
    d = scraper.normalize_job(jobs[0])
    assert (d["city"], d["state"]) == ("Washington", "MO")


@pytest.mark.parametrize("name, want", [
    ("Washington, NJ 07882 | 40.756493624 | -74.987585327", ("Washington", "NJ")),
    ("Washington, PA 15301 | 40.174941685 | -80.252478506", ("Washington", "PA")),
    ("Nevada, MO 64772 | 37.836049005 | -94.356290188", ("Nevada", "MO")),
    ("Sicklerville, NJ 08081 | 39.738815196 | -74.986871161", ("Sicklerville", "NJ")),
    ("Washington, District of Columbia ", ("Washington", "DC")),
    ("Washington, D.C.", ("Washington", "DC")),
    ("Washington, D.C", ("Washington", "DC")),
    ("Indiana, PA 15701 | 40.622348628 | -79.162165964", ("Indiana", "PA")),
    ("Texas, Louisiana ", ("Texas", "TX")),
])
def test_greenhouse_reads_the_text_before_the_coordinates(name, want):
    assert scraper._greenhouse_city_state({"location": {"name": name}}) == want


def test_workday_bullet_town_named_like_a_state():
    # OhioHealth, Grady Memorial (Delaware OH), 2026-10-21 list read
    oh = ["Regular", "JR156929", "Physician Office", "Ohio", "43015", "DELAWARE"]
    assert scraper._wd_bullet_location(oh) == ("Delaware", "OH")
    assert scraper._wd_bullet_location(["R1", "Washington", "Pennsylvania"]) == ("Washington", "PA")
    # unchanged: a second copy of the state is no town, a posted bullet neither
    assert scraper._wd_bullet_location(["R4452218", "Arizona", "Arizona"]) == ("", "AZ")
    assert scraper._wd_bullet_location(["R1", "Wyoming", "Posted 30+ Days Ago"]) == ("", "WY")
    assert scraper._wd_bullet_location(["Casual", "JR1", "Core Lab", "Ohio", "45891", "VAN WERT"])[1] == "OH"


def test_promedica_bay_park_is_oregon_ohio():
    city, state = parse_city_state("Oregon")
    assert scraper._wd_facility("ProMedica", "Oregon", city, state) == (None, "Oregon", "OH")
    # the tenant's other sites are untouched
    assert scraper._wd_facility("ProMedica", "Toledo", "Toledo", "") == (None, "Toledo", "")


def test_commonspirit_slug_fallback_la_crosse_is_wisconsin():
    assert scraper.COMMONSPIRIT_CITY_STATE["la-crosse"] == "WI"
    assert scraper.COMMONSPIRIT_CITY_STATE["la crosse"] == "WI"
