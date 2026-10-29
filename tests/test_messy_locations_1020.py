"""2026-10-20 (owner: "fix the messy locations"): the city column held
buildings, offices, facilities, worker types and parse debris, the facility
field held street addresses, and some Workday tenants put sites in the wrong
state. Values below are the live ones of 2026-10-20 (active rows), and the
list / facet shapes are the tenants' own CXS responses that day.

Covered: city_utils (not_a_place, is_plausible_city, clean_city, the known
towns, embedded_town, town_window, site_leftover); parse_city_state
(Richmond's "(Staten Island, NY)", Ochsner's "Town - X Region - State",
"AU Medical Center, Inc."); _wd_bullet_location (OhioHealth's bullets);
_wd_facility (Freeman, Trinity MMCIA, Wellstar, Summit BHC, NYP, Sentara,
KU, Sanford, Rochester); normalize_job (street-address facility, a refused
city's town, the home-town gate) and the worker-process state."""
import pytest

import city_utils as CU
import scraper
from scraper import Job

NOT_TOWNS = [
    "Regular", "Casual", "Provisional (Fixed Term)", "Temporary (Fixed Term)", "PRN",
    "Digital Innovation Office", "Memorial Executive Office Building", "Olathe Medical Pavilion A", "Offices",
    "Tower Bldg", "Clinton Crossings Building D", "Suite A", "Unit B", "Georgia Mobile Unit", "EHC Location",
    "Default Location", "Varied Location", "BUILDING A", "System Office", "Emergency Department",
    "Wellstar MCG Health", "Peak View Behavioral Health", "Twin Lakes Recovery Center", "English Mountain Recovery",
    "Brown Health Medical Group", "Business Operations Center", "Memorial Support Services", "Stow Health and Wellness",
    "MUSC HEALTH AT TIDELANDS", "Freeman Heart and Vascular Institute", "Joplin Urgent Care", "Long Pond Data Center",
    "The Ranch at Dove Tree", "Inc.", "LLC", "NY)", "Green Building)", "NYP/Hudson Valley", "Place", "WORK AT HOME",
    "USA", "Select a Location; West Chester", "Business Center", "The Farley Center",
]
TOWNS = [
    "Rockville Centre", "Sun City Center", "Sioux Center", "Centre Hall", "Brooklyn Center", "Mansfield Center",
    "Lewis Center", "State Center", "Hale Center", "Pavilion", "Institute", "Medicine Lodge", "Medical Lake",
    "Tower City", "Willow Street", "Raleigh/Durham", "Winston-Salem", "Coeur d'Alene", "King of Prussia",
    "St. Louis", "Columbus", "West Des Moines", "Staten Island", "New York", "Center Point", "Independence",
]


def _job(city, state, system, name=None, loc="", title="Registered Nurse"):
    return Job(title=title, hospital_system=system, hospital_name=name or system, city=city, state=state,
               location=loc, specialty="", job_type="", url="https://x.test/job/1", job_id="1",
               posted_date="", description="", ats_platform="Workday")


def test_not_a_place_and_is_plausible_city_on_the_live_values():
    for c in NOT_TOWNS:
        assert not CU.is_plausible_city(c), c
        assert CU.clean_city(c) == "", c
    for c in TOWNS:
        assert CU.is_plausible_city(c), c
    # the rules before 2026-10-20 still answer for the gate in normalize_job
    assert CU.is_plausible_city("Regular", legacy=True)
    assert CU.clean_city("Digital Innovation Office", legacy=True) == "Digital Innovation Office"
    assert CU.clean_city("Akron General Medical Center", legacy=True) == ""
    # a known town before a bracketed site is the town
    assert CU.clean_city("Knoxville (Middlebrook Pike)") == "Knoxville"
    assert CU.clean_city("Huntsville (Cochran Center)") == "Huntsville"


def test_known_towns_file_and_runtime_towns():
    assert CU.known_town("Olathe", "KS") == "Olathe"
    assert CU.known_town("Sioux Center") == "Sioux Center"
    assert CU.known_town("Business Center") is None
    assert CU.known_town("Maryland", "MD") is None, "a state's own name is no town of it"
    assert CU.known_town("Zzqx Falls", "OH") is None
    CU.set_runtime_towns([("ZZQX FALLS", "OH")])
    assert CU.known_town("Zzqx Falls", "OH") == "Zzqx Falls"
    assert ("Zzqx Falls", "OH") in CU.runtime_towns()
    CU.set_runtime_towns([])
    assert CU.known_town("Zzqx Falls", "OH") is None, "a run's towns replace the last run's"


def test_embedded_town_and_town_window():
    assert CU.embedded_town("Bridgeport Medical Office Building", "WV") == "Bridgeport"
    assert CU.embedded_town("Olathe Medical Pavilion A", "KS") == "Olathe"
    assert CU.embedded_town("Sentara Office Building Williamsburg", "VA") == "Williamsburg"
    assert CU.embedded_town("Memorial Executive Office Building", "FL") == ""
    assert CU.embedded_town("Great Oaks Recovery Center", "TX") == "", "Center, TX is no site's town"
    assert CU.embedded_town("Sentara Independence", "VA") == "", "the rest must be a site word"
    assert CU.town_window("MUSC Health Summerville FSED", "SC", "MUSC Health") == "Summerville"
    assert CU.town_window("MUSC Health Bluffton Medical Pavilion", "SC", "MUSC Health") == "Bluffton"
    assert CU.town_window("Stow Health and Wellness", "OH", "Cleveland Clinic") == ""
    assert CU.site_leftover("Stow Health and Wellness", "Cleveland Clinic") == ["Stow"]
    assert CU.site_leftover("Memorial Executive Office Building", "Memorial Healthcare System") == []
    assert CU.site_leftover("Varied Location") == ["Varied Location"]
    assert CU.site_leftover("USA") == ["USA"]


def test_parse_city_state_richmond_ochsner_and_inc():
    assert scraper.parse_city_state("Richmond University Medical Center (Staten Island, NY)") == ("Staten Island", "NY")
    assert scraper.parse_city_state("Algiers - New Orleans Region - Louisiana") == ("Algiers", "LA")
    assert scraper.parse_city_state("Breaux Bridge - Acadiana Region - Louisiana") == ("Breaux Bridge", "LA")
    assert scraper.parse_city_state("Houma - Houma/Raceland Region - Louisiana") == ("Houma", "LA")
    assert scraper.parse_city_state("Decatur - Central Mississippi Region - Mississippi") == ("Decatur", "MS")
    city, st = scraper.parse_city_state("Slidell Memorial Hospital - Northshore Region - Louisiana")
    assert (city, st) == ("Slidell Memorial Hospital", "LA"), "the site, for the CMS lookup; never the region"
    assert scraper.parse_city_state("AU Medical Center, Inc.")[0] != "Inc."
    # unchanged shapes
    assert scraper.parse_city_state("Saint Francis Hospital - Hartford, CT") == ("Hartford", "CT")
    assert scraper.parse_city_state("Ochsner Health Center - Lake Terrace") == ("Lake Terrace", "")


def test_bullet_location_takes_the_town_after_the_state():
    oh = ["Regular", "JR158436", "Food Production", "Ohio", "43215", "COLUMBUS"]
    assert scraper._wd_bullet_location(oh) == ("Columbus", "OH")
    assert scraper._wd_bullet_location(["Casual", "JR1", "Core Lab", "Ohio", "45891", "VAN WERT"])[1] == "OH"
    assert scraper._wd_bullet_location(["Casual", "JR1", "Core Lab", "Ohio", "45891", "VAN WERT"])[0].lower() == "van wert"
    assert scraper._wd_bullet_location(["R4446997", "Arizona", "Sun City West"]) == ("Sun City West", "AZ")
    assert scraper._wd_bullet_location(["Regular", "JR1", "Ohio"]) == ("", "OH"), "never the worker type"
    assert scraper._wd_bullet_location(["JR1"]) == ("", "")


@pytest.mark.parametrize("system,loc,parsed,expect", [
    ("Freeman Health System", "Freeman Bentonville Hospital", ("Freeman Bentonville Hospital", ""), (None, "Bentonville", "AR")),
    ("Freeman Health System", "Freeman Springdale Hospital", ("Freeman Springdale Hospital", ""), (None, "Springdale", "AR")),
    ("Freeman Health System", "Freeman Willow Creek Hospital", ("Freeman Willow Creek Hospital", ""), (None, "Johnson", "AR")),
    ("Freeman Health System", "Freeman Cardiology - Bentonville", ("Bentonville", ""), (None, "Bentonville", "AR")),
    ("Freeman Health System", "1111 McIntosh Cir David A. Ball, MD, Medical Building", ("David A. Ball", "MD"), (None, "Joplin", "MO")),
    ("Freeman Health System", "1905 W. 32nd Street Thousand Oaks", ("Thousand Oaks", ""), (None, "Joplin", "MO")),
    ("Freeman Health System", "Freeman Fort Scott (KS)", ("Freeman Fort Scott", "KS"), ("Freeman Fort Scott", "Fort Scott", "KS")),
    ("Freeman Health System", "Freeman Clinic of Baxter Springs (KS)", ("Freeman Clinic of Baxter Springs", "KS"), (None, "Baxter Springs", "KS")),
    # 2026-10-29 (owner calls): an Iowa Heart Center site shows the system
    # with its town; Elkader's hospital its own name
    ("Trinity Health", "MMCIA - MercyOne IHC Ottumwa Central IA", ("MMCIA - MercyOne IHC Ottumwa Central IA", ""), (None, "Ottumwa", "IA")),
    ("Trinity Health", "MMCIA - MercyOne Elkader Central Community Hospital", ("MMCIA - MercyOne Elkader Central Community Hospital", ""), ("MercyOne Elkader Medical Center", "Elkader", "IA")),
    ("Trinity Health", "MMCIA - MercyOne Des Moines Medical Center Central IA", ("MMCIA - MercyOne Des Moines Medical Center Central IA", ""), ("MercyOne Des Moines Medical Center", "Des Moines", "IA")),
    ("Trinity Health", "St. Mary's Health Care System, Georgia", ("St. Mary's Health Care System", "GA"), (None, "Athens", "GA")),
    ("Wellstar Health", "Wellstar MCG Health", ("Wellstar MCG Health", ""), (None, "Augusta", "GA")),
    ("Wellstar Health", "Wellstar MCG Health Warm Springs, Inc.", ("Wellstar MCG Health Warm Springs", ""), (None, "Warm Springs", "GA")),
    ("Wellstar Health", "AU Medical Center, Inc.", ("AU Medical Center", ""), (None, "Augusta", "GA")),
    ("Wellstar Health", "1800 Parkway Place", ("Place", ""), (None, "Marietta", "GA")),
    ("Wellstar Health (Providers)", "Cobb Hospital", ("Cobb Hospital", ""), (None, "Austell", "GA")),
    # 2026-10-25 coverage stage 2: a Summit site that is a CMS hospital takes its own name.
    ("Summit BHC", "Peak View Behavioral Health", ("Peak View Behavioral Health", ""), ("Peak View Behavioral Health", "Colorado Springs", "CO")),
    ("Summit BHC", "The Farley Center", ("The Farley Center", ""), (None, "Williamsburg", "VA")),
    ("Summit BHC", "Fair Oaks Recovery Center", ("Fair Oaks Recovery Center", ""), (None, "Sacramento", "CA")),
    ("NewYork-Presbyterian", "NYP/Hudson Valley", ("NYP/Hudson Valley", ""), (None, "Cortlandt Manor", "NY")),
    ("NewYork-Presbyterian", "NYP/Queens Medical Group", ("NYP/Queens Medical Group", ""), (None, "Flushing", "NY")),
    ("Sentara Healthcare", "Sentara Independence", ("Sentara Independence", ""), (None, "Virginia Beach", "VA")),
    ("The University of Kansas Health System", "Olathe Medical Pavilion A", ("Olathe Medical Pavilion A", ""), (None, "Olathe", "KS")),
    ("The University of Kansas Health System", "Kansas Drive Medical Pavilion", ("Kansas Drive Medical Pavilion", ""), (None, "Paola", "KS")),
    ("Sanford Health", "Marsh Med Ctr", ("Marsh Med Ctr", ""), (None, "Marshfield", "WI")),
    ("Sanford Health", "GSS KS Hays Ctr", ("GSS KS Hays Ctr", ""), (None, "Hays", "KS")),
    ("Sanford Health", "GSS NE Omaha Millard Ctr", ("GSS NE Omaha Millard Ctr", ""), (None, "Omaha", "NE")),
    ("University of Rochester", "Clinton Crossings Building D", ("Clinton Crossings Building D", ""), (None, "Rochester", "NY")),
])
def test_wd_facility_site_maps(system, loc, parsed, expect):
    assert scraper._wd_facility(system, loc, *parsed) == expect


def test_existing_map_semantics_hold():
    # a prefix hit still only fills what the parse left blank
    assert scraper._wd_facility("Trinity Health", "Mount Carmel East", "Columbus", "OH") == ("Mount Carmel East", "Columbus", "OH")
    assert scraper._wd_facility("North Mississippi Medical", "Tupelo - North MS Medical Center", "North MS Medical Center", "") \
        == ("North Mississippi Medical Center", "Tupelo", "MS")


def _norm(city, state, system, name=None, loc=""):
    d = scraper.normalize_job(_job(city, state, system, name, loc))
    return d["hospital_name"], d["city"], d["state"], d["location"]


def test_normalize_job_street_address_facility_takes_the_system():
    assert _norm("TAMPA", "FL", "Orlando Health", "2727 W DR MARTIN LUTHER KING")[:3] == ("Orlando Health", "TAMPA", "FL")
    assert _norm("Philadelphia", "PA", "Penn Medicine", "3535 Market Street")[0] == "Penn Medicine"
    assert _norm("Smithtown", "NY", "X Health", "360 Behavioral Health")[0] == "360 Behavioral Health"


def test_normalize_job_refused_city_takes_its_town_or_keeps_none():
    # the town the site names
    assert _norm("Bridgeport Medical Office Building", "WV", "WVU Medicine")[1:3] == ("Bridgeport", "WV")
    assert _norm("Knoxville (Middlebrook Pike)", "TN", "Fast Pace Health")[1] == "Knoxville"
    # a site string of site words only takes the system's home town (same state)
    assert _norm("Memorial Executive Office Building", "FL", "Memorial Healthcare System")[1:] == ("Hollywood", "FL", "Hollywood, FL")
    assert _norm("Regular", "OH", "OhioHealth")[1] == "Columbus"
    # a site string with another word may name a town the list lacks: no guess
    assert _norm("Stow Health and Wellness", "OH", "Cleveland Clinic")[1:3] == ("", "OH")
    assert _norm("Sentara Therapy Center Quinton", "VA", "Sentara Healthcare")[1:3] == ("", "VA")
    assert _norm("Varied Location", "NJ", "RWJBarnabas Health")[1:3] == ("", "NJ")
    # a value the old rules refused keeps the old fallback
    assert _norm("Akron General Medical Center", "OH", "Cleveland Clinic")[1] == "Akron"
    # a real town is untouched
    assert _norm("Rockville Centre", "NY", "Catholic Health (Long Island)")[1] == "Rockville Centre"
    assert _norm("Sun City Center", "FL", "HCA Healthcare")[1] == "Sun City Center"


def test_normalize_job_system_named_city():
    # the system's own first word opens a site name: no town
    assert _norm("Sentara Independence", "VA", "Sentara Healthcare")[1] == ""
    assert _norm("Akumin Orlando", "FL", "Akumin")[1] in ("", "Orlando")
    assert _norm("Mercyhealth Burchard Hills- Freeport", "IL", "Mercyhealth")[1] == "Freeport"
    # a known town that shares the system's first word stays
    assert _norm("Rochester Hills", "MI", "Rochester Regional Health")[1] == "Rochester Hills"


def test_cms_facility_name_in_the_city_column_and_the_brand_rule():
    scraper.set_cms_lookup([
        {"hospital_name": "PALMS BEHAVIORAL HEALTH", "city": "HARLINGEN", "state": "TX"},
        {"hospital_name": "THOMAS MEMORIAL HOSPITAL", "city": "SOUTH CHARLESTON", "state": "WV"},
        {"hospital_name": "ST MARY'S HOSPITAL", "city": "ATHENS", "state": "GA"},
    ])
    assert _norm("Palms Behavioral Health", "TX", "Summit BHC")[1] == "Harlingen"
    # "Thomas Medical Office Pavilion": Thomas Memorial is in South Charleston
    assert scraper._town_in_site("Thomas Medical Office Pavilion", "WV") == ""
    assert scraper._town_in_site("St. Mary's Health Care System", "GA") == ""
    assert scraper._town_in_site("Bridgeport Medical Office Building", "WV") == "Bridgeport"


def test_worker_processes_get_the_run_state():
    scraper.set_cms_lookup([{"hospital_name": "ZZQX GENERAL HOSPITAL", "city": "ZZQX FALLS", "state": "OH"}])
    towns, brands, cms_towns = CU.runtime_towns(), dict(scraper._CMS_NAME_TOWNS), frozenset(scraper._CMS_TOWNS)
    assert ("Zzqx Falls", "OH") in towns and brands and cms_towns
    scraper.set_cms_lookup([])
    assert CU.known_town("Zzqx Falls", "OH") is None and not scraper._CMS_NAME_TOWNS
    scraper._finalize_worker_init({}, {}, towns, brands, cms_towns)
    assert CU.known_town("Zzqx Falls", "OH") == "Zzqx Falls"
    assert scraper._CMS_NAME_TOWNS == brands and scraper._CMS_TOWNS == cms_towns
    scraper.set_cms_lookup([])
    # the pool seam hands all three over
    import inspect
    src = inspect.getsource(scraper._finalize_pool)
    assert "runtime_towns()" in src and "_CMS_NAME_TOWNS" in src and "_CMS_TOWNS" in src
