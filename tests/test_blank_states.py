"""Blank-state recovery, 2026-09-17: location formats the parser missed,
Workday facility maps, the state facet fill, and the iCIMS facility map."""
import scraper
from scraper import parse_city_state, _wd_facility, _wd_apply_states, _wd_apply_locations, _wd_facet_groups, _parse_icims_cards


def test_dash_and_parenthesis_formats():
    assert parse_city_state("Rochester - NY") == ("Rochester", "NY")
    assert parse_city_state("Dallas - TX (LBJ FWY)") == ("Dallas", "TX")
    assert parse_city_state("MOUNT PLEASANT (SC)") == ("MOUNT PLEASANT", "SC")
    assert parse_city_state("Remote Work - New York")[1] == "NY"


def test_facility_city_state_strings():
    assert parse_city_state("Saint Francis Hospital - Hartford, CT") == ("Hartford", "CT")
    assert parse_city_state("St. Peter's Hospital - Albany, New York") == ("Albany", "NY")
    assert parse_city_state("MercyOne North Iowa Medical Center - East Campus, Mason City, Iowa") == ("Mason City", "IA")
    assert parse_city_state("Saint Joseph Mercy Health System Hospital Campus - Ann Arbor, Mi") == ("Ann Arbor", "MI")


def test_state_code_dash_part():
    assert parse_city_state("Kennebunk, ME - Huntington Common") == ("Kennebunk", "ME")
    assert parse_city_state("Broken Arrow - OK") == ("Broken Arrow", "OK")


def test_intermountain_prefixed_facility():
    assert _wd_facility("Intermountain Health (IMH)", "Intermountain Health Alta View Hospital", "", "") == \
        ("Alta View Hospital", "Sandy", "UT")
    assert _wd_facility("Intermountain Health (IMH)", "Intermountain Health Intermountain Medical Center", "", "") == \
        ("Intermountain Medical Center", "Murray", "UT")
    assert _wd_facility("Trinity Health", "Mount Carmel East", "", "")[0] == "Mount Carmel East"


def test_facet_groups_flatten_nested_location_groups():
    data = {"facets": [
        {"facetParameter": "timeType", "values": [{"descriptor": "Full time", "id": "1", "count": 5}]},
        {"facetParameter": "locationMainGroup", "values": [
            {"descriptor": "Location Region/State/Province", "facetParameter": "locationRegionStateProvince",
             "values": [{"descriptor": "Washington", "id": "w", "count": 9}]},
            {"descriptor": "Locations", "facetParameter": "locations",
             "values": [{"descriptor": "Auburn, Washington", "id": "a", "count": 9}]},
        ]},
    ]}
    groups = _wd_facet_groups(data)
    assert [(p, label) for p, label, _ in groups] == [
        ("timeType", ""), ("locationRegionStateProvince", "Location Region/State/Province"), ("locations", "Locations")]


def test_apply_locations_fills_city_and_state():
    a, b = _J(), _J("", "WA")
    ext = {id(a): "/a", id(b): "/b"}
    n = _wd_apply_locations([a, b], ext, {"/a": "Auburn", "/b": "Spokane"}, {"/a": "WA"}, None)
    assert n == (1, 0)
    assert (a.city, a.state, b.city, b.state) == ("Auburn", "WA", "Spokane", "WA")


def test_existing_formats_unchanged():
    assert parse_city_state("Houston, TX") == ("Houston", "TX")
    assert parse_city_state("Irving, TX, United States") == ("Irving", "TX")
    assert parse_city_state("Chicago, Illinois") == ("Chicago", "IL")
    assert parse_city_state("TX, Irving") == ("Irving", "TX")
    assert parse_city_state("Houston, Texas Medical Center, TX") == ("Houston", "TX")
    assert parse_city_state("Remote") == ("Remote", "")
    assert parse_city_state("") == ("", "")


def test_wd_facility_map_fills_only_blanks():
    assert _wd_facility("WVU Medicine", "Ruby Memorial Hospital (WVUH)", "", "") == \
        ("J.W. Ruby Memorial Hospital", "Morgantown", "WV")
    assert _wd_facility("WVU Medicine", "Uniontown Hospital (UNTWN)", "", "") == \
        ("Uniontown Hospital", "Uniontown", "PA")
    # a parsed city wins over the map's city; the facility label still applies
    assert _wd_facility("WVU Medicine", "Camden Clark Ambulance", "Vienna", "WV") == \
        ("Camden Clark Medical Center", "Vienna", "WV")
    assert _wd_facility("Trinity Health", "SJHSYR-MAINCAMPUS", "", "") == \
        ("St. Joseph's Health Hospital", "Syracuse", "NY")
    assert _wd_facility("Trinity Health", "MNEIA  - Waterloo Medical Center", "", "") == \
        ("MercyOne Waterloo Medical Center", "Waterloo", "IA")
    assert _wd_facility("University of Rochester", "Physician Office Center", "", "") == (None, "", "")
    assert _wd_facility("No Such Tenant", "Anything", "", "") == (None, "", "")


class _J:
    def __init__(self, city="", state=""):
        self.city, self.state = city, state


def test_state_facet_then_default():
    a, b, c, d = _J(), _J("Toledo", ""), _J("Boise", "ID"), _J()
    jobs = [a, b, c, d]
    ext = {id(a): "/job/x/a", id(b): "/job/x/b", id(c): "/job/x/c", id(d): "/job/x/d"}
    path_state = {"/job/x/a": "MI", "/job/x/b": "OH"}
    by_facet, by_default = _wd_apply_states(jobs, ext, path_state, ("Morgantown", "WV"))
    assert (by_facet, by_default) == (2, 1)
    assert (a.state, b.state, b.city) == ("MI", "OH", "Toledo")
    assert (c.city, c.state) == ("Boise", "ID")          # located rows untouched
    assert (d.city, d.state) == ("Morgantown", "WV")     # default only for the leftovers
    # multi-state tenant without a default: leftovers stay blank
    e = _J()
    assert _wd_apply_states([e], {id(e): "/job/x/e"}, {}, None) == (0, 0)
    assert e.state == ""


COVENANT_CARD = '''
<li class="iCIMS_JobCardItem"> <div class="row">
<div class="col-xs-12 title"> <a href="https://careers-covenanthealth.icims.com/jobs/51234/rn/job" class="iCIMS_Anchor">
<span class="sr-only field-label">Title</span> <h3 > Registered Nurse</h3> </a> </div>
<div class="col-xs-12 description"> Fort Sanders Regional is a 541-bed hospital.</div>
<dl class="iCIMS_JobHeaderGroup">
<div class="iCIMS_JobHeaderTag"> <dt class="iCIMS_JobHeaderField">Facility</dt> <dd class="iCIMS_JobHeaderData"><span > Fort Sanders Regional Medical Center</span> </dd> </div>
<div class="iCIMS_JobHeaderTag"> <dt class="iCIMS_JobHeaderField">Type</dt> <dd class="iCIMS_JobHeaderData"><span > Full-Time</span> </dd> </div>
</dl> </div> </li>
'''


def test_covenant_facility_from_dl_gives_city():
    j = _parse_icims_cards(COVENANT_CARD, "Covenant Health", "careers-covenanthealth.icims.com")[0]
    assert j.hospital_name == "Fort Sanders Regional Medical Center"
    assert (j.city, j.state) == ("Knoxville", "TN")
    assert j.job_type == "Full-Time"


def test_defaults_exist_for_the_blank_systems():
    for k in ("wvu medicine", "multicare", "covenant health", "ohsu", "kettering health",
              "legacy health", "promedica", "university of rochester"):
        assert k in scraper.SYSTEM_LOCATION_DEFAULTS
    assert scraper.SYSTEM_CITY_STATE["promedica"]["monroe"] == "MI"
    assert scraper.SYSTEM_CITY_STATE["legacy health"]["vancouver"] == "WA"
