"""VA facility names on USAJOBS rows (2026-10-27). No network, no database.

The owner, on a Clearwater, FL posting: "for VA jobs, using 'va hospitals' is
not acceptable. Scrape the real facility name." 1,288 of the 3,473 active
"VA Hospitals" rows named only the agency, and the rest spelled their
facilities 630 ways. va_facilities gives one name per VA station, or per
medical center when the value names one with a name of its own (rule of
2026-10-27); usajobs_va_facility names each VA row (hiring-organization line, the stored
name, the summary, the town, in that order). Every raw value below is
quoted from a stored hospital_jobs row or a usajobs.gov job page read
2026-10-27 (claud_outputinput/va_facility_2026-10-27/page_facts.tsv).
"""
import pytest

import scraper
import va_facilities as V


# ── 1. One name per station ─────────────────────────────────────────────────

@pytest.mark.parametrize("raw, state, want", [
    # the owner's example (job 887865200): the hiring-organization line
    ("Bay Pines VA Healthcare System", "FL", "Bay Pines VA Healthcare System"),
    # the North Florida/South Georgia pair and its other spellings
    ("North Florida/South Georgia VA Healthcare System", "FL", "North Florida/South Georgia Veterans Health System"),
    ("North Florida/South Georgia Veterans Health System", "GA", "North Florida/South Georgia Veterans Health System"),
    ("North Florida/ South Georgia VA Healthcare System", "FL", "North Florida/South Georgia Veterans Health System"),
    ("North Florida/South Georgia Health Care System", "FL", "North Florida/South Georgia Veterans Health System"),
    # a trailing acronym, a town, a department
    ("VA North Texas Health Care System (VANTHCS)", "TX", "VA North Texas Health Care System"),
    ("VA North Texas Health Care System (VANTHCS) -Dallas VA Health Care System", "TX",
     "VA North Texas Health Care System"),
    ("VA North Texas Health Care System, Fort Worth, TX", "TX", "VA North Texas Health Care System"),
    ("South Texas Veterans Health Care System (STVHCS) - North West San Antonio VA Clinic", "TX",
     "South Texas Veterans Health Care System"),
    ("Kansas City VA Medical Center - Sterile Processing Services", "MO", "Kansas City VA Medical Center"),
    ("Tennessee Valley Healthcare System (TVHS) | Servcie: Anesthesiology", "TN", "Tennessee Valley Healthcare System"),
    ("Michael E. DeBakey VA Medical Center; Houston, TX", "TX", "Michael E. DeBakey VA Medical Center"),
    ("John J. Pershing VAMC, Poplar Bluff, MO. 63901", "MO", "John J. Pershing VA Medical Center"),
    ("Louis A. Johnson VA Medical Center is located in Clarksburg, West Virginia", "WV",
     "Louis A. Johnson VA Medical Center"),
    # the agency or a network in front
    ("Veterans Health Administration-Bronx, NY VAMC", "NY", "James J. Peters VA Medical Center"),
    ("Veterans' Health Administration/Oklahoma City VA Health Care System/Nursing/Acute Care", "OK",
     "Oklahoma City VA Health Care System"),
    ("VA Midwest Health Care Network (VISN 23), Fargo VA Health Care System", "ND", "Fargo VA Health Care System"),
    ("VA Heartland Healthcare Network (VISN 15) - Kansas City VA Healthcare System - Medical Sub Specialty", "KS",
     "Kansas City VA Medical Center"),
    ("Purpose. The John J. Pershing VA Medical Center", "MO", "John J. Pershing VA Medical Center"),
    # a name with ", Jr." and a nickname
    ("Edward Hines, Jr. VA Hospital", "IL", "Edward Hines Jr. VA Hospital"),
    ("G.V Sonny Montgomery VA Medical Center", "MS", "G.V. (Sonny) Montgomery VA Medical Center"),
    # typos the VA's own lines carry
    ("Mann-Granstaff VA Medical Center", "WA", "Mann-Grandstaff VA Medical Center"),
    ("Celment J. Zablocki VA Medical Center", "WI", "Clement J. Zablocki VA Medical Center"),
    ("Pharmacy Services - Altanta VA Healthcare System", "GA", "Atlanta VA Health Care System"),
    # a town and its state beside a bare facility word
    ("Beckley, West Virginia VA Medical Center", "WV", "Beckley VA Medical Center"),
    ("VA Medical Center, Lexington KY", "KY", "Lexington VA Health Care System"),
    ("Martinsburg West Virginia VA Medical Center", "WV", "Martinsburg VA Medical Center"),
    # a medical center with a name of its own keeps it (rule of 2026-10-27, HOSPITALS)
    ("C.W. Bill Young VA Medical Center", "FL", "C.W. Bill Young VA Medical Center"),
    ("Baltimore VA Medical Center", "MD", "Baltimore VA Medical Center"),
    ("Malcom Randall Department of Veterans Affairs Medical Center", "FL", "Malcom Randall VA Medical Center"),
    ("Tibor Rubin VA Medical Center", "CA", "Tibor Rubin VA Medical Center"),
    # a state that is the name
    ("VA Maine Healthcare System - Geriatrics and Extended Care", "ME", "VA Maine Healthcare System"),
    ("Washington DC VA Medical Center", "DC", "Washington DC VA Medical Center"),
    ("Central Alabama Veterans Health Care Systems (CAVHCS)", "AL", "Central Alabama Veterans Health Care System"),
    # the agency contact address of a job page
    ("VA TX Valley Coastal Bend HCS", "TX", "VA Texas Valley Coastal Bend Health Care System"),
    ("VA Nebraska-Western Iowa HCS - Omaha", "NE", "VA Nebraska-Western Iowa Health Care System"),
    ("Chalmers P. Wylie VA ACC", "OH", "VA Central Ohio Healthcare System"),
    ("San Francisco VA Medical Facility", "CA", "San Francisco VA Health Care System"),
])
def test_every_spelling_gives_the_stations_one_name(raw, state, want):
    assert V.canonical(raw, state) == want


@pytest.mark.parametrize("raw", [
    "Veterans Health Administration", "Department of Veterans Affairs", "", None,
    "TX VA Medical Center", "AL VA Medical Center", "Ohio Veterans Affairs Medical Center",
    "VA New York Healthcare System",
])
def test_the_agency_and_a_bare_facility_word_name_no_facility(raw):
    assert V.canonical(raw, "TX") == ""
    assert V.is_generic(raw, "TX")


def test_a_name_two_stations_share_needs_the_state():
    # Fayetteville, NC: the station's former name (one medical center); Fayetteville, AR: the Ozarks' hospital
    assert V.canonical("Fayetteville VA Medical Center", "NC") == "Fayetteville VA Coastal Health Care System"
    assert V.canonical("Fayetteville VA Medical Center", "AR") == "Fayetteville VA Medical Center"
    assert V.station_for("Fayetteville VA Medical Center", "AR") == "Veterans Healthcare System of the Ozarks"
    assert V.station_for("Fayetteville VA Medical Center") == ""
    assert V.hospital_for("Fayetteville VA Medical Center") == ""


def test_a_town_on_a_bare_facility_word_is_that_towns_station():
    # Veterans Canteen Service summaries: "VCS Optical Shops at the Lake Charles LA VA Medical Center"
    assert V.canonical("Lake Charles LA VA Medical Center", "LA") == "Alexandria VA Health Care System"


# ── 2. VA units that hire in their own name ─────────────────────────────────

@pytest.mark.parametrize("raw, want", [
    ("National Oncology Program", "VA National Oncology Program"),
    ("The National Tele-Critical Care Program", "VA National Tele-Critical Care Program"),
    ("National CARDIO-VET Network", "VA National CARDIO-VET Network"),
    ("VISN 4 VA Heart of America Health Network", "VISN 4 VA Heart of America Health Network"),
    ("VA Mid-Atlantic Health Care Network (VISN 6)", "VA Mid-Atlantic Health Care Network"),
    ("VHA Central Office (VHACO), SCI/D NPO", "VHA Central Office"),
    ("Readjustment Counseling Services (RCS)", "VA Readjustment Counseling Service"),
    ("Veterans Canteen Service", "Veterans Canteen Service"),
    ("VISN 12 - VA Great Lakes Health Care System", "VA Great Lakes Health Care System"),
    # a department or a site of a station is not one: the town names it
    ("Pharmacy Service", ""),
    ("Radiology", ""),
    ("Veterans Health Administration, Office of Research and Development", ""),
    ("Veterans Health Administration, Care Management", ""),
    ("Community Base Out-patient Clinics (CBOC), Waynesville or Marshfield, Missouri", ""),
    ("Hamilton CBOC and Tinton Falls Community Based Outpatient Clinic", ""),
    ("VHA VISN 20 Human Resources", ""),
])
def test_hiring_units_keep_their_own_name(raw, want):
    assert V.hiring_org_name(raw, "") == want


@pytest.mark.parametrize("raw, state, want", [
    ("San Francisco Veterans Health Administration", "CA", "San Francisco VA Health Care System"),
    ("C. W. Bill Young Veterans Health Administration", "FL", "C.W. Bill Young VA Medical Center"),
    ("Maryland Veterans Health Care Administration", "MD", "VA Maryland Health Care System"),
    ("Charleston Veterans Health Administration", "SC", "Ralph H. Johnson VA Health Care System"),
    ("Veterans Health Administration, VANCHCS, Pharmacy", "CA", "VA Northern California Healthcare System"),
])
def test_a_place_or_an_acronym_beside_the_agency_names_the_station(raw, state, want):
    assert V.hiring_org_name(raw, state) == want


# ── 3. Summaries ────────────────────────────────────────────────────────────

@pytest.mark.parametrize("text, want", [
    ("Serve as a Physician-Primary Care-Section Chief for Eastern Colorado Health Care System (ECHCS) located in "
     "Pueblo, Colorado.", "VA Eastern Colorado Health Care System"),
    ("The position is located within the Greater Los Angeles Healthcare System within the Community Engagement",
     "Greater Los Angeles VA Health Care System"),
    ("This position is located at the North Pinellas VA Clinic located in Clearwater, Florida. The Physician CMO is "
     "to direct ... Primary Care in the CWBY Bay Pines VAMC and its community-based outpatient clinics (CBOCs).",
     "Bay Pines VA Healthcare System"),
    ("This position is eligible for the Education Debt Reduction Program (EDRP).", ""),
    ("The Eastern Colorado VA is currently recruiting", ""),
])
def test_a_summary_names_a_station_by_any_of_its_names(text, want):
    assert V.text_station(text, "") == want


# ── 4. Towns ────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("city, state, want", [
    ("Clearwater", "FL", "Bay Pines VA Healthcare System"),
    ("Colorado Springs", "CO", "VA Eastern Colorado Health Care System"),
    ("Kings", "NY", "VA NY Harbor Healthcare System"),
    ("Saint Louis", "MO", "VA St. Louis Health Care System"),
    ("Durham County", "NC", "Durham VA Medical Center"),
    ("", "DC", "Washington DC VA Medical Center"),
    ("White City", "OR", "VA Southern Oregon Rehabilitation Center and Clinics"),
    # a clinic across a state line from its station (the job pages' contact address)
    ("Kenosha", "WI", "Captain James A. Lovell Federal Health Care Center"),
    ("Fort Detrick", "MD", "Martinsburg VA Medical Center"),
    ("Hyannis", "MA", "VA Providence Healthcare System"),
    # towns left out: not certain (two candidates) or no VA site
    ("Vallejo", "CA", ""),
    ("Westchester", "IL", ""),
    ("Springfield", "IL", ""),
    ("", "", ""),
    ("Clearwater", "", ""),
])
def test_town_facility_only_where_certain(city, state, want):
    assert V.town_facility(city, state) == want


def test_the_tables_hold_together():
    names = [s[0] for s in V.STATIONS]
    assert len(names) == len(set(names))
    for name, states, others in V.STATIONS:
        for st in states.split():
            assert V.canonical(name, st) == name
            for o in others:
                assert V.canonical(o, st) == name, (o, st)
    for (town, st), name in V.TOWNS.items():
        assert name in V.STATION_NAMES, name
        assert st in V.STATION_STATES[name], (town, st, name)
        assert town == V._town_key(town)
    for acr, name in V.ACRONYMS.items():
        assert name in V.STATION_NAMES, acr
    hospitals = [h[0] for h in V.HOSPITALS]
    assert len(hospitals) == len(set(hospitals)) and not set(hospitals) & V.STATION_NAMES
    for name, station, others in V.HOSPITALS:
        assert station in V.STATION_NAMES, name
        for st in V.STATION_STATES[station]:
            for o in (name,) + tuple(others):
                assert V.canonical(o, st) == name, (o, st)
                assert V.station_for(o, st) == station, (o, st)
        assert V.station_of(name) == station and not V.is_town_only(name)


# ── 5. The adapter ──────────────────────────────────────────────────────────

def _m(sub="", summary="", qual="", org="Veterans Health Administration"):
    return {"OrganizationName": org, "QualificationSummary": qual,
            "UserArea": {"Details": {"SubAgencyName": sub, "JobSummary": summary}}}


def test_va_name_order(monkeypatch):
    monkeypatch.setattr(scraper, "_STORED_USAJOBS", {})
    f = scraper.usajobs_va_facility
    # 1. a facility the location names ahead of its town
    assert f(_m("Bay Pines VA Healthcare System"), "Tampa", "FL", "James A. Haley Veterans' Hospital") == (
        "James A. Haley VA Medical Center", "location")
    # 2. the hiring-organization line
    assert f(_m("Bay Pines VA Healthcare System"), "Clearwater", "FL") == ("Bay Pines VA Healthcare System",
                                                                           "hiring_org")
    assert f(_m("National Oncology Program"), "Boston", "MA") == ("VA National Oncology Program", "hiring_org")
    # 4. the summary, 5. the town, 6. the agency
    assert f(_m("Veterans Health Administration", "The Ralph H. Johnson VA Medical Center is seeking a nurse."),
             "Savannah", "GA") == ("Ralph H. Johnson VA Health Care System", "text")
    assert f(_m("Veterans Health Administration", "This position is eligible for EDRP."), "Colorado Springs",
             "CO") == ("VA Eastern Colorado Health Care System", "town")
    assert f(_m("", "Work at a VA Medical Center near you."), "Vallejo", "CA") == (
        "Veterans Health Administration", "agency")


def test_a_stored_facility_is_kept_when_the_posting_names_none(monkeypatch):
    scraper.set_stored_usajobs_names([
        {"hospital_system": "VA Hospitals", "job_id": "CBSS-13065141-26-TS", "hospital_name": "John J. Pershing VA Medical Center"},
        {"hospital_system": "VA Hospitals", "job_id": "CBFF-1-OCA-vallejo-ca", "hospital_name": "Veterans Health Administration"},
        {"hospital_system": "Indian Health Service", "job_id": "X1", "hospital_name": "Gallup Indian Medical Center"},
    ])
    try:
        assert scraper._STORED_USAJOBS == {"CBSS-13065141-26-TS": "John J. Pershing VA Medical Center"}
        f = scraper.usajobs_va_facility
        # Cape Girardeau, MO: the page's contact address named John J. Pershing (README, page_contact)
        assert f(_m(), "Cape Girardeau", "MO", "", "CBSS-13065141-26-TS") == (
            "John J. Pershing VA Medical Center", "stored")
        # the hiring-organization line still wins over the stored name
        assert f(_m("Veterans Canteen Service"), "Cape Girardeau", "MO", "", "CBSS-13065141-26-TS") == (
            "Veterans Canteen Service", "hiring_org")
        # a stored agency label is no facility
        assert f(_m(), "Vallejo", "CA", "", "CBFF-1-OCA-vallejo-ca")[1] == "agency"
    finally:
        scraper.set_stored_usajobs_names([])


def test_stored_names_load_nothing_without_credentials(monkeypatch):
    monkeypatch.delenv("SUPABASE_URL", raising=False)
    assert scraper.load_stored_usajobs_names() == 0
    assert scraper._STORED_USAJOBS == {}


def _item(pid, sub, city, state, summary="", control="887865200", org="Veterans Health Administration"):
    return {"MatchedObjectId": control, "MatchedObjectDescriptor": {
        "PositionID": pid, "PositionTitle": "Physician (Primary Chief Medical Officer)",
        "PositionURI": f"https://www.usajobs.gov:443/job/{control}",
        "PositionLocation": [{"LocationName": f"{city}, {state}", "CountryCode": "United States",
                              "CountrySubDivisionCode": state, "CityName": f"{city}, {state}"}],
        "OrganizationName": org, "DepartmentName": "Department of Veterans Affairs",
        "JobCategory": [{"Name": "Medical Officer", "Code": "0602"}],
        "PositionSchedule": [{"Name": "Full-time", "Code": "1"}],
        "QualificationSummary": "Basic requirements.",
        "PositionRemuneration": [{"MinimumRange": "250000", "MaximumRange": "300000", "RateIntervalCode": "PA"}],
        "PublicationStartDate": "2026-10-20T00:00:00.0000",
        "UserArea": {"Details": {"JobSummary": summary, "SubAgencyName": sub}},
    }}


def test_the_owners_clearwater_posting(monkeypatch):
    monkeypatch.setattr(scraper, "_STORED_USAJOBS", {})
    rows = scraper._usajobs_jobs(_item("CWBY-26-PCMO", "Bay Pines VA Healthcare System", "Clearwater", "Florida",
                                       "This position is located at the North Pinellas VA Clinic located in "
                                       "Clearwater, Florida."), "VA Hospitals")
    assert [(j.hospital_system, j.hospital_name, j.city, j.state) for j in rows] == [
        ("VA Hospitals", "Bay Pines VA Healthcare System", "Clearwater", "FL")]


def test_an_agency_only_posting_takes_its_towns_station(monkeypatch):
    monkeypatch.setattr(scraper, "_STORED_USAJOBS", {})
    stats = {}
    rows = scraper._usajobs_jobs(_item("CBSU-13088508-27-TS", "Veterans Health Administration", "Colorado Springs",
                                       "Colorado", control="888999200"), "VA Hospitals", stats)
    assert [(j.hospital_system, j.hospital_name) for j in rows] == [
        ("VA Hospitals", "VA Eastern Colorado Health Care System")]
    assert stats.get("va_town") == 1


def test_a_networks_name_on_a_one_town_posting_is_the_towns_station(monkeypatch):
    monkeypatch.setattr(scraper, "_STORED_USAJOBS", {})
    f = scraper.usajobs_va_facility
    # VISN 12's HR files North Chicago's cardiologist under the network's name (job 883327300)
    assert f(_m("VISN 12 - VA Great Lakes Health Care System"), "North Chicago", "IL", "", "", 1) == (
        "Captain James A. Lovell Federal Health Care Center", "town")
    # a network office's own posting lists many towns: the network is the employer (job 889893100)
    assert f(_m("VISN 4 VA Heart of America Health Network"), "Albuquerque", "NM", "", "", 20) == (
        "VISN 4 VA Heart of America Health Network", "hiring_org")
    # a one-town network posting in a town the table does not know keeps the network
    assert f(_m("VISN 12- VA Great Lakes Health Care System"), "La Crosse", "WI", "", "", 1) == (
        "VA Great Lakes Health Care System", "hiring_org")
    # a national program is not a network
    assert f(_m("National CARDIO-VET Network"), "Goshen", "NY", "", "", 1) == (
        "VA National CARDIO-VET Network", "hiring_org")
    assert V.is_network("VA Mid-Atlantic Health Care Network") and not V.is_network("Bay Pines VA Healthcare System")


def test_a_town_on_a_bare_facility_word_is_never_the_name(monkeypatch):
    monkeypatch.setattr(scraper, "_STORED_USAJOBS", {})
    # Veterans Canteen Service, Tyler, TX (job 890099600): the hiring line names VCS and the summary
    # "VCS Optical Shops at the Tyler TX VA Medical Center", where no VA medical center stands
    m = _m("Veterans Canteen Service", "Health Tech (Opticians) ... at the Tyler TX VA Medical Center .")
    assert scraper.usajobs_va_facility(m, "Tyler", "TX") == ("Veterans Canteen Service", "hiring_org")
    # without the hiring line, the town's station when the table knows the town ...
    m = _m("", "Health Tech (Opticians) ... at the Tyler TX VA Medical Center .")
    assert scraper.usajobs_va_facility(m, "Tyler", "TX") == ("VA North Texas Health Care System", "text")
    # ... and never the made-up "Springfield VA Medical Center" when it does not
    m = _m("", "Health Tech (Opticians) ... at the Springfield IL VA Medical Center .")
    assert scraper.usajobs_va_facility(m, "Springfield", "IL") == ("Veterans Health Administration", "agency")
    assert V.is_town_only("Springfield VA Medical Center") and not V.is_town_only("Battle Creek VA Medical Center")


def test_other_federal_systems_are_as_before(monkeypatch):
    monkeypatch.setattr(scraper, "_STORED_USAJOBS", {})
    rows = scraper._usajobs_jobs(_item("HSJT-26-1-DHA", "Military Treatment Facilities under DHA", "Colorado Springs",
                                       "Colorado", org="Military Treatment Facilities under DHA"),
                                 "Military Health System")
    assert [j.hospital_name for j in rows] == ["Military Treatment Facilities under DHA"]
    rows = scraper._usajobs_jobs(_item("IHS-1", "Indian Health Service", "Gallup", "New Mexico",
                                       "Join the team at Gallup Indian Medical Center today.",
                                       org="Indian Health Service"), "Indian Health Service")
    assert [j.hospital_name for j in rows] == ["Gallup Indian Medical Center"]


# ── 6. Medical centers with a name of their own (rule of 2026-10-27) ─────────
# The owner wants the real facility: a row whose own name, or whose job page's
# hiring-organization line, names a specific VA medical center keeps it, under
# one name; clinics, towns, agency-only rows and summaries still give the
# station. Every raw value is a stored row's name or a job page's line.

@pytest.mark.parametrize("raw, state, want", [
    ("Bath VA Medical Center", "NY", "Bath VA Medical Center"),
    ("VA Finger Lakes Healthcare System- Bath VA Medical Center", "NY", "Bath VA Medical Center"),
    ("Canandaigua VA Medical Center", "NY", "Canandaigua VA Medical Center"),
    ("Canandaigua NY VA Medical Center", "NY", "Canandaigua VA Medical Center"),
    ("Fingerlakes VA Medical Center (Canandaigua)", "NY", "Canandaigua VA Medical Center"),
    ("Alvin C. York Veterans' Administration Medical Center", "TN", "Alvin C. York VA Medical Center"),
    ("Louis Stokes Cleveland VA Medical Center", "OH", "Louis Stokes Cleveland VA Medical Center"),
    ("Cleveland VA Medical Center", "OH", "Louis Stokes Cleveland VA Medical Center"),
    ("Rocky Mountain Regional VA Medical Center", "CO", "Rocky Mountain Regional VA Medical Center"),
    ("Carl T. Hayden VA Medical Center - Phoenix, AZ", "AZ", "Carl T. Hayden VA Medical Center"),
    ("Carl T Hayden Veterans' Administration Medical Center", "AZ", "Carl T. Hayden VA Medical Center"),
    # the medical center in brackets after the system's name
    ("Miami VA Healthcare System (Bruce W. Carter VA Medical Center)", "FL", "Bruce W. Carter VA Medical Center"),
    ("Veterans Health Administration-Bruce W Carter Department of Veterans Affairs Medical Center", "FL",
     "Bruce W. Carter VA Medical Center"),
    # every C.W. Bill Young spelling
    ("CW Bill Young Department of Veterans Affairs Medical Center", "FL", "C.W. Bill Young VA Medical Center"),
    ("CW Bill Young VA Medical Center, Veterans Health Administration", "FL", "C.W. Bill Young VA Medical Center"),
    ("Sepulveda VA Medical Center", "CA", "Sepulveda VA Medical Center"),
    # the hospital's current name
    ("West Palm Beach VA Medical Center", "FL", "Thomas H. Corey West Palm Beach VA Medical Center"),
    ("Salt Lake City VA Medical Center", "UT", "George E. Wahlen VA Medical Center"),
    ("Atlanta VA Medical Center - Veterans Health Administration", "GA",
     "Joseph Maxwell Cleland Atlanta VA Medical Center"),
    ("Muskogee VA Medical Center", "OK", "Jack C. Montgomery VA Medical Center"),
    ("Eastern Oklahoma Health Care System - James Mountain Inhofe VAMC, OK - Nursing Perioperative", "OK",
     "James Mountain Inhofe VA Medical Center"),
    ("Central Arkansas Veteran Healthcare System John L. McClellan Memorial Veterans Hospital", "AR",
     "John L. McClellan Memorial Veterans Hospital"),
    ("Central VA Medical Center Montgomery , Al", "AL", "Central Alabama VA Medical Center-Montgomery"),
    ("Raymond G Murphy Department of the VAMC", "NM", "Raymond G. Murphy VA Medical Center"),
    ("Wm. Jennings Bryan Dorn Department of Veterans Affairs Medical Center", "SC",
     "Wm. Jennings Bryan Dorn VA Medical Center"),
    ("Veterans Health Administration, Vancouver, WA VAMC Office of Community Care", "WA",
     "Vancouver VA Medical Center"),
    # a lone medical center that is only the station's own name: the station's one name (the VA's current one)
    ("Cheyenne VA Medical Center", "WY", "Cheyenne VA Health Care System"),
    ("Alexandria VA Medical Center", "LA", "Alexandria VA Health Care System"),
    ("Birmingham VA Medical Center Department of Pharmacy", "AL", "Birmingham VA Health Care System"),
    ("Pharmacy Service, Ralph H. Johnson VA Medical Center", "SC", "Ralph H. Johnson VA Health Care System"),
    ("Veterans Health Administration, Salem VA Medical Center, Salem, Virginia", "VA", "VA Salem Health Care System"),
    # a station's regional name on a medical-center word picks none of its medical centers
    ("New York Harbor VA Medical Center", "NY", "VA NY Harbor Healthcare System"),
    ("Puget Sound VA Medical Center", "WA", "VA Puget Sound Health Care System"),
    ("St. Louis VA Medical Center", "MO", "VA St. Louis Health Care System"),
    ("Central Alabama VA Medical Center", "AL", "Central Alabama Veterans Health Care System"),
    ("Fingerlakes VA Medical Center", "NY", "VA Finger Lakes Healthcare System"),
    ("Western New York Health Care Systems at the Buffalo and Batavia VA Medical Center", "NY",
     "VA Western New York Healthcare System"),
    # a town in brackets after a system's name is not a medical center
    ("VA Southern Nevada Healthcare System (Las Vegas)", "NV", "VA Southern Nevada Healthcare System"),
    ("Western New York VA Healthcare System (Buffalo)", "NY", "VA Western New York Healthcare System"),
    # a clinic or an ambulatory care center is the station, also a clinic of a named medical center
    ("Chalmers P. Wylie VA ACC", "OH", "VA Central Ohio Healthcare System"),
    ("Central Alabama VA Medical Center-Montgomery - Robert S. Poydasheff VA Clinic", "GA",
     "Central Alabama Veterans Health Care System"),
])
def test_a_named_medical_center_keeps_its_one_name(raw, state, want):
    assert V.canonical(raw, state) == want
    assert V.hiring_org_name(raw, state) == want


def test_a_medical_center_gives_its_station_where_the_station_is_asked_for():
    assert V.station_for("VA Finger Lakes Healthcare System- Bath VA Medical Center", "NY") == \
        "VA Finger Lakes Healthcare System"
    assert V.station_for("C.W. Bill Young VA Medical Center", "FL") == "Bay Pines VA Healthcare System"
    assert V.station_of("Bruce W. Carter VA Medical Center") == "Miami VA Healthcare System"
    assert V.station_of("Bay Pines VA Healthcare System") == "Bay Pines VA Healthcare System"
    # a state the medical center is not in
    assert V.hospital_for("Lake City VA Medical Center", "UT") == ""
    assert V.canonical("Salt Lake City VA Medical Center", "UT") == "George E. Wahlen VA Medical Center"


def test_the_adapter_keeps_a_named_medical_center(monkeypatch):
    monkeypatch.setattr(scraper, "_STORED_USAJOBS", {})
    f = scraper.usajobs_va_facility
    # the hiring-organization line names the medical center
    assert f(_m("VA Finger Lakes Healthcare System- Bath VA Medical Center"), "Bath", "NY") == (
        "Bath VA Medical Center", "hiring_org")
    assert f(_m("Miami VA Healthcare System (Bruce W. Carter VA Medical Center)"), "Miami", "FL") == (
        "Bruce W. Carter VA Medical Center", "hiring_org")
    # a summary gives the station, also when it names a medical center (agency-only postings)
    assert f(_m("Veterans Health Administration", "The incumbent will be working at the Baltimore VA Medical "
                "Center located in Baltimore, Maryland."), "Baltimore", "MD") == (
        "VA Maryland Health Care System", "text")
    assert f(_m("Veterans Health Administration", "Primary Care in the CWBY Bay Pines VAMC and its CBOCs."),
             "Clearwater", "FL") == ("Bay Pines VA Healthcare System", "text")
    # the medical center the cleanup stored stays
    scraper.set_stored_usajobs_names([{"hospital_system": "VA Hospitals", "job_id": "CBFF-WPB-1",
                                       "hospital_name": "Thomas H. Corey West Palm Beach VA Medical Center"}])
    try:
        assert f(_m(), "West Palm Beach", "FL", "", "CBFF-WPB-1") == (
            "Thomas H. Corey West Palm Beach VA Medical Center", "stored")
    finally:
        scraper.set_stored_usajobs_names([])


@pytest.mark.parametrize("name, city, state, want", [
    # a multi-town posting under a medical center's hiring line: its rows in the station's other towns
    ("Thomas H. Corey West Palm Beach VA Medical Center", "Vero Beach", "FL", "West Palm Beach VA Healthcare System"),
    ("Seattle VA Medical Center", "Tacoma", "WA", "VA Puget Sound Health Care System"),
    ("Malcom Randall VA Medical Center", "Lake City", "FL", "North Florida/South Georgia Veterans Health System"),
    ("Central Alabama VA Medical Center-Montgomery", "Tuskegee", "AL", "Central Alabama Veterans Health Care System"),
    ("Bruce W. Carter VA Medical Center", "Monroe County", "FL", "Miami VA Healthcare System"),
    ("C.W. Bill Young VA Medical Center", "Cape Coral", "FL", "Bay Pines VA Healthcare System"),
    # the medical center's own town, or no town: the medical center
    ("Thomas H. Corey West Palm Beach VA Medical Center", "West Palm Beach", "FL",
     "Thomas H. Corey West Palm Beach VA Medical Center"),
    ("Thomas H. Corey West Palm Beach VA Medical Center", "", "", "Thomas H. Corey West Palm Beach VA Medical Center"),
    ("John Cochran VA Medical Center", "Saint Louis", "MO", "John Cochran VA Medical Center"),
    ("Joseph Maxwell Cleland Atlanta VA Medical Center", "Decatur", "GA", "Joseph Maxwell Cleland Atlanta VA Medical Center"),
    # any other name as is
    ("Bay Pines VA Healthcare System", "Clearwater", "FL", "Bay Pines VA Healthcare System"),
    ("VA National Oncology Program", "Boston", "MA", "VA National Oncology Program"),
])
def test_a_medical_center_is_named_only_in_its_own_town(name, city, state, want):
    assert V.at_town(name, city, state) == want


def test_every_medical_center_has_its_towns():
    assert set(V.HOSPITAL_TOWNS) == set(V.HOSPITAL_STATION)
    for name, towns in V.HOSPITAL_TOWNS.items():
        assert towns and all(t == V._town_key(t) for t in towns), name


def test_the_adapter_names_the_station_for_a_row_in_another_town(monkeypatch):
    monkeypatch.setattr(scraper, "_STORED_USAJOBS", {})
    f = scraper.usajobs_va_facility
    m = _m("Miami VA Healthcare System (Bruce W. Carter VA Medical Center)")
    assert f(m, "Miami", "FL", "", "", 2) == ("Bruce W. Carter VA Medical Center", "hiring_org")
    assert f(m, "Homestead", "FL", "", "", 2) == ("Miami VA Healthcare System", "hiring_org")
