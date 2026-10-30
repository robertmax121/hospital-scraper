"""2026-10-30 (facility scale round 2, group mhs): the military treatment facility
a Military Health System USAJOBS posting names (facilities_mhs, hooked in
scraper._usajobs_jobs through _fac_hook).

Fixture (tests/fixtures/mhs_r2_1030/usajobs_mhs.json), real, read 2026-10-30
from the USAJOBS public historic-JOA API (data.usajobs.gov/api/historicjoa and
its announcementtext; no key): 53 Military Health System announcements still
open, each with its hiring-organization line (hiringSubelementName, the search
API's UserArea.Details.SubAgencyName), its duty locations as USAJOBS writes
them, its Summary paragraph (the API's JobSummary) for the ten whose text was
read, and the names and towns the stored rows held that day. The adapter with
the hook switched off reproduces every stored name from these payloads (the
payload is faithful); with it, the rows take the MTF's own name, never a
directorate, and agency labels and many-station postings keep the system.
No network, no database."""
import json
import os
import re

import pytest

import facilities_mhs as M
import scraper

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "mhs_r2_1030")
with open(os.path.join(FIX, "usajobs_mhs.json"), encoding="utf-8") as _f:
    CASES = {c["control_number"]: c for c in json.load(_f)}
MHS = M.MHS


def _item(c, summary=None, sub=None, locations=None, org=None):
    locs = locations if locations is not None else c["locations"]
    return {"MatchedObjectDescriptor": {
        "PositionTitle": c["title"], "PositionID": c["announcement"],
        "PositionURI": f"https://www.usajobs.gov/job/{c['control_number']}",
        "OrganizationName": org if org is not None else c["organization"], "QualificationSummary": "",
        "PositionRemuneration": [{}],
        "PositionLocation": [{"LocationName": f"{a}, {b}", "CityName": f"{a}, {b}", "CountryCode": cc or "United States",
                              "CountrySubDivisionCode": b} for a, b, cc in locs],
        "UserArea": {"Details": {"SubAgencyName": c["sub_agency"] if sub is None else sub,
                                 "JobSummary": c["summary"] if summary is None else summary}}}}


def _names(item):
    return {(j.city, j.state): j.hospital_name for j in scraper._usajobs_jobs(item, MHS)}


def _hook_off(monkeypatch):
    monkeypatch.setattr(M, "mhs_facility", lambda *a, **k: None)


# control number -> {(city, state): hospital_name} the branch writes, reviewed
# one by one against the posting (hiring line or summary quoted in the note)
EXPECT = {
    # hiring line names the MTF; the directorate or department is dropped
    "877915800": {("Fort Campbell", "TN"): "Blanchfield Army Community Hospital"},      # "Army Community Hospital Blanchfield: Directorate of Clinical Support"
    "888516400": {("Fort Campbell", "TN"): "Blanchfield Army Community Hospital"},      # "Defense Health Agency; Blanchfield Army Community Hospital"
    "879864700": {("Fort Gordon", "GA"): "Dwight D. Eisenhower Army Medical Center"},   # "... Dwight D. Eisenhower (DDEAMC) Army Medical Center"
    "886949700": {("Bethesda", "MD"): "Walter Reed National Military Medical Center"},  # "..., DIR for Medicine, Dept of Neurology, Office of Chief"
    "878046100": {("Bethesda", "MD"): "Walter Reed National Military Medical Center"},  # "DHA - NCR Market, WR Nat'l Mil Med Ctr, Director for ..."
    "886966700": {("Bethesda", "MD"): "Walter Reed National Military Medical Center"},  # "WRNMMC, Dept of MH Consultation & Education, ..."
    "878556800": {("Tacoma", "WA"): "Madigan Army Medical Center"},                     # "MADIGAN DIR OF MEDICINE DEPT OFMEDICINE ..."
    "888970500": {("Tacoma", "WA"): "Madigan Army Medical Center"},                     # "Military Treatment Facility, Madigan Ancillary Services, Pathology"
    "884519300": {("Olympia", "WA"): "Madigan Army Medical Center"},                    # "Madigan Army Medical Center, Dir. of Family Medicine" (its clinic town)
    "886338400": {("Fort Bragg", "NC"): "Womack Army Medical Center"},                  # "AMC WOMACK, Directorate of Medicine, ..."
    "883412500": {("Beaufort", "SC"): "Naval Hospital Beaufort"},                       # "Naval Hospital  Beaufort S.C. - Director of Medicine"
    "876504700": {("Portsmouth", "VA"): "Naval Medical Center Portsmouth"},             # "Naval Medical Center Portsmouth, DIR of Nursing, OB/GYN IP Ward"
    "888475100": {("Portsmouth", "VA"): "Naval Medical Center Portsmouth"},             # "NAVAL MED CENTER PORTSMOUTH DIR OF MENTAL HEALTH ..."
    # clinics and health centers the hospital-word reader did not take
    "887280700": {("Fort Sill", "OK"): "Reynolds Army Health Clinic"},
    "874369900": {("Fort Leavenworth", "KS"): "Munson Army Health Center"},             # "Munson Army Heath Clinic"
    "879277800": {("Fort Drum", "NY"): "Guthrie Army Health Clinic"},                   # "Defense Health Agency, Guthrie Army Health Clinic, Family Medicine."
    "889150600": {("Fort Drum", "NY"): "Guthrie Army Health Clinic"},                   # "The position is located within Guthrie Army Health Clinic ..."
    "884646800": {("Fort Rucker", "AL"): "Lyster Army Health Clinic"},                  # "Lyster Army Health Clinic, Directorate of Dental"
    "889537500": {("Fort Lee", "VA"): "Kenner Army Health Clinic"},                     # "Army Health Clinic Kenner, Director of Medicine, Optometry"
    "889959200": {("Fort Jackson", "SC"): "Moncrief Army Health Clinic"},               # "Defense Health Agency; Army Health Clinic Moncrief; Directorate of Dental"
    "887880700": {("", "MD"): "Kimbrough Ambulatory Care Center"},                      # one location, "Fort Meade Naval Facilities" (no town)
    "887239300": {("Kittery Point", "ME"): "Naval Branch Health Clinic Portsmouth"},
    "889583300": {("Bangor", "WA"): "Naval Branch Health Clinic Bangor"},               # "Naval Health Clinic Bangor"
    "889454800": {("Naval Air Station Oceana", "VA"): "Naval Branch Health Clinic Oceana"},  # "DHN Atlantic, NBRHLTHCLINIC NAS Oceana, ..."
    "888902900": {("Corpus Christi", "TX"): "Naval Health Clinic Corpus Christi"},      # "Naval Health Clinic" (the one in its town)
    "888463400": {("Cherry Point", "NC"): "Naval Health Clinic Cherry Point"},          # "DHN ATLANTIC MTF CHERRY POINT OPERATIONS ..."
    "888530200": {("Lackland AFB", "TX"): "Wilford Hall Ambulatory Surgical Center"},   # "MTF WILFORD HALL"
    "887697500": {("Travis AFB", "CA"): "David Grant USAF Medical Center"},             # "60th MDG, Travis AFB"
    "884374000": {("Keesler AFB", "MS"): "Keesler Medical Center"},                     # "Keesler Medical Group"
    "889594700": {("McConnell AFB", "KS"): "22nd Medical Group"},                       # "22nd Medical Group, McConnell AFB, Kansas"
    # the location line names the row's facility
    "887614600": {("San Diego", "CA"): "Naval Medical Center San Diego"},               # "Naval Medical Center, San Diego"
    "886230500": {("", "FL"): "Naval Hospital Jacksonville"},
    "878187400": {("", "HI"): "Tripler Army Medical Center"},
    "888680400": {("", "HI"): "Tripler Army Medical Center"},
    # the summary names the MTF in the row's own town
    "877205300": {("Tacoma", "WA"): "Madigan Army Medical Center"},                     # "located at Army Medical Center Madigan, Tacoma, Washington"
    "878228000": {("Fort Sam Houston", "TX"): "Brooke Army Medical Center"},            # "located at Brooke Army Medical Center in San Antonio, Texas"
    "885030600": {("Fort Stewart", "GA"): "Winn Army Community Hospital"},              # "located at WINN Army Medical Hospital, Fort Stewart, GA"
    "886706300": {("Camp Lejeune", "NC"): "Naval Medical Center Camp Lejeune"},         # "located at Naval Medical Center Camp Lejeune, North Carolina"
    # no MTF named: a base, a market, a network, a directorate, or no text
    "876046700": {("Fort Hood", "TX"): MHS},            # "This position located at Fort Hood, Texas."
    "875067100": {("Randolph AFB", "TX"): MHS},         # "located at Randolph Air Force Base, San Antonio, Texas"
    "889995700": {("Fort Wainwright", "AK"): MHS},      # "located at Fort Wainwright, Alaska"
    "877899900": {("Fort Drum", "NY"): MHS},            # "located at DENTAC, Fort Drum, New York" (the dental activity)
    "874964200": {("El Paso", "TX"): MHS},              # an open continuous announcement, no place
    "889078300": {("Bethesda", "MD"): MHS},             # "Medical Home Port Team", no facility
    "872642700": {("Schofield Barracks", "HI"): MHS},   # "Defense Health Agency Indo Pacific Market Hawaii Region"
    "889981800": {("San Diego", "CA"): MHS},            # "Defense Health Agency- San Diego Market"
    "887645600": {("Langley AFB", "VA"): MHS},          # "Joint Base Langley - Eustis - Langley Air Force Base - Directorate ..."
    "878866000": {("Tacoma", "WA"): MHS},               # "DIR OF MEDICINE DEPT OF MEDICINE INTERNAL MEDICINE SERVICE"
    "887591100": {("Fort Stewart", "GA"): MHS},         # "Defense Health Agency, CBMH Richmond Hill, Army Health Clinics, ..."
    "886980800": {("Fort Belvoir", "VA"): MHS},         # "Dilorenzo Health Clinic Pentagon" (no table site; not Fort Belvoir's)
    "888956500": {("Dover AFB", "DE"): MHS},            # "ARMED FORCES MEDICAL EXAMINER SYSTEM (AFMES)"
}
MANY = ("878613000", "874211700")     # 18 and 16 duty stations


def test_every_case_is_expected():
    assert set(EXPECT) | set(MANY) == set(CASES)


@pytest.mark.parametrize("cn", sorted(EXPECT))
def test_real_postings(cn):
    got = _names(_item(CASES[cn]))
    for place, name in EXPECT[cn].items():
        assert got[place] == name, (cn, place, got)


@pytest.mark.parametrize("cn", sorted(CASES))
def test_without_the_hook_the_stored_names_come_back(cn, monkeypatch):
    """The payloads are faithful: the adapter as it was writes what is stored."""
    _hook_off(monkeypatch)
    c = CASES[cn]
    got = _names(_item(c))
    stored = {}
    for city, st, name in c["stored"]:
        stored[(city, st)] = name
    hits = [(k, v) for k, v in stored.items() if k in got]
    if not hits and len(got) == len(stored) == 1:
        # one row whose town an older run read ("Fort Meade Naval Facilities"; clean_city now drops it)
        hits = [(next(iter(got)), next(iter(stored.values())))]
    assert hits, (cn, sorted(got), sorted(stored))
    for k, v in hits:
        assert got[k] == v, (cn, k, got[k], v)


@pytest.mark.parametrize("cn", MANY)
def test_many_duty_stations_keep_the_system(cn):
    got = _names(_item(CASES[cn]))
    assert len(got) >= 14
    assert set(got.values()) == {MHS}


def test_many_stations_name_only_the_mtf_town_of_a_hiring_line():
    c = CASES["878613000"]
    got = _names(_item(c, sub="Brooke Army Medical Center"))
    assert got[("Fort Sam Houston", "TX")] == "Brooke Army Medical Center"
    others = {v for (city, st), v in got.items() if city not in ("Fort Sam Houston", "San Antonio")}
    assert others == {MHS}
    # ... and a summary of a many-station posting names none
    got = _names(_item(c, summary="This position is located at Brooke Army Medical Center in San Antonio, Texas."))
    assert set(got.values()) == {MHS}


def test_summary_names_only_its_own_town():
    c = CASES["877205300"]
    locs = [["Tacoma", "Washington", "United States"], ["Yakima", "Washington", "United States"]]
    got = _names(_item(c, locations=locs))
    assert got == {("Tacoma", "WA"): "Madigan Army Medical Center", ("Yakima", "WA"): MHS}


def test_a_base_or_its_town_never_names_an_mtf():
    c = CASES["876046700"]
    for s in ("This position is located at Fort Eisenhower, Georgia.", "Duty station: Fort Hood, Texas (Killeen).",
              "Join the team at Fort Leonard Wood, Missouri.", "Located at Fort Irwin, California."):
        for city, st in (("Fort Gordon", "Georgia"), ("Fort Hood", "Texas"), ("Fort Leonard Wood", "Missouri"),
                         ("Fort Irwin", "California")):
            got = _names(_item(c, summary=s, sub="", locations=[[city, st, "United States"]]))
            assert set(got.values()) == {MHS}, (s, city)


def test_the_generic_navy_form_needs_the_town():
    c = CASES["888902900"]
    got = _names(_item(c, sub="Naval Health Clinic", locations=[["Kingsville", "Texas", "United States"]]))
    assert set(got.values()) == {MHS}
    got = _names(_item(c, sub="Naval Medical Center", locations=[["Portsmouth", "Virginia", "United States"]]))
    assert set(got.values()) == {"Naval Medical Center Portsmouth"}
    # a named Navy center in another town is not this town's
    got = _names(_item(c, sub="", summary="Supports Naval Medical Center Portsmouth.",
                       locations=[["San Diego", "California", "United States"]]))
    assert set(got.values()) == {MHS}


def test_two_mtfs_of_one_place_name_none():
    c = CASES["878228000"]
    s = "Serves Brooke Army Medical Center and Wilford Hall Ambulatory Surgical Center in San Antonio, Texas."
    got = _names(_item(c, summary=s, locations=[["San Antonio", "Texas", "United States"]]))
    assert set(got.values()) == {MHS}


def test_air_force_groups():
    assert M.canonical_name("22nd Medical Group, McConnell AFB, Kansas", "McConnell AFB", "KS") == "22nd Medical Group"
    assert M.canonical_name("375th Medical Group", "Scott AFB", "IL") == "375th Medical Group"
    assert M.canonical_name("412th Medical Group, Edwards Air Force Base, California.", "Edwards AFB", "CA") == "412th Medical Group"
    assert M.canonical_name("87th Medical Group, MTF McGuire, Joint Base McGuire-Dix-Lakehurst, NJ", "McGuire AFB", "NJ") \
        == "87th Medical Group"
    assert M.canonical_name("60th Medical Group, Travis Air Force Base", "Travis AFB", "CA") == "David Grant USAF Medical Center"
    # a group named at another base, and a number that is no group, name none
    assert M.canonical_name("22nd Medical Group", "Scott AFB", "IL") is None
    assert M.canonical_name("joins 3 medical group practices", "Seymour Johnson AFB", "NC") is None
    assert M._ordinal(22) == "22nd" and M._ordinal(11) == "11th" and M._ordinal(633) == "633rd" and M._ordinal(341) == "341st"


# stored names read whole from a hiring line (2026-10-30, active rows), each the MTF
STORED_NAMES = [
    ("Womack Army Medical Center; Directorate of Medicine; Department of Pediatrics", "Fort Bragg", "NC",
     "Womack Army Medical Center"),
    ("DHN East Martin Army Community Hospital Directorate of Nursing Services", "Fort Benning", "GA",
     "Martin Army Community Hospital"),
    ("DHN East, Army Community Hospital Martin, Department of Radiology", "Fort Benning", "GA", "Martin Army Community Hospital"),
    ("Augusta Market Army Medical Center Eisenhower, Directorate of Mental Health Psychology Services.", "Fort Gordon", "GA",
     "Dwight D. Eisenhower Army Medical Center"),
    ("EVANS ARMY COMMUNITY HOSPITAL", "Fort Carson", "CO", "Evans Army Community Hospital"),
    ("Army Medical Center William Beaumont", "El Paso", "TX", "William Beaumont Army Medical Center"),
    ("General Leonard Wood Community Hospital", "Fort Leonard Wood", "MO", "General Leonard Wood Army Community Hospital"),
    ("Fort Leonard Wood Community Hospital", "Fort Leonard Wood", "MO", "General Leonard Wood Army Community Hospital"),
    ("DHN EAST Army Community Hospital Blancfield Directorate of Clinical Support Dept of Pathology", "Fort Campbell", "TN",
     "Blanchfield Army Community Hospital"),
    ("Carl R. Darnall Army Medical Center (CRDAMC), Fort Hood, TX", "Fort Hood", "TX", "Carl R. Darnall Army Medical Center"),
    ("Raymond W Bliss Army Community Hospital", "Fort Huachuca", "AZ", "Raymond W. Bliss Army Health Center"),
    ("This position is located at Bayne-Jones Army Community Hospital in Fort Polk, Louisiana.", "Fort Polk", "LA",
     "Bayne-Jones Army Community Hospital"),
    ("Defense Health Network National Capital Region, A. T. Augusta Military Medical Center", "Fort Belvoir", "VA",
     "Alexander T. Augusta Military Medical Center"),
    ("AT Augusta Military Medical Center", "Fort Belvoir", "VA", "Alexander T. Augusta Military Medical Center"),
    ("Camp Lejeune Medical Center, Family Medicine", "Camp Lejeune", "NC", "Naval Medical Center Camp Lejeune"),
    ("Pacific Rim Naval Hospital", "Bremerton", "WA", "Naval Hospital Bremerton"),
    ("Jacksonville Naval Hospital", "", "FL", "Naval Hospital Jacksonville"),
    ("Naval Medical Center", "San Diego", "CA", "Naval Medical Center San Diego"),
    ("Irwin Army Community Hospital- Dental Clinics", "Fort Riley", "KS", "Irwin Army Community Hospital"),
    ("Army Health Center McDonald", "Fort Eustis", "VA", "McDonald Army Health Center"),
    ("WR National Military Medical Center, Director for Surgery, Dept of Rehabilitation", "Bethesda", "MD",
     "Walter Reed National Military Medical Center"),
    ("60th MDG, David Grant Medical Center, Travis AFB", "Travis AFB", "CA", "David Grant USAF Medical Center"),
]


@pytest.mark.parametrize("name,city,st,want", STORED_NAMES)
def test_stored_names_read_as_their_mtf(name, city, st, want):
    assert M.canonical_name(name, city, st) == want
    # and through the adapter, as the hiring line of a one-town posting
    c = CASES["877915800"]
    sn = {"NC": "North Carolina", "GA": "Georgia", "CO": "Colorado", "TX": "Texas", "MO": "Missouri", "TN": "Tennessee",
          "AZ": "Arizona", "LA": "Louisiana", "VA": "Virginia", "WA": "Washington", "FL": "Florida", "CA": "California",
          "KS": "Kansas", "MD": "Maryland"}[st]
    loc = [[city, sn, "United States"]] if city else [[sn, sn, "United States"]]
    got = _names(_item(c, sub=name, summary="", locations=loc))
    assert list(got.values()) == [want], got


def test_no_directorate_or_department_is_ever_written():
    part = re.compile(r"\b(?:directorate|director|dir|department|dept|service|services|clinic(?:al)? support)\b", re.I)
    for name in M.all_names():
        assert not part.search(name), name
    for cn in CASES:
        for name in _names(_item(CASES[cn])).values():
            assert not part.search(name), (cn, name)


def test_a_dod_organization_that_is_no_facility_keeps_its_name():
    c = CASES["889981800"]
    loc = [["Boston", "Massachusetts", "United States"]]
    got = _names(_item(c, sub="", summary="", locations=loc, org="U.S. Military Entrance Processing Command"))
    assert list(got.values()) == ["U.S. Military Entrance Processing Command"]
    # the agency labels of the health system itself give the system
    for org in ("Military Treatment Facilities under DHA", "Defense Health Agency", "U.S. Army Medical Command",
                "Department of the Air Force Headquarters", "Naval Medical Command"):
        got = _names(_item(c, sub="", summary="", locations=loc, org=org))
        assert list(got.values()) == [MHS], org


def test_hook_that_raises_keeps_what_the_adapter_read(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("unforeseen")
    monkeypatch.setattr(M, "mhs_facility", boom)
    c = CASES["877915800"]
    got = _names(_item(c))
    assert list(got.values()) == ["Army Community Hospital Blanchfield: Directorate of Clinical Support"]


def test_other_federal_systems_are_untouched():
    c = CASES["877915800"]
    for system in ("Indian Health Service", "NIH Clinical Center"):
        rows = scraper._usajobs_jobs(_item(c), system)
        assert [j.hospital_name for j in rows] == ["Army Community Hospital Blanchfield: Directorate of Clinical Support"]


def test_tables_are_clean():
    names = M.all_names()
    assert len(names) == len(set(names))
    for f in list(M.MTFS) + list(M.AF_MTFS.values()):
        assert "\u2014" not in f.name and "\u2013" not in f.name
        assert f.states and all(len(s) == 2 for s in f.states)
        assert f.towns and f.kind in ("hospital", "clinic")
    # every MTF reads its own name
    for f in M.MTFS:
        hits = [h for h, bare in M.find_mtfs(f.name)]
        assert f in hits, f.name
    for n, f in M.AF_MTFS.items():
        assert M.AF_GROUPS[n][0] in f.states
    assert M.is_agency_label("Military Treatment Facilities under DHA") and not M.is_agency_label("Tripler Army Medical Center")
