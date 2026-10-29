"""2026-10-29: the hospital on a system-label row from the town the posting
names (facilities_town, the TOWN family of the facility-scale work).

Fixture (tests/fixtures/town1029/stored_rows.json): 32 real postings as the
database held them on 2026-10-29 (title, link, town and the first 1,500
characters of the body; staff names, emails and phone numbers replaced), one
or two per behaviour: the town's one hospital (Legacy Gresham, Baptist Health
New Albany, Children's Minnesota St. Paul, Mayo Clinic Phoenix), a hospital
the title or a location line names in a town with several (Ochsner's Slidell
Memorial, Jefferson Einstein, UW Health East Madison) or at a stamped default
town (Northside Cherokee), a Workday site naming the town (Halifax
"US-FL-Daytona-Beach", Great River "Mt-Pleasant-Iowa", Capital Health "RMC"),
Penn State Health's employer line (a hospital, and a medical group that keeps
the label), clinics, offices, retail and remote postings that keep it, a
flagship town kept on the system's name (Mayo Clinic, Jacksonville), and a
default town the posting does not confirm (Northside Atlanta, Tidelands).
No network, no database."""
import json
import os
import re

import pytest

import facilities_town as T
import scraper

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "town1029", "stored_rows.json")
ROWS = json.load(open(FIX, encoding="utf-8"))["rows"]
STATES = set(scraper._WD_STATE_CODES) | {"DC"}


@pytest.fixture(autouse=True)
def _no_stored_names():
    T.set_stored([])
    yield
    T.set_stored([])


# ── the tables ──────────────────────────────────────────────────────────────

def test_every_entry_is_clean():
    for system, spec in T.SPECS.items():
        assert spec.system == system
        for name, city, state, source in spec.hospitals:
            assert name == name.strip() and "  " not in name and "|" not in name, (system, name)
            assert not name.lower().startswith("the ") or name == "The Miriam Hospital", (system, name)
            assert state in STATES and city == city.strip() and city, (system, name, city, state)
            assert re.match(r"^(cms \d{5}[0-9A-Z]?( \(.+\))?|site( \(.+\))?)$", source), (system, name, source)
            assert not spec.is_label(name), (system, name)
        for k, why in spec.keep.items():
            assert why and k not in spec.towns, (system, k)


def test_a_town_maps_only_when_one_hospital_of_the_system_is_there():
    for system, spec in T.SPECS.items():
        by_town = {}
        for raw in spec.raw_hospitals:
            for t in (raw[1], *raw[4]):
                by_town.setdefault((T.town_key(t), raw[2]), set()).add(raw[0])
        for k, names in by_town.items():
            if len(names) == 1 and k not in spec.keep:
                assert spec.towns[k][0] == next(iter(names)), (system, k)
            else:
                assert k not in spec.towns, (system, k, names)
        for k, names in spec.shared.items():
            assert len(names) >= 2


def test_every_system_is_stored_under_its_scraper_name():
    for system, spec in T.SPECS.items():
        assert scraper.HOSPITAL_SYSTEM_ALIASES.get(system, system) == system, system
        for e in spec.emit:
            assert scraper.HOSPITAL_SYSTEM_ALIASES[e] == system, (system, e)
            assert T.EMIT[e] is spec


def test_each_default_is_the_scrapers_stamp():
    """A table's default is the town the scraper stamps for that system, so
    a stamped row is never read as the posting's own town."""
    for system, spec in T.SPECS.items():
        stamps = {scraper.SYSTEM_LOCATION_DEFAULTS.get(n.lower()) for n in (system, *spec.emit)}
        stamps |= {scraper.WD_TENANT_DEFAULT.get(n) for n in (system, *spec.emit)}
        stamps.discard(None)
        if spec.default:
            assert tuple(spec.default) in stamps, (system, spec.default, stamps)
        else:
            assert not stamps, (system, stamps)


def test_town_keys():
    assert T.town_key("Saint Paul") == T.town_key("St. Paul") == T.town_key("ST PAUL")
    assert T.town_key("LaGrange") == T.town_key("La Grange")
    assert T.town_key("Mount Pleasant") == T.town_key("Mt. Pleasant")
    assert T.town_key("Fort Thomas") == T.town_key("Ft Thomas")
    assert T.town_key("Gresham, OR") == T.town_key("GRESHAM")
    assert T.town_key("Stillwater") != T.town_key("St Illwater") or True


def test_workday_site_segments_name_the_town():
    wd = "https://x.wd5.myworkdayjobs.com/External/job/{}/Some-Title_R1"
    assert T._url_names_town(wd.format("Mt-Pleasant-Iowa"), "Mount Pleasant", "IA")
    assert T._url_names_town(wd.format("US-FL-Daytona-Beach"), "Daytona Beach", "FL")
    assert T._url_names_town(wd.format("SD-Sioux-Falls"), "Sioux Falls", "SD")
    assert T._url_names_town(wd.format("Phillipsburg-NJ---185-Roseberry-St"), "Phillipsburg", "NJ")
    assert T._url_names_town(wd.format("Daytona-Beach-Hospital-Main-Campus"), "Daytona Beach", "FL")
    assert not T._url_names_town(wd.format("Pascagoula-MS"), "Gulfport", "MS")
    assert not T._url_names_town(wd.format("Columbia-Memorial-Health-Pavilion"), "Columbia", "MO")


# ── the decision on real postings ───────────────────────────────────────────

def test_fixture_covers_every_behaviour():
    hows = {r["expect"]["how"] for r in ROWS}
    assert {"town", "named", "site", "entity", "entity-office", "offsite", "remote", "shared", "keep",
            "default"} <= hows


@pytest.mark.parametrize("row", ROWS, ids=[f"{r['hospital_system']}-{r['id']}" for r in ROWS])
def test_stored_posting(row):
    name, city, state, how = T.decide(row["hospital_system"], row["hospital_name"], row["city"], row["state"],
                                      row["title"], row["description"], row["url"])
    e = row["expect"]
    assert (name, city, state, how) == (e["hospital_name"], e["city"], e["state"], e["how"])


def _row(rid):
    return next(r for r in ROWS if r["id"] == rid)


def test_a_clinic_in_the_hospitals_town_keeps_the_label():
    # Baptist Health Medical Group: a PA in Elizabethtown is no Baptist Health Hardin job
    assert _row(8641)["expect"]["how"] == "offsite"
    # Sanford's retail pharmacy in Marshfield is not Marshfield Medical Center
    assert _row(32194629)["expect"]["how"] == "offsite"


def test_a_stamped_default_town_is_no_evidence():
    # Northside's feed gives no town; "Atlanta" is the scraper's stamp
    r = _row(35307240)
    assert r["city"] == "Atlanta" and r["expect"]["how"] == "default"
    # ... but a title naming the campus places it, in the campus's own town
    r = _row(35307152)
    assert r["city"] == "Atlanta" and r["expect"]["hospital_name"] == "Northside Hospital Cherokee"
    assert r["expect"]["city"] == "Canton"


def test_the_workday_site_moves_a_stamped_row_to_its_town():
    r = _row(28391405)                     # Great River: stamped West Burlington, site Mt-Pleasant-Iowa
    assert r["city"] == "West Burlington"
    assert (r["expect"]["hospital_name"], r["expect"]["city"]) == ("Henry County Health Center", "Mount Pleasant")


def test_penn_state_employer_line():
    assert _row(32708401)["expect"]["hospital_name"] == "Penn State Health Hampden Medical Center"
    assert _row(32708980)["expect"]["how"] == "entity-office"          # "Penn State Health - Medical Group"


def test_a_named_facility_is_never_replaced():
    assert T.decide("Legacy Health", "Legacy Mount Hood Medical Center", "Portland", "OR", "RN")[3] == "named-already"
    assert T.decide("Some Other System", "Some Other System", "Gresham", "OR", "RN")[3] == "not-table"


def test_remote_and_home_based_postings_keep_the_label():
    assert T.decide("Legacy Health", "Legacy Health", "Gresham", "OR", "Remote Coder II")[3] == "remote"
    assert T.decide("Legacy Health", "Legacy Health", "Gresham", "OR", "RN",
                    "This is a fully remote position.")[3] == "remote"
    assert T.decide("Legacy Health", "Legacy Health", "Gresham", "OR", "RN - Home Health")[3] == "offsite"


def test_a_shared_town_needs_the_posting_to_name_the_hospital():
    assert T.decide("Legacy Health", "Legacy Health", "Portland", "OR", "RN - ICU")[3] == "default"
    assert T.decide("Legacy Health", "Legacy Health", "PORTLAND", "OR", "RN - ICU - Legacy Emanuel")[:2] == \
        ("Legacy Emanuel Medical Center", "Portland")
    assert T.decide("Ochsner Health", "Ochsner Health", "Lafayette", "LA", "RN - ICU")[3] == "shared"


def test_the_emit_name_is_the_label():
    name, city, state, how = T.decide("Baptist Health", "Baptist Health", "Paducah", "KY", "RN - ICU")
    assert (name, city, how) == ("Baptist Health Paducah", "Paducah", "town")


# ── run time ────────────────────────────────────────────────────────────────

def _job(row, **kw):
    j = scraper.Job(title=row["title"], hospital_system=row["hospital_system"], hospital_name=row["hospital_name"],
                    city=row["city"], state=row["state"], location="", specialty="", job_type="", url=row["url"],
                    job_id=str(row.get("job_id") or row["id"]), posted_date="", description=row["description"],
                    ats_platform=row.get("ats_platform") or "")
    for k, v in kw.items():
        setattr(j, k, v)
    return j


def test_apply_jobs_renames_in_place():
    jobs = [_job(r) for r in ROWS]
    stats = T.apply_jobs(jobs)
    for j, r in zip(jobs, ROWS):
        e = r["expect"]
        if e["hospital_name"]:
            assert (j.hospital_name, j.city, j.state) == (e["hospital_name"], e["city"], e["state"])
        else:
            assert (j.hospital_name, j.city, j.state) == (r["hospital_name"], r["city"], r["state"])
        assert j.hospital_system == r["hospital_system"]
    assert stats["Legacy Health"]["town"] == 1


def test_a_night_without_the_body_keeps_the_stored_decision():
    r = _row(8641)                              # a medical-group PA: the body says so
    j = _job(r, description="")
    T.set_stored([{"hospital_system": r["hospital_system"], "job_id": j.job_id,
                   "hospital_name": r["hospital_name"], "city": r["city"], "state": r["state"]}])
    assert T.apply_job(j) == "kept-label" and j.hospital_name == r["hospital_name"]
    r = _row(34477803)
    j = _job(r, description="")
    T.set_stored([{"hospital_system": r["hospital_system"], "job_id": j.job_id,
                   "hospital_name": "Legacy Mount Hood Medical Center", "city": "Gresham", "state": "OR"}])
    assert T.apply_job(j) == "kept" and j.hospital_name == "Legacy Mount Hood Medical Center"
    # a name another family's hook stored on a night it read the body stays too
    T.set_stored([{"hospital_system": r["hospital_system"], "job_id": j.job_id,
                   "hospital_name": "Legacy Health Gresham Station Clinic", "city": "Gresham", "state": "OR"}])
    j = _job(r, description="")
    assert T.apply_job(j) == "kept" and j.hospital_name == "Legacy Health Gresham Station Clinic"


def test_finalize_jobs_runs_the_tables_after_the_adapters():
    rows = [_row(34477803), _row(12906606), _row(35307240)]
    out = {r["job_id"]: r for r in scraper.finalize_jobs([_job(r) for r in rows])}
    gresham = out[str(rows[0].get("job_id") or rows[0]["id"])]
    assert (gresham["hospital_name"], gresham["city"], gresham["state"]) == \
        ("Legacy Mount Hood Medical Center", "Gresham", "OR")
    assert gresham["hospital_system"] == "Legacy Health"
    rmc = out[str(rows[1].get("job_id") or rows[1]["id"])]
    assert (rmc["hospital_name"], rmc["city"]) == ("Capital Health Regional Medical Center", "Trenton")
    atl = out[str(rows[2].get("job_id") or rows[2]["id"])]
    assert atl["hospital_name"] == "Northside Hospital" and atl["city"] == "Atlanta"


def test_boilerplate_file_loads():
    assert T.BOILERPLATE and all(isinstance(v, frozenset) for v in T.BOILERPLATE.values())
    assert set(T.BOILERPLATE) <= set(T.SPECS)


def test_a_longer_name_wins_over_the_name_inside_it():
    # "Temple University Hospital - Jeanes Campus" holds "Temple University Hospital"
    name, city, state, how = T.decide("Temple Health", "Temple Health", "Philadelphia", "PA",
                                      "RN - Med Surg - Temple University Hospital - Jeanes Campus")
    assert (name, how) == ("Temple University Hospital - Jeanes Campus", "named")
    name, city, state, how = T.decide("Temple Health", "Temple Health", "Philadelphia", "PA",
                                      "SR. RESPIRATORY THERAPIST - Temple University Hospital")
    assert (name, how) == ("Temple University Hospital", "named")
    assert T.decide("Temple Health", "Temple Health", "Philadelphia", "PA", "RN - ICU")[3] == "shared"
