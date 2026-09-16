"""New York board fixes, 2026-09-16: card-header locations (Catholic Health
Long Island), Mount Sinai site mapping, and the Layer 4 exemption."""
import scraper
from scraper import _parse_icims_cards, _mount_sinai_loc


CHSLI_CARD = '''
<ul class="container-fluid iCIMS_JobsTable"> <li class="iCIMS_JobCardItem"> <div class="row">
<div class="col-xs-6 header left"> <span class="sr-only field-label">Location</span> <span > US-NY-West Islip</span> </div>
<div class="col-xs-6 header right"> <span class="sr-only field-label">ID</span> <span > 2026-74333</span> </div>
<div class="col-xs-12 title"> <a href="https://careers-chsli.icims.com/jobs/74333/cna/job" class="iCIMS_Anchor" title="74333 - CNA">
<span class="sr-only field-label">Title</span> <h3 > CNA</h3> </a> </div>
<div class="col-xs-12 description"> Enjoy a brighter career by the Great South Bay.</div>
<div class="col-xs-12 additionalFields"> <dl class="iCIMS_JobHeaderGroup">
<div class="iCIMS_JobHeaderTag"> <dt class="iCIMS_JobHeaderField">Category</dt> <dd class="iCIMS_JobHeaderData"><span > Direct Care / Aides</span> </dd> </div>
<div class="iCIMS_JobHeaderTag"> <dt class="iCIMS_JobHeaderField">Shift</dt> <dd class="iCIMS_JobHeaderData"><span > Day shift</span> </dd> </div>
</dl> </div> </div> </li> </ul>
'''


def test_chsli_card_header_location_and_campus():
    jobs = _parse_icims_cards(CHSLI_CARD, "Catholic Health (Long Island)", "careers-chsli.icims.com")
    assert len(jobs) == 1
    j = jobs[0]
    assert (j.city, j.state) == ("West Islip", "NY")
    assert j.location == "US-NY-West Islip"
    assert j.hospital_name == "Good Samaritan University Hospital"
    assert j.job_id == "74333"
    assert j.url == "https://careers-chsli.icims.com/jobs/74333/cna/job"
    assert j.specialty == "Direct Care / Aides"


def test_chsli_unknown_city_keeps_system_name():
    card = CHSLI_CARD.replace("US-NY-West Islip", "US-NY-Melville")
    j = _parse_icims_cards(card, "Catholic Health (Long Island)", "careers-chsli.icims.com")[0]
    assert (j.city, j.state) == ("Melville", "NY")
    assert j.hospital_name == "Catholic Health (Long Island)"


def test_mount_sinai_site_from_title():
    assert _mount_sinai_loc("MRI Technologist- Mount Sinai Morningside Monday - Friday", "", "") == \
        ("Mount Sinai Morningside", "New York", "NY")
    assert _mount_sinai_loc("RN - Emergency Department - MSSN", "", "") == \
        ("Mount Sinai South Nassau", "Oceanside", "NY")
    assert _mount_sinai_loc("Nurse Practitioner - Mount Sinai Brooklyn", "", "") == \
        ("Mount Sinai Brooklyn", "Brooklyn", "NY")
    assert _mount_sinai_loc("Patient Care Associate - MSH Cardiology", "", "") == \
        ("The Mount Sinai Hospital", "New York", "NY")


def test_mount_sinai_defaults_and_located_rows():
    # No site, no location: the system's home city.
    assert _mount_sinai_loc("Program Manager, Discharge Planning", "", "") == \
        ("Mount Sinai Health System", "New York", "NY")
    # Located rows keep their location and only gain the facility.
    assert _mount_sinai_loc("PET/CT Technologist- Mount Sinai Queens", "Astoria", "NY") == \
        ("Mount Sinai Queens", "Astoria", "NY")
    # A state without a city or a site is left alone (New Jersey rows).
    assert _mount_sinai_loc("Physician Assistant - Surgery", "", "NJ") == \
        ("Mount Sinai Health System", "", "NJ")
    # A New York row with a state but no city takes the site's city.
    assert _mount_sinai_loc("RN - Labor and Delivery - MSW", "", "NY") == ("Mount Sinai West", "New York", "NY")


def test_ny_boards_are_configured_once():
    assert scraper.ICIMS_ORGS["Catholic Health (Long Island)"] == "careers-chsli.icims.com"
    for label in ("Garnet Health", "Maimonides Health", "Bassett Healthcare Network"):
        assert label not in scraper.ICIMS_ORGS
        assert scraper.JIBE_SITES[label].startswith("https://")
        assert label in scraper.JIBE_FACILITY_NAME
    assert scraper.ORACLE_ORGS["Mount Sinai Health System"] == ("https://ejis.fa.us6.oraclecloud.com", "CX_1")


def test_layer4_exempts_partial_systems(monkeypatch):
    """mark_inactive_jobs must not bump or deactivate rows of exempt systems."""
    import database

    active_rows = [
        {"id": 1, "job_id": "a", "hospital_system": "HCA Healthcare", "consecutive_scrape_misses": 2},
        {"id": 2, "job_id": "b", "hospital_system": "Ellis Medicine", "consecutive_scrape_misses": 2},
        {"id": 3, "job_id": "c", "hospital_system": "Ellis Medicine", "consecutive_scrape_misses": 1},
    ]
    updates = []

    class _Q:
        def __init__(self, rows):
            self._rows = rows
            self._patch = None
        def select(self, *_a, **_k): return self
        def eq(self, *_a): return self
        def gt(self, _col, last_id):
            self._rows = [r for r in self._rows if r["id"] > last_id]
            return self
        def order(self, *_a, **_k): return self
        def limit(self, _n): return self
        def update(self, patch):
            self._patch = patch
            return self
        def in_(self, _col, ids):
            updates.append((self._patch, list(ids)))
            return self
        def execute(self):
            class R: pass
            r = R(); r.data = list(self._rows) if self._patch is None else []
            return r

    class _DB:
        def table(self, _name): return _Q(active_rows)

    monkeypatch.setattr(database, "client", lambda: _DB())
    out = database.mark_inactive_jobs([{"hospital_system": "Ellis Medicine", "job_id": "c"}],
                                      miss_threshold=3, exclude_systems={"HCA Healthcare"})
    assert out["excluded_rows"] == 1
    assert out["excluded_systems"] == ["HCA Healthcare"]
    assert out["deactivated"] == 1            # Ellis row b hit the threshold
    assert out["reset_to_found"] == 1         # Ellis row c was in the scrape
    deactivated_ids = [ids for patch, ids in updates if patch.get("is_active") is False]
    assert deactivated_ids == [[2]]           # HCA row 1 untouched
