"""push3/reqfix (2026-09-24): University Health TalentBrew page detail,
requirement false positives found in the 40-posting hand check, one-line
bodies of the still-low systems, and the small misfiles (Licensures, VITAS
"equivalent experience or licensure", Phenom schedule line inside 12,000)."""
import asyncio
import re
import os

import scraper
from scraper import Job

FIX = os.path.join(os.path.dirname(__file__), "fixtures")


def _job(**kw):
    base = dict(title="RN", hospital_system="X", hospital_name="X", city="", state="", location="",
                specialty="", job_type="", url="https://x/job/1", job_id="1", posted_date="",
                description="", ats_platform="T")
    base.update(kw)
    return Job(**base)


class _R:
    def __init__(self, body):
        self.status, self._body = 200, body

    async def text(self):
        return self._body


class _Ctx:
    def __init__(self, r):
        self.r = r

    async def __aenter__(self):
        return self.r

    async def __aexit__(self, *a):
        return False


def test_university_health_page_without_jsonld(monkeypatch):
    html = open(os.path.join(FIX, "uh_tb_page_100998069312.html"), encoding="utf-8").read()
    monkeypatch.setattr(scraper, "req", lambda session, method, url, **kw: _Ctx(_R(html)))
    monkeypatch.setattr(scraper, "_fetch_html", lambda *a, **k: asyncio.sleep(0, result=html))
    url = "https://careers.universityhealth.com/job/san-antonio/med-surg-technician/43277/100998069312"
    old = _job(hospital_system="University Health (San Antonio)", url=url, job_id="100998069312")
    assert asyncio.run(scraper._jsonld_detail(None, old)) is False          # the old pass: nothing
    new = _job(hospital_system="University Health (San Antonio)", url=url, job_id="100998069312")
    assert asyncio.run(scraper._tb_page_detail(None, new)) is True
    assert len(new.description) >= 500
    assert "high school diploma" in new.description.lower()
    rq = scraper.extract_requirements(new.description)
    assert rq["qualifications"]["required"] and rq["education"]


def test_university_health_uses_tb_page_detail(monkeypatch):
    assert "University Health (San Antonio)" in scraper.TB_PAGE_DETAIL_ORGS
    used = []

    async def fake_scrape(session, sys, url, rpp):
        return [_job(hospital_system=sys, url=f"https://x/{sys}", job_id=sys)]

    async def fake_pass(session, system, jobs, budget, fetch_one, label, **kw):
        assert budget is scraper.TB_DESC_BUDGET
        await fetch_one(jobs[0])

    async def page(session, job):
        used.append(("page", job.hospital_system))
        return True

    async def jsonld(session, job):
        used.append(("jsonld", job.hospital_system))
        return True
    monkeypatch.setattr(scraper, "DETAIL_FETCH", True)
    monkeypatch.setattr(scraper, "scrape_talentbrew", fake_scrape)
    monkeypatch.setattr(scraper, "_detail_pass", fake_pass)
    monkeypatch.setattr(scraper, "_tb_page_detail", page)
    monkeypatch.setattr(scraper, "_jsonld_detail", jsonld)
    asyncio.run(scraper.run_talentbrew(None))
    assert ("page", "University Health (San Antonio)") in used
    assert ("jsonld", "ScionHealth") in used
    assert ("jsonld", "University Health (San Antonio)") not in used


QUALS = os.path.join(FIX, "quals")


def _rq(name):
    return scraper.extract_requirements(open(os.path.join(QUALS, name), encoding="utf-8").read())


def _all(rq):
    return (rq["qualifications"]["required"] + rq["qualifications"]["preferred"]
            + [x[0] for f in ("certifications", "licensure", "education") for x in rq[f]])


def test_licensures_none_stores_nothing():
    rq = scraper.extract_requirements("Qualifications\nLicensures and Certifications: None.\nEducation: None.\n")
    assert _all(rq) == []


def test_vitas_equivalent_experience_is_not_education():
    t = ("QUALIFICATIONS\nCurrently licensed to practice nursing in the state where the VITAS program is located.\n"
         "A minimum of two years of nursing experience in hospice, home health, or community health in the last five years.\n"
         "Equivalent experience or licensure may be considered\n")
    rq = scraper.extract_requirements(t)
    assert rq["education"] == []
    assert rq["licensure"] and len(rq["qualifications"]["required"]) == 3


def test_phenom_schedule_line_inside_facts_window():
    long_desc = "<p>" + "Provide compassionate care to patients and families every day. " * 60 + "</p>"
    jd = {"description": long_desc * 5, "shift": "Night", "type": "Full time"}
    desc, jt, _ = scraper._phenom_posting_text(jd)
    line = "Schedule: Night shift"
    assert line in desc and desc.index(line) + len(line) <= 12000
    f = scraper.extract_posting_facts(desc, jt)
    assert f["shift"] and f["shift"][0][0] == "Nights"


def test_choa_single_space_headings():
    rq = _rq("wall_choa_903635.txt")
    assert any("Master" in e[0] for e in rq["education"])
    assert rq["certifications"] and rq["licensure"]
    assert not any(x.startswith("N/A") or x.startswith("No minimum") for x in _all(rq))


def test_st_charles_caps_headings_and_na():
    rq = _rq("wall_stcharles_905128.txt")
    assert rq["education"][0][0].startswith("High school graduate or GED")
    assert any("Basic Life Support" in c[0] for c in rq["certifications"])
    assert not any("N/A" in x for x in _all(rq))
    assert not any("driver" in c[0].lower() for c in rq["certifications"])       # a driver's licence is not a certification


def test_halifax_headless_bullets():
    rq = _rq("bullets_halifax_581830.txt")
    assert rq["licensure"] == [["RN – State of Florida", False]]
    assert rq["education"] and rq["education"][0][1] is True                     # "bachelor degree preferred"
    assert not any(x.startswith(("Reports to work", "Maintains", "Administers")) for x in _all(rq))


def test_franciscan_working_conditions_and_training_are_not_education():
    rq = _rq("oracle_franciscan_29196482.txt")
    assert [e[0] for e in rq["education"]] == ["High school or equivalent"]
    assert not any(re.search(r"exposure|temperature|repetitive|stoop|daily operator", x, re.I) for x in _all(rq))


def test_great_river_knowledge_of_benefits_keeps_the_block():
    rq = _rq("workday_greatriver_3697322.txt")
    certs = " ".join(c[0] for c in rq["certifications"])
    assert "Advanced Cardiac Life Support" in certs and "Neonatal Resuscitation" in certs


def test_glued_caps_headings_entities_and_names():
    t = ("Performs other duties as assigned.REQUIRED QUALIFICATIONSA Diagnostic Medical Sonographer Certificate OR "
         "Bachelor&#39;s Degree in Ultrasound is required for all new hires.\nPREFERRED QUALIFICATIONS\nBLS Certification\n"
         "Work at MultiCare and AdventHealth.\n")
    rq = scraper.extract_requirements(t)
    assert rq["qualifications"]["required"] == [
        "A Diagnostic Medical Sonographer Certificate OR Bachelor's Degree in Ultrasound is required for all new hires."]
    assert "MultiCare" in scraper._rq_unwall(scraper._rq_unglue("Join MultiCare and AdventHealth today"), True)


def test_physical_requirements_end_the_block():
    t = ("Qualifications\nCurrent RN license required.\nPHYSICAL REQUIREMENTS: Continually (75% or more): Standing and walking.\n"
         "Never (0%): Climbing ladder.\nRarely (10%): Climbing stairs.\n")
    rq = scraper.extract_requirements(t)
    assert rq["qualifications"]["required"] == ["Current RN license required."]


def test_abbreviation_does_not_split_a_clause():
    assert scraper._rq_clauses("Bachelor's degree for external applicants at metro hospitals and St. Francis. BLS required") == [
        "Bachelor's degree for external applicants at metro hospitals and St. Francis.", "BLS required"]


def test_cleveland_nbsp_items_and_minimum_qualifications_for():
    t = ("Assist in keeping patient rooms in order.\nMinimum qualifications for the ideal future caregiver include:\n"
         "High School Diploma or GED Successful completion of Basic Life Support (BLS) through American Heart Association (AHA)"
         " Prior job or educational experience providing basic computer knowledge and skills\n"
         "Physical Requirements:\nMedium Work - Exerting 20 to 50 pounds of force occasionally\n"
         "Join the Cleveland Clinic team in the State of Ohio.\n")
    rq = scraper.extract_requirements(t)
    assert rq["qualifications"]["required"][:2] == [
        "High School Diploma or GED",
        "Successful completion of Basic Life Support (BLS) through American Heart Association (AHA)"]
    assert rq["education"] == [["High School Diploma or GED", False]]
    assert not any("pounds" in x for x in rq["qualifications"]["required"])
