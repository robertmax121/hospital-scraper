"""Detail passes and posting facts for the job page v2 (2026-09-21)."""
import scraper
from scraper import (_jobposting_from_html, _apply_posting, _oracle_posting_text, _phenom_posting_text,
                     extract_posting_facts, extract_schedule, extract_benefits, canonical_job_type, Job)


def _job(**kw):
    base = dict(title="RN", hospital_system="X", hospital_name="X", city="", state="", location="",
                specialty="", job_type="", url="https://x/job/1", job_id="1", posted_date="",
                description="", ats_platform="T")
    base.update(kw)
    return Job(**base)


LD = '''<html><head><script type="application/ld+json">
{"@context":"https://schema.org","@type":"JobPosting","title":"Lab Supervisor","datePosted":"2026-9-16",
 "employmentType":"Full Time","description":"<p>Job Summary and Responsibilities</p><p>%s</p>",
 "qualifications":"<ul><li>Bachelor's degree in clinical laboratory science</li><li>ASCP certification</li></ul>",
 "baseSalary":{"@type":"MonetaryAmount","currency":"USD","value":{"@type":"QuantitativeValue","minValue":52.15,"maxValue":77.9,"unitText":"HOUR"}}}
</script></head><body></body></html>''' % ("As a Lab Supervisor you lead the bench. " * 8)


def test_jsonld_posting_fills_description_type_date_and_pay():
    p = _jobposting_from_html(LD)
    assert p and p["@type"] == "JobPosting"
    j = _job()
    assert _apply_posting(j, p) is True
    assert "Lab Supervisor you lead the bench" in j.description
    assert "Qualifications" in j.description and "ASCP certification" in j.description
    assert j.job_type == "Full Time"
    assert j.posted_date == "2026-09-16"
    assert (j.wage_min, j.wage_max, j.wage_unit) == (52.15, 77.9, "hour")


def test_jsonld_posting_never_overwrites():
    j = _job(description="already here " * 80, job_type="Per diem", posted_date="2026-01-01")
    p = _jobposting_from_html(LD)
    assert _apply_posting(j, p) is False          # a longer stored body stays
    assert j.job_type == "Per diem" and j.posted_date == "2026-01-01"
    teaser = _job(description="short teaser " * 20)   # 260 chars: a fetched full body replaces it
    assert _apply_posting(teaser, p) is True and "lead the bench" in teaser.description


def test_no_jsonld_is_none():
    assert _jobposting_from_html("<html><body>nothing</body></html>") is None


def test_oracle_posting_text():
    it = {"JobSchedule": "Full time", "JobShift": "Shift1 - Day", "WorkHours": 36, "WorkDays": None,
          "ExternalDescriptionStr": "<p>Schedule</p><p>Full Time: 36 hours per week; 3 12 hour shifts</p>",
          "ExternalResponsibilitiesStr": "<p>Essential Functions: obtains vital signs.</p>",
          "ExternalQualificationsStr": "<p>Minimum Education: High School Diploma or GED Required</p>",
          "ExternalPostedStartDate": "2026-09-11T16:48:53+00:00"}
    desc, sched, start = _oracle_posting_text(it)
    assert desc.startswith("Schedule: Full time · Shift1 - Day · 36 hours per week")
    assert "Essential Functions" in desc and "Minimum Education" in desc
    assert sched == "Full time" and start == "2026-09-11"


def test_phenom_posting_text():
    desc, jt, created = _phenom_posting_text({"description": "<p>" + "Care for residents. " * 20 + "</p>", "shift": "Day", "type": "Full-time", "dateCreated": "2026-06-16T09:52:43.962+0000"})
    assert desc.startswith("Schedule: Day") and jt == "Full-time" and created == "2026-06-16"


TEXT = ("Full Time: 36 hours per week; 3 12 hour shifts (7:30 AM - 8:30 pm) with weekend rotation, Friday - Sunday. "
        "$10,000 sign-on bonus for experienced RNs and a $3,000 relocation package. "
        "Benefits from day one: medical, dental and vision coverage, paid time off, 403(b) retirement plan, "
        "4 weeks 100% paid parental leave, and tuition reimbursement. Requires BLS and a current RN license.")


def test_facts_schedule_bonus_benefits():
    f = extract_posting_facts(TEXT)
    assert f["schedule"].startswith("Fri–Sun · 3×12h")
    assert f["hours"] == 36
    assert f["signon"] == 10000 and f["relocation"] == 3000
    assert f["benefits"][:3] == ["Medical, dental and vision", "Benefits from day one", "403(b)"]
    assert "Paid time off" in f["benefits"]
    assert any(c[0] == "BLS" for c in f["certs"])


def test_facts_amounts_need_dollars_and_sanity():
    assert (extract_posting_facts("Sign-on bonus available. Great benefits.") or {}).get("signon") is None
    assert extract_posting_facts("Sign on bonus up to $15k for nights")["signon"] == 15000
    assert (extract_posting_facts("relocation of $250,000 is not a bonus") or {}).get("relocation") is None
    both = extract_posting_facts("$3,000 relocation assistance and a $10,000 sign-on bonus")
    assert (both["signon"], both["relocation"]) == (10000, 3000)


def test_schedule_only_hours():
    summary, hours = extract_schedule("This is a 24 hours per week position.", [])
    assert (summary, hours) == ("24 hrs/wk", 24)
    assert extract_schedule("No schedule words here.", []) == (None, None)


def test_benefits_cap_and_401k_dedupe():
    b = extract_benefits("401(k) with company match, 401(k), PTO, dental and vision, pension, EAP, childcare, tuition reimbursement")
    assert len(b) == 5 and "401(k) with match" in b and "401(k)" not in b


def test_job_type_canon():
    assert canonical_job_type("FULL_TIME") == "Full time"
    assert canonical_job_type("Full-time") == "Full time"
    assert canonical_job_type("Regular Full time") == "Full time"
    assert canonical_job_type("PRN/Per Diem") == "Per diem"
    assert canonical_job_type("Part Time") == "Part time"
    assert canonical_job_type("", "RN, PRN Nights") == "Per diem"
    assert canonical_job_type("Regular", "Registered Nurse") == ""
    assert canonical_job_type("Travel Nurse") == "Travel"
    assert canonical_job_type("Fellowship") == "Intern"
    assert canonical_job_type("Weekend Program") == "Weekend Program"


def test_budgets_and_flags_exist():
    assert scraper.DETAIL_FETCH in (True, False)
    for b in (scraper.ORACLE_DESC_BUDGET, scraper.TB_DESC_BUDGET, scraper.PHENOM_DESC_BUDGET):
        assert b.remaining >= 0


def test_bonus_ignores_salary_gaps():
    f = extract_posting_facts("Compensation: Salary up to $100k based on experience. Incentives: Sign on bonus up to $10k and student debt repayment")
    assert f["signon"] == 10000
    f2 = extract_posting_facts("Compensation: starting at $76,000 annual base. Incentives: potential for sign on bonuses")
    assert (f2 or {}).get("signon") is None
    f3 = extract_posting_facts("Physician Gastroenterology: CME allowance, Sign-on bonus - $100K, paid malpractice")
    assert f3["signon"] == 100000


def test_budget_share_and_skip(monkeypatch):
    import asyncio
    b = scraper._DescBudget(100)
    seen = []
    async def fetch(job):
        seen.append(job.url); return True
    jobs = [_job(url=f"https://x/job/{i}", job_id=str(i)) for i in range(80)]
    jobs += [_job(url=f"https://acme.taleo.net/j/{i}", job_id=f"t{i}") for i in range(20)]
    asyncio.run(scraper._detail_pass(None, "T", jobs, b, fetch, "Test", skip=lambda j: "taleo" in j.url, share=30))
    assert len(seen) == 30 and all("taleo" not in u for u in seen)
    assert b.remaining == 70 and b.spent == 30
