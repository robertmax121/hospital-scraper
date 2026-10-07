# 2026-10-07 (push 10, run time): the substring gates in front of the heavy
# facts / requirements patterns (outputs must not change), the facts hash
# that lets normalize_job skip an unchanged body, and the finalize_jobs
# process pool with its sequential fallback.
import glob
import os
import re

import pytest

import scraper
from scraper import Job

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")

BODY = ("Registered Nurse, ICU. The unit cares for adult critical care patients. " * 6
        + "\nMinimum Qualifications:\n"
        "Current RN license in the State of Texas required. BLS certification required; ACLS preferred.\n"
        "Bachelor's degree in nursing (BSN) preferred.\n"
        "2+ years of ICU experience required.\n"
        "Shift: Nights, 3x12s.\n"
        "Benefits: medical, dental and vision; 401(k) with match; PTO.\n")


def _job(**kw):
    base = dict(title="Registered Nurse ICU", hospital_system="Test Health", hospital_name="Test Hospital",
                city="Austin", state="TX", location="Austin, TX", specialty="", job_type="Full time",
                url="https://example.org/j/1", job_id="1", posted_date="2026-10-01", description=BODY,
                ats_platform="Workday")
    base.update(kw)
    return Job(**base)


def _corpus(limit=120):
    """Bodies the suite already ships, as the parsers see them (strip_html
    on markup), for the gate checks; plus the synthetic body above."""
    out = [BODY]
    for p in sorted(glob.glob(os.path.join(FIXTURES, "**", "*"), recursive=True)):
        if not os.path.isfile(p):
            continue
        try:
            with open(p, encoding="utf-8", errors="ignore") as f:
                t = f.read()
        except OSError:
            continue
        if "<" in t:
            t = scraper.strip_html(t)
        if len(t) >= 300:
            out.append(t[:12000])
        if len(out) >= limit:
            break
    return out


def _units(bodies):
    for t in bodies:
        for line in t.split("\n"):
            for s in re.split(r"(?<=[.!?;])\s+", line.strip()):
                if s:
                    yield s


# ── gates ───────────────────────────────────────────────────────────────────

def test_gate_text_and_may_match():
    assert scraper._gate_text("Current RN License") == "current rn license"
    # the letters IGNORECASE folds onto ASCII turn the gate off
    for ch in ("\u0130", "\u0131", "\u017f", "\u212a"):
        assert scraper._gate_text("pen" + ch + "ion") is None
    assert scraper._may_match(None, ("zzz",)) is True
    assert scraper._may_match("abc", ("zzz",)) is False
    assert scraper._may_match("abc", ("zzz", "b")) is True


@pytest.mark.parametrize("pattern,words", [
    ("_CERT_GATE_RX", "_CERT_GATE_WORDS"),
    ("_EDU_GATE_RX", "_EDU_GATE_WORDS"),
    ("_RQ_CUE_RX", "_RQ_CUE_GATE_WORDS"),
    ("_RQ_BLOCK_END_RX", "_RQ_BLOCK_END_GATE_WORDS"),
    ("_RQ_HARD_STOP_RX", "_RQ_HARD_STOP_GATE_WORDS"),
    ("_RQ_EDU_RX", "_RQ_EDU_GATE_WORDS"),
    ("_RQ_LIC_RX", "_RQ_LIC_GATE_WORDS"),
    ("_RQ_LIC_CODE_RX", "_RQ_LIC_GATE_WORDS"),
])
def test_gate_words_are_necessary_for_a_match(pattern, words):
    rx, ws = getattr(scraper, pattern), getattr(scraper, words)
    checked = 0
    for s in _units(_corpus()):
        if rx.search(s):
            checked += 1
            assert scraper._may_match(scraper._gate_text(s), ws), (pattern, s[:120])
    assert checked > 0, pattern


def test_gate_words_cover_the_known_short_forms():
    # the fragments that are easy to miss when reading the patterns
    assert scraper._CERT_GATE_RX.search("Valid BCLS Certification") and scraper._may_match("valid bcls certification", scraper._CERT_GATE_WORDS)
    assert scraper._EDU_GATE_RX.search("B.S. in Nursing") and scraper._may_match("b.s. in nursing", scraper._EDU_GATE_WORDS)
    assert scraper._RQ_EDU_RX.search("MS in Nursing") and scraper._may_match("ms in nursing", scraper._RQ_EDU_GATE_WORDS)
    assert scraper._RQ_LIC_CODE_RX.search("VA RN License") and scraper._may_match("va rn license", scraper._RQ_LIC_GATE_WORDS)
    assert scraper._RQ_LIC_RX.search("Current DEA registration") and scraper._may_match("current dea registration", scraper._RQ_LIC_GATE_WORDS)
    assert scraper._NURSE_LICENSE_RX.search("licensed to practice nursing in Texas") and "licen" in "licensed to practice nursing in texas"


def test_benefit_gates_are_necessary_for_a_match():
    bodies = _corpus()
    for label, rx in scraper._BENEFIT_RXS:
        gate = scraper._BENEFIT_GATES[label]
        for t in bodies:
            if rx.search(t) or (label == "Continuing education" and scraper._PRO_DEV_RX.search(t)):
                assert scraper._may_match(scraper._gate_text(t), gate), (label, t[:80])


def test_shift_field_gate_is_necessary_for_a_match():
    bodies = _corpus() + ["Work Shift:\n3 - Night (United States of America)\n", "Schedule: 7am-7pm day shift\n",
                          "shift : Days\n", "Night shift differential offered.\n"]
    hits = 0
    for t in bodies:
        if any(rx.search(t) for _, rx in scraper._SHIFT_FIELD_RXS):
            hits += 1
            assert scraper._SHIFT_FIELD_GATE_RX.search(t), t[:80]
    assert hits >= 3
    assert not scraper._SHIFT_FIELD_GATE_RX.search("Night shift differential offered.\n")


def test_unglue_prechecks_are_necessary_for_a_change():
    cases = {
        "caps": (scraper._RQ_CAPS_GLUED_RX, scraper._RQ_CAPS_HEAD_PRE_RX, "assigned.REQUIRED QUALIFICATIONSA Diagnostic"),
        "title": (scraper._RQ_GLUED_TITLE_RX, scraper._rq_glued_title_may, "No Degree or DiplomaAdditional Job Description:"),
        "years": (scraper._RQ_YEARS_GLUED_RX, scraper._RQ_YEARS_GLUED_PRE_RX, "Board Eligible2 years of experience preferred"),
        "the": (scraper._RQ_THE_BREAK_RX, scraper._RQ_THE_BREAK_PRE_RX, "licensed by the\nState of Texas"),
        "stop": (scraper._RQ_HEAD_AFTER_STOP_RX, scraper._RQ_HEAD_AFTER_STOP_PRE_RX, "meets OSHA.Licensure: RN"),
        "camel": (scraper._RQ_CAMEL_RX, scraper._RQ_CAMEL_PRE_RX, "injectionPrior experience required"),
        "sentence": (scraper._RQ_GLUED_SENTENCE_RX, scraper._RQ_GLUED_SENTENCE_PRE_RX, "paid annually.Essential Functions"),
        "slash": (scraper._RQ_SLASH_HEAD_INLINE_RX, scraper._RQ_SLASH_HEAD_PRE_RX, "the United States Licensure/Certification Maintains current BLS"),
    }
    bodies = _corpus()
    for name, (rx, pre, example) in cases.items():
        passes = pre if callable(pre) and not hasattr(pre, "search") else pre.search
        assert passes(example), name
        repl = scraper._rq_camel if name == "camel" else ("\n\\1\n" if name == "slash" else "\n")
        assert rx.sub(repl, example) != example, name
        for t in bodies:
            if rx.sub(repl, t) != t:
                assert passes(t), (name, t[:80])


def test_gates_do_not_change_the_facts(monkeypatch):
    """Every gate forced open (the pre-change behaviour) gives the same
    posting_facts as the gated run on the suite's bodies."""
    bodies = _corpus(80)
    gated = [scraper.posting_facts_for(t, "full_time", "Registered Nurse") for t in bodies]
    always = re.compile("")
    monkeypatch.setattr(scraper, "_gate_text", lambda s: None)
    monkeypatch.setattr(scraper, "_BENEFIT_GATES", {})
    monkeypatch.setattr(scraper, "_SHIFT_FIELD_GATE_RX", always)
    monkeypatch.setattr(scraper, "_rq_glued_title_may", lambda t: True)
    for name in ("_RQ_CAPS_HEAD_PRE_RX", "_RQ_SLASH_HEAD_PRE_RX", "_RQ_THE_BREAK_PRE_RX", "_RQ_YEARS_GLUED_PRE_RX",
                 "_RQ_HEAD_AFTER_STOP_PRE_RX", "_RQ_CAMEL_PRE_RX", "_RQ_GLUED_SENTENCE_PRE_RX"):
        monkeypatch.setattr(scraper, name, always)
    open_gates = [scraper.posting_facts_for(t, "full_time", "Registered Nurse") for t in bodies]
    assert open_gates == gated
    assert any(f and f.get("requirements", {}).get("licensure") for f in gated)   # the corpus exercised the readers


# ── facts hash and the skip ─────────────────────────────────────────────────

def test_facts_hash_covers_every_input():
    h = scraper._facts_hash("RN", "full_time", "text", BODY)
    assert re.fullmatch(r"[0-9a-f]{12}", h)
    assert h == scraper._facts_hash("RN", "full_time", "text", BODY)
    assert h != scraper._facts_hash("RN II", "full_time", "text", BODY)
    assert h != scraper._facts_hash("RN", "part_time", "text", BODY)
    assert h != scraper._facts_hash("RN", "full_time", "field", BODY)
    assert h != scraper._facts_hash("RN", "full_time", "text", BODY + " BLS required.")
    assert scraper._facts_hash("RN", "full_time", None, "short teaser " * 5) is None


def test_normalize_job_stamps_the_hash_then_skips_an_unchanged_body():
    scraper.set_known_bodies([])
    first = scraper.normalize_job(_job())
    facts = first["posting_facts"]
    assert facts and facts["v"] == scraper.FACTS_VERSION and re.fullmatch(r"[0-9a-f]{12}", facts["h"])
    assert facts.get("pay_src") in (None, "text")          # BODY states no wage: no pay source
    stored = {"hospital_system": "Test Health", "job_id": "1", "ats_platform": "Findly-Google",
              "desc_len": len(BODY), "wage_min": None, "fv": scraper.FACTS_VERSION, "fh": facts["h"]}
    # a platform outside the detail passes: no known body, but the hash is known
    scraper.set_known_bodies([stored])
    assert ("Test Health", "1") not in scraper._KNOWN_BODIES
    assert scraper._KNOWN_FACTS_H[("Test Health", "1")] == facts["h"]
    scraper._FACTS_SKIPS["n"] = 0
    second = scraper.normalize_job(_job())
    assert second["posting_facts"] is None and scraper._FACTS_SKIPS["n"] == 1
    assert {k: v for k, v in second.items() if k not in ("posting_facts", "scraped_at")} == \
           {k: v for k, v in first.items() if k not in ("posting_facts", "scraped_at")}
    # facts from older rules are parsed again
    scraper.set_known_bodies([dict(stored, fv=scraper.FACTS_VERSION - 1)])
    assert ("Test Health", "1") not in scraper._KNOWN_FACTS_H
    assert scraper.normalize_job(_job())["posting_facts"]["h"] == facts["h"]
    # a changed title, body or pay source parses again and carries a new hash
    scraper.set_known_bodies([stored])
    for changed in (_job(title="Registered Nurse ICU Nights"), _job(description=BODY + "\nNRP required."),
                    _job(wage_min=40.0, wage_max=55.0, wage_unit="hour")):
        row = scraper.normalize_job(changed)
        assert row["posting_facts"] is not None and row["posting_facts"]["h"] != facts["h"]
    scraper.set_known_bodies([])


def test_teaser_never_carries_or_uses_a_hash():
    scraper.set_known_bodies([])
    row = scraper.normalize_job(_job(description="RN NIGHTS. BLS required. Apply now for this great role today."))
    assert not (row["posting_facts"] or {}).get("h")


def test_set_known_bodies_platform_filter_and_hash_map():
    rows = [
        {"hospital_system": "A", "job_id": "1", "ats_platform": "Workday", "desc_len": 2000, "fv": scraper.FACTS_VERSION, "fh": "aaaaaaaaaaaa"},
        {"hospital_system": "A", "job_id": "2", "ats_platform": "Findly-Google", "desc_len": 2000, "fv": scraper.FACTS_VERSION, "fh": "bbbbbbbbbbbb"},
        {"hospital_system": "A", "job_id": "3", "desc_len": 2000, "fv": scraper.FACTS_VERSION},            # no platform key: as before
        {"hospital_system": "A", "job_id": "4", "ats_platform": "Workday", "desc_len": 2000, "fv": None, "fh": "cccccccccccc"},
        {"hospital_system": "A", "job_id": "5", "ats_platform": "Workday", "desc_len": 150, "fv": scraper.FACTS_VERSION, "fh": "dddddddddddd"},
    ]
    assert scraper.set_known_bodies(rows) == 3
    assert set(scraper._KNOWN_BODIES) == {("A", "1"), ("A", "3"), ("A", "4")}
    assert scraper._KNOWN_FACTS_H == {("A", "1"): "aaaaaaaaaaaa", ("A", "2"): "bbbbbbbbbbbb"}
    scraper.set_known_bodies([])
    assert scraper._KNOWN_FACTS_H == {}


# ── the process pool ────────────────────────────────────────────────────────

def _five():
    return [_job(job_id=str(i), title=f"Registered Nurse ICU {i}", description=BODY + f"\nRequisition {i}.") for i in range(5)]


def test_normalize_jobs_single_chunk_stays_sequential(monkeypatch):
    monkeypatch.setattr(scraper, "FINALIZE_WORKERS", 4)
    monkeypatch.setattr(scraper, "FINALIZE_CHUNK", 5000)
    monkeypatch.setattr(scraper, "_finalize_pool", lambda n: (_ for _ in ()).throw(AssertionError("pool used")))
    jobs = _five()
    assert scraper.normalize_jobs(jobs) == [scraper.normalize_job(j) for j in jobs]


# 2026-10-07 (push 10 review): os.cpu_count() is the host's count, not the
# container's quota. An unset FINALIZE_WORKERS must stay at the cap however
# many CPUs the kernel reports; an explicit setting is used as given.

def test_finalize_workers_default_never_exceeds_the_cap(monkeypatch):
    monkeypatch.setattr(scraper, "FINALIZE_WORKERS", 0)
    monkeypatch.setattr(os, "cpu_count", lambda: 64)
    monkeypatch.setattr(os, "sched_getaffinity", lambda pid: set(range(64)), raising=False)   # the Linux path
    assert scraper.FINALIZE_WORKERS_CAP == 2
    assert scraper._cpus_seen() == 64
    assert scraper._finalize_workers() == 2
    monkeypatch.delattr(os, "sched_getaffinity", raising=False)                               # the Windows / macOS path
    assert scraper._cpus_seen() == 64
    assert scraper._finalize_workers() == 2
    # fewer CPUs than the cap, or no answer at all: never more than there are, never below one
    monkeypatch.setattr(os, "cpu_count", lambda: 1)
    assert scraper._finalize_workers() == 1
    monkeypatch.setattr(os, "cpu_count", lambda: None)
    assert scraper._cpus_seen() == 1 and scraper._finalize_workers() == 1
    # an affinity call that fails falls through to cpu_count
    monkeypatch.setattr(os, "cpu_count", lambda: 64)
    monkeypatch.setattr(os, "sched_getaffinity", lambda pid: (_ for _ in ()).throw(OSError("no affinity")), raising=False)
    assert scraper._cpus_seen() == 64 and scraper._finalize_workers() == 2
    # an explicit setting is the operator's call, above or below the cap
    monkeypatch.setattr(scraper, "FINALIZE_WORKERS", 6)
    assert scraper._finalize_workers() == 6
    monkeypatch.setattr(scraper, "FINALIZE_WORKERS", 1)
    assert scraper._finalize_workers() == 1


def test_normalize_jobs_logs_the_chosen_worker_count_before_the_pool_starts(monkeypatch, caplog):
    jobs = _five()
    scraper.set_known_bodies([])
    monkeypatch.setattr(scraper, "FINALIZE_WORKERS", 0)
    monkeypatch.setattr(scraper, "FINALIZE_CHUNK", 2)
    monkeypatch.setattr(os, "cpu_count", lambda: 64)
    monkeypatch.setattr(os, "sched_getaffinity", lambda pid: set(range(64)), raising=False)
    seen = []

    def broken(n):
        seen.append(n)
        raise RuntimeError("no pool tonight")
    monkeypatch.setattr(scraper, "_finalize_pool", broken)
    with caplog.at_level("INFO"):
        out = scraper.normalize_jobs(jobs)
    assert seen == [2]                                      # 64 CPUs seen, 3 chunks, still 2 workers
    assert out == [scraper.normalize_job(j) for j in jobs]
    msgs = [r.getMessage() for r in caplog.records]
    chosen = [m for m in msgs if m.startswith("finalize_jobs: 2 workers for 3 chunks of 2 ")]
    assert chosen and "FINALIZE_WORKERS=0" in chosen[0] and "64 CPUs seen" in chosen[0] and "default cap 2" in chosen[0]
    # the count line precedes the pool failure line
    assert msgs.index(chosen[0]) < next(i for i, m in enumerate(msgs) if "process pool stopped after 0 of 5 rows" in m)


def test_normalize_jobs_pool_matches_sequential_and_moves_the_counters(monkeypatch):
    jobs = _five()
    scraper.set_known_bodies([])
    monkeypatch.setattr(scraper, "FINALIZE_WORKERS", 1)
    seq = scraper.normalize_jobs(jobs)
    # night 2: the hashes are known, the workers must see the map
    scraper.set_known_bodies([{"hospital_system": "Test Health", "job_id": r["job_id"], "ats_platform": "Workday",
                               "desc_len": len(r["description"]), "fv": scraper.FACTS_VERSION, "fh": r["posting_facts"]["h"]}
                              for r in seq])
    monkeypatch.setattr(scraper, "FINALIZE_WORKERS", 2)
    monkeypatch.setattr(scraper, "FINALIZE_CHUNK", 2)
    scraper._FACTS_SKIPS["n"] = 0
    monkeypatch.delenv("SCRAPER_WORKER", raising=False)
    par = scraper.normalize_jobs(jobs)
    assert "SCRAPER_WORKER" not in os.environ
    assert scraper._FACTS_SKIPS["n"] == 5
    assert [r["posting_facts"] for r in par] == [None] * 5
    assert [{k: v for k, v in r.items() if k != "posting_facts"} for r in par] == \
           [{k: v for k, v in r.items() if k != "posting_facts"} for r in seq]
    scraper.set_known_bodies([])


def test_normalize_jobs_falls_back_to_sequential_when_the_pool_fails(monkeypatch, caplog):
    jobs = _five()
    scraper.set_known_bodies([])
    monkeypatch.setattr(scraper, "FINALIZE_WORKERS", 2)
    monkeypatch.setattr(scraper, "FINALIZE_CHUNK", 2)

    def broken(n):
        raise RuntimeError("no pool tonight")
    monkeypatch.setattr(scraper, "_finalize_pool", broken)
    with caplog.at_level("WARNING"):
        out = scraper.normalize_jobs(jobs)
    assert out == [scraper.normalize_job(j) for j in jobs]
    assert any("process pool stopped after 0 of 5 rows" in r.getMessage() for r in caplog.records)


def test_finalize_jobs_keeps_order_and_the_longer_body_through_the_pool(monkeypatch):
    scraper.set_known_bodies([])
    monkeypatch.setattr(scraper, "FINALIZE_WORKERS", 2)
    monkeypatch.setattr(scraper, "FINALIZE_CHUNK", 1)
    jobs = _five()
    jobs.insert(2, _job(job_id="1", title="Registered Nurse ICU 1", description=BODY[:300]))   # a shorter copy of job 1
    rows = scraper.finalize_jobs(jobs)
    assert [r["job_id"] for r in rows] == ["0", "1", "2", "3", "4"]
    assert rows[1]["description"].endswith("Requisition 1.")
