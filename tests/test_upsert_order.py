# 2026-10-12: the nightly hospital upsert sends systems stalest first.
# Runs 4-6 (10-10 to 10-12) landed 4,475 / 52,750 / 52,475 of ~325,000 rows
# before the breaker abandoned the rest, and every night the rows that landed
# were the head of the adapter order (Playwright + Workday). Ascension (Oracle,
# 29th of 59 adapters) was never refreshed after 10-09, so its hyphen-id fix
# (b25710a) never ran. No database, no network: the fake PostgREST of
# test_content_hash.py.
import copy

import scraper
from test_content_hash import FakeDB, _row

RUN0 = "2026-10-09T00:00:39.076420Z"       # the last night every system landed
RUN_LATE = "2026-10-12T00:04:50.564568Z"   # a stalled night that landed only the head
TONIGHT = "2026-10-13T00:05:00.000000Z"


def _rows(system, n):
    return [{"hospital_system": system, "job_id": str(i)} for i in range(n)]


def _systems(rows):
    return list(dict.fromkeys(r["hospital_system"] for r in rows))


# -- order_rows_for_upsert ---------------------------------------------------

def test_each_system_stays_together_and_keeps_its_row_order():
    rows = [{"hospital_system": s, "job_id": str(i)} for i, s in enumerate("ABABCA")]
    out = scraper.order_rows_for_upsert(rows, {}, TONIGHT)
    assert sorted(map(id, out)) == sorted(map(id, rows))          # the same dicts, none lost
    seq = [r["hospital_system"] for r in out]
    assert sum(1 for k, s in enumerate(seq) if k == 0 or seq[k - 1] != s) == 3
    for s in "ABC":
        assert ([r["job_id"] for r in out if r["hospital_system"] == s]
                == [r["job_id"] for r in rows if r["hospital_system"] == s])


def test_stalest_system_goes_first_and_a_never_stored_system_before_all():
    rows = _rows("Workday Board", 3) + _rows("Taleo Board", 2) + _rows("Ascension", 2) + _rows("New Board", 1)
    seen = {"Workday Board": "2026-10-12T00:04:50", "Taleo Board": "2026-10-11T00:01:59",
            "Ascension": "2026-10-09T00:00:39"}
    out = scraper.order_rows_for_upsert(rows, seen, TONIGHT)
    assert _systems(out) == ["New Board", "Ascension", "Taleo Board", "Workday Board"]


def test_without_stored_times_the_order_rotates_by_night_and_holds_within_one():
    systems = [f"Board {i:02d}" for i in range(40)]
    rows = [r for s in systems for r in _rows(s, 2)]

    def order(run):
        return _systems(scraper.order_rows_for_upsert(rows, {}, run))

    nights = [f"2026-10-{d:02d}T00:05:00.000000Z" for d in range(13, 20)]
    orders = [order(n) for n in nights]
    assert all(sorted(o) == systems for o in orders)
    assert order("2026-10-13T23:59:59.000000Z") == orders[0]       # one night, one order
    assert len({tuple(o) for o in orders}) == len(nights)
    assert len({o[-1] for o in orders}) > 1                         # nobody is last every night


def test_ties_in_staleness_use_the_nightly_rotation():
    systems = [f"Board {i:02d}" for i in range(12)]
    rows = [r for s in systems for r in _rows(s, 1)]
    same = {s: "2026-10-09T00:00:39" for s in systems}
    assert (scraper.order_rows_for_upsert(rows, same, TONIGHT)
            == scraper.order_rows_for_upsert(rows, {}, TONIGHT))


def test_seen_key_reads_both_timestamp_shapes():
    assert scraper._seen_key("2026-10-09 00:00:39.07642+00") == "2026-10-09T00:00:39"
    assert scraper._seen_key("2026-10-09T00:00:39.07642+00:00") == "2026-10-09T00:00:39"
    assert scraper._seen_key("2026-10-13T00:05:00.000000Z") == "2026-10-13T00:05:00"
    assert scraper._seen_key(None) == ""


# -- the nightly upsert ------------------------------------------------------

def _seed(db, system, n, scraped_at):
    """Stored active rows without a hash yet (sql/100 landed after the last
    full night), last refreshed at scraped_at."""
    for i in range(n):
        r = _row(system, i)
        r.update(id=db.next_id, scraped_at=scraped_at, first_seen=RUN0, is_active=True, content_hash=None)
        db.rows[(system, str(i))] = r
        db.next_id += 1


def _stalled_night(monkeypatch, pin_adapter_order=False):
    """Tonight's rows come in adapter order (Workday first, Ascension last);
    the database stops taking writes after 200 rows, as on 10-10 to 10-12."""
    db = FakeDB(monkeypatch)
    _seed(db, "Workday Board", 400, RUN_LATE)
    _seed(db, "Ascension", 50, RUN0)
    db.upsert_fail = lambda batch: ((503, "upstream connect error")
                                    if sum(len(b) for b in db.posts) >= 200 else None)
    if pin_adapter_order:
        monkeypatch.setattr(scraper, "order_rows_for_upsert", lambda rows, seen, run: rows)
    tonight = [_row("Workday Board", i) for i in range(400)] + [_row("Ascension", i) for i in range(50)]
    sent = scraper._upsert_hospital_jobs_to_supabase(copy.deepcopy(tonight), TONIGHT)
    return db, sent


def _refreshed(db, system):
    return sum(1 for r in db.rows.values() if r["hospital_system"] == system and r["scraped_at"] == TONIGHT)


def test_a_stalled_night_lands_the_stalest_system_first(monkeypatch):
    db, sent = _stalled_night(monkeypatch)
    assert scraper.LAST_RUN_COUNTS["hash_mode"] == "on"
    assert sent == 200
    first = [r["hospital_system"] for b in db.posts for r in b][:50]
    assert first == ["Ascension"] * 50
    assert _refreshed(db, "Ascension") == 50                       # all of it, so it can be swept
    assert _refreshed(db, "Workday Board") == 150
    assert {r["hospital_system"] for r in scraper.LAST_UPSERT_FAILED} == {"Workday Board"}


def test_the_adapter_order_starved_the_tail_on_the_same_night(monkeypatch):
    # The bug this fixes: in adapter order the same stall never reaches Ascension.
    db, sent = _stalled_night(monkeypatch, pin_adapter_order=True)
    assert sent == 200
    assert _refreshed(db, "Ascension") == 0
    assert _refreshed(db, "Workday Board") == 200
