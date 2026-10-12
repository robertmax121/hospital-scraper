# 2026-10-11: content hash. A row whose final payload hashes like the stored
# one is not upserted; it gets the narrow seen stamp (stamp_seen, or
# stamp_seen_posted when only its posted_date text moved), so scraped_at,
# posted_date and the sweep read exactly what the full upsert would leave.
# No database, no network: a fake PostgREST that keeps rows in a dict.
import copy
import io
import json
import urllib.error
import urllib.parse
import urllib.request

import pytest

import scraper
import scheduler

RUN1 = "2026-10-10T00:09:00.000000Z"
RUN2 = "2026-10-11T00:09:00.000000Z"


def _row(system, i, **kw):
    r = {"hospital_system": system, "job_id": str(i), "title": "Registered Nurse",
         "hospital_name": system, "url": f"https://example.org/jobs/{i}", "job_type": "",
         "description": "Body " * 60, "state": "TX", "city": "Odessa", "posted_date": "Posted 3 Days Ago"}
    r.update(kw)
    return r


class _Resp:
    def __init__(self, body=b"", content_range=""):
        self.body = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.headers = {"Content-Range": content_range}
    def read(self): return self.body
    def __enter__(self): return self
    def __exit__(self, *a): return False


class FakeDB:
    """hospital_jobs as {(system, job_id): row}; records every call."""

    def __init__(self, monkeypatch, has_column=True, stamp_fail=None, upsert_fail=None):
        self.rows, self.next_id = {}, 1
        self.posts, self.rpcs, self.patches, self.loads = [], [], [], 0
        self.has_column, self.stamp_fail, self.upsert_fail = has_column, stamp_fail, upsert_fail
        monkeypatch.setenv("SUPABASE_URL", "https://fake.invalid")
        monkeypatch.setenv("SUPABASE_KEY", "test-key")
        monkeypatch.setattr(urllib.request, "urlopen", self.urlopen)
        monkeypatch.setattr(scraper.time, "sleep", lambda _s: None)
        monkeypatch.setattr(scraper, "_HJ_CONTENT_HASH", {})
        monkeypatch.setattr(scraper, "_HJ_FIRST_SEEN", {"ok": True})

    def _err(self, u, code, body):
        raise urllib.error.HTTPError(u, code, "err", {}, io.BytesIO(body.encode()))

    def urlopen(self, rq, timeout=None):
        u, m = rq.full_url, rq.get_method()
        if "/qa_link_audit" in u:
            return _Resp()
        if "/rpc/stamp_seen" in u:
            body = json.loads(rq.data.decode())
            fn = u.rsplit("/", 1)[1]
            self.rpcs.append((fn, body))
            if self.stamp_fail:
                err = self.stamp_fail(fn, body)
                if err:
                    self._err(u, *err)
            by_id = {r["id"]: r for r in self.rows.values()}
            left = []
            for n, i in enumerate(body["p_ids"]):
                r = by_id.get(i)
                if r is None or not r["is_active"]:
                    left.append(i)
                    continue
                r["scraped_at"] = body["p_ts"]
                if fn == "stamp_seen_posted":
                    r["posted_date"] = body["p_posted"][n]
            return _Resp(left)
        if m == "POST":
            rows = json.loads(rq.data.decode())
            if self.upsert_fail:
                err = self.upsert_fail(rows)
                if err:
                    self._err(u, *err)
            self.posts.append(rows)
            for r in rows:
                k = (r["hospital_system"], r["job_id"])
                cur = self.rows.get(k)
                if cur is None:
                    cur = self.rows[k] = {"id": self.next_id}
                    self.next_id += 1
                cur.update(r)
            return _Resp()
        if m == "GET" and "select=content_hash&limit=1" in u:
            if not self.has_column:
                self._err(u, 400, '{"code":"42703"}')
            return _Resp([])
        if m == "GET" and "select=first_seen" in u:
            return _Resp([])
        if m == "GET" and "select=id,job_id,hospital_system,content_hash,posted_date" in u:
            self.loads += 1
            last = int(u.split("id=gt.")[1].split("&")[0])
            page = sorted((r for r in self.rows.values() if r["is_active"] and r["id"] > last),
                          key=lambda r: r["id"])[:scraper.KNOWN_BODY_PAGE]
            return _Resp([{"id": r["id"], "job_id": r["job_id"], "hospital_system": r["hospital_system"],
                           "content_hash": r.get("content_hash"), "posted_date": r.get("posted_date")}
                          for r in page])
        system = urllib.parse.unquote(u.split("hospital_system=eq.")[1].split("&")[0])
        if m == "GET":
            n = sum(1 for r in self.rows.values() if r["hospital_system"] == system and r["is_active"])
            return _Resp(content_range=f"0-0/{n}")
        cutoff = urllib.parse.unquote(u.split("scraped_at=lt.")[1].split("&")[0])
        self.patches.append(system)
        for r in self.rows.values():
            if r["hospital_system"] == system and r["is_active"] and r["scraped_at"] < cutoff:
                r["is_active"] = False
        return _Resp(content_range="*/0")

    def posted_keys(self, i=None):
        posts = self.posts if i is None else self.posts[i:]
        return [(r["hospital_system"], r["job_id"]) for b in posts for r in b]


def _night(rows, run):
    return scraper._upsert_hospital_jobs_to_supabase(copy.deepcopy(rows), run)


# -- the hash itself ---------------------------------------------------------

def test_hash_ignores_seen_stamp_columns_posted_date_and_key_order():
    a = _row("A Board", 1)
    b = dict(reversed(list(a.items())))
    b.update(scraped_at=RUN2, first_seen=RUN1, is_active=False, posted_date="Posted 4 Days Ago",
             content_hash="old")
    assert scraper.content_hash_for(a) == scraper.content_hash_for(b)


@pytest.mark.parametrize("field,value", [("description", "Body " * 61), ("title", "LPN"),
                                         ("url", "https://example.org/jobs/x"), ("wage_min", 40),
                                         ("specialty", "ICU"), ("apply_verified", True)])
def test_hash_changes_with_any_payload_column(field, value):
    a = _row("A Board", 1)
    b = dict(a, **{field: value})
    assert scraper.content_hash_for(a) != scraper.content_hash_for(b)


def test_hash_version_forces_a_rewrite(monkeypatch):
    a = _row("A Board", 1)
    h1 = scraper.content_hash_for(a)
    monkeypatch.setattr(scraper, "HASH_VERSION", scraper.HASH_VERSION + 1)
    assert scraper.content_hash_for(a) != h1


def test_facts_skip_night_hashes_like_the_night_that_wrote_the_facts(monkeypatch):
    wrote = _row("A Board", 1, posting_facts={"v": 7, "h": "abc123abc123", "shift": "night"})
    skipped = _row("A Board", 1, posting_facts=None)
    monkeypatch.setattr(scraper, "_KNOWN_FACTS_H", {("A Board", "1"): "abc123abc123"})
    assert scraper.content_hash_for(wrote) == scraper.content_hash_for(skipped)
    other = _row("A Board", 1, posting_facts={"v": 7, "h": "ffffffffffff"})
    assert scraper.content_hash_for(other) != scraper.content_hash_for(wrote)
    monkeypatch.setattr(scraper, "_KNOWN_FACTS_H", {})
    assert scraper.content_hash_for(skipped) != scraper.content_hash_for(wrote)


# -- posted_date: never in the hash, written exactly as today ---------------

@pytest.mark.parametrize("stored,tonight,write", [
    ("Posted 3 Days Ago", "Posted 3 Days Ago", False),     # same text: the stamp alone is today's result
    ("Posted 3 Days Ago", "Posted 4 Days Ago", True),      # precise label moved: written with scraped_at
    ("Posted 30+ Days Ago", "Posted 30+ Days Ago", False), # coarse label unchanged
    ("2 weeks ago", "3 weeks ago", True),                  # coarse label moved: written too
    ("2026-10-01", "Posted 3 Days Ago", False),            # stored ISO: the trigger keeps it anyway
    ("2026-10-01", None, False),
    ("2026-10-01", "2026-10-02", True),
    (None, "Today", True),
    ("Yesterday", "2026-10-09", True),
    (None, None, False),
])
def test_posted_date_write_rule_matches_the_enrichment_trigger(stored, tonight, write):
    assert scraper._posted_needs_write(stored, tonight) is write


def test_relative_labels_are_sent_as_the_list_gave_them(monkeypatch):
    db = FakeDB(monkeypatch)
    _night([_row("A Board", i) for i in range(12)], RUN1)
    assert {r["posted_date"] for b in db.posts for r in b} == {"Posted 3 Days Ago"}
    rows = [_row("A Board", i, posted_date="Posted 4 Days Ago") for i in range(12)]
    _night(rows, RUN2)
    assert len(db.posts) == 1                                   # nothing re-upserted
    assert [fn for fn, _ in db.rpcs] == ["stamp_seen_posted"]
    for r in db.rows.values():
        assert (r["posted_date"], r["scraped_at"], r["is_active"]) == ("Posted 4 Days Ago", RUN2, True)


# -- partitioning ------------------------------------------------------------

def test_partition_new_changed_unchanged_reactivated_and_no_stored_hash():
    rows = [_row("A Board", i) for i in range(6)]
    for r in rows:
        r["content_hash"] = scraper.content_hash_for(r)
    stored = {
        ("A Board", "1"): (11, "different", "Posted 3 Days Ago"),             # changed
        ("A Board", "2"): (12, rows[2]["content_hash"], "Posted 3 Days Ago"),  # unchanged, same label
        ("A Board", "3"): (13, rows[3]["content_hash"], "Posted 2 Days Ago"),  # unchanged, label moved
        ("A Board", "4"): (14, None, "Posted 3 Days Ago"),                    # stored before the column
        # 0 is new; 5 is inactive in the DB, so the active-only load omits it
    }
    full, seen, posted, ids, counts = scraper.partition_by_hash(rows, stored)
    assert [r["job_id"] for r in full] == ["0", "1", "4", "5"]
    assert [r["job_id"] for r in seen] == ["2"]
    assert [r["job_id"] for r in posted] == ["3"]
    assert ids == {("A Board", "2"): 12, ("A Board", "3"): 13}
    assert counts == {"new": 2, "changed": 2, "unchanged": 2}


def test_second_night_stamps_unchanged_rows_and_the_sweep_retires_only_unseen(monkeypatch):
    db = FakeDB(monkeypatch)
    night1 = [_row("A Board", i) for i in range(30)]
    assert _night(night1, RUN1) == 30
    assert all(r.get("content_hash") for r in db.rows.values())
    # Night 2: 0-19 unchanged, 20-24 changed, 25-29 closed, 30-31 new.
    night2 = ([_row("A Board", i) for i in range(20)]
              + [_row("A Board", i, title="Charge Nurse") for i in range(20, 25)]
              + [_row("A Board", i) for i in range(30, 32)])
    before, p0 = len(db.posts), len(db.patches)
    assert _night(night2, RUN2) == 27                            # stamped rows count as landed
    assert sorted(db.posted_keys(before), key=lambda k: int(k[1])) == \
        [("A Board", str(i)) for i in [20, 21, 22, 23, 24, 30, 31]]
    assert all("content_hash" in r for b in db.posts for r in b)
    stamped = [i for fn, b in db.rpcs if fn == "stamp_seen" for i in b["p_ids"]]
    assert len(stamped) == 20 and all(b["p_ts"] == RUN2 for _, b in db.rpcs)
    assert db.patches[p0:] == ["A Board"]
    active = {k[1] for k, r in db.rows.items() if r["is_active"]}
    assert active == {str(i) for i in list(range(25)) + [30, 31]}
    assert all(db.rows[("A Board", str(i))]["scraped_at"] == RUN2 for i in range(20))
    c = scraper.LAST_RUN_COUNTS
    assert (c["hash_new"], c["hash_changed"], c["hash_unchanged"], c["stamp_landed"]) == (2, 5, 20, 20)
    assert c["upsert_sent"] == 27 and not scraper.LAST_UPSERT_FAILED


def test_a_row_reactivated_tonight_takes_the_full_upsert(monkeypatch):
    db = FakeDB(monkeypatch)
    _night([_row("A Board", i) for i in range(12)], RUN1)
    db.rows[("A Board", "3")]["is_active"] = False               # retired since (front-pool check)
    before = len(db.posts)
    _night([_row("A Board", i) for i in range(12)], RUN2)
    assert db.posted_keys(before) == [("A Board", "3")]
    assert db.rows[("A Board", "3")]["is_active"] is True


def test_a_row_retired_between_load_and_stamp_goes_back_to_the_upsert(monkeypatch):
    db = FakeDB(monkeypatch)
    _night([_row("A Board", i) for i in range(12)], RUN1)
    real = db.urlopen

    def racing(rq, timeout=None):
        if "/rpc/stamp_seen" in rq.full_url:
            db.rows[("A Board", "5")]["is_active"] = False
        return real(rq, timeout)
    monkeypatch.setattr(urllib.request, "urlopen", racing)
    before = len(db.posts)
    assert _night([_row("A Board", i) for i in range(12)], RUN2) == 12
    assert db.posted_keys(before) == [("A Board", "5")]
    assert db.rows[("A Board", "5")]["is_active"] is True
    assert scraper.LAST_RUN_COUNTS["stamp_missed"] == 1


def test_a_failed_stamp_batch_blocks_that_systems_sweep(monkeypatch):
    db = FakeDB(monkeypatch)
    rows1 = [_row("A Board", i) for i in range(15)] + [_row("B Board", i) for i in range(15)]
    _night(rows1, RUN1)
    # Night 2: B's stamps are refused; both boards lost row 14 (closed).
    db.stamp_fail = lambda fn, b: (400, '{"code":"P0001"}') if any(
        db.rows[k]["id"] in b["p_ids"] for k in db.rows if k[0] == "B Board") else None
    rows2 = [_row("A Board", i) for i in range(14)] + [_row("B Board", i) for i in range(14)]
    p0 = len(db.patches)
    sent = _night(rows2, RUN2)
    assert db.patches[p0:] == ["A Board"]                         # B is not swept tonight
    assert db.rows[("A Board", "14")]["is_active"] is False
    assert all(r["is_active"] for k, r in db.rows.items() if k[0] == "B Board")
    failed = {(r["hospital_system"], r["job_id"]) for r in scraper.LAST_UPSERT_FAILED}
    assert failed == {("B Board", str(i)) for i in range(14)}
    assert sent == 14 and scraper.LAST_RUN_COUNTS["stamp_failed"] == 14
    # The delayed retry re-sends them as full rows (the upsert writes them as before).
    db.stamp_fail = None
    assert scraper.retry_failed_hospital_upsert() == 14
    assert all(db.rows[("B Board", str(i))]["scraped_at"] == RUN2 for i in range(14))


def test_stamp_batches_split_and_retry_like_the_upsert(monkeypatch):
    db = FakeDB(monkeypatch)
    _night([_row("A Board", i) for i in range(600)], RUN1)
    db.stamp_fail = lambda fn, b: (500, '{"code":"57014"}') if len(b["p_ids"]) > 150 else None
    p0 = len(db.patches)
    assert _night([_row("A Board", i) for i in range(600)], RUN2) == 600
    sizes = [len(b["p_ids"]) for _, b in db.rpcs]
    assert sizes[:3] == [500, 250, 125]                          # 500 timed out, then split in half
    assert all(r["scraped_at"] == RUN2 and r["is_active"] for r in db.rows.values())
    assert db.patches[p0:] == ["A Board"] and not scraper.LAST_UPSERT_FAILED


# -- kill switch and the missing column ---------------------------------------

def _payloads(monkeypatch, **fake):
    db = FakeDB(monkeypatch, **fake)
    _night([_row("A Board", i) for i in range(12)], RUN1)
    _night([_row("A Board", i) for i in range(12)], RUN2)
    return db


def test_hash_skip_0_is_the_full_upsert_with_a_null_hash(monkeypatch):
    absent = _payloads(monkeypatch, has_column=False)
    monkeypatch.setenv("HASH_SKIP", "0")
    off = _payloads(monkeypatch)
    assert not off.rpcs and off.loads == 0 and not absent.rpcs and absent.loads == 0
    assert len(off.posts) == len(absent.posts) == 2
    for a, b in zip(absent.posts, off.posts):
        assert [dict(r, content_hash=None) for r in a] == b       # same rows, plus content_hash: null
    assert scraper.LAST_RUN_COUNTS["hash_mode"] == "off"
    assert off.patches == absent.patches == ["A Board", "A Board"]


def test_without_the_column_the_payload_is_unchanged(monkeypatch):
    db = _payloads(monkeypatch, has_column=False)
    assert all("content_hash" not in r for b in db.posts for r in b)
    assert scraper.LAST_RUN_COUNTS["hash_mode"] == "absent"
    assert "hash_new" not in scraper.LAST_RUN_COUNTS


@pytest.mark.parametrize("v,on", [("0", False), ("off", False), ("false", False), ("1", True), ("", True)])
def test_hash_skip_values(monkeypatch, v, on):
    monkeypatch.setenv("HASH_SKIP", v)
    assert scraper.hash_skip_enabled() is on


# -- bookkeeping ---------------------------------------------------------------

def test_notes_and_meta_carry_the_counts():
    counts = {"hash_mode": "on", "hash_new": 2, "hash_changed": 5, "hash_unchanged": 20,
              "stamp_landed": 19, "stamp_posted": 4, "stamp_failed": 1, "stamp_missed": 0}
    assert scheduler.hash_notes(counts) == (
        "; content hash: new 2, changed 5, unchanged stamped 19 (posted_date refreshed 4), "
        "stamp not landed 1, stamp back to upsert 0")
    assert scheduler.hash_meta(counts) == {"mode": "on", "hash_new": 2, "hash_changed": 5,
                                           "hash_unchanged": 20, "stamp_landed": 19, "stamp_posted": 4,
                                           "stamp_failed": 1, "stamp_missed": 0}
    assert scheduler.hash_notes({"hash_mode": "off"}) == "; content hash off (HASH_SKIP=0), full upsert"
    assert scheduler.hash_notes({}) == "" and scheduler.hash_meta({}) == {"mode": "absent"}


def test_finish_run_record_writes_inserted_and_updated(monkeypatch):
    import database
    sent = {}

    class _T:
        def update(self, patch):
            sent.update(patch)
            return self
        def eq(self, *a):
            return self
        def execute(self):
            return None

    class _C:
        def table(self, name):
            return _T()
    monkeypatch.setattr(database, "service_client", lambda: _C())
    assert database.finish_run_record(7, "success", RUN2, jobs_inserted=2, jobs_updated=5)
    assert sent["jobs_inserted"] == 2 and sent["jobs_updated"] == 5
