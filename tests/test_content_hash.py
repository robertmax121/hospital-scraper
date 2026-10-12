# 2026-10-11: content hash. A row whose final payload hashes like the stored
# one is not upserted; it gets the narrow seen stamp (stamp_seen, or
# stamp_seen_posted when only its posted_date text moved), so scraped_at,
# posted_date and the sweep read exactly what the full upsert would leave.
# No database, no network: a fake PostgREST that keeps rows in a dict.
import copy
import io
import os
import re
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
        self.posts, self.rpcs, self.patches, self.loads, self.probes = [], [], [], 0, []
        self.has_column, self.stamp_fail, self.upsert_fail = has_column, stamp_fail, upsert_fail
        self.column_probes, self.column_probe_fail = 0, []
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
            # The run's opening RPC probe (empty ids) is kept apart from the stamps.
            (self.rpcs if body["p_ids"] else self.probes).append((fn, body))
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
                else:
                    self.update(k, **r)
            return _Resp()
        if m == "GET" and "select=content_hash&limit=1" in u:
            self.column_probes += 1
            if self.column_probe_fail:
                self._err(u, self.column_probe_fail.pop(0), "upstream")
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

    def update(self, k, **cols):
        """An UPDATE of row k (the upsert's conflict path, or manual SQL),
        with the content-hash guard trigger applied as sql/100 defines it."""
        old = dict(self.rows[k])
        new = dict(old, **cols)
        _content_hash_guard(old, new, cols)
        self.rows[k].update(new)

    def posted_keys(self, i=None):
        posts = self.posts if i is None else self.posts[i:]
        return [(r["hospital_system"], r["job_id"]) for b in posts for r in b]


# sql/100: trg_zz_content_hash_guard BEFORE UPDATE OF exactly these columns
# (every payload column that feeds content_hash_for), firing after
# trg_preserve_enrichment (same timing, name order).
GUARD_COLUMNS = ("title", "hospital_system", "hospital_name", "city", "state", "location",
                 "specialty", "job_type", "url", "job_id", "description", "ats_platform",
                 "wage_min", "wage_max", "wage_unit", "has_signon", "posting_facts",
                 "apply_verified", "derived_job_type", "employer_type")


def _content_hash_guard(old, new, set_cols):
    """hj_content_hash_guard(): an UPDATE naming a hashed column that leaves
    content_hash as stored while a hashed column changes clears the hash, so
    the next night writes the row in full instead of stamping it."""
    if not set(set_cols) & set(GUARD_COLUMNS):
        return                                   # the column list: stamps never fire it
    if new.get("content_hash") == old.get("content_hash") and any(
            new.get(c) != old.get(c) for c in GUARD_COLUMNS):
        new["content_hash"] = None


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
        if "/rpc/stamp_seen" in rq.full_url and json.loads(rq.data.decode())["p_ids"]:
            db.rows[("A Board", "5")]["is_active"] = False   # a stamp, not the opening probe
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


# -- the database guard (sql/100, trg_zz_content_hash_guard) --------------------

def test_guard_columns_are_exactly_the_columns_the_hash_reads(monkeypatch):
    db = FakeDB(monkeypatch)
    job = scraper.Job(title="Registered Nurse", hospital_system="A Board", hospital_name="A Board",
                      city="Odessa", state="TX", location="Odessa, TX", specialty="", job_type="Full time",
                      url="https://example.org/jobs/1", job_id="1", posted_date="Posted 3 Days Ago",
                      description="Body " * 60, ats_platform="workday")
    _night([scraper.normalize_job(job)], RUN1)
    sent = db.posts[0][0]
    # The stamps and the bookkeeping are sent but neither hashed nor guarded.
    assert {"scraped_at", "posted_date", "first_seen", "is_active", "content_hash"} <= set(sent)
    assert set(sent) - scraper._HASH_EXCLUDE == set(GUARD_COLUMNS)
    assert len(GUARD_COLUMNS) == len(set(GUARD_COLUMNS))


SQL_100 = os.path.join(os.environ.get("WAYPOINT_SQL_DIR", r"C:\Users\19403\OneDrive\claud_outputinput"
                                      r"\redesign_2026-09-09\sql"), "100_hj_content_hash.sql")


@pytest.mark.skipif(not os.path.exists(SQL_100), reason="sql/100 is kept outside the repo")
def test_sql_100_guard_lists_the_same_columns_and_fires_after_the_enrichment_trigger():
    text = io.open(SQL_100, encoding="utf-8").read()
    m = re.search(r"create or replace trigger (trg_zz_content_hash_guard) before update of (.+?) "
                  r"on public\.hospital_jobs for each row execute function public\.hj_content_hash_guard\(\);",
                  text)
    assert m and tuple(c.strip() for c in m.group(2).split(",")) == GUARD_COLUMNS
    # Same-timing row triggers fire in name order: the guard sees the row as
    # trg_preserve_enrichment (and trg_search_doc) left it.
    assert sorted(["trg_preserve_enrichment", "trg_search_doc", m.group(1)])[-1] == m.group(1)
    fn = next(ln for ln in text.splitlines() if "function public.hj_content_hash_guard()" in ln)
    for c in GUARD_COLUMNS:
        assert re.search(rf"\bnew\.{c}\b", fn) and re.search(rf"\bold\.{c}\b", fn), c
    assert "new.content_hash is not distinct from old.content_hash" in fn
    assert "new.content_hash := null" in fn


def _hashes(db):
    return {k: r.get("content_hash") for k, r in db.rows.items()}


@pytest.mark.parametrize("how", ["probe_failed", "older_checkout"])
def test_a_night_without_the_hash_that_changes_a_row_clears_its_hash(monkeypatch, how):
    """A failed column probe and an older checkout (hca_local_push,
    tools/wd_local_detail) upsert without content_hash; the guard clears the
    hash of each row they change, so a later night that sees the earlier
    content writes it in full instead of stamping over the changed row."""
    db = FakeDB(monkeypatch)
    _night([_row("A Board", i) for i in range(12)], RUN1)
    h0 = _hashes(db)
    real_probe = scraper._hospital_jobs_has_content_hash
    if how == "probe_failed":
        monkeypatch.setattr(scraper, "_HJ_CONTENT_HASH", {})
        db.column_probe_fail = [503] * 4
    else:
        monkeypatch.setattr(scraper, "_hospital_jobs_has_content_hash", lambda *a: False)
    _night([_row("A Board", i, title="Charge Nurse" if i == 3 else "Registered Nurse")
            for i in range(12)], RUN2)
    assert scraper.LAST_RUN_COUNTS["hash_mode"] == "absent"
    assert db.rows[("A Board", "3")]["content_hash"] is None          # changed: hash cleared
    assert all(_hashes(db)[k] == h0[k] for k in h0 if k != ("A Board", "3"))
    # Night 3, hash back on, the listing shows the night-1 title again.
    monkeypatch.setattr(scraper, "_hospital_jobs_has_content_hash", real_probe)
    monkeypatch.setattr(scraper, "_HJ_CONTENT_HASH", {})
    before = len(db.posts)
    _night([_row("A Board", i) for i in range(12)], "2026-10-12T00:09:00.000000Z")
    assert db.posted_keys(before) == [("A Board", "3")]                # written in full, not stamped
    assert db.rows[("A Board", "3")]["title"] == "Registered Nurse"
    assert db.rows[("A Board", "3")]["content_hash"] == h0[("A Board", "3")]


def test_a_manual_relabel_clears_the_hash_and_the_next_night_writes_the_row(monkeypatch):
    db = FakeDB(monkeypatch)
    _night([_row("A Board", i) for i in range(12)], RUN1)
    db.update(("A Board", "4"), specialty="ICU")                      # relabel SQL
    assert db.rows[("A Board", "4")]["content_hash"] is None
    before = len(db.posts)
    _night([_row("A Board", i) for i in range(12)], RUN2)
    assert db.posted_keys(before) == [("A Board", "4")]                # as every night before the hash
    assert db.rows[("A Board", "4")]["content_hash"]


def test_the_stamps_and_the_scrapers_own_writes_keep_the_hash(monkeypatch):
    db = FakeDB(monkeypatch)
    _night([_row("A Board", i) for i in range(12)], RUN1)
    h0 = _hashes(db)
    db.update(("A Board", "1"), scraped_at=RUN2, posted_date="2026-10-01", is_active=True)
    assert _hashes(db) == h0                                           # not in the column list
    _night([_row("A Board", i, posted_date="Posted 4 Days Ago",
                 title="LPN" if i == 2 else "Registered Nurse") for i in range(12)], RUN2)
    h1 = _hashes(db)
    assert all(h1.values()) and h1[("A Board", "2")] != h0[("A Board", "2")]
    assert all(h1[k] == h0[k] for k in h0 if k != ("A Board", "2"))


# -- the column probe retries ---------------------------------------------------

def test_column_probe_retries_a_blip_before_falling_back(monkeypatch):
    db = FakeDB(monkeypatch)
    pauses = []
    monkeypatch.setattr(scraper.time, "sleep", pauses.append)
    db.column_probe_fail = [503, 502, 500]
    assert scraper._hospital_jobs_has_content_hash("https://fake.invalid", "k") is True
    assert db.column_probes == 4 and pauses == list(scraper.HASH_PROBE_PAUSES)
    assert scraper._HJ_CONTENT_HASH == {"ok": True}


def test_column_probe_gives_up_after_three_retries_without_caching(monkeypatch):
    db = FakeDB(monkeypatch)
    db.column_probe_fail = [503] * 4
    assert scraper._hospital_jobs_has_content_hash("https://fake.invalid", "k") is False
    assert db.column_probes == 4 and scraper._HJ_CONTENT_HASH == {}


def test_column_probe_400_is_definite_and_not_retried(monkeypatch):
    db = FakeDB(monkeypatch, has_column=False)
    assert scraper._hospital_jobs_has_content_hash("https://fake.invalid", "k") is False
    assert db.column_probes == 1 and scraper._HJ_CONTENT_HASH == {"ok": False}


# -- the stamp RPC probe, and a stamp RPC that disappears mid-run ---------------

@pytest.mark.parametrize("fn,err", [("stamp_seen", (404, '{"code":"PGRST202"}')),
                                    ("stamp_seen_posted", (403, '{"code":"42501"}')),
                                    ("stamp_seen_posted", (503, "upstream"))])
def test_a_failed_rpc_probe_turns_the_hash_off_for_the_run(monkeypatch, fn, err):
    db = FakeDB(monkeypatch)
    _night([_row("A Board", i) for i in range(12)], RUN1)
    db.stamp_fail = lambda f, b: err if f == fn and not b["p_ids"] else None
    loads, before = db.loads, len(db.posts)
    assert _night([_row("A Board", i) for i in range(12)], RUN2) == 12
    c = scraper.LAST_RUN_COUNTS
    assert c["hash_mode"] == "off" and fn in c["hash_off_reason"] and "hash_new" not in c
    assert not db.rpcs and db.loads == loads                         # no stamps, no hash load
    assert len(db.posted_keys(before)) == 12                          # every row in full
    assert all(r["content_hash"] is None for b in db.posts[before:] for r in b)
    assert all(r["scraped_at"] == RUN2 for r in db.rows.values()) and not scraper.LAST_UPSERT_FAILED
    assert scheduler.hash_notes(c) == f"; content hash off ({c['hash_off_reason']}), full upsert"
    assert scheduler.hash_meta(c)["reason"] == c["hash_off_reason"]


def test_the_rpc_probe_sends_empty_arrays(monkeypatch):
    db = FakeDB(monkeypatch)
    _night([_row("A Board", i) for i in range(3)], RUN1)
    assert [(fn, b["p_ids"], b.get("p_posted")) for fn, b in db.probes] == [
        ("stamp_seen", [], None), ("stamp_seen_posted", [], [])]
    assert scraper.LAST_RUN_COUNTS["hash_mode"] == "on"


@pytest.mark.parametrize("code", [404, 401, 403])
def test_a_stamp_rpc_gone_mid_run_sends_the_rest_through_the_full_upsert(monkeypatch, code):
    db = FakeDB(monkeypatch)
    _night([_row("A Board", i) for i in range(600)], RUN1)
    # Night 2: the first stamp batch (500) lands, then the RPC is gone; rows
    # 590-599 also need their posted_date written (the other partition).
    db.stamp_fail = lambda f, b: (code, '{"code":"PGRST202"}') if b["p_ids"] and db.rpcs[1:] else None
    rows2 = [_row("A Board", i, posted_date="Posted 4 Days Ago" if i >= 590 else "Posted 3 Days Ago")
             for i in range(600)]
    before, p0 = len(db.posts), len(db.patches)
    assert _night(rows2, RUN2) == 600
    assert [(fn, len(b["p_ids"])) for fn, b in db.rpcs[-2:]] == [("stamp_seen", 500), ("stamp_seen", 90)]
    assert len(db.posted_keys(before)) == 100                         # 90 + 10, none marked failed
    assert not scraper.LAST_UPSERT_FAILED and db.patches[p0:] == ["A Board"]
    assert all(r["scraped_at"] == RUN2 and r["is_active"] for r in db.rows.values())
    assert all(db.rows[("A Board", str(i))]["posted_date"] == "Posted 4 Days Ago" for i in range(590, 600))
    c = scraper.LAST_RUN_COUNTS
    assert (c["stamp_landed"], c["stamp_fallback"], c["stamp_failed"], c["stamp_posted"]) == (500, 100, 0, 0)
    assert f"HTTP {code}" in c["hash_off_reason"] and c["hash_mode"] == "on"
    assert scheduler.hash_notes(c).endswith(f"{c['hash_off_reason']}: 100 sent through the full upsert")
