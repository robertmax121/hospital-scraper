# 2026-09-24: HCA PARTIAL detection (_hca_fetch_slice) and the saved-rows /
# --replay path of hca_local_push.py. Offline: _curl_fetch is faked with
# synthetic search pages, the shared upsert is replaced by a recorder, and the
# shared-folder credentials are never loaded, so nothing reaches the network
# or the database.
import asyncio
import json
import os
from datetime import datetime, timedelta, timezone

import pytest

import scraper
import hca_local_push as hlp

# Cloudflare's bot-detection beacon: present on EVERY normal HCA page, with or
# without cards (verified on live pages 2026-09-24). The old challenge test
# matched its 'challenge-platform' path and flagged every slice PARTIAL.
CF_BEACON = ("<script>(function(){var a=document.createElement('script');"
             "a.src='/cdn-cgi/challenge-platform/scripts/jsd/main.js';"
             "document.getElementsByTagName('head')[0].appendChild(a);})();</script>")
EMPTY_PAGE = "<html><head>" + CF_BEACON + "</head><body><p>0 jobs</p></body></html>"
CHALLENGE_PAGE = ("<html><head><title>Just a moment...</title></head>"
                  "<body><div id='cf-chl-widget'></div></body></html>")


def _card(job_id, state="TX", city="Plano", parseable=True):
    anchor = (f'<a class="neu-link" href="https://careers.hcahealthcare.com/jobs/{job_id}-registered-nurse">'
              f'Registered Nurse {job_id}</a>' if parseable else f'<span>Registered Nurse {job_id}</span>')
    return ('<div class="jobs-section__item-outer"><div class="jobs-section__item">'
            f'<div class="neu-text--caption">Medical City {city}</div>'
            f'<div class="neu-text--caption neu-margin--bottom-10"> {city}, {state}, United States </div>'
            f'<h2 class="neu-text--h6">{anchor}</h2>'
            '<i class="material-icons">work</i> Full-time</div></div>')


def _page(ids, total=None, state="TX", unparseable=()):
    head = ""
    if total is not None:
        head = (f'<div class="jobs-heading neu-text--support"><span class="d-none d-lg-inline">'
                f'Showing </span>1-{len(ids)} of {total} results</div>')
    cards = "".join(_card(i, state, parseable=i not in unparseable) for i in ids)
    return "<html><head>" + CF_BEACON + "</head><body>" + head + cards + "</body></html>"


class _Resp:
    def __init__(self, text):
        self.text = text


def _serve(monkeypatch, pages):
    """pages: {page_number: html}. Any other page is the empty past-the-end
    page, beacon included. Returns the list of page numbers requested."""
    served = []

    def fake_fetch(method, url, impersonate, timeout=60, **kw):
        p = int(kw["params"]["page"])
        served.append(p)
        return _Resp(pages.get(p, EMPTY_PAGE))

    monkeypatch.setattr(scraper, "_curl_fetch", fake_fetch)
    monkeypatch.setattr(scraper.time, "sleep", lambda *_: None)
    return served


def _ids(start, n):
    return [str(start + k) for k in range(n)]


# ── PARTIAL detection ────────────────────────────────────────────────────
def test_beacon_on_the_empty_page_past_the_end_is_not_a_challenge(monkeypatch):
    # An inventory that is an exact multiple of the page size still needs the
    # empty page past the end; that page carries the beacon and must finish.
    monkeypatch.setattr(scraper, "HCA_PAGE_SIZE", 5)
    served = _serve(monkeypatch, {1: _page(_ids(100, 5), total=10), 2: _page(_ids(200, 5), total=10)})
    jobs, finished, capped = scraper._hca_fetch_slice("tx-texas")
    assert (len(jobs), finished, capped) == (10, True, False)
    assert served == [1, 2, 3]
    assert "challenge-platform" in EMPTY_PAGE and not scraper._hca_is_challenge(EMPTY_PAGE)


def test_last_nights_texas_shape_finishes_on_the_short_page(monkeypatch):
    # 09-23 home run: TX 4,558 = 9 full pages of 500 + 58, then the empty
    # page 11 was called a challenge. Now page 10 ends the slice, no page 11.
    total = 4558
    pages = {p: _page(_ids(p * 1000, 500), total=total) for p in range(1, 10)}
    pages[10] = _page(_ids(10 * 1000, 58), total=total)
    served = _serve(monkeypatch, pages)
    jobs, finished, capped = scraper._hca_fetch_slice("tx-texas")
    assert (len(jobs), finished, capped) == (4558, True, False)
    assert served == list(range(1, 11))
    assert len({j.job_id for j in jobs}) == 4558 and all(j.state == "TX" for j in jobs)


def test_one_page_state_needs_one_request(monkeypatch):
    # ar-arkansas (3 jobs) used to request page 2 and get marked PARTIAL.
    served = _serve(monkeypatch, {1: _page(_ids(1, 3), total=3, state="AR")})
    jobs, finished, capped = scraper._hca_fetch_slice("ar-arkansas")
    assert (len(jobs), finished, capped) == (3, True, False)
    assert served == [1]


@pytest.mark.parametrize("challenge", [CHALLENGE_PAGE, "<html><body><form id='cf-chl-form'></form></body></html>"])
def test_a_real_challenge_page_still_marks_the_slice_partial(monkeypatch, challenge):
    monkeypatch.setattr(scraper, "HCA_PAGE_SIZE", 5)
    _serve(monkeypatch, {1: _page(_ids(100, 5), total=40), 2: challenge})
    jobs, finished, capped = scraper._hca_fetch_slice("tx-texas")
    assert (len(jobs), finished, capped) == (5, False, False)


def test_a_listing_that_ends_well_short_of_the_site_total_is_partial(monkeypatch):
    monkeypatch.setattr(scraper, "HCA_PAGE_SIZE", 5)
    # short page at 7 of 100 listed
    _serve(monkeypatch, {1: _page(_ids(100, 5), total=100), 2: _page(_ids(200, 2), total=100)})
    assert scraper._hca_fetch_slice("tx-texas")[1:] == (False, False)
    # empty page at 10 of 100 listed
    _serve(monkeypatch, {1: _page(_ids(100, 5), total=100), 2: _page(_ids(200, 5), total=100)})
    jobs, finished, _ = scraper._hca_fetch_slice("tx-texas")
    assert len(jobs) == 10 and not finished


def test_a_per_page_cap_below_our_page_size_is_partial_not_a_short_finish(monkeypatch):
    # If the site ever served 100 per page, page 1 would look "short"; the
    # listed total keeps that from finishing (and sweeping) a truncated state.
    _serve(monkeypatch, {1: _page(_ids(1, 100), total=4561)})
    jobs, finished, capped = scraper._hca_fetch_slice("tx-texas")
    assert (len(jobs), finished, capped) == (100, False, False)


def test_a_small_drift_from_the_listed_total_still_finishes(monkeypatch):
    # Postings close while the crawl pages; a few missing is not a truncation.
    total = 4561
    pages = {p: _page(_ids(p * 1000, 500), total=total) for p in range(1, 10)}
    pages[10] = _page(_ids(10 * 1000, 58), total=total)          # 4,558 of 4,561
    _serve(monkeypatch, pages)
    jobs, finished, _ = scraper._hca_fetch_slice("tx-texas")
    assert len(jobs) == 4558 and finished


def test_a_card_the_parser_skips_does_not_end_the_slice_early(monkeypatch):
    # Page 1 holds 5 card containers but only 4 parse: still a full page.
    monkeypatch.setattr(scraper, "HCA_PAGE_SIZE", 5)
    served = _serve(monkeypatch, {1: _page(_ids(100, 5), unparseable={"102"}),
                                  2: _page(_ids(200, 3))})
    jobs, finished, capped = scraper._hca_fetch_slice("tx-texas")
    assert (len(jobs), finished, capped) == (7, True, False)
    assert served == [1, 2]


def test_a_slice_that_fills_the_page_cap_is_capped(monkeypatch):
    monkeypatch.setattr(scraper, "HCA_PAGE_SIZE", 5)
    monkeypatch.setattr(scraper, "HCA_PAGE_CAP", 2)
    served = _serve(monkeypatch, {1: _page(_ids(100, 5)), 2: _page(_ids(200, 5))})
    assert scraper._hca_fetch_slice("tx-texas")[1:] == (True, True)
    assert served == [1, 2]


def test_total_parser_reads_both_header_variants():
    assert scraper._hca_total('<span class="d-none d-lg-inline">Showing </span>1-<span class="sr-only"> to '
                              '</span>20 of 4561 results') == 4561
    assert scraper._hca_total("Showing 1-500 of 12,034 results") == 12034
    assert scraper._hca_total("<p>no header</p>") is None


def test_run_hca_is_not_partial_when_every_slice_finishes(monkeypatch):
    pytest.importorskip("curl_cffi")   # run_hca returns [] without it, by design
    monkeypatch.setattr(scraper, "HCA_PAGE_SIZE", 5)
    by_slug = {"tx-texas": {1: _page(_ids(100, 5), total=7), 2: _page(_ids(200, 2), total=7)},
               "ar-arkansas": {1: _page(_ids(300, 3), total=3, state="AR")}}

    def fake_fetch(method, url, impersonate, timeout=60, **kw):
        slug = url.rsplit("/", 1)[-1]
        return _Resp(by_slug[slug].get(int(kw["params"]["page"]), EMPTY_PAGE))

    monkeypatch.setattr(scraper, "_curl_fetch", fake_fetch)
    monkeypatch.setattr(scraper.time, "sleep", lambda *_: None)
    monkeypatch.setattr(scraper, "_hca_discover_state_slugs", lambda: ["tx-texas", "ar-arkansas"])
    jobs = asyncio.run(scraper.run_hca(None))
    assert len(jobs) == 10
    assert "HCA Healthcare" not in scraper.PARTIAL_SYSTEMS and scraper._HCA_FAILED_SLICES == []


# ── Saved rows and --replay ──────────────────────────────────────────────
@pytest.fixture(autouse=True)
def _no_credentials_no_writes(monkeypatch):
    """Never load the shared-folder key, and fail loudly if anything reaches
    the real upsert; tests that push install a recorder instead."""
    monkeypatch.setattr(hlp, "SHARED_ENV_CANDIDATES", [])
    monkeypatch.setattr(hlp, "_load_shared_env", lambda: None)

    def refuse(*a, **k):
        raise AssertionError("the real upsert was called")

    monkeypatch.setattr(scraper, "_upsert_hospital_jobs_to_supabase", refuse)
    yield
    assert not os.environ.get("SUPABASE_KEY")


class _Recorder:
    def __init__(self, land=None):
        self.calls = []
        self.land = land          # rows to report as sent; None = all

    def __call__(self, rows, run_started_iso):
        self.calls.append({"rows": [dict(r) for r in rows], "run_started_iso": run_started_iso,
                           "partial": "HCA Healthcare" in scraper.PARTIAL_SYSTEMS})
        return len(rows) if self.land is None else self.land


def _rows():
    """Finalized rows the way crawl_and_push builds them: parsed cards through
    finalize_jobs, a detail-pass body on some."""
    html = _page(_ids(500, 3), state="CO") + _page(_ids(600, 4), state="TX") + _page(_ids(700, 2), state="FL")
    jobs = scraper._parse_hca_cards(html)
    for j in jobs[:5]:
        j.description = "Registered Nurse. " * 120
    return scraper.finalize_jobs(jobs)


def _iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _save(tmp_path, rows=None, hours_ago=6, partial=False, name="hca_rows_20260923.json"):
    rows = _rows() if rows is None else rows
    started = _iso(datetime.now(timezone.utc) - timedelta(hours=hours_ago))
    path = hlp.save_rows(rows, str(tmp_path / name), started, partial,
                         ["tx-texas"] if partial else [])
    assert path == str(tmp_path / name)
    return path, rows, started


def test_saved_rows_load_back_unchanged(tmp_path):
    path, rows, started = _save(tmp_path)
    assert not os.path.exists(path + ".tmp")
    data = hlp.load_rows_file(path)
    assert data["rows"] == json.loads(json.dumps(rows))
    assert data["run_started_iso"] == started and data["partial"] is False and data["dupes"] == 0
    assert 5.9 < data["age_hours"] < 6.1
    assert len(rows) == 9 and {r["hospital_system"] for r in rows} == {"HCA Healthcare"}


def test_rows_file_path_uses_the_logs_folder():
    assert hlp.rows_file_path("20260923") == os.path.join(hlp.LOGS_DIR, "hca_rows_20260923.json")


def test_save_keeps_only_the_newest_files(tmp_path, monkeypatch):
    monkeypatch.setattr(hlp, "ROWS_KEEP", 3)
    for d in ("20260917", "20260918", "20260919", "20260920", "20260921"):
        (tmp_path / f"hca_rows_{d}.json").write_text("{}", encoding="utf-8")
    (tmp_path / "hca_push.log").write_text("keep me", encoding="utf-8")
    _save(tmp_path, name="hca_rows_20260923.json")
    left = sorted(os.listdir(tmp_path))
    assert left == ["hca_push.log", "hca_rows_20260920.json", "hca_rows_20260921.json", "hca_rows_20260923.json"]


def _write(tmp_path, payload, name="bad.json"):
    p = tmp_path / name
    p.write_text(json.dumps(payload) if not isinstance(payload, str) else payload, encoding="utf-8")
    return str(p)


def _good_payload(rows):
    return {"format": hlp.ROWS_FORMAT, "hospital_system": "HCA Healthcare",
            "run_started_iso": _iso(datetime.now(timezone.utc)), "saved_at": "x",
            "partial": False, "failed_slices": [], "row_count": len(rows), "rows": rows}


@pytest.mark.parametrize("mutate, why", [
    (lambda p: p.update(format="something/else"), "not an HCA rows file"),
    (lambda p: p.update(hospital_system="Tenet Healthcare"), "saved for"),
    (lambda p: p.update(row_count=p["row_count"] + 1), "truncated"),
    (lambda p: p.update(rows=[]), "no rows"),
    (lambda p: p["rows"][0].update(hospital_system="Tenet Healthcare"), "not HCA rows"),
    (lambda p: p["rows"][1].update(job_id=""), "not HCA rows"),
    (lambda p: p.update(run_started_iso="yesterday"), "bad run_started_iso"),
    (lambda p: p.update(run_started_iso="2026-09-24T04:00:09"), "no time zone"),
])
def test_load_refuses_files_that_must_not_be_pushed(tmp_path, mutate, why):
    payload = _good_payload(_rows())
    mutate(payload)
    with pytest.raises(hlp.ReplayError, match=why):
        hlp.load_rows_file(_write(tmp_path, payload))


def test_load_refuses_missing_and_unreadable_files(tmp_path):
    with pytest.raises(hlp.ReplayError, match="no such file"):
        hlp.load_rows_file(str(tmp_path / "nope.json"))
    with pytest.raises(hlp.ReplayError, match="unreadable"):
        hlp.load_rows_file(_write(tmp_path, '{"format": "hca_rows/1", "rows": [', name="cut.json"))


def test_load_drops_repeated_job_ids(tmp_path):
    rows = _rows()
    payload = _good_payload(rows + [dict(rows[0])])
    data = hlp.load_rows_file(_write(tmp_path, payload))
    assert len(data["rows"]) == len(rows) and data["dupes"] == 1


def test_replay_upserts_the_saved_rows_texas_first_at_the_crawl_timestamp(tmp_path, monkeypatch, capsys):
    rec = _Recorder()
    monkeypatch.setattr(scraper, "_upsert_hospital_jobs_to_supabase", rec)
    path, rows, started = _save(tmp_path)
    assert hlp.replay(path) == 0
    assert len(rec.calls) == 1
    call = rec.calls[0]
    assert call["run_started_iso"] == started
    assert call["partial"] is False                     # a complete crawl is swept
    states = [r["state"] for r in call["rows"]]
    assert states == ["TX"] * 4 + ["CO"] * 3 + ["FL"] * 2
    assert sorted(r["job_id"] for r in call["rows"]) == sorted(r["job_id"] for r in rows)
    out = capsys.readouterr().out
    assert "9 rows, 5 with a full description" in out and "DONE: 9 of 9 rows sent." in out


def test_replay_of_a_partial_crawl_never_sweeps(tmp_path, monkeypatch):
    rec = _Recorder()
    monkeypatch.setattr(scraper, "_upsert_hospital_jobs_to_supabase", rec)
    path, _, _ = _save(tmp_path, partial=True)
    assert hlp.replay(path) == 0
    assert rec.calls[0]["partial"] is True


def test_replay_refuses_a_stale_file_unless_allowed_and_then_never_sweeps(tmp_path, monkeypatch):
    rec = _Recorder()
    monkeypatch.setattr(scraper, "_upsert_hospital_jobs_to_supabase", rec)
    path, _, _ = _save(tmp_path, hours_ago=72)
    with pytest.raises(hlp.ReplayError, match="72 h old"):
        hlp.replay(path)
    assert rec.calls == []
    assert hlp.replay(path, allow_stale=True) == 0
    assert rec.calls[0]["partial"] is True


def test_replay_check_sends_nothing(tmp_path, monkeypatch, capsys):
    path, _, _ = _save(tmp_path, hours_ago=72)
    assert hlp.replay(path, check_only=True) == 0      # the autouse fixture refuses any upsert
    assert "CHECK ONLY: nothing sent" in capsys.readouterr().out


def test_an_incomplete_upsert_points_at_the_replay(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(scraper, "_upsert_hospital_jobs_to_supabase", _Recorder(land=4))
    path, _, _ = _save(tmp_path)
    assert hlp.replay(path) == 1
    out = capsys.readouterr().out
    assert "DONE: 4 of 9 rows sent." in out and f"--replay {path}" in out


def test_main_replay_flags(tmp_path, monkeypatch, capsys):
    rec = _Recorder()
    monkeypatch.setattr(scraper, "_upsert_hospital_jobs_to_supabase", rec)
    path, _, _ = _save(tmp_path)
    assert hlp.main(["--replay", path, "--check"]) == 0 and rec.calls == []
    assert hlp.main(["--replay", path]) == 0 and len(rec.calls) == 1
    assert hlp.main(["--replay", str(tmp_path / "missing.json")]) == 2
    assert "REPLAY REFUSED" in capsys.readouterr().out
    with pytest.raises(SystemExit):
        hlp.main(["--check"])                           # --check needs --replay


def test_crawl_saves_before_it_pushes(tmp_path, monkeypatch):
    # The crawl path end to end with the crawl, detail pass and CMS load faked:
    # the file exists (and holds every row) by the time the upsert runs.
    jobs = scraper._parse_hca_cards(_page(_ids(1, 1200)))
    monkeypatch.setattr(scraper, "run_hca", lambda session: asyncio.sleep(0, result=jobs))
    monkeypatch.setattr(scraper, "load_cms_lookup", lambda: 0)
    monkeypatch.setattr(hlp, "_hca_detail_pass", lambda js: None)
    monkeypatch.setattr(hlp, "LOGS_DIR", str(tmp_path))
    monkeypatch.setattr(hlp, "rows_file_path", lambda d: os.path.join(str(tmp_path), f"hca_rows_{d}.json"))
    seen = {}

    def upsert(rows, run_started_iso):
        files = [n for n in os.listdir(tmp_path) if n.startswith("hca_rows_")]
        seen["files"] = files
        seen["saved"] = hlp.load_rows_file(os.path.join(str(tmp_path), files[0]))["rows"] if files else []
        return len(rows)

    monkeypatch.setattr(scraper, "_upsert_hospital_jobs_to_supabase", upsert)
    assert hlp.crawl_and_push() == 0
    assert len(seen["files"]) == 1 and len(seen["saved"]) == 1200
