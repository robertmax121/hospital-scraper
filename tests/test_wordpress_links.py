"""WordPress (WMCHealth) Apply links, 2026-10-13.

The QA link gate logged "ALARM QA link gate [WordPress]: 778/778 malformed
{'no_path': 778}" on every run since the WMCHealth adapter landed (10-04).
The links were never broken: wmchealthjobs.org runs WordPress on plain
permalinks, so a posting lives at /?jobs=<slug>-<req>, which is the page's
own rel=canonical (the /jobs/<slug>/ form answers 301 to it). The gate only
looked at the path. These tests pin both halves: the gate passes a
query-addressed posting and still fails a bare domain, and the HTML-list
adapter never emits a home-page Apply link (nor makes one by stripping a
whole query for its utm_ tags).

Fixtures in fixtures/wordpress/ are trimmed copies of the live pages fetched
on 2026-10-13. No network.
"""
import os
import re

import scraper

WP = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures", "wordpress")


def _fx(name):
    with open(os.path.join(WP, name), encoding="utf-8") as f:
        return f.read()


def _wmc_jobs(html=None):
    cfg = scraper.HTML_LIST_SITES["WMCHealth"]
    return scraper._parse_html_list_page(html or _fx("wmc_search_2026-10-13.html"), "WMCHealth", cfg)


# ── the adapter emits each posting's own URL ────────────────────────────

def test_wmchealth_rows_carry_the_postings_own_query_url():
    jobs = _wmc_jobs()
    assert [j.job_id for j in jobs] == ["48609", "49982", "49983"]
    assert [j.url for j in jobs] == [
        "https://wmchealthjobs.org/?jobs=access-center-ambassador-48609",
        "https://wmchealthjobs.org/?jobs=access-center-ambassador-49982",
        "https://wmchealthjobs.org/?jobs=access-center-ambassador-49983",
    ]
    assert all(j.ats_platform == "WordPress" for j in jobs)


def test_the_emitted_url_is_the_posting_pages_rel_canonical():
    head = _fx("wmc_posting_48609_head.html")
    canonical = re.search(r'<link rel="canonical" href="([^"]+)"', head).group(1)
    assert _wmc_jobs()[0].url == canonical
    # and the page is that posting, not the home page
    assert "Access Center Ambassador-48609" in head


# ── the QA link gate ────────────────────────────────────────────────────

def test_gate_passes_query_addressed_wordpress_postings():
    for j in _wmc_jobs():
        assert scraper._structural_url_ok(j.url, "WordPress") == (True, ""), j.url
    for url in ("https://wmchealthjobs.org/?p=99503",
                "https://example.org/?page_id=12",
                "https://example.org?jobs=registered-nurse-4410",
                "https://example.org/?job_id=4410&utm_source=x"):
        assert scraper._structural_url_ok(url, "WordPress") == (True, ""), url


def test_gate_still_fails_a_bare_domain_or_a_root_with_no_posting_key():
    for url in ("https://wmchealthjobs.org",
                "https://wmchealthjobs.org/",
                "https://wmchealthjobs.org/?utm_source=feed&utm_medium=rss",
                "https://wmchealthjobs.org/?s=nurse",
                "https://wmchealthjobs.org/?post_type=jobs",
                "https://wmchealthjobs.org/?paged=2",
                "https://wmchealthjobs.org/?jobs=",
                "https://wmchealthjobs.org/#jobs-48609"):
        assert scraper._structural_url_ok(url, "WordPress") == (False, "no_path"), url


def test_gate_raises_no_alarm_on_the_recorded_wmchealth_rows(monkeypatch):
    seen = []

    class _Log:
        def info(self, msg, *a, **k):
            seen.append(("info", msg))

        def warning(self, msg, *a, **k):
            seen.append(("warning", msg))

    monkeypatch.setattr(scraper, "logger", _Log())
    # 27 rows: the gate only alarms at 20+ rows on a platform
    rows = [{"ats_platform": "WordPress", "url": j.url} for j in _wmc_jobs()] * 9
    scraper._qa_link_gate(rows, "2026-10-13T00:00:00Z", "", "")
    assert not [m for lvl, m in seen if lvl == "warning"], seen
    assert any("QA link gate [WordPress]: 0/27 malformed" in m for _, m in seen), seen

    seen.clear()
    bare = [{"ats_platform": "WordPress", "url": "https://wmchealthjobs.org/"}] * 27
    scraper._qa_link_gate(bare, "2026-10-13T00:00:00Z", "", "")
    assert any("ALARM QA link gate [WordPress]: 27/27" in m and "no_path" in m for _, m in seen), seen


# ── the HTML-list adapter never emits a home-page Apply link ────────────

def test_a_card_linking_the_home_page_is_not_emitted():
    html = _fx("wmc_search_2026-10-13.html").replace(
        "https://wmchealthjobs.org/?jobs=access-center-ambassador-49982", "https://wmchealthjobs.org/")
    jobs = _wmc_jobs(html)
    assert [j.job_id for j in jobs] == ["48609", "49983"]
    assert all(scraper._structural_url_ok(j.url, j.ats_platform)[0] for j in jobs)


def test_utm_tags_are_dropped_without_dropping_the_posting_key():
    html = _fx("wmc_search_2026-10-13.html").replace(
        "?jobs=access-center-ambassador-48609",
        "?jobs=access-center-ambassador-48609&amp;utm_source=feed&amp;utm_medium=rss")
    j = _wmc_jobs(html)[0]
    # before 2026-10-13 the whole query went: https://wmchealthjobs.org/
    assert j.url == "https://wmchealthjobs.org/?jobs=access-center-ambassador-48609"
    assert j.job_id == "48609"


def test_drop_utm_keeps_a_path_url_unchanged_apart_from_the_tags():
    url = ("https://rush.fxrecruiter.com/jobs/details/united-states/il/chicago/work-type/"
           "medical-assistant-lisle/13072?utm_source=FXFeedManagerRSS&utm_medium=FXFeedManager")
    assert scraper._hl_drop_utm(url) == (
        "https://rush.fxrecruiter.com/jobs/details/united-states/il/chicago/work-type/"
        "medical-assistant-lisle/13072")
    assert scraper._hl_drop_utm("https://x.org/job/5?id=5&utm_campaign=c") == "https://x.org/job/5?id=5"
