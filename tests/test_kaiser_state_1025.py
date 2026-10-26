# 2026-10-25, Search Console fix plan, fix 11 (scraper part): Kaiser's result
# cards print "City, ST, Onsite, ..." (span.job-location inside the job's
# <a>). The adapter read only the URL's city slug, so a town missing from
# KAISER_CITY_STATE went out with no state and SYSTEM_LOCATION_DEFAULTS filed
# it under the Oakland HQ: 491 active rows on 10-25 were "CA" in OR, WA, HI,
# GA, MD, VA, DC and CO. The fixture is a live result list (?p=150) plus two
# cards from ?p=1. No network, no database.
import asyncio
import os

import pytest

import scraper
from conftest import FIXTURES


def _kaiser_cards():
    with open(os.path.join(FIXTURES, "gsc1025", "kaiser_cards.html"), encoding="utf-8") as f:
        return f.read()


@pytest.mark.parametrize("text,state", [
    ("Spokane, WA, Onsite, Regular, Full-time, Day shift, 40 hours", "WA"),
    ("Fremont, Alameda, CA, Onsite, Regular, Full-time", "CA"),
    ("Washington, DC, Onsite, Regular", "DC"),
    ("Hillsboro, Washington, OR, Onsite", "OR"),      # county named like a state: the code wins
    ("Kapolei, Hawaii, Onsite", "HI"),
    ("Remote", ""),
    ("", ""),
])
def test_kaiser_location_state(text, state):
    assert scraper.kaiser_location_state(text) == state


def test_kaiser_cards_give_each_job_its_own_state():
    st = scraper.kaiser_card_states(_kaiser_cards())
    assert len(st) == 17
    by_city = {}
    for _path, city, _slug, jid in scraper.KAISER_JOB_PATTERN.findall(_kaiser_cards()):
        by_city.setdefault(city, set()).add(st[jid])
    assert by_city["jonesboro"] == {"GA"} and by_city["sharpsburg"] == {"GA"} and by_city["tucker"] == {"GA"}
    assert by_city["seattle"] == {"WA"} and by_city["portland"] == {"OR"} and by_city["honolulu"] == {"HI"}
    assert by_city["fremont"] == {"CA"} and by_city["bakersfield"] == {"CA"}
    # Jonesboro, Sharpsburg, Tucker and Bakersfield are not in the slug map:
    # before this fix they went out stateless and became CA (Oakland HQ).
    for c in ("jonesboro", "sharpsburg", "tucker", "bakersfield"):
        assert c not in scraper.KAISER_CITY_STATE


class _KaiserSession:
    def __init__(self, html):
        self.html, self.pages = html, []

    def get(self, url, **kw):
        page = int(url.rsplit("p=", 1)[1])
        self.pages.append(page)
        html = self.html if page == 1 else "<html></html>"

        class R:
            status = 200
            async def text(s): return html
            async def __aenter__(s): return s
            async def __aexit__(s, *a): return False
        return R()


def test_scrape_kaiser_html_files_out_of_state_towns_under_their_own_state(monkeypatch):
    monkeypatch.setattr(scraper, "req", lambda session, method, url, **kw: getattr(session, method)(url, **kw))
    async def _nojitter(): return None
    monkeypatch.setattr(scraper, "jitter", _nojitter)
    jobs = asyncio.run(scraper.scrape_kaiser_html(_KaiserSession(_kaiser_cards())))
    got = {(j.city, j.state, j.location) for j in jobs}
    assert ("Jonesboro", "GA", "Jonesboro, GA") in got
    assert ("Tucker", "GA", "Tucker, GA") in got
    assert ("Bakersfield", "CA", "Bakersfield, CA") in got
    assert all(j.state for j in jobs)
    # and normalize keeps the card's state (the Oakland CA default no longer applies)
    tucker = next(j for j in jobs if j.city == "Tucker")
    d = scraper.normalize_job(tucker)
    assert d["state"] == "GA" and d["city"] == "Tucker"
