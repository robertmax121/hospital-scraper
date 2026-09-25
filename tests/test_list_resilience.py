"""List pagination under load (2026-09-25).

The 09-25 run cut 51 systems (37,012 rows held by the yield guard) at exact
page multiples or to zero. These tests reproduce the three conditions with
localhost servers and scripted sessions (no external network, no database):
a connection pool held by slow detail requests, a page that times out once,
and a 429 on page 2. The REAL adapters (scrape_greenhouse, scrape_workday,
scrape_oracle) run against a localhost server through a session that
rewrites the employer URL to it."""
import asyncio

import aiohttp
from aiohttp import web

import scraper
from scraper import ProxyRotator, _FallbackResponse, _queue_proof_timeout


def _no_proxies(monkeypatch):
    monkeypatch.delenv("WEBSHARE_API_KEY", raising=False)
    monkeypatch.setenv("PROXY_LIST", "")
    monkeypatch.setenv("PROXY_FILE", "no-such-file.txt")
    monkeypatch.setattr(scraper, "proxies", ProxyRotator())


def _fast(monkeypatch):
    waits = []

    async def fake_sleep(seconds):
        waits.append(seconds)
    monkeypatch.setattr(scraper, "_retry_sleep", fake_sleep)

    async def no_jitter():
        return None
    monkeypatch.setattr(scraper, "jitter", no_jitter)
    monkeypatch.setattr(scraper, "DETAIL_FETCH", False)
    monkeypatch.setattr(scraper, "WD_FETCH_DESCRIPTIONS", False)
    return waits


class _Redirect:
    """A real aiohttp session whose requests all go to one localhost base,
    keeping the path (and query), so the real adapters can run offline.
    `cap` scales the adapters' timeouts down (25 s -> 1 s) so a test can
    hold the pool for 2 s instead of 30."""
    def __init__(self, session, base, cap=None):
        self._s, self._base, self._cap = session, base, cap

    def _kw(self, kw):
        t = kw.get("timeout")
        if self._cap and isinstance(t, aiohttp.ClientTimeout):
            c = lambda v: None if v is None else min(v, self._cap)
            kw = dict(kw, timeout=aiohttp.ClientTimeout(total=c(t.total), connect=c(t.connect),
                                                        sock_connect=c(t.sock_connect), sock_read=c(t.sock_read)))
        return kw

    def _to(self, url):
        from urllib.parse import urlsplit
        p = urlsplit(str(url))
        return self._base + p.path + (("?" + p.query) if p.query else "")

    def get(self, url, **kw):
        return self._s.get(self._to(url), **self._kw(kw))

    def post(self, url, **kw):
        return self._s.post(self._to(url), **self._kw(kw))


async def _serve(routes):
    app = web.Application()
    for method, path, handler in routes:
        app.router.add_route(method, path, handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    return runner, f"http://127.0.0.1:{runner.addresses[0][1]}"


async def _slow(request):
    await asyncio.sleep(float(request.query.get("s", "3")))
    return web.Response(text="x")


def _hogs(session, base, n, seconds):
    """n requests that each hold a pooled connection for `seconds` (the
    detail passes of other tenants)."""
    async def one():
        try:
            async with session.get(f"{base}/slow?s={seconds}") as r:
                await r.read()
        except Exception:
            pass
    return [asyncio.ensure_future(one()) for _ in range(n)]


# ── the timeout rewrite itself ───────────────────────────────────────────────
def test_total_timeout_becomes_connect_and_read_bounds():
    kw = _queue_proof_timeout({"timeout": aiohttp.ClientTimeout(total=25), "json": {}})
    t = kw["timeout"]
    assert t.total is None and t.connect is None
    assert t.sock_connect == 25 and t.sock_read == 25
    # a timeout that already sets its own fields, or none, is left alone
    own = aiohttp.ClientTimeout(total=10, sock_read=3)
    assert _queue_proof_timeout({"timeout": own})["timeout"] is own
    assert _queue_proof_timeout({}) == {}


def test_greenhouse_board_behind_a_full_pool_is_not_zero(monkeypatch):
    """BAYADA / One Medical on 09-25: one big request, queued behind detail
    fetches for longer than its 25 s budget, came back as 0 rows. Scaled
    down: a pool of 2 held for 2 s by other requests, a 1 s timeout."""
    _no_proxies(monkeypatch)
    _fast(monkeypatch)
    scraper_cap = 1.0
    board = {"jobs": [{"id": i, "title": f"RN {i}", "location": {"name": "Norfolk, VA"},
                       "absolute_url": f"https://x/{i}", "updated_at": "2026-09-20T00:00:00",
                       "content": "<p>Care</p>", "departments": []} for i in range(300)]}

    async def jobs(request):
        return web.json_response(board)

    async def go():
        runner, base = await _serve([("GET", "/slow", _slow), ("GET", "/v1/boards/{org}/jobs", jobs)])
        try:
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=2)) as s:
                hogs = _hogs(s, base, 2, 2)
                await asyncio.sleep(0.2)
                rows = await scraper.scrape_greenhouse(_Redirect(s, base, scraper_cap), "BAYADA Home Health Care", "bayada")
                await asyncio.gather(*hogs)
                return rows
        finally:
            await runner.cleanup()
    rows = asyncio.run(go())
    assert len(rows) == 300


def _wd_handler(total, fail=None, calls=None):
    """Workday CXS list: 20 per page; `fail(offset, n)` may return a response
    to send instead (a 429, a stall)."""
    seen = {}

    async def handler(request):
        body = await request.json()
        off = body["offset"]
        seen[off] = seen.get(off, 0) + 1
        if calls is not None:
            calls.append(off)
        if fail:
            alt = await fail(off, seen[off])
            if alt is not None:
                return alt
        n = max(0, min(20, total - off))
        return web.json_response({"total": total, "jobPostings": [
            {"title": f"RN {off + i}", "locationsText": "Sacramento, CA",
             "externalPath": f"/job/Sac/RN_R-{off + i}", "bulletFields": [f"R-{100000 + off + i}"],
             "postedOn": "Posted Today"} for i in range(n)]})
    return handler


async def _run_workday(handler, pool=30, hog=None, cap=None):
    runner, base = await _serve([("GET", "/slow", _slow),
                                 ("POST", "/wday/cxs/{t}/{site}/jobs", handler)])
    try:
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=pool)) as s:
            hogs = []
            if hog:
                n, secs, after = hog

                async def later():
                    await asyncio.sleep(after)
                    hogs.extend(_hogs(s, base, n, secs))
                starter = asyncio.ensure_future(later())
            rows = await scraper.scrape_workday(_Redirect(s, base, cap), "Sutter Health", ("sutterhealth", "1", "SH"))
            if hog:
                await starter
                await asyncio.gather(*hogs)
            return rows
    finally:
        await runner.cleanup()


def test_workday_page_two_behind_a_full_pool_keeps_paging(monkeypatch):
    """Sutter / NewYork-Presbyterian on 09-25: page 1 landed, page 2 waited
    for a connection past its budget and the crawl stopped at 20 rows."""
    _no_proxies(monkeypatch)
    _fast(monkeypatch)
    scraper_cap = 1.0

    async def pace(off, n):
        await asyncio.sleep(0.3)           # the pool fills while page 1 is in flight
        return None

    async def gap():                       # the pause between pages, as jitter() makes
        await asyncio.sleep(0.2)
    monkeypatch.setattr(scraper, "jitter", gap)
    rows = asyncio.run(_run_workday(_wd_handler(95, pace), pool=2, hog=(2, 2, 0.1), cap=scraper_cap))
    assert len(rows) == 95


def test_workday_429_on_page_two_is_retried(monkeypatch):
    _no_proxies(monkeypatch)
    waits = _fast(monkeypatch)

    async def throttle(off, n):
        if off == 20 and n == 1:
            return web.json_response({}, status=429, headers={"Retry-After": "1"})
        return None
    calls = []
    rows = asyncio.run(_run_workday(_wd_handler(70, throttle, calls)))
    assert len(rows) == 70
    assert calls.count(20) == 2 and len(waits) == 1


def test_workday_page_that_times_out_once_is_retried(monkeypatch):
    """A slow page (a stall longer than the request's timeout) used to end
    the tenant's pagination; it is retried within the per-host caps."""
    _no_proxies(monkeypatch)
    waits = _fast(monkeypatch)
    scraper_cap = 0.5

    async def stall(off, n):
        if off == 40 and n == 1:
            await asyncio.sleep(1.5)
        return None
    calls = []
    rows = asyncio.run(_run_workday(_wd_handler(90, stall, calls), cap=scraper_cap))
    assert len(rows) == 90
    assert calls.count(40) == 2 and len(waits) == 1


def test_a_host_that_keeps_timing_out_still_gives_up(monkeypatch):
    """The retry is bounded: after HTTP_RETRIES the timeout reaches the
    adapter, which keeps what it listed (no endless run)."""
    _no_proxies(monkeypatch)
    waits = _fast(monkeypatch)
    scraper_cap = 0.3

    async def dead(off, n):
        if off >= 40:
            await asyncio.sleep(1.0)
        return None
    rows = asyncio.run(_run_workday(_wd_handler(90, dead), cap=scraper_cap))
    assert len(rows) == 40
    assert len(waits) == scraper.HTTP_RETRIES


def test_refused_host_is_not_retried(monkeypatch):
    _no_proxies(monkeypatch)
    waits = _fast(monkeypatch)

    class _Refuse:
        async def __aenter__(self):
            raise aiohttp.ClientConnectorError(None, OSError(10061, "refused"))

        async def __aexit__(self, *a):
            return None

    class _S:
        def get(self, url, **kw):
            return _Refuse()

    async def go():
        async with _FallbackResponse(_S(), "get", "https://gone.example/x", None, {}) as r:
            return r.status
    try:
        asyncio.run(go())
        raised = False
    except aiohttp.ClientConnectorError:
        raised = True
    assert raised and waits == []


def test_oracle_zero_tenant_behind_a_full_pool_lists(monkeypatch):
    """Tenet / Mount Sinai on 09-25: 0 rows. Page 1 of an Oracle tenant
    queued behind the pool; with the rewrite it waits and lists."""
    _no_proxies(monkeypatch)
    _fast(monkeypatch)
    scraper_cap = 1.0

    async def reqs(request):
        finder = request.query.get("finder", "")
        off = int(finder.rsplit("offset=", 1)[1])
        items = [{"Id": str(off + i), "Title": f"RN {off + i}", "PrimaryLocation": "Dallas, TX, United States"}
                 for i in range(max(0, min(25, 60 - off)))]
        return web.json_response({"items": [{"TotalJobsCount": 60, "requisitionList": {"items": items}}]})

    async def go():
        runner, base = await _serve([("GET", "/slow", _slow),
                                     ("GET", "/hcmRestApi/resources/latest/recruitingCEJobRequisitions", reqs)])
        try:
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=2)) as s:
                hogs = _hogs(s, base, 2, 2)
                await asyncio.sleep(0.2)
                rows = await scraper.scrape_oracle(_Redirect(s, base, scraper_cap), "Tenet Healthcare",
                                                   ("https://eodr.fa.us2.oraclecloud.com", "CX_1001"))
                await asyncio.gather(*hogs)
                return rows
        finally:
            await runner.cleanup()
    assert len(asyncio.run(go())) == 60
