"""Direct-request retry, per-host pacing and Kronos paging (2026-09-24).
No external network: fake sessions script the responses, and one test runs
a real aiohttp session against a localhost server."""
import asyncio
import json

import scraper
from scraper import _FallbackResponse, ProxyRotator


class _Resp:
    def __init__(self, status, headers=None, body=None):
        self.status = status
        self.headers = headers or {}
        self._body = body

    async def json(self, content_type=None):
        return self._body

    async def text(self):
        return json.dumps(self._body)


class _Ctx:
    def __init__(self, status=200, headers=None, body=None, exc=None):
        self.resp = _Resp(status, headers, body)
        self.exc, self.exited = exc, False

    async def __aenter__(self):
        if self.exc:
            raise self.exc
        return self.resp

    async def __aexit__(self, *a):
        self.exited = True


class _Session:
    def __init__(self, script):
        self.script, self.calls = list(script), []

    def get(self, url, **kw):
        self.calls.append((url, kw.get("proxy"), dict(kw.get("params") or {})))
        return self.script.pop(0)

    post = get


def _no_proxies(monkeypatch, entries=()):
    monkeypatch.delenv("WEBSHARE_API_KEY", raising=False)
    monkeypatch.setenv("PROXY_LIST", ",".join(entries))
    monkeypatch.setenv("PROXY_FILE", "no-such-file.txt")
    rot = ProxyRotator()
    monkeypatch.setattr(scraper, "proxies", rot)
    return rot


def _record_sleeps(monkeypatch):
    waits = []

    async def fake(seconds):
        waits.append(seconds)
    monkeypatch.setattr(scraper, "_retry_sleep", fake)
    return waits


async def _one(session, url="https://host.example/x", proxy=None):
    async with _FallbackResponse(session, "get", url, proxy, {}) as r:
        return r.status


def test_429_is_retried_after_retry_after(monkeypatch):
    _no_proxies(monkeypatch)
    waits = _record_sleeps(monkeypatch)
    first = _Ctx(429, {"Retry-After": "3"})
    s = _Session([first, _Ctx(200)])
    assert asyncio.run(_one(s)) == 200
    assert len(s.calls) == 2 and first.exited
    assert len(waits) == 1 and 3.0 <= waits[0] <= 4.0      # Retry-After plus a little jitter


def test_5xx_backoff_is_bounded_and_returns_last_response(monkeypatch):
    _no_proxies(monkeypatch)
    waits = _record_sleeps(monkeypatch)
    ctxs = [_Ctx(503) for _ in range(scraper.HTTP_RETRIES + 1)]
    s = _Session(ctxs)
    assert asyncio.run(_one(s)) == 503                     # the adapter still sees the failure
    assert len(s.calls) == scraper.HTTP_RETRIES + 1
    assert all(c.exited for c in ctxs[:-1])                 # every retried response was closed
    assert len(waits) == scraper.HTTP_RETRIES
    base = scraper.HTTP_RETRY_BASE
    for i, w in enumerate(waits):                           # exponential with jitter
        assert base * 3 ** i / 2 <= w <= base * 3 ** i


def test_404_and_403_are_not_retried(monkeypatch):
    _no_proxies(monkeypatch)
    waits = _record_sleeps(monkeypatch)
    for status in (404, 403):
        s = _Session([_Ctx(status)])
        assert asyncio.run(_one(s)) == status
        assert len(s.calls) == 1
    assert waits == []


def test_long_retry_after_is_not_waited_for(monkeypatch):
    _no_proxies(monkeypatch)
    waits = _record_sleeps(monkeypatch)
    s = _Session([_Ctx(429, {"Retry-After": str(int(scraper.HTTP_RETRY_AFTER_CAP) + 60)})])
    assert asyncio.run(_one(s)) == 429
    assert len(s.calls) == 1 and waits == []


def test_http_date_retry_after_is_read():
    r = _Resp(429, {"Retry-After": "Wed, 21 Oct 2015 07:28:00 GMT"})
    assert scraper._retry_after_seconds(r) == 0.0           # a past date means now
    assert scraper._retry_after_seconds(_Resp(429, {"Retry-After": "7"})) == 7.0
    assert scraper._retry_after_seconds(_Resp(429, {"Retry-After": "soon"})) is None
    assert scraper._retry_after_seconds(_Resp(429)) is None


def test_dead_host_stops_retrying_until_a_success(monkeypatch):
    _no_proxies(monkeypatch)
    waits = _record_sleeps(monkeypatch)
    trip, per = scraper.HTTP_RETRY_HOST_TRIP, scraper.HTTP_RETRIES + 1

    async def go():
        # `trip` requests exhaust their retries ...
        s = _Session([_Ctx(500) for _ in range(trip * per)])
        for _ in range(trip):
            assert await _one(s) == 500
        assert len(s.calls) == trip * per
        # ... so the next one on that host gets a single attempt,
        s2 = _Session([_Ctx(500)])
        assert await _one(s2) == 500 and len(s2.calls) == 1
        # another host is unaffected,
        s3 = _Session([_Ctx(500), _Ctx(200)])
        assert await _one(s3, "https://other.example/y") == 200 and len(s3.calls) == 2
        # and a success on the first host turns its retries back on.
        s4 = _Session([_Ctx(200), _Ctx(502), _Ctx(200)])
        assert await _one(s4) == 200
        assert await _one(s4) == 200 and len(s4.calls) == 3
        return scraper._retry_summary()

    summary = asyncio.run(go())
    assert "recovered" in summary and "host.example" in summary
    assert len(waits) == trip * scraper.HTTP_RETRIES + 2


def test_host_sleep_cap_bounds_retries(monkeypatch):
    _no_proxies(monkeypatch)
    waits = _record_sleeps(monkeypatch)
    monkeypatch.setattr(scraper, "HTTP_RETRY_HOST_SLEEP_CAP", 5.0)
    s = _Session([_Ctx(503) for _ in range(scraper.HTTP_RETRIES + 1)])
    assert asyncio.run(_one(s)) == 503
    assert sum(waits) <= 5.0 and len(s.calls) < scraper.HTTP_RETRIES + 1


def test_proxied_429_falls_back_direct_then_retries_direct(monkeypatch):
    rot = _no_proxies(monkeypatch)
    waits = _record_sleeps(monkeypatch)
    s = _Session([_Ctx(429), _Ctx(429), _Ctx(200)])
    assert asyncio.run(_one(s, proxy="http://u:p@1.2.3.4:5")) == 200
    assert [c[1] for c in s.calls] == ["http://u:p@1.2.3.4:5", None, None]
    assert rot.fallbacks == 1 and len(waits) == 1


def test_gather_by_host_limits_in_flight_per_host(monkeypatch):
    monkeypatch.setattr(scraper, "HOST_TENANT_SPACING", (0.0, 0.0))
    live: dict[str, int] = {}
    peak: dict[str, int] = {}

    async def scrape(session, system, cfg):
        host = cfg
        live[host] = live.get(host, 0) + 1
        peak[host] = max(peak.get(host, 0), live[host])
        await asyncio.sleep(0.01)
        live[host] -= 1
        if system == "boom":
            raise RuntimeError("tenant failed")
        return [system]

    items = [(f"a{i}", "a.example") for i in range(7)] + [(f"b{i}", "b.example") for i in range(3)] + [("boom", "a.example")]
    results = asyncio.run(scraper._gather_by_host(None, items, scrape, lambda cfg: cfg))
    assert peak == {"a.example": scraper.HOST_CONCURRENCY, "b.example": scraper.HOST_CONCURRENCY}
    assert [r for r in results if isinstance(r, list)] == [[s] for s, _ in items if s != "boom"]
    assert isinstance(results[-1], RuntimeError)            # same shape as gather(return_exceptions=True)


def _kronos_page(ids, total, page, size):
    return {"job_requisitions": [{"id": i, "job_title": f"RN {i}", "location": {"city": "Vernon", "state": "TX"}}
                                 for i in ids],
            "_paging": {"offset": page, "size": size, "total": total}}


def test_kronos_offset_is_a_page_number(monkeypatch):
    _no_proxies(monkeypatch)

    async def no_jitter():
        return None
    monkeypatch.setattr(scraper, "jitter", no_jitter)
    size = scraper._KRONOS_PAGE
    total = 2 * size + 19
    all_ids = list(range(1000, 1000 + total))
    pages = [all_ids[i:i + size] for i in range(0, total, size)]
    s = _Session([_Ctx(200, body=_kronos_page(p, total, n + 1, size)) for n, p in enumerate(pages)])
    jobs = asyncio.run(scraper.scrape_kronos(s, "Ridgeview", ("secure7.saashr.com", "6104389", "TX")))
    assert len(jobs) == total and len({j.job_id for j in jobs}) == total
    assert [c[2]["offset"] for c in s.calls] == [1, 2, 3]  # page numbers, not row offsets
    assert all(c[2]["size"] == size for c in s.calls)
    assert s.calls[0][0].startswith("https://secure7.saashr.com/ta/rest/ui/recruitment/companies/%7C6104389/")


def test_kronos_stops_on_a_repeated_page(monkeypatch):
    _no_proxies(monkeypatch)

    async def no_jitter():
        return None
    monkeypatch.setattr(scraper, "jitter", no_jitter)
    size = scraper._KRONOS_PAGE
    first = list(range(size))
    s = _Session([_Ctx(200, body=_kronos_page(first, 0, 1, size)),
                  _Ctx(200, body=_kronos_page(first, 0, 2, size))])   # no total, endpoint repeats
    jobs = asyncio.run(scraper.scrape_kronos(s, "X", ("prd01-hcm01.prd", "1")))
    assert len(jobs) == size and len(s.calls) == 2
    assert scraper._url_host(scraper._kronos_base("prd01-hcm01.prd")) == "prd01-hcm01.prd.mykronos.com"


def test_real_aiohttp_session_retries_against_a_local_server(monkeypatch):
    """Same path with a real aiohttp session and a localhost server (no
    external network): 429 with Retry-After 0, then a 503, then 200."""
    from aiohttp import web
    import aiohttp
    _no_proxies(monkeypatch)
    waits = _record_sleeps(monkeypatch)
    answers = [(429, {"Retry-After": "0"}), (503, {}), (200, {})]
    seen = []

    async def handler(request):
        status, hdrs = answers[len(seen)]
        seen.append(status)
        return web.json_response({"n": len(seen)}, status=status, headers=hdrs)

    async def go():
        app = web.Application()
        app.router.add_post("/search", handler)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = runner.addresses[0][1]
        try:
            async with aiohttp.ClientSession() as s:
                async with scraper.req(s, "post", f"http://127.0.0.1:{port}/search", json={"q": 1}) as r:
                    return r.status, await r.json()
        finally:
            await runner.cleanup()

    status, body = asyncio.run(go())
    assert status == 200 and body == {"n": 3}
    assert seen == [429, 503, 200] and len(waits) == 2
