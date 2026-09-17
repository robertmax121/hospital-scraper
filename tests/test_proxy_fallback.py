"""Proxy fallback, 2026-09-17: a failed proxied request retries direct, and
three failures retire the address for the run."""
import asyncio
import scraper
from scraper import _FallbackResponse, ProxyRotator


class _Resp:
    def __init__(self, status):
        self.status = status


class _Ctx:
    def __init__(self, status=200, exc=None):
        self.status, self.exc, self.exited = status, exc, False

    async def __aenter__(self):
        if self.exc:
            raise self.exc
        return _Resp(self.status)

    async def __aexit__(self, *a):
        self.exited = True


class _Session:
    def __init__(self, script):
        self.script, self.calls = list(script), []

    def get(self, url, **kw):
        self.calls.append(kw.get("proxy"))
        return self.script.pop(0)


def _rotator(monkeypatch, entries=()):
    monkeypatch.delenv("WEBSHARE_API_KEY", raising=False)
    monkeypatch.setenv("PROXY_LIST", ",".join(entries))
    monkeypatch.setenv("PROXY_FILE", "no-such-file.txt")
    rot = ProxyRotator()
    monkeypatch.setattr(scraper, "proxies", rot)
    return rot


def _run(session, proxy):
    async def go():
        async with _FallbackResponse(session, "get", "https://x", proxy, {}) as r:
            return r.status
    return asyncio.run(go())


def test_blocked_address_retries_direct(monkeypatch):
    rot = _rotator(monkeypatch)
    first = _Ctx(403)
    s = _Session([first, _Ctx(200)])
    assert _run(s, "http://u:p@1.2.3.4:5") == 200
    assert s.calls == ["http://u:p@1.2.3.4:5", None]
    assert first.exited                      # the blocked response was closed
    assert rot.fallbacks == 1


def test_proxy_exception_retries_direct(monkeypatch):
    rot = _rotator(monkeypatch)
    s = _Session([_Ctx(exc=TimeoutError("proxy hung")), _Ctx(200)])
    assert _run(s, "http://u:p@1.2.3.4:5") == 200
    assert s.calls == ["http://u:p@1.2.3.4:5", None]
    assert rot.fallbacks == 1


def test_direct_request_is_untouched(monkeypatch):
    rot = _rotator(monkeypatch)
    s = _Session([_Ctx(404)])
    assert _run(s, None) == 404                # no retry, no strike
    assert s.calls == [None]
    assert rot.fallbacks == 0


def test_three_strikes_retire_an_address(monkeypatch):
    rot = _rotator(monkeypatch, ["1.1.1.1:10:u:p", "2.2.2.2:20:u:p"])
    a = rot.get()
    assert a == "http://u:p@1.1.1.1:10"
    for _ in range(scraper.PROXY_STRIKES):
        rot.mark_bad(a, "HTTP 403")
    assert a in rot.retired
    assert {rot.get() for _ in range(6)} == {"http://u:p@2.2.2.2:20"}
    for _ in range(scraper.PROXY_STRIKES):
        rot.mark_bad("http://u:p@2.2.2.2:20")
    assert rot.get() is None                   # whole pool retired: run direct
