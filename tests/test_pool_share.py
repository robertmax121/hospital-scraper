"""Detail passes hold at most DETAIL_POOL_SHARE of a session's pool (2026-09-25).

A localhost server whose detail route is slow; detail requests sent from
inside a _detail_scope coroutine fill as much of the pool as they may, and a
list page sent meanwhile must not queue behind them."""
import asyncio
import time

import aiohttp
from aiohttp import web

import scraper


async def _serve():
    async def slow(request):
        await asyncio.sleep(float(request.query.get("s", "0.6")))
        return web.Response(text="ok")

    async def fast(request):
        return web.Response(text="page")
    app = web.Application()
    app.router.add_get("/detail", slow)
    app.router.add_get("/list", fast)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    return runner, f"http://127.0.0.1:{runner.addresses[0][1]}"


async def _list_wait(connector_cls, **kw):
    runner, base = await _serve()
    try:
        async with aiohttp.ClientSession(connector=connector_cls(limit=4, **kw)) as s:
            async def detail():
                async with s.get(base + "/detail?s=0.6") as r:
                    await r.read()

            @scraper._detail_scope
            async def detail_pass():
                await asyncio.gather(*[detail() for _ in range(12)])

            task = asyncio.create_task(detail_pass())
            await asyncio.sleep(0.15)          # the pass has taken what it may
            t0 = time.monotonic()
            async with s.get(base + "/list") as r:
                assert r.status == 200
                await r.read()
            waited = time.monotonic() - t0
            await task
            return waited, s.connector
    finally:
        await runner.cleanup()


def test_list_page_does_not_queue_behind_a_detail_pass():
    waited, conn = asyncio.run(_list_wait(scraper._ListFirstConnector, detail_limit=2))
    assert waited < 0.3, waited
    # every detail slot came back when its connection was released
    assert conn._detail_slots._value == 2


def test_plain_connector_queues_the_list_page():
    # the 09-25 condition, for contrast: 12 detail requests on a pool of 4
    waited, _ = asyncio.run(_list_wait(aiohttp.TCPConnector))
    assert waited >= 0.4, waited


def test_detail_scope_is_reset_after_the_pass():
    seen = []

    @scraper._detail_scope
    async def detail_pass():
        seen.append(scraper._IN_DETAIL.get())

    async def runner():
        await detail_pass()
        seen.append(scraper._IN_DETAIL.get())     # the runner's next list page
    asyncio.run(runner())
    assert seen == [True, False]


def test_detail_limit_never_takes_the_whole_pool():
    async def make():
        return scraper._ListFirstConnector(limit=5, detail_limit=50)
    c = asyncio.run(make())
    assert c._detail_slots._value == 4


def test_detail_drivers_and_run_all_use_the_split():
    import inspect
    assert scraper._detail_pass.__wrapped__
    assert scraper._workday_fetch_details.__wrapped__
    src = inspect.getsource(scraper.run_all)
    assert "_ListFirstConnector(limit=30, ssl=False)" in src
    assert "_ListFirstConnector(limit=30)" in src
