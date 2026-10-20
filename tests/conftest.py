# 2026-09-10 (S-scraper-2): fixture-based suite, no network, no database.
# scraper.py opens logs/run_<date>.log relative to the cwd at import, so the
# tests run from the repo root (logs/ is gitignored) with credentials unset,
# which makes every upsert / CMS load a documented no-op.
import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FIXTURES = os.path.join(REPO, "tests", "fixtures")
os.chdir(REPO)
sys.path.insert(0, REPO)
# 2026-10-15: CRON_SECRET / SITE_URL too, so no test can reach the site's
# revalidate endpoint (site_revalidate skips without the secret).
for _k in ("SUPABASE_URL", "SUPABASE_KEY", "SUPABASE_SERVICE_ROLE_KEY", "CRON_SECRET", "REVALIDATE_SECRET", "SITE_URL"):
    os.environ.pop(_k, None)

import pytest  # noqa: E402
import retire_guard  # noqa: E402
import scraper  # noqa: E402
import site_revalidate  # noqa: E402


@pytest.fixture(autouse=True)
def _clean_module_state(tmp_path, monkeypatch):
    """Every test starts with no partial-run or complete-crawl flags, no CMS
    lookup, no rows left over from an earlier upsert, and the Layer 4
    zero-yield history in a temp file (2026-10-07) rather than state/."""
    scraper.PARTIAL_SYSTEMS.clear()
    scraper.PARTIAL_BACKSTOP_SYSTEMS.clear()
    scraper.COMPLETE_SYSTEMS.clear()
    scraper.LISTED_SYSTEMS.clear()
    scraper.EMITTED_SYSTEMS.clear()
    scraper.COMPLETE_FEEDS.clear()
    scraper.set_cms_lookup([])
    scraper.LAST_UPSERT_FAILED.clear()
    site_revalidate.clear()
    monkeypatch.setattr(retire_guard, "YIELD_HISTORY_PATH", str(tmp_path / "layer4_yields.json"))
    # 2026-10-11: the fake databases in the older tests predate content_hash
    # (sql/100), so they model a table without it: the upsert path exactly as
    # before. tests/test_content_hash.py turns it on per test.
    monkeypatch.setattr(scraper, "_HJ_CONTENT_HASH", {"ok": False})
    monkeypatch.delenv("HASH_SKIP", raising=False)
    yield
    scraper.PARTIAL_SYSTEMS.clear()
    scraper.PARTIAL_BACKSTOP_SYSTEMS.clear()
    scraper.COMPLETE_SYSTEMS.clear()
    scraper.LISTED_SYSTEMS.clear()
    scraper.EMITTED_SYSTEMS.clear()
    scraper.COMPLETE_FEEDS.clear()
    scraper.set_cms_lookup([])
    scraper.LAST_UPSERT_FAILED.clear()
    site_revalidate.clear()


@pytest.fixture
def fixture_text():
    def _read(name):
        with open(os.path.join(FIXTURES, name), encoding="utf-8") as f:
            return f.read()
    return _read
