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
for _k in ("SUPABASE_URL", "SUPABASE_KEY", "SUPABASE_SERVICE_ROLE_KEY"):
    os.environ.pop(_k, None)

import pytest  # noqa: E402
import scraper  # noqa: E402


@pytest.fixture(autouse=True)
def _clean_module_state():
    """Every test starts with no partial-run flags and no CMS lookup."""
    scraper.PARTIAL_SYSTEMS.clear()
    scraper.set_cms_lookup([])
    yield
    scraper.PARTIAL_SYSTEMS.clear()
    scraper.set_cms_lookup([])


@pytest.fixture
def fixture_text():
    def _read(name):
        with open(os.path.join(FIXTURES, name), encoding="utf-8") as f:
            return f.read()
    return _read
