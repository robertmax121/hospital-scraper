# 2026-10-23 (Railway breach response): the DATA project key moves from the
# legacy service_role JWT (leaked with Railway's environment variables) to a
# new-style secret key. These tests pin the read order, that no module reads
# the key names on its own, that the clients accept an sb_secret_ key with the
# same timeouts as before, and that no helper ever puts a key value in a log.
# No network: create_client only builds the client, it does not connect.
import io
import os
import re

import pytest

import sb_keys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FAKE_SECRET = "sb_secret_" + "x" * 32
FAKE_LEGACY = "eyJhbGciOiJIUzI1NiJ9.eyJyb2xlIjoic2VydmljZV9yb2xlIn0.c2ln"


@pytest.fixture
def no_keys(monkeypatch):
    for k in sb_keys.KEY_ENV_ORDER:
        monkeypatch.delenv(k, raising=False)
    return monkeypatch


def test_read_order_secret_then_service_role_then_key(no_keys):
    no_keys.setenv("SUPABASE_KEY", "plain")
    assert sb_keys.supabase_key() == "plain"
    assert sb_keys.key_source() == "SUPABASE_KEY"
    no_keys.setenv("SUPABASE_SERVICE_ROLE_KEY", FAKE_LEGACY)
    assert sb_keys.supabase_key() == FAKE_LEGACY
    assert sb_keys.key_source() == "SUPABASE_SERVICE_ROLE_KEY"
    no_keys.setenv("SUPABASE_SECRET_KEY", FAKE_SECRET)
    assert sb_keys.supabase_key() == FAKE_SECRET
    assert sb_keys.key_source() == "SUPABASE_SECRET_KEY"


def test_blank_values_are_skipped(no_keys):
    no_keys.setenv("SUPABASE_SECRET_KEY", "   ")
    no_keys.setenv("SUPABASE_SERVICE_ROLE_KEY", FAKE_LEGACY)
    assert sb_keys.supabase_key() == FAKE_LEGACY


def test_mapping_argument_for_the_side_scripts():
    env = {"SUPABASE_SERVICE_ROLE_KEY": FAKE_LEGACY, "SUPABASE_SECRET_KEY": FAKE_SECRET}
    assert sb_keys.supabase_key(env) == FAKE_SECRET
    assert sb_keys.privileged_key({"SUPABASE_KEY": "plain"}) == ""
    assert sb_keys.privileged_key({"SUPABASE_KEY": "plain", "SUPABASE_SERVICE_ROLE_KEY": FAKE_LEGACY}) == FAKE_LEGACY
    assert sb_keys.supabase_key({}) == ""


def test_privileged_flag(no_keys):
    no_keys.setenv("SUPABASE_KEY", "plain")
    assert not sb_keys.has_privileged_key()
    no_keys.setenv("SUPABASE_SECRET_KEY", FAKE_SECRET)
    assert sb_keys.has_privileged_key()


def test_describe_names_the_variable_never_the_value(no_keys):
    assert "none set" in sb_keys.describe()
    no_keys.setenv("SUPABASE_SERVICE_ROLE_KEY", FAKE_LEGACY)
    line = sb_keys.describe()
    assert line == "Supabase key: SUPABASE_SERVICE_ROLE_KEY (legacy-jwt)"
    no_keys.setenv("SUPABASE_SECRET_KEY", FAKE_SECRET)
    line = sb_keys.describe()
    assert line == "Supabase key: SUPABASE_SECRET_KEY (secret)"
    assert FAKE_SECRET not in line and "xxxx" not in line


def test_key_kind():
    assert sb_keys.key_kind(FAKE_SECRET) == "secret"
    assert sb_keys.key_kind("sb_publishable_abc") == "publishable"
    assert sb_keys.key_kind(FAKE_LEGACY) == "legacy-jwt"
    assert sb_keys.key_kind("") == "none"
    assert sb_keys.key_kind("something") == "unknown"


# Every module reads the key through sb_keys. Only the shared-env loader in
# hca_local_push (which copies a key INTO the environment) names the
# variables itself.
_DIRECT_READ = re.compile(
    r"""(os\.environ\.get\(\s*|os\.environ\[\s*|\benv\.get\(\s*|\benv\[\s*)["']SUPABASE_(SECRET_KEY|SERVICE_ROLE_KEY|KEY)["']""")
_ALLOWED = {"sb_keys.py", "hca_local_push.py"}


def _py_files():
    for root, dirs, files in os.walk(REPO):
        dirs[:] = [d for d in dirs if d not in (".git", "tests", "logs", "__pycache__", "node_modules", ".venv", "venv")]
        for f in files:
            if f.endswith(".py"):
                yield os.path.join(root, f)


def test_no_module_reads_the_key_names_directly():
    offenders = []
    for p in _py_files():
        if os.path.basename(p) in _ALLOWED:
            continue
        with io.open(p, encoding="utf-8", errors="replace") as fh:
            for n, line in enumerate(fh, 1):
                if _DIRECT_READ.search(line):
                    offenders.append(f"{os.path.relpath(p, REPO)}:{n}")
    assert offenders == []


def test_requirements_pin_a_supabase_py_that_takes_new_keys():
    with io.open(os.path.join(REPO, "requirements.txt"), encoding="utf-8") as fh:
        pins = dict(l.strip().split("==", 1) for l in fh if "==" in l)
    major, minor = (int(x) for x in pins["supabase"].split(".")[:2])
    assert (major, minor) >= (2, 16), "supabase-py < 2.16 rejects sb_secret_ keys ('Invalid API key')"


def _installed_supabase():
    import importlib.metadata as md
    return tuple(int(x) for x in md.version("supabase").split(".")[:2])


@pytest.mark.skipif(_installed_supabase() < (2, 16), reason="installed supabase-py predates new API keys")
def test_clients_accept_a_secret_key_with_the_old_timeouts(no_keys):
    import database
    no_keys.setenv("SUPABASE_URL", "https://example.supabase.co")
    no_keys.setenv("SUPABASE_SECRET_KEY", FAKE_SECRET)
    no_keys.setenv("SUPABASE_SERVICE_ROLE_KEY", FAKE_LEGACY)
    for make, timeout in ((database.client, database.CLIENT_TIMEOUT_S),
                          (database.service_client, database.SERVICE_TIMEOUT_S)):
        c = make()
        sess = c.postgrest.session
        assert sess.headers["apikey"] == FAKE_SECRET          # the secret key won over the legacy one
        assert sess.headers["authorization"] == f"Bearer {FAKE_SECRET}"
        assert sess.timeout.read == timeout


def test_shared_env_prefers_the_new_secret_key(monkeypatch, tmp_path):
    import hca_local_push as hlp
    env = tmp_path / ".env.local"
    env.write_text(f"SUPABASE_SERVICE_ROLE_KEY={FAKE_LEGACY}\nSUPABASE_SECRET_KEY={FAKE_SECRET}\n", encoding="utf-8")
    monkeypatch.setattr(hlp, "SHARED_ENV_CANDIDATES", [str(env)])
    for k in sb_keys.KEY_ENV_ORDER + ("SUPABASE_URL", "CRON_SECRET", "REVALIDATE_SECRET"):
        monkeypatch.delenv(k, raising=False)
    try:
        hlp._load_shared_env()
        assert sb_keys.supabase_key() == FAKE_SECRET
        assert sb_keys.key_source() == "SUPABASE_SECRET_KEY"
        assert not os.environ.get("SUPABASE_KEY")
    finally:
        for k in sb_keys.KEY_ENV_ORDER + ("SUPABASE_URL",):
            os.environ.pop(k, None)


def test_shared_env_legacy_key_still_maps_to_supabase_key(monkeypatch, tmp_path):
    import hca_local_push as hlp
    env = tmp_path / ".env.local"
    env.write_text(f"SUPABASE_SERVICE_ROLE_KEY={FAKE_LEGACY}\n", encoding="utf-8")
    monkeypatch.setattr(hlp, "SHARED_ENV_CANDIDATES", [str(env)])
    for k in sb_keys.KEY_ENV_ORDER + ("SUPABASE_URL", "CRON_SECRET", "REVALIDATE_SECRET"):
        monkeypatch.delenv(k, raising=False)
    try:
        hlp._load_shared_env()
        assert os.environ.get("SUPABASE_KEY") == FAKE_LEGACY
        assert not os.environ.get("SUPABASE_SECRET_KEY")
    finally:
        for k in sb_keys.KEY_ENV_ORDER + ("SUPABASE_URL",):
            os.environ.pop(k, None)
