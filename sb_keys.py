"""Which Supabase key the scraper uses (2026-10-23, Railway breach response).

Railway disclosed on 2026-10-21 that customer environment variables were
read; ours held the DATA project's legacy service_role JWT. The fix is the
new Supabase API keys: a secret key (sb_secret_...) for this backend, then
the legacy anon / service_role JWTs are disabled.

Every place in this repo that talks to the DATA project reads its key here,
in this order, first non-empty value wins:

    SUPABASE_SECRET_KEY        new secret key (sb_secret_...), preferred
    SUPABASE_SERVICE_ROLE_KEY  legacy service_role JWT (until it is disabled)
    SUPABASE_KEY               oldest name; whatever was put there

A new-style key needs supabase-py 2.16.0 or newer (1.2.0 rejects any key
that is not shaped like a JWT with "Invalid API key"); requirements.txt pins
a version that accepts it. Nothing here ever logs or returns a key value
except to the caller that sends it to Supabase; key_source() and key_kind()
give the variable NAME and the key TYPE for the run log.
"""

import os

KEY_ENV_ORDER = ("SUPABASE_SECRET_KEY", "SUPABASE_SERVICE_ROLE_KEY", "SUPABASE_KEY")

# The names that grant writes; the CMS / facility lookups read with any of
# them, the privileged writes (site_stats, scraper_runs, RPCs) want one of
# the first two.
PRIVILEGED_ENV = ("SUPABASE_SECRET_KEY", "SUPABASE_SERVICE_ROLE_KEY")


def _get(env, name: str) -> str:
    return ((env if env is not None else os.environ).get(name) or "").strip()


def supabase_key(env=None) -> str:
    """The DATA project key: SUPABASE_SECRET_KEY, then
    SUPABASE_SERVICE_ROLE_KEY, then SUPABASE_KEY; "" when none is set.
    `env` is any mapping (the side scripts pass their parsed .env file);
    it defaults to os.environ."""
    for name in KEY_ENV_ORDER:
        v = _get(env, name)
        if v:
            return v
    return ""


def key_source(env=None) -> str:
    """The NAME of the variable supabase_key() would use, or "" (never the value)."""
    for name in KEY_ENV_ORDER:
        if _get(env, name):
            return name
    return ""


def privileged_key(env=None) -> str:
    """SUPABASE_SECRET_KEY, then SUPABASE_SERVICE_ROLE_KEY; never the plain
    SUPABASE_KEY (for scripts that only ever ran on the service key)."""
    for name in PRIVILEGED_ENV:
        v = _get(env, name)
        if v:
            return v
    return ""


def has_privileged_key(env=None) -> bool:
    """True when the key comes from SUPABASE_SECRET_KEY or
    SUPABASE_SERVICE_ROLE_KEY (the ones that bypass row-level security)."""
    return key_source(env) in PRIVILEGED_ENV


def key_kind(key: str) -> str:
    """'secret', 'publishable', 'legacy-jwt', 'unknown' or 'none' (for logs)."""
    k = (key or "").strip()
    if not k:
        return "none"
    if k.startswith("sb_secret_"):
        return "secret"
    if k.startswith("sb_publishable_"):
        return "publishable"
    if k.count(".") == 2 and k.startswith("eyJ"):
        return "legacy-jwt"
    return "unknown"


def describe(env=None) -> str:
    """One log line: which variable and which key type, never the value."""
    src = key_source(env)
    if not src:
        return "Supabase key: none set (SUPABASE_SECRET_KEY / SUPABASE_SERVICE_ROLE_KEY / SUPABASE_KEY)"
    return f"Supabase key: {src} ({key_kind(supabase_key(env))})"
