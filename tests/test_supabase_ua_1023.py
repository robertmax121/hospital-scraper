"""Supabase refuses a secret key (sb_secret_) sent with a browser User-Agent (2026-10-23,
Railway breach key migration). The two link validators run inside aiohttp sessions whose
default User-Agent is a browser string, so every Supabase header dict there must name the
caller explicitly."""
import ast
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]


def _dicts_with_apikey(path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Dict):
            keys = [k.value for k in node.keys if isinstance(k, ast.Constant)]
            if "apikey" in keys:
                yield node, keys


def test_validator_supabase_headers_name_the_caller():
    for name in ("validate_front_pool_urls.py", "validate_travel_urls.py"):
        found = list(_dicts_with_apikey(ROOT / name))
        assert found, name
        for node, keys in found:
            assert "User-Agent" in keys, f"{name}:{node.lineno} Supabase headers without a User-Agent"


def test_supabase_ua_is_not_a_browser():
    import validate_front_pool_urls as a
    import validate_travel_urls as b
    for ua in (a.SUPABASE_UA, b.SUPABASE_UA):
        assert "Mozilla" not in ua and "Chrome" not in ua and "Safari" not in ua
