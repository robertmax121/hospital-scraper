"""
Waypoint Hospital Pay Index — monthly snapshot writer.

Aggregates hospital_wages (per-hospital RN pay ranges parsed from each
hospital's own postings) into national / per-state / per-system statistics
and upserts them into pay_index_snapshots keyed by (month, scope, key).
The /pay-index page renders these snapshots and shows month-over-month
deltas once two months exist — the time-series nobody else has.

Scheduled monthly (1st, 6:45 AM) as Windows task "WaypointPayIndexSnapshot".
Safe to re-run within a month: upserts overwrite the current month's rows.
"""
import json
import logging
import os
import re
import statistics
import sys
import urllib.request
from datetime import date

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("pay_index")

ENV_LOCAL = r"C:\Users\rober\OneDrive\claud_outputinput\.env.local"
MIN_STATE_HOSPITALS = 3     # states with fewer parsed hospitals are noise
RANGE_RE = re.compile(r"\$?\s*([\d,.]+)\s*[-–to]+\s*\$?\s*([\d,.]+)", re.I)
LOC_STATE_RE = re.compile(r",\s*([A-Z]{2})\s*$")


def load_env():
    env = dict(os.environ)
    if os.path.exists(ENV_LOCAL):
        for line in open(ENV_LOCAL, encoding="utf-8"):
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                env.setdefault(k.strip(), v.strip())
    return env


def sb(env, path, method="GET", body=None, prefer=None):
    key = env["SUPABASE_SERVICE_ROLE_KEY"]
    url = f"{env.get('SUPABASE_URL', 'https://tlzxajgonuevbqaelhvf.supabase.co').rstrip('/')}/rest/v1/{path}"
    headers = {"apikey": key, "Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    if prefer:
        headers["Prefer"] = prefer
    req = urllib.request.Request(url, data=json.dumps(body).encode() if body is not None else None,
                                 headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=60) as r:
        raw = r.read()
        return json.loads(raw) if raw else None


def parse_range(s):
    if not s or not isinstance(s, str):
        return None
    m = RANGE_RE.search(s)
    if not m:
        return None
    try:
        lo = float(m.group(1).replace(",", ""))
        hi = float(m.group(2).replace(",", ""))
    except ValueError:
        return None
    # Sanity: hourly RN pay. Junk parses (annual figures, typos) get dropped.
    if not (10 <= lo <= 250 and lo <= hi <= 350):
        return None
    return lo, hi


def med(vals):
    return round(statistics.median(vals), 2) if vals else None


def main():
    env = load_env()
    rows = []
    off = 0
    while True:
        page = sb(env, "hospital_wages?select=hospital_name,hospital_system,location,nursing_pay_scale"
                       f"&limit=1000&offset={off}")
        if not page:
            break
        rows.extend(page)
        if len(page) < 1000:
            break
        off += 1000
    logger.info("hospital_wages rows: %d", len(rows))

    parsed = []
    for r in rows:
        rng = parse_range(r.get("nursing_pay_scale"))
        if not rng:
            continue
        mstate = LOC_STATE_RE.search(r.get("location") or "")
        parsed.append({
            "system": (r.get("hospital_system") or "").strip(),
            "state": mstate.group(1) if mstate else None,
            "lo": rng[0], "hi": rng[1], "mid": (rng[0] + rng[1]) / 2,
        })
    logger.info("hospitals with parsed RN range: %d", len(parsed))
    if len(parsed) < 100:
        logger.error("too few parsed rows - refusing to snapshot")
        return 1

    month = date.today().replace(day=1).isoformat()
    out = [{
        "month": month, "scope": "national", "key": "US",
        "lo": med([p["lo"] for p in parsed]),
        "hi": med([p["hi"] for p in parsed]),
        "mid": med([p["mid"] for p in parsed]),
        "n": len(parsed),
    }]

    by_state = {}
    for p in parsed:
        if p["state"]:
            by_state.setdefault(p["state"], []).append(p)
    for st, ps in sorted(by_state.items()):
        if len(ps) < MIN_STATE_HOSPITALS:
            continue
        out.append({"month": month, "scope": "state", "key": st,
                    "lo": med([p["lo"] for p in ps]), "hi": med([p["hi"] for p in ps]),
                    "mid": med([p["mid"] for p in ps]), "n": len(ps)})

    by_system = {}
    for p in parsed:
        if p["system"]:
            by_system.setdefault(p["system"], []).append(p)
    for sysname, ps in sorted(by_system.items()):
        out.append({"month": month, "scope": "system", "key": sysname,
                    "lo": med([p["lo"] for p in ps]), "hi": med([p["hi"] for p in ps]),
                    "mid": med([p["mid"] for p in ps]), "n": len(ps)})

    for i in range(0, len(out), 200):
        sb(env, "pay_index_snapshots?on_conflict=month,scope,key", method="POST",
           body=out[i:i + 200], prefer="resolution=merge-duplicates,return=minimal")
    logger.info("DONE: snapshot %s written - national=1 states=%d systems=%d",
                month, len([o for o in out if o['scope'] == 'state']),
                len([o for o in out if o['scope'] == 'system']))
    return 0


if __name__ == "__main__":
    sys.exit(main())
