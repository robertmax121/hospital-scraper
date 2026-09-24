"""
Retirement guards shared by the two passes that take jobs off the board.

2026-09-24. There are two retirement passes each night:

  1. The per-system sweep in scraper._upsert_hospital_jobs_to_supabase
     retires, at once, every active row of a system that this run did not
     re-stamp.
  2. Layer 4 in database.mark_inactive_jobs counts misses per row and retires
     a row after MISS_THRESHOLD consecutive runs without it.

Until today only the sweep checked the system's yield (the 80% guard from
2026-07-29 / 89098b9). Layer 4 bumped every unseen row, so a board that
answered nothing for three runs (a 429 burst, a WAF, a tenant that flapped)
lost its whole inventory row by row: Rolling Plains after 09-22, and 26 CMS
hospitals, 7 in Texas, were one or two runs from the same fate on 09-24.

Both passes now ask the same question with the same numbers:

  guard_reason(active_n, yield_n)
    "zero-yield"     the run produced no row for a system that has active rows
    "partial-yield"  the system has GUARD_MIN_ACTIVE+ active rows and the run
                     produced under GUARD_RATIO of them
    None             healthy (or a tiny system with some yield): retire as usual

A guarded system is not swept, and in Layer 4 its unseen rows keep their miss
count instead of rising. Rows the run did see are still reset to 0: a posting
seen tonight is live whatever the rest of the crawl did.

Backstop: the guard never protects a row nobody has seen for BACKSTOP_DAYS.
Once an unseen row's scraped_at is older than that, Layer 4 counts it as
usual again, so a system that is really gone still leaves the board, about
BACKSTOP_DAYS plus the remaining misses after it went dark.

Explicit exclusions (exclude_systems: PARTIAL_SYSTEMS and HCA while it is
pushed from a residential IP) are a stronger promise: those rows are left
untouched, backstop included, because another process maintains them.

Pure Python on purpose: no database, no network, importable from the tests.
"""

import re
from collections import Counter
from datetime import datetime, timedelta, timezone

# The run must yield at least GUARD_RATIO of a system's active rows before
# anything of that system is retired, for systems of GUARD_MIN_ACTIVE rows or
# more (89098b9 values; the sweep and Layer 4 read them from here).
GUARD_MIN_ACTIVE = 20
GUARD_RATIO = 0.80
# A row unseen for this long loses the yield guard's protection in Layer 4.
BACKSTOP_DAYS = 7


def guard_reason(active_n: int, yield_n: int) -> str | None:
    """Why a system must not be retired from tonight, or None if it may be.

    active_n: the system's active rows in the database.
    yield_n:  the system's distinct (system, job_id) keys in this run.
    """
    if active_n <= 0:
        return None
    if yield_n <= 0:
        return "zero-yield"
    if active_n >= GUARD_MIN_ACTIVE and yield_n < GUARD_RATIO * active_n:
        return "partial-yield"
    return None


_TS_RX = re.compile(r"^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})(?:\.(\d+))?(.*)$")


def parse_ts(value) -> datetime | None:
    """PostgREST timestamptz text (or a datetime) to an aware UTC datetime.

    Tolerates 'Z', '+00', '+0000', a space separator and 1-9 fractional
    digits, which datetime.fromisoformat before 3.11 does not. Returns None
    when the value cannot be read; callers treat None as 'recent' so an odd
    timestamp can never retire a row.
    """
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    s = str(value).strip()
    m = _TS_RX.match(s)
    if m:
        date, clock, frac, tz = m.group(1), m.group(2), m.group(3) or "", m.group(4).strip()
        if tz in ("Z", "z"):
            tz = "+00:00"
        elif re.fullmatch(r"[+-]\d{2}", tz):
            tz += ":00"
        elif re.fullmatch(r"[+-]\d{4}", tz):
            tz = tz[:3] + ":" + tz[3:]
        s = f"{date}T{clock}" + (f".{(frac + '000000')[:6]}" if frac else "") + tz
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def plan_layer4(active_rows: list[dict],
                current_keys: set[tuple[str, str]],
                miss_threshold: int,
                exclude_systems: set[str] | None = None,
                now: datetime | None = None,
                backstop_days: int = BACKSTOP_DAYS) -> dict:
    """Decide, without touching the database, what Layer 4 does to each row.

    active_rows:  dicts with id, job_id, hospital_system,
                  consecutive_scrape_misses and scraped_at.
    current_keys: (hospital_system, str(job_id)) for every row of this run.

    Returns a dict:
      found_ids          seen tonight with a non-zero miss count: reset to 0
      deactivate_ids     unseen, miss count reaches miss_threshold: retire
      bump_by_new_count  {new_count: [ids]} unseen, below the threshold
      excluded_rows      rows of exclude_systems (untouched)
      guarded            {system: {"reason", "active", "yield", "frozen", "backstop"}}
      frozen_rows        unseen rows of guarded systems kept at their count
      backstop_rows      unseen rows of guarded systems past the backstop,
                         counted as usual
    """
    exclude = exclude_systems or set()
    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(days=backstop_days)

    active_by_system = Counter(r["hospital_system"] for r in active_rows)
    yield_by_system = Counter(s for s, _k in current_keys)

    guarded: dict[str, dict] = {}
    for system, active_n in active_by_system.items():
        if system in exclude:
            continue
        yield_n = yield_by_system.get(system, 0)
        reason = guard_reason(active_n, yield_n)
        if reason:
            guarded[system] = {"reason": reason, "active": active_n, "yield": yield_n,
                               "frozen": 0, "backstop": 0}

    found_ids: list[int] = []
    deactivate_ids: list[int] = []
    bump_by_new_count: dict[int, list[int]] = {}
    excluded_rows = frozen_rows = backstop_rows = 0

    for r in active_rows:
        system = r["hospital_system"]
        if system in exclude:
            excluded_rows += 1
            continue
        misses = int(r.get("consecutive_scrape_misses") or 0)
        if (system, str(r["job_id"])) in current_keys:
            # Only rows with a non-zero count need an UPDATE.
            if misses > 0:
                found_ids.append(r["id"])
            continue
        g = guarded.get(system)
        if g is not None:
            seen = parse_ts(r.get("scraped_at"))
            if seen is None or seen >= cutoff:
                g["frozen"] += 1
                frozen_rows += 1
                continue
            g["backstop"] += 1
            backstop_rows += 1
        new_count = misses + 1
        if new_count >= miss_threshold:
            deactivate_ids.append(r["id"])
        else:
            bump_by_new_count.setdefault(new_count, []).append(r["id"])

    return {
        "found_ids": found_ids,
        "deactivate_ids": deactivate_ids,
        "bump_by_new_count": bump_by_new_count,
        "excluded_rows": excluded_rows,
        "guarded": guarded,
        "frozen_rows": frozen_rows,
        "backstop_rows": backstop_rows,
    }
