"""
Purge the site's cached pages of hospital_jobs rows this process retired
(2026-10-15).

Why: the site's job page (/jobs/<slug>) is ISR for 6 h and its row read sits
in the Data Cache for 6 h more, so a posting the nightly retired could keep
offering Apply for about 12 h (owner, 10-15: Amedisys job 56815, hospital_jobs
28140695). The site now answers POST /api/revalidate/jobs with {"ids": [...]}
(at most 2,000 per call, 500 recommended) by expiring each row's cache tag
and page path; the next visit re-reads that one row and sees is_active=false.

How: every pass that retires hospital_jobs rows notes the ids it actually
flipped (note_retired): the per-system sweep in
scraper._upsert_hospital_jobs_to_supabase, Layer 4 in
database.mark_inactive_jobs (the 3-miss count, the backstop and the first-miss
path), and the front-pool link check (validate_front_pool_urls). The nightly
calls flush() once, after the last of them (scheduler STEP 2f);
hca_local_push.py flushes after its own sweep. Travel retirements are not
sent: travel_jobs ids are another table's ids and the endpoint looks ids up
in hospital_jobs.

Auth: Authorization: Bearer <CRON_SECRET>, the credential the site's cron
routes (and ops/job_alerts_send.py) already use. When CRON_SECRET is not set
in this process's environment the flush logs one line and does nothing: the
pages then expire on their own TTL, as before. SITE_URL (optional, as in the
site's ops scripts) points it at a preview deployment.

Never raises and never fails a run: every error is logged and counted.
Rate-safe: POSTs of BATCH ids, PAUSE_SEC apart, at most MAX_IDS ids and
DEADLINE_SEC seconds per flush; a 401/403/404 stops at once (wrong secret, or
the endpoint is not deployed), three failed batches in a row stop the rest.
Idempotent on both sides: ids are deduplicated here, and a repeated purge only
makes the next visit re-read one row.
"""
from __future__ import annotations

import json
import logging
import os
import time
import urllib.error
import urllib.request
from urllib.parse import urlsplit

logger = logging.getLogger(__name__)

SECRET_ENV = "CRON_SECRET"
SITE_ENV = "SITE_URL"
DEFAULT_SITE = "https://jobs.waypointrecruit.com"
ENDPOINT_PATH = "/api/revalidate/jobs"

BATCH = 500             # the site's recommended batch (its ceiling is 2,000 per call)
MIN_BATCH = 50          # a 413 / 400 halves a batch down to this
PAUSE_SEC = 1.0         # between batches
RETRY_PAUSES = (3.0, 8.0)   # before each retry of a 429 / 5xx / network fault
TIMEOUT_SEC = 45        # the route's maxDuration is 60 s
DEADLINE_SEC = 300      # the whole flush
MAX_IDS = 20000         # per flush; the rest expire on their own TTL
MAX_FAILED_IN_A_ROW = 3
USER_AGENT = "WaypointScraper/1.0 (job page revalidate)"

# Counters summed from the site's answers (lib/job-revalidate.js on the site).
_ANSWER_COUNTS = ("accepted", "tags_revalidated", "paths_revalidated", "found", "not_found",
                  "inactive", "still_active", "invalid", "duplicates", "lookup_failed_chunks")

# id -> the pass that retired it (the first one wins); insertion-ordered.
_RETIRED: dict[int, str] = {}


def note_retired(ids, source: str = "") -> int:
    """Remember hospital_jobs ids a pass just retired. Ints or digit strings;
    anything else is ignored. Returns how many new ids were added. Never raises."""
    added = 0
    try:
        for v in ids or ():
            try:
                i = int(v)
            except (TypeError, ValueError):
                continue
            if i <= 0 or i in _RETIRED:
                continue
            _RETIRED[i] = source
            added += 1
    except Exception as e:      # an odd iterable must not break a retirement pass
        logger.warning(f"site revalidate: note_retired({source}) failed: {e}")
    return added


def pending() -> list[int]:
    return list(_RETIRED)


def pending_by_source() -> dict[str, int]:
    out: dict[str, int] = {}
    for s in _RETIRED.values():
        out[s or "?"] = out.get(s or "?", 0) + 1
    return out


def clear() -> None:
    _RETIRED.clear()


def _site_base(site: str | None) -> str | None:
    """The site origin to POST to: https only (the request carries the cron
    secret), or http on localhost for a local dev server. None when refused."""
    base = (site or os.environ.get(SITE_ENV) or DEFAULT_SITE).strip().rstrip("/")
    try:
        p = urlsplit(base)
    except Exception:
        return None
    host = (p.hostname or "").lower()
    if p.scheme == "https" and host:
        return base
    if p.scheme == "http" and host in ("localhost", "127.0.0.1", "[::1]", "::1"):
        return base
    return None


def _post_once(url: str, secret: str, chunk: list[int], opener) -> tuple[int, dict | None, str]:
    """One POST. Returns (status, parsed JSON or None, short detail); status 0
    for a network fault or timeout."""
    body = json.dumps({"ids": chunk}).encode()
    rq = urllib.request.Request(url, data=body, method="POST", headers={
        "Authorization": f"Bearer {secret}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": USER_AGENT,
    })
    try:
        with opener(rq, timeout=TIMEOUT_SEC) as resp:
            status = getattr(resp, "status", None) or resp.getcode()
            raw = resp.read() or b""
    except urllib.error.HTTPError as e:
        try:
            raw = e.read() or b""
        except Exception:
            raw = b""
        return e.code, None, raw[:200].decode("utf-8", "replace")
    except Exception as e:
        return 0, None, f"{type(e).__name__}: {e}"[:200]
    try:
        data = json.loads(raw.decode("utf-8")) if raw else None
    except Exception:
        data = None
    return int(status or 0), data if isinstance(data, dict) else None, raw[:200].decode("utf-8", "replace")


def post_ids(ids, *, secret: str | None = None, site: str | None = None, label: str = "",
             batch: int = BATCH, opener=None, sleep=time.sleep, clock=time.monotonic,
             max_ids: int = MAX_IDS, deadline_sec: float = DEADLINE_SEC) -> dict:
    """POST ids to the site's revalidate endpoint in batches. Never raises.

    Returns {"ids", "sent", "batches", "ok_batches", "failed_batches",
    "unsent", "capped", "stopped" (why it stopped early, or None), "skipped"
    (why nothing was sent, or None), and the summed answer counters}."""
    tag = f"site revalidate{f' ({label})' if label else ''}"
    out: dict = {"ids": 0, "sent": 0, "batches": 0, "ok_batches": 0, "failed_batches": 0,
                 "unsent": 0, "capped": 0, "stopped": None, "skipped": None}
    for k in _ANSWER_COUNTS:
        out[k] = 0
    try:
        uniq: list[int] = []
        seen: set[int] = set()
        for v in ids or ():
            try:
                i = int(v)
            except (TypeError, ValueError):
                continue
            if i > 0 and i not in seen:
                seen.add(i)
                uniq.append(i)
        out["ids"] = len(uniq)
        if not uniq:
            return out
        secret = secret if secret is not None else os.environ.get(SECRET_ENV, "")
        secret = (secret or "").strip()
        if not secret:
            out["skipped"] = f"{SECRET_ENV} not set"
            out["unsent"] = len(uniq)
            logger.info(f"{tag}: {SECRET_ENV} is not set in this environment; {len(uniq):,} retired job "
                        f"page(s) not purged (they expire on the site's cache TTL, up to ~12 h). "
                        f"Add {SECRET_ENV} (the site's cron credential) to this service's variables.")
            return out
        base = _site_base(site)
        if not base:
            out["skipped"] = f"{SITE_ENV} refused (https only)"
            out["unsent"] = len(uniq)
            logger.warning(f"{tag}: {SITE_ENV} is not an https origin; nothing sent")
            return out
        if len(uniq) > max_ids:
            out["capped"] = len(uniq) - max_ids
            logger.warning(f"{tag}: {len(uniq):,} ids retired; purging the first {max_ids:,} "
                           f"(the rest expire on the site's cache TTL)")
            uniq = uniq[:max_ids]
        url = base + ENDPOINT_PATH
        opener = opener or urllib.request.urlopen
        started = clock()
        queue: list[list[int]] = [uniq[i:i + batch] for i in range(0, len(uniq), max(1, batch))]
        failed_in_a_row = 0
        first = True
        while queue:
            if clock() - started > deadline_sec:
                out["stopped"] = f"deadline {int(deadline_sec)} s"
                break
            if not first:
                sleep(PAUSE_SEC)
            first = False
            chunk = queue.pop(0)
            out["batches"] += 1
            status, data, detail = _post_once(url, secret, chunk, opener)
            for pause in RETRY_PAUSES:
                if not (status == 0 or status == 429 or status >= 500):
                    break
                sleep(pause)
                status, data, detail = _post_once(url, secret, chunk, opener)
            if 200 <= status < 300:
                out["ok_batches"] += 1
                out["sent"] += len(chunk)
                failed_in_a_row = 0
                for k in _ANSWER_COUNTS:
                    v = (data or {}).get(k)
                    if isinstance(v, int) and not isinstance(v, bool):
                        out[k] += v
                continue
            if status in (400, 413) and len(chunk) > MIN_BATCH:
                mid = len(chunk) // 2
                queue[:0] = [chunk[:mid], chunk[mid:]]
                logger.warning(f"{tag}: HTTP {status} on {len(chunk)} ids; halving the batch ({detail[:120]})")
                continue
            out["failed_batches"] += 1
            if status in (401, 403):
                out["stopped"] = f"HTTP {status}: the site refused {SECRET_ENV}"
                logger.warning(f"{tag}: HTTP {status}; the site refused the credential. Check that "
                               f"{SECRET_ENV} here matches the site's {SECRET_ENV}. Nothing more sent.")
                break
            if status == 404:
                out["stopped"] = "HTTP 404: endpoint not deployed"
                logger.warning(f"{tag}: HTTP 404 at {ENDPOINT_PATH}; the site has not deployed the "
                               f"endpoint yet. Nothing more sent.")
                break
            logger.warning(f"{tag}: batch of {len(chunk)} ids failed: "
                           f"{'HTTP ' + str(status) if status else 'no answer'} {detail[:160]}")
            failed_in_a_row += 1
            if failed_in_a_row >= MAX_FAILED_IN_A_ROW:
                out["stopped"] = f"{MAX_FAILED_IN_A_ROW} failed batches in a row"
                break
        out["unsent"] = out["ids"] - out["capped"] - out["sent"]
        took = clock() - started
        logger.info(f"{tag}: {out['sent']:,} of {out['ids']:,} ids sent in {out['ok_batches']}/{out['batches']} "
                    f"batch(es), {took:.0f}s; site: {out['tags_revalidated']:,} tags, "
                    f"{out['paths_revalidated']:,} paths, {out['inactive']:,} inactive, "
                    f"{out['still_active']:,} still active, {out['not_found']:,} not found"
                    + (f"; stopped: {out['stopped']}" if out["stopped"] else "")
                    + (f"; {out['unsent']:,} not sent" if out["unsent"] else ""))
    except Exception as e:
        out["stopped"] = f"error: {type(e).__name__}"
        logger.warning(f"{tag}: failed (non-fatal): {e}")
    return out


def flush(label: str = "", **kw) -> dict:
    """POST every id noted since the last flush, then forget them (sent or
    not: a retry belongs to the next retirement, and the page TTL covers the
    rest). Never raises."""
    try:
        ids = pending()
        by_src = pending_by_source()
        clear()
        if not ids:
            logger.info(f"site revalidate{f' ({label})' if label else ''}: no retired rows to purge")
            return {"ids": 0, "sent": 0, "by_source": {}}
        logger.info(f"site revalidate{f' ({label})' if label else ''}: {len(ids):,} retired row(s) "
                    f"to purge ({', '.join(f'{s} {n:,}' for s, n in sorted(by_src.items()))})")
        out = post_ids(ids, label=label, **kw)
        out["by_source"] = by_src
        return out
    except Exception as e:
        logger.warning(f"site revalidate: flush failed (non-fatal): {e}")
        return {"ids": 0, "sent": 0, "stopped": f"error: {type(e).__name__}"}
