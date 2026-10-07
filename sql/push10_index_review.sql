-- push10_index_review.sql  (2026-10-07, push 10, item 4: DB write phases)
--
-- READ-ONLY REVIEW FOR THE OWNER. Nothing in this file runs by itself: every
-- DROP is commented out. Run the verification block first, decide, then
-- uncomment one DROP at a time in the Supabase SQL editor (DROP INDEX
-- CONCURRENTLY cannot run inside a transaction block, so run each statement
-- on its own).
--
-- Why this exists. The hospital upsert + sweep went 524 s (09-25) -> 1,228 s
-- (10-04) -> ~2,130 s (10-06), and travel post-scrape + Layer 4 629 s ->
-- ~1,140 s (approval_2026-10-06/cron-run-2026-10-06-analysis.md). Every
-- night all ~312k rows are rewritten with scraped_at and is_active re-sent,
-- so no update is HOT and each row writes an entry into EVERY index on the
-- table. Measured 2026-10-07 02:30 UTC with the Management API (read-only):
--   hospital_jobs: 863 MB heap, 770 MB of indexes, 32 indexes
--   pg_stat_user_tables since the 2026-09-14 stats reset:
--     n_tup_upd 12,686,565 / n_tup_hot_upd 1,485,742 (11.7% HOT)
--     n_live_tup 871,056, n_dead_tup 26,734, last autovacuum 2026-10-07 02:01 UTC
-- Each index dropped removes one index write per updated row per night
-- (~312k row writes) and its vacuum / bloat share.
--
-- ---------------------------------------------------------------------------
-- 1. Verify first: scans and sizes of the candidates (pg_stat counts are
--    cumulative since 2026-09-14; re-run after a week if in doubt).
-- ---------------------------------------------------------------------------
select i.indexrelname, i.idx_scan, i.idx_tup_read,
       pg_size_pretty(pg_relation_size(i.indexrelid)) as size,
       pg_get_indexdef(i.indexrelid) as definition
from pg_stat_user_indexes i
where i.relname = 'hospital_jobs'
  and i.indexrelname in ('idx_specialty', 'idx_hospital_jobs_specialty', 'idx_hj_bing_queue',
                         'idx_hj_indexing_queue', 'idx_hj_sitemap_cohort', 'idx_hj_sitemap_cohort_v2',
                         'idx_hospital_jobs_managed', 'idx_dead_check_at', 'idx_hj_indexing_dead', 'idx_ats')
order by i.idx_scan;

-- ---------------------------------------------------------------------------
-- 2. Candidates (DROP statements commented out). Scan counts as measured on
--    2026-10-07; "readers checked" lists the code that touches the column.
-- ---------------------------------------------------------------------------

-- (a) idx_specialty: an exact duplicate of idx_hospital_jobs_specialty.
--     Both are "btree (specialty)" on the whole table. idx_scan 105,536 vs
--     1,179,772: the planner picks either at random, so dropping one loses
--     nothing (the other serves every query the same way). 17 MB.
-- drop index concurrently if exists public.idx_specialty;

-- (b) idx_hj_bing_queue: 0 scans, 18 MB.
--     Partial predicate: is_active AND apply_verified AND char_length(description) >= 200
--     AND bing_submitted_at IS NULL. Reader checked: bing_submitter.py:117-121
--     queries is_active=eq.true & apply_verified=eq.true & desc_len=gte.200 &
--     bing_submitted_at=is.null & order=scraped_at.desc. desc_len is the
--     generated column char_length(description), but the planner cannot prove
--     "desc_len >= 200" implies the index's "char_length(description) >= 200",
--     so the index is never chosen. EXPLAIN of that exact query shape (10-07):
--       Index Scan using idx_hj_sitemap_cohort_v2 ... Filter: (bing_submitted_at IS NULL)
--     The script keeps working without it (served by the _v2 cohort index
--     with a filter; the first 200 unsubmitted rows are near the top of the
--     scraped_at order). If that ever gets slow, recreate it on desc_len:
--       create index concurrently idx_hj_bing_queue_v2 on public.hospital_jobs (scraped_at desc)
--         where is_active and apply_verified and desc_len >= 200 and bing_submitted_at is null;
-- drop index concurrently if exists public.idx_hj_bing_queue;

-- (c) idx_hj_indexing_queue: 0 scans, 18 MB. Same predicate shape and the
--     same reason as (b). Reader checked: indexing_publisher.py:149-153
--     (desc_len=gte.200). Its dead-row query (indexing_publisher.py:127-129,
--     is_active=eq.false & indexing_submitted_at=not.is.null) uses
--     idx_hj_indexing_dead (EXPLAIN confirmed), which stays.
-- drop index concurrently if exists public.idx_hj_indexing_queue;

-- (d) idx_hj_sitemap_cohort: 30 scans, 18 MB. The char_length(description)
--     version of idx_hj_sitemap_cohort_v2 (161 scans, desc_len). Readers
--     checked: refresh_hospital_sitemap_cohort() (pg_cron job 4) filters on
--     desc_len >= 200, so it uses _v2; no function, view or script references
--     char_length(description) any more. The 30 scans date from before the
--     _v2 switch or from ad-hoc queries; re-check the count after a week
--     before dropping.
-- drop index concurrently if exists public.idx_hj_sitemap_cohort;

-- (e) idx_hospital_jobs_managed: 0 scans, 8 KB (partial: managed_by_client_id
--     IS NOT NULL). Readers checked: the site reads managed_by_client_id as a
--     column (waypoint-jobs lib/open-jobs.js:167, app/api/client/scraped-roles
--     routes) and never filters on it. The index is tiny and its predicate is
--     almost never true, so its nightly write cost is ~0; dropping it is
--     housekeeping only.
-- drop index concurrently if exists public.idx_hospital_jobs_managed;

-- (f) idx_dead_check_at: 24 scans, 15 MB, a full btree on last_dead_check_at.
--     OWNER'S CALL, with a trade-off. Reader checked: the only query found is
--     the admin link-audit card (waypoint-jobs-main app/api/admin/link-audit/
--     route.js:91-92: order by last_dead_check_at desc limit 1), which this
--     index serves. Nothing else filters or sorts on the column
--     (validate_front_pool_urls.py only writes it; database.py, scheduler.py,
--     the pg functions and views do not read it).
--     Cost of keeping it: last_dead_check_at is in no other index, so the
--     validator's nightly 300-row PATCHes (validate_front_pool_urls.py:246-249,
--     ~14k rows) can never be HOT updates while it exists; they rewrite 32
--     index entries per row and hit the 8 s statement timeout (57014, 900 rows
--     unstamped on 10-06). Without it those updates become HOT-eligible.
--     Cost of dropping it: the admin card's max(last_dead_check_at) becomes a
--     sequential scan over ~870k rows (a few seconds on one admin page), or
--     the card reads the stamp from ops_heartbeat / a small side table instead.
-- drop index concurrently if exists public.idx_dead_check_at;

-- ---------------------------------------------------------------------------
-- 3. Reviewed and KEPT (not candidates)
-- ---------------------------------------------------------------------------
-- idx_hj_indexing_dead       1 scan, 40 KB: used by indexing_publisher.py:127 (EXPLAIN: Index Scan using idx_hj_indexing_dead).
-- idx_ats                    324 scans, 33 MB: low use; the known-body read stopped filtering on
--                            ats_platform in push 10, so expect fewer scans. Re-check in a month.
-- idx_active                 71,673 scans: used.
-- idx_hj_sitemap_cohort_v2   161 scans: the cohort refresh and the queue scripts (see b, c).
-- every other index          16k to 9.5M scans: used by the site.
--
-- ---------------------------------------------------------------------------
-- 4. After any DROP: confirm nothing regressed
-- ---------------------------------------------------------------------------
-- explain select id, title from hospital_jobs
--   where is_active and apply_verified and desc_len >= 200 and bing_submitted_at is null
--   order by scraped_at desc limit 200;                      -- expect idx_hj_sitemap_cohort_v2
-- explain select id from hospital_jobs where is_active = false and indexing_submitted_at is not null limit 500;
--                                                             -- expect idx_hj_indexing_dead
-- explain select * from hospital_jobs where specialty = 'Physical Therapy' and is_active limit 20;
--                                                             -- expect idx_hospital_jobs_specialty or a composite
-- select relname, n_tup_upd, n_tup_hot_upd from pg_stat_user_tables where relname = 'hospital_jobs';
--                                                             -- HOT share should rise after (f)
