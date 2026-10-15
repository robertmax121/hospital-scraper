-- Rochester Regional re-key (2026-10-14): retire the 48 ZIP-keyed rows.
-- OWNER-RUN, and only AFTER the first nightly run with the re-key scraper
-- (wd_list_job_id). Run section A first; run B only if A shows ZIP rows still
-- active. B retires nothing unless the REQ_-keyed rows have landed, so running
-- it too early is a no-op, not an outage.
--
-- Why: Rochester's Workday bulletFields are [site, ZIP, "REQ_237858", town].
-- The ZIP keyed the row, so 1,331 live postings sat on 53 active rows. After
-- the push every posting is keyed by its REQ_ bullet (1,326 new rows), and the
-- 48 ZIP-keyed rows are no longer produced. The nightly sweep should retire
-- them on the first run (Rochester is a complete crawl, ~1,331 rows against
-- 53 active), and Layer 4 retires them after 3 missed runs if it does not.
-- This file is the backstop for a night the sweep skips Rochester (a failed
-- upsert batch, a partial crawl).
--
-- Read on 2026-10-14 ~22:45 UTC (wp.sql, read-only):
--   Rochester Regional Health: 53 active rows (48 ZIP-keyed, 4 REQ_-keyed:
--   REQ_199863 REQ_199875 REQ_223614 REQ_243317, 1 path-keyed:
--   /job/Pulmonary-Medicine-Physician_REQ_EG_17), 27 inactive. The 5 non-ZIP
--   rows keep their keys after the re-key. No sponsor/managed/override fields
--   set on any Rochester row; no foreign keys point at hospital_jobs.

-- ══════════════════════════════════════════════════════════════════════════
-- A. Counts first (read-only).
-- ══════════════════════════════════════════════════════════════════════════
select
  count(*) filter (where is_active)                                        as active_rows,
  count(*) filter (where is_active and job_id like 'REQ\_%')               as active_req_keyed,
  count(*) filter (where is_active and job_id ~ '^\d{5}$')                 as active_zip_keyed,
  count(*) filter (where is_active and job_id like '/%')                   as active_path_keyed,
  max(scraped_at) filter (where job_id like 'REQ\_%')                      as last_req_stamp,
  max(scraped_at) filter (where is_active and job_id ~ '^\d{5}$')          as last_zip_stamp
from public.hospital_jobs
where hospital_system = 'Rochester Regional Health';
-- Expected after the first post-push run: active_req_keyed ~1,330,
-- active_zip_keyed 0 (the sweep took them), active_path_keyed 1.
-- If active_zip_keyed is still 48 (or any number above 0), run B.

-- ══════════════════════════════════════════════════════════════════════════
-- B. Retire the ZIP-keyed rows (only those listed, only if the REQ_ rows
--    have landed, only rows the latest run did not re-stamp).
-- ══════════════════════════════════════════════════════════════════════════
update public.hospital_jobs z
set is_active = false
where z.hospital_system = 'Rochester Regional Health'
  and z.is_active
  and z.job_id in (
    '13214','13617','13635','13642','13662','13676','14001','14020','14103','14416',
    '14420','14424','14432','14450','14454','14456','14467','14468','14472','14480',
    '14502','14513','14525','14527','14534','14551','14559','14564','14580','14589',
    '14605','14606','14607','14608','14609','14610','14611','14612','14613','14617',
    '14618','14621','14622','14623','14624','14625','14626','19044')
  and (select count(*) from public.hospital_jobs r
       where r.hospital_system = 'Rochester Regional Health'
         and r.is_active and r.job_id like 'REQ\_%') >= 1000
  and z.scraped_at < (select max(r.scraped_at) from public.hospital_jobs r
                      where r.hospital_system = 'Rochester Regional Health'
                        and r.job_id like 'REQ\_%');
-- Expected: UPDATE 48 (or 0 if the sweep already retired them, or if the
-- REQ_ rows have not landed yet: then wait for the next run and re-run A).

-- ══════════════════════════════════════════════════════════════════════════
-- C. After-check (read-only): section A again; active_zip_keyed must be 0.
-- ══════════════════════════════════════════════════════════════════════════
