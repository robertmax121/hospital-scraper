-- Push 10 (2026-10-07), worktree "body", item 3: MyMichigan Health.
-- OWNER APPROVAL REQUIRED. Nothing here runs by itself; the scraper never
-- writes SQL. Read-only numbers from 2026-10-07 01:xx UTC.
--
-- What changes with the push: MyMichigan Health moves from the Playwright
-- CUSTOM_SITES entry (careers.mymichigan.org/jobs rendered in a browser) to
-- JIBE_SITES (the same /api/jobs feed BJC uses). The old route stored 71
-- active rows with ats_platform 'Custom', a job_id that carries the query
-- string ("49176?lang=en-us"), a title glued to the next line
-- ("Registered Nurse RN - ED\n\nReq ID: 49092"), no description and no job
-- type. The Jibe feed lists 968 postings with full bodies under the clean
-- job_id ("49176"), so the new rows never match the old (job_id,
-- hospital_system) key: the old 71 would sit until the miss counter retires
-- them, duplicated beside the new rows for a few nights.
--
-- Run AFTER the first nightly run that writes the Jibe rows (check with the
-- first query), outside 00:00-00:45 UTC and never while a run is writing.

-- 1. Preview: the new rows have landed when this returns rows with a body.
select ats_platform, count(*) n, count(*) filter (where coalesce(desc_len, 0) >= 200) with_body,
       min(scraped_at) first_stamp, max(scraped_at) last_stamp
  from hospital_jobs
 where is_active and hospital_system = 'MyMichigan Health'
 group by 1;

-- 2. Preview the rows the deactivate below touches (expected: 71, all
--    ats_platform 'Custom', job_id like '%?lang=%').
select id, job_id, left(title, 60) title, desc_len, scraped_at
  from hospital_jobs
 where is_active and hospital_system = 'MyMichigan Health'
   and ats_platform = 'Custom' and job_id like '%?lang=%'
 order by id;

-- 3. Retire the Playwright rows once the Jibe rows exist (step 1 shows
--    ats_platform 'iCIMS' rows with a body).
-- update hospital_jobs
--    set is_active = false
--  where is_active and hospital_system = 'MyMichigan Health'
--    and ats_platform = 'Custom' and job_id like '%?lang=%';

-- 4. Alternative, if the old rows should stay until the sweep retires them:
--    only fix the glued titles (the scraper's normalize_job now cuts a title
--    at its first line break, but these rows are never re-scraped by the
--    old route).
-- update hospital_jobs
--    set title = split_part(title, E'\n', 1)
--  where hospital_system = 'MyMichigan Health' and title like E'%\n%';
