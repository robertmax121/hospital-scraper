-- push 10 (2026-10-07): pg_cron job 4 "refresh-hospital-cohort" gets a timeout that
-- actually applies, and moves to 05:30 UTC. OWNER-RUN in the Supabase SQL editor of
-- the DATA project (tlzxajgonuevbqaelhvf). Nothing in the scraper depends on it; apply
-- it on its own. Read-only preview first, then the one change, then the checks.
--
-- Why: cron.job_run_details shows job 4 ('30 1 * * *', select public.refresh_hospital_
-- sitemap_cohort()) dying at exactly 120.0 s on 10-03, 10-06 and 10-07 ("canceling
-- statement due to statement timeout", inside dedupe_hospital_sitemap_cohort) and
-- taking 105-115 s of its 120 s on the nights it survived. The function already says
-- "set local statement_timeout = '600s'", and that cannot help: Postgres arms the
-- timeout timer when the statement STARTS, from the value in force at that moment
-- (statement_timeout = 120000 ms, from postgresql.conf), and a GUC changed inside the
-- running statement does not re-arm the timer. A wrapper function that sets it and
-- then calls the refresh would fail the same way, for the same reason.
--
-- What works: set the value BEFORE the statement starts. pg_cron here runs jobs over
-- libpq (cron.use_background_workers = off, pg_cron 1.6.4, cron.host = localhost), so a
-- job command is one simple-protocol query string, and Postgres applies
-- statement_timeout to each statement of a multi-statement string separately (the
-- timer is disabled between them and re-armed, from the current GUC, when the next
-- statement starts). A SET placed before the SELECT in the same command is therefore
-- in force when the SELECT starts. The SET is session-level in the job's own
-- connection, which pg_cron opens for the run and closes after it; no other session
-- sees it.
--
-- Why 05:30 UTC: the nightly starts 00:09 UTC and its DB-write phases (upsert plus
-- sweep, Layer 4, the travel window fetch) have run past 01:30 on every night since
-- 10-03, and the refresh failed on exactly the nights 01:30 overlapped them. The
-- 02:00-02:40 jobs (ops-heartbeat, expire-open-roles, board-health-snapshot,
-- system-domains, cms-coverage-snapshot) are not touched. The sitemap reads the
-- cohort table at request time, so a later refresh costs nothing on the site.

-- 1) Preview the job as it is now (expected: jobid 4, '30 1 * * *',
--    'select public.refresh_hospital_sitemap_cohort()').
select jobid, jobname, schedule, command, active
from cron.job
where jobid = 4;

-- 2) The change: same jobid, same name, new schedule and command.
select cron.alter_job(
  job_id   := 4,
  schedule := '30 5 * * *',
  command  := $cmd$set statement_timeout = '600s'; select public.refresh_hospital_sitemap_cohort();$cmd$
);

-- 3) Verify the row.
select jobid, jobname, schedule, command, active
from cron.job
where jobid = 4;

-- 4) The next morning (after 05:30 UTC): the newest run should read 'succeeded', and
--    a duration above 120 s is now allowed. If it still says "canceling statement due
--    to statement timeout" at exactly 600 s, the function itself needs to get cheaper
--    (dedupe_hospital_sitemap_cohort's window function over the whole cohort join).
select runid, status, left(return_message, 90) as msg, start_time,
       end_time - start_time as took
from cron.job_run_details
where jobid = 4
order by start_time desc
limit 3;

-- Fallback if cron.alter_job is unavailable on this instance (it is present on
-- pg_cron 1.6.4): unschedule and schedule again under the same name. This allocates
-- a new jobid; adjust the checks above accordingly.
-- select cron.unschedule(4);
-- select cron.schedule('refresh-hospital-cohort', '30 5 * * *',
--   $cmd$set statement_timeout = '600s'; select public.refresh_hospital_sitemap_cohort();$cmd$);
