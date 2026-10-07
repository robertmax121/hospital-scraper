-- Push 10 (2026-10-07), pay item 7: AMN Healthcare travel rows store the TOP
-- of the weekly range in weekly_pay_numeric; every other agency (Aya, Vivian)
-- stores the LOW end, which is what every weekly-pay sort, min-pay filter and
-- the MCP min_weekly_pay gte read. The adapter (_amn_map in scraper.py) now
-- takes the low end; this file repairs the rows already stored.
--
-- Owner-run. Read-only counts first, then the UPDATE in id chunks. Nothing in
-- this file runs by itself; each statement is pasted and run by hand.
--
-- Evidence (read 2026-10-07 via wp.sql, read-only): agency_name = 'AMN
-- Healthcare' holds 44,340 rows (13,573 active), 44,211 with a numeric,
-- 44,223 whose display starts with "$<digits>", and on all 44,211 priced rows
-- the numeric differs from the display's first figure (it is the second one):
--   id 1884989  '$2240-$2355/wk'  numeric 2355  ->  2240
--   id 1884994  '$2443-$2518/wk'  numeric 2518  ->  2443
-- ids run 1,884,985 .. 7,111,127.

-- 1. Count first. Expect about 44,211 rows (13,5xx active); the per-chunk
--    counts below must sum to the same.
select count(*)                                   as amn_priced,
       count(*) filter (where is_active)          as amn_priced_active,
       count(*) filter (where weekly_pay_numeric <> (regexp_match(weekly_pay_display, '^\$(\d+(?:\.\d+)?)'))[1]::numeric) as numeric_is_not_first_figure
from travel_jobs
where agency_name = 'AMN Healthcare'
  and weekly_pay_numeric is not null
  and weekly_pay_display ~ '^\$\d';

-- 2. Preview ten rows of what the write will do.
select id, is_active, weekly_pay_display, weekly_pay_numeric as before,
       (regexp_match(weekly_pay_display, '^\$(\d+(?:\.\d+)?)'))[1]::numeric as after
from travel_jobs
where agency_name = 'AMN Healthcare'
  and weekly_pay_numeric is not null
  and weekly_pay_display ~ '^\$\d'
  and weekly_pay_numeric <> (regexp_match(weekly_pay_display, '^\$(\d+(?:\.\d+)?)'))[1]::numeric
order by id
limit 10;

-- 3. The write, chunked by id (about 500k ids a chunk; each chunk is its own
--    short transaction, so the nightly upsert is never blocked for long).
--    Run the chunks one at a time; each reports its row count. Re-running a
--    chunk is harmless: the WHERE only matches rows whose numeric is still
--    not the first figure.
update travel_jobs
   set weekly_pay_numeric = (regexp_match(weekly_pay_display, '^\$(\d+(?:\.\d+)?)'))[1]::numeric
 where agency_name = 'AMN Healthcare'
   and weekly_pay_numeric is not null
   and weekly_pay_display ~ '^\$\d'
   and weekly_pay_numeric <> (regexp_match(weekly_pay_display, '^\$(\d+(?:\.\d+)?)'))[1]::numeric
   and id >= 1800000 and id < 2300000;

update travel_jobs
   set weekly_pay_numeric = (regexp_match(weekly_pay_display, '^\$(\d+(?:\.\d+)?)'))[1]::numeric
 where agency_name = 'AMN Healthcare'
   and weekly_pay_numeric is not null
   and weekly_pay_display ~ '^\$\d'
   and weekly_pay_numeric <> (regexp_match(weekly_pay_display, '^\$(\d+(?:\.\d+)?)'))[1]::numeric
   and id >= 2300000 and id < 2800000;

update travel_jobs
   set weekly_pay_numeric = (regexp_match(weekly_pay_display, '^\$(\d+(?:\.\d+)?)'))[1]::numeric
 where agency_name = 'AMN Healthcare'
   and weekly_pay_numeric is not null
   and weekly_pay_display ~ '^\$\d'
   and weekly_pay_numeric <> (regexp_match(weekly_pay_display, '^\$(\d+(?:\.\d+)?)'))[1]::numeric
   and id >= 2800000 and id < 3300000;

update travel_jobs
   set weekly_pay_numeric = (regexp_match(weekly_pay_display, '^\$(\d+(?:\.\d+)?)'))[1]::numeric
 where agency_name = 'AMN Healthcare'
   and weekly_pay_numeric is not null
   and weekly_pay_display ~ '^\$\d'
   and weekly_pay_numeric <> (regexp_match(weekly_pay_display, '^\$(\d+(?:\.\d+)?)'))[1]::numeric
   and id >= 3300000 and id < 3800000;

update travel_jobs
   set weekly_pay_numeric = (regexp_match(weekly_pay_display, '^\$(\d+(?:\.\d+)?)'))[1]::numeric
 where agency_name = 'AMN Healthcare'
   and weekly_pay_numeric is not null
   and weekly_pay_display ~ '^\$\d'
   and weekly_pay_numeric <> (regexp_match(weekly_pay_display, '^\$(\d+(?:\.\d+)?)'))[1]::numeric
   and id >= 3800000 and id < 4300000;

update travel_jobs
   set weekly_pay_numeric = (regexp_match(weekly_pay_display, '^\$(\d+(?:\.\d+)?)'))[1]::numeric
 where agency_name = 'AMN Healthcare'
   and weekly_pay_numeric is not null
   and weekly_pay_display ~ '^\$\d'
   and weekly_pay_numeric <> (regexp_match(weekly_pay_display, '^\$(\d+(?:\.\d+)?)'))[1]::numeric
   and id >= 4300000 and id < 4800000;

update travel_jobs
   set weekly_pay_numeric = (regexp_match(weekly_pay_display, '^\$(\d+(?:\.\d+)?)'))[1]::numeric
 where agency_name = 'AMN Healthcare'
   and weekly_pay_numeric is not null
   and weekly_pay_display ~ '^\$\d'
   and weekly_pay_numeric <> (regexp_match(weekly_pay_display, '^\$(\d+(?:\.\d+)?)'))[1]::numeric
   and id >= 4800000 and id < 5300000;

update travel_jobs
   set weekly_pay_numeric = (regexp_match(weekly_pay_display, '^\$(\d+(?:\.\d+)?)'))[1]::numeric
 where agency_name = 'AMN Healthcare'
   and weekly_pay_numeric is not null
   and weekly_pay_display ~ '^\$\d'
   and weekly_pay_numeric <> (regexp_match(weekly_pay_display, '^\$(\d+(?:\.\d+)?)'))[1]::numeric
   and id >= 5300000 and id < 5800000;

update travel_jobs
   set weekly_pay_numeric = (regexp_match(weekly_pay_display, '^\$(\d+(?:\.\d+)?)'))[1]::numeric
 where agency_name = 'AMN Healthcare'
   and weekly_pay_numeric is not null
   and weekly_pay_display ~ '^\$\d'
   and weekly_pay_numeric <> (regexp_match(weekly_pay_display, '^\$(\d+(?:\.\d+)?)'))[1]::numeric
   and id >= 5800000 and id < 6300000;

update travel_jobs
   set weekly_pay_numeric = (regexp_match(weekly_pay_display, '^\$(\d+(?:\.\d+)?)'))[1]::numeric
 where agency_name = 'AMN Healthcare'
   and weekly_pay_numeric is not null
   and weekly_pay_display ~ '^\$\d'
   and weekly_pay_numeric <> (regexp_match(weekly_pay_display, '^\$(\d+(?:\.\d+)?)'))[1]::numeric
   and id >= 6300000 and id < 6800000;

update travel_jobs
   set weekly_pay_numeric = (regexp_match(weekly_pay_display, '^\$(\d+(?:\.\d+)?)'))[1]::numeric
 where agency_name = 'AMN Healthcare'
   and weekly_pay_numeric is not null
   and weekly_pay_display ~ '^\$\d'
   and weekly_pay_numeric <> (regexp_match(weekly_pay_display, '^\$(\d+(?:\.\d+)?)'))[1]::numeric
   and id >= 6800000 and id < 7300000;

-- Rows added after this file was written (id >= 7,300,000) come from the
-- fixed adapter and need no repair; if the count in step 1 still shows
-- leftovers, add one more chunk for the id range it reports.

-- 4. Verify: must be 0.
select count(*) as still_top_of_range
from travel_jobs
where agency_name = 'AMN Healthcare'
  and weekly_pay_numeric is not null
  and weekly_pay_display ~ '^\$\d'
  and weekly_pay_numeric <> (regexp_match(weekly_pay_display, '^\$(\d+(?:\.\d+)?)'))[1]::numeric;
