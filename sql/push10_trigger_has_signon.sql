-- push 10 (2026-10-07): the enrichment trigger ORs has_signon with the stored value.
-- OWNER-RUN in the Supabase SQL editor of the DATA project (tlzxajgonuevbqaelhvf).
--
-- APPLY THIS BEFORE THE PUSH-10 SCRAPER IS DEPLOYED. From push 10 the upsert sends
-- has_signon on every row (scraper.normalize_job computes it from the title and body
-- in hand, with the same two rules the nightly SQL pass flag_signon_jobs applied
-- since 2026-08-08), and the nightly SQL pass is a no-op. On a night that sends a
-- blank body for a row (a list-only crawl, or a stored body the detail pass kept, so
-- the payload is empty and the trigger keeps the stored text), the payload's answer
-- is title-only; without this OR, a flag set from a full body on an earlier night
-- would be cleared. With it, has_signon can only be set, never cleared, by the upsert
-- (the same "never un-flags" rule the SQL pass had). If a run lands before this is
-- applied, flag_signon_jobs(full_pass=True) re-flags whatever was cleared.
--
-- preserve_scraped_enrichment() is the BEFORE UPDATE trigger function on BOTH
-- public.hospital_jobs (trg_preserve_enrichment) and public.travel_jobs (same trigger
-- name). travel_jobs has no has_signon column, so the new line sits inside the
-- existing "if tg_table_name = 'hospital_jobs'" branch (plpgsql resolves new.<field>
-- at run time; outside that branch it would raise on travel_jobs updates).
--
-- The body below is the current definition (pg_get_functiondef, read 2026-10-07)
-- reformatted for reading, plus the one marked line. Nothing else changes.

-- 1) Preview: the current source. Keep this output in case a rollback is wanted.
select pg_get_functiondef('public.preserve_scraped_enrichment'::regproc);

-- 2) Replace the function in place (the triggers keep pointing at it).
create or replace function public.preserve_scraped_enrichment()
returns trigger
language plpgsql
as $function$
begin
  if tg_table_name = 'hospital_jobs' then
    if char_length(coalesce(new.description, '')) < 1500
       and char_length(coalesce(new.description, '')) < char_length(coalesce(old.description, '')) then
      new.description := old.description;
    end if;
  elsif char_length(coalesce(new.description, '')) < 200
        and char_length(coalesce(old.description, '')) >= 200 then
    new.description := old.description;
  end if;

  if coalesce(new.posted_date, '') !~ '^\d{4}-\d{2}-\d{2}'
     and coalesce(old.posted_date, '') ~ '^\d{4}-\d{2}-\d{2}' then
    new.posted_date := old.posted_date;
  end if;

  if tg_table_name = 'hospital_jobs' then
    if new.wage_min is null and old.wage_min is not null then
      new.wage_min := old.wage_min;
      new.wage_max := old.wage_max;
      new.wage_unit := old.wage_unit;
    end if;
    if new.posting_facts is null and old.posting_facts is not null then
      new.posting_facts := old.posting_facts;
    end if;
    if coalesce(btrim(new.job_type), '') = '' and coalesce(btrim(old.job_type), '') <> '' then
      new.job_type := old.job_type;
      if coalesce(old.derived_job_type, 'standard') <> 'standard' then
        new.derived_job_type := old.derived_job_type;
      end if;
    end if;
    -- push 10 (2026-10-07): the upsert sends has_signon on every row; a flag set
    -- from a full body is kept over a title-only answer from a blank-body night.
    new.has_signon := coalesce(new.has_signon, false) or coalesce(old.has_signon, false);
  end if;

  if coalesce(btrim(new.state), '') !~ '^[A-Za-z]{2}$'
     and coalesce(btrim(old.state), '') ~ '^[A-Za-z]{2}$' then
    new.state := old.state;
  end if;
  if coalesce(btrim(new.city), '') = '' and coalesce(btrim(old.city), '') <> '' then
    new.city := old.city;
  end if;
  return new;
end
$function$;

-- 3) Verify: the new line is in the stored source, and both triggers still use it.
select position('new.has_signon := coalesce(new.has_signon, false) or coalesce(old.has_signon, false)'
                in pg_get_functiondef('public.preserve_scraped_enrichment'::regproc)) > 0 as has_or_line;
select c.relname, t.tgname, t.tgenabled
from pg_trigger t
join pg_class c on c.oid = t.tgrelid
join pg_proc p on p.oid = t.tgfoid
where p.proname = 'preserve_scraped_enrichment';

-- 4) Baseline for the morning after the first push-10 run: the flagged count must
--    not fall (43,137 of 352,680 active rows on 2026-10-07). A drop means a run
--    landed before this file was applied; flag_signon_jobs(full_pass=True) repairs it.
select count(*) filter (where has_signon) as flagged, count(*) as active
from public.hospital_jobs
where is_active;
