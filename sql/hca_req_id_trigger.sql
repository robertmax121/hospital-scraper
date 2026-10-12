-- 2026-10-12 (HCA requisition number): the enrichment trigger MERGES a req_id-only
-- posting_facts object instead of replacing the stored facts with it.
-- OWNER-RUN in the Supabase SQL editor of the DATA project (tlzxajgonuevbqaelhvf).
-- NOT APPLIED by the agent that wrote it.
--
-- Why: HCA's job_id is the Talemetry URL id (18284308); the posting prints the
-- requisition number recruiters use ("Job ID: 4981331"). From this change the
-- scraper stores it as posting_facts.req_id. The live trigger (read with
-- pg_get_functiondef on 2026-10-12) handles posting_facts with one rule:
--     if new.posting_facts is null and old.posting_facts is not null then
--       new.posting_facts := old.posting_facts;
-- so a null keeps the stored object and any non-null object REPLACES it whole.
-- On most nights most HCA rows have no body in hand (the local detail pass reads
-- 2,000 of ~23,000 pages), so their facts go out as null and req_id cannot ride
-- along; sending {"req_id": "4981331"} instead would wipe the stored certs,
-- education, shift and pay source of ~20,500 rows. With this file applied:
--   * an object holding only req_id is merged into the stored facts
--     (old || new), so every HCA row gets its number in one push;
--   * a stored req_id survives a later object built without one (an older
--     checkout, or a card that lacked the line).
-- Then the scraper's REQ_ID_ONLY_FACTS=1 switch (default off) can send the
-- req_id-only object. Leave the switch off until step 3 below says true.
--
-- preserve_scraped_enrichment() is the BEFORE UPDATE trigger function on BOTH
-- public.hospital_jobs (trg_preserve_enrichment) and public.travel_jobs. The new
-- lines sit inside the existing "if tg_table_name = 'hospital_jobs'" branch
-- (travel_jobs has no posting_facts column). Nothing else changes: the body below
-- is the 2026-10-12 live definition, reformatted, plus the marked lines.

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
    -- 2026-10-12 (HCA req_id): an object holding only req_id is merged, not swapped in.
    elsif old.posting_facts is not null and jsonb_typeof(old.posting_facts) = 'object'
          and jsonb_typeof(new.posting_facts) = 'object'
          and new.posting_facts ? 'req_id' and (new.posting_facts - 'req_id') = '{}'::jsonb then
      new.posting_facts := old.posting_facts || new.posting_facts;
    end if;
    -- 2026-10-12 (HCA req_id): a stored req_id survives an object built without one.
    if old.posting_facts ? 'req_id' and jsonb_typeof(new.posting_facts) = 'object'
       and not (new.posting_facts ? 'req_id') then
      new.posting_facts := new.posting_facts || jsonb_build_object('req_id', old.posting_facts -> 'req_id');
    end if;
    if coalesce(btrim(new.job_type), '') = '' and coalesce(btrim(old.job_type), '') <> '' then
      new.job_type := old.job_type;
      if coalesce(old.derived_job_type, 'standard') <> 'standard' then
        new.derived_job_type := old.derived_job_type;
      end if;
    end if;
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

-- 3) Verify: the merge line is in the stored source, and both triggers still use it.
--    true here = REQ_ID_ONLY_FACTS=1 is safe for the next HCA local push.
select position('new.posting_facts := old.posting_facts || new.posting_facts'
                in pg_get_functiondef('public.preserve_scraped_enrichment'::regproc)) > 0 as has_req_id_merge;
select c.relname, t.tgname, t.tgenabled
from pg_trigger t
join pg_class c on c.oid = t.tgrelid
join pg_proc p on p.oid = t.tgfoid
where p.proname = 'preserve_scraped_enrichment';

-- 4) After the next HCA local push: how many active HCA rows carry the number
--    (0 of 23,071 on 2026-10-12), and how many kept their stored facts (facts_has_v
--    was 6,394 on 2026-10-12; it must not fall).
select count(*) filter (where posting_facts ? 'req_id') as with_req_id,
       count(*) filter (where posting_facts ? 'v')      as facts_has_v,
       count(*)                                          as active
from public.hospital_jobs
where hospital_system = 'HCA Healthcare' and is_active;
