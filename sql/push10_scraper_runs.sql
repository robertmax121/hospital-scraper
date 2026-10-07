-- push 10 (2026-10-07): run observability. public.scraper_runs gets one row a nightly:
-- the scheduler inserts it at start (status 'running') and updates it at the end
-- (success / partial / failed) through the service client. OWNER-RUN in the Supabase
-- SQL editor of the DATA project (tlzxajgonuevbqaelhvf). Apply before or after the
-- push-10 deploy: until the columns exist the insert fails and the scheduler logs
-- "scraper_runs insert failed (non-fatal)" and carries on.
--
-- The table already exists (0 rows on 2026-10-07) with a per-adapter shape:
--   id, ats_platform text NOT NULL, hospital_system, status text NOT NULL
--   CHECK (running / success / partial / failed / blocked), started_at timestamptz
--   NOT NULL DEFAULT now(), finished_at, duration_ms, jobs_seen, jobs_inserted,
--   jobs_updated, jobs_deactivated, error_message, error_class, http_status, meta jsonb;
--   indexes on started_at desc, (ats_platform, started_at desc), (status, started_at
--   desc); RLS enabled with no policies (service_role and postgres bypass it; anon and
--   authenticated have SELECT and see nothing through RLS).
-- This file is idempotent: it creates that table if it is ever missing, then adds the
-- run-level columns. The nightly writes ats_platform = 'nightly' (hospital_system null)
-- so per-adapter rows can share the table later.
--
-- Column meaning (nightly rows):
--   run_started_at / run_finished_at  the run's UTC start and end (the scheduler's
--                                     clock; started_at / finished_at carry the same)
--   run_day         the UTC day ordinal fixed at start (scraper.fix_run_day); the pay
--                   and refresh slots key on it
--   rows_upserted   hospital rows that landed (first pass plus the delayed retry)
--   deactivated     per-system sweep + Layer 4 + front-pool retirements (jobs_deactivated
--                   carries the same number); the split is in notes
--   field_priced    rows of tonight's payload priced from a structured pay field
--   text_priced     rows of tonight's payload priced from the body text
--                   (both count what the run READ, not the board's priced total: a row
--                   whose stored body was kept sends a blank and counts under neither)
--   notes           one line: payload size, landed / not landed, the deactivation split,
--                   Layer 4 guard count, partial adapters, active total after the run
--   meta            the Layer 4 summary, the front-pool summary, partial and guarded
--                   system lists (jsonb)

create table if not exists public.scraper_runs (
  id               bigserial primary key,
  ats_platform     text not null,
  hospital_system  text,
  status           text not null check (status in ('running', 'success', 'partial', 'failed', 'blocked')),
  started_at       timestamptz not null default now(),
  finished_at      timestamptz,
  duration_ms      integer,
  jobs_seen        integer default 0,
  jobs_inserted    integer default 0,
  jobs_updated     integer default 0,
  jobs_deactivated integer default 0,
  error_message    text,
  error_class      text,
  http_status      integer,
  meta             jsonb
);

alter table public.scraper_runs
  add column if not exists run_started_at  timestamptz,
  add column if not exists run_finished_at timestamptz,
  add column if not exists run_day         integer,
  add column if not exists rows_upserted   integer,
  add column if not exists deactivated     integer,
  add column if not exists field_priced    integer,
  add column if not exists text_priced     integer,
  add column if not exists notes           text;

comment on column public.scraper_runs.run_day is
  'UTC day ordinal fixed at run start (scraper.fix_run_day); the pay and refresh slots key on it';
comment on column public.scraper_runs.rows_upserted is
  'hospital rows that landed in the upsert (first pass plus the delayed retry)';
comment on column public.scraper_runs.deactivated is
  'per-system sweep + Layer 4 + front-pool retirements in this run';
comment on column public.scraper_runs.field_priced is
  'rows of the run payload priced from a structured pay field (posting_facts.pay_src = field)';
comment on column public.scraper_runs.text_priced is
  'rows of the run payload priced from the body text (posting_facts.pay_src = text)';

create index if not exists idx_scraper_runs_run_day on public.scraper_runs (run_day desc);

-- Verify the shape.
select column_name, data_type, is_nullable, column_default
from information_schema.columns
where table_schema = 'public' and table_name = 'scraper_runs'
order by ordinal_position;

-- The morning after the first push-10 run: one 'nightly' row, closed.
select id, status, run_started_at, run_finished_at, run_day, rows_upserted, deactivated,
       field_priced, text_priced, left(notes, 160) as notes
from public.scraper_runs
order by id desc
limit 5;
