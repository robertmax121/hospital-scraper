-- push 10 (2026-10-07), item 5: HealthcareSource tenant "bch" is Boulder
-- Community Health (Boulder CO), not Brattleboro Memorial (VT). scraper.py now
-- writes that tenant's rows as "Boulder Community Health"; this file relabels
-- the stored rows. OWNER-RUN, read-only counts first, then the writes, in one
-- session. Safe to run before or after the push lands: the new config no
-- longer writes the old label, and a job_id that already exists under the new
-- label is deactivated instead of relabelled (unique key: job_id, hospital_system).
--
-- Read on 2026-10-07 (wp.sql, read-only): 205 rows under "Brattleboro Memorial"
-- (142 active), hospital_name "Boulder Community Health" on every active row,
-- 0 job_id collisions with "Boulder Community Health", 1 system_domains row,
-- no hospital_cms_alias / ahrq_system_label / system_profiles rows. hospital_wages
-- already has 2 rows under "Boulder Community Health" and none under the old
-- label. "Brattleboro Retreat" (a real VT employer in other adapters) is untouched.

-- 1. Count first.
select count(*) as rows_total,
       count(*) filter (where j.is_active) as rows_active,
       count(*) filter (where exists (select 1 from public.hospital_jobs n
                                      where n.hospital_system = 'Boulder Community Health'
                                        and n.job_id = j.job_id)) as job_id_collisions,
       count(*) filter (where j.hospital_name = 'Brattleboro Memorial') as name_is_old_label
from public.hospital_jobs j
where j.hospital_system = 'Brattleboro Memorial';

-- 2. Relabel every non-colliding row (active and inactive, so history follows
--    the tenant). hospital_name only changes where it repeats the old label.
update public.hospital_jobs j
   set hospital_system = 'Boulder Community Health',
       hospital_name = case when coalesce(j.hospital_name, '') in ('', 'Brattleboro Memorial')
                            then 'Boulder Community Health' else j.hospital_name end
 where j.hospital_system = 'Brattleboro Memorial'
   and not exists (select 1 from public.hospital_jobs n
                   where n.hospital_system = 'Boulder Community Health' and n.job_id = j.job_id);

-- 3. Collisions (expected 0 on 2026-10-07): the new-label row is the live one.
update public.hospital_jobs
   set is_active = false
 where hospital_system = 'Brattleboro Memorial'
   and is_active;

-- 4. The one system_domains row.
update public.system_domains
   set hospital_system = 'Boulder Community Health'
 where hospital_system = 'Brattleboro Memorial'
   and not exists (select 1 from public.system_domains d where d.hospital_system = 'Boulder Community Health');

-- 5. Verify: expect 0 active under the old label and about 142 active under the new one.
select hospital_system,
       count(*) filter (where is_active) as active,
       count(*) as total
from public.hospital_jobs
where hospital_system in ('Brattleboro Memorial', 'Boulder Community Health')
group by 1
order by 1;
