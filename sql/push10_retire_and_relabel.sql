-- push 10 (2026-10-07), item 7: retire the placeholder twins and the Atria
-- Group LLC rows, and relabel the two mislabelled Oracle tenants. OWNER-RUN,
-- in this order, in one session; every write is preceded by a count. Safe to
-- run before or after the push lands: scraper.py no longer writes any of the
-- old labels, and the unique key (job_id, hospital_system) is handled below.
--
-- Read on 2026-10-07 (wp.sql, read-only):
--   Placeholder twins: Paycor Hospital 2 333 active / Kronos Hospital 2 33 /
--     Kronos Hospital 3 107, and EVERY one of the 473 has its job_id under the
--     real label already (Insight Health 334 active, Northern Regional
--     Hospital 40, Pikeville Medical Center 102), so the 67c relabel skipped
--     them all. 1 system_domains row per old label; no alias rows.
--   Atria: 966 active rows, all with the smartrecruiters.com/AtriaGroupLLC
--     URL (an IT staffing firm: Java, SAP, .NET, released 2012-2017), no
--     other URL under that label; 1 system_domains row, 1 hospital_wages row.
--   "United Regional" (Oracle erqh): 3,292 rows, 892 active, 883 NJ + 9 blank
--     state; live postings name Atlantic Health Overlook Medical Center and
--     Atlantic Mobile Health: ATLANTIC HEALTH SYSTEM. 0 collisions with that
--     label. 6 hospital_cms_alias rows (Morristown, Chilton, Newton, Overlook,
--     CentraState, AHS Hospital Corp; all NJ, all Atlantic Health), 1
--     system_domains row, 1 hospital_wages row (6 already under Atlantic).
--     "United Regional Health Care System" (iaoxqy, Wichita Falls TX) is a
--     different system and is untouched.
--   "Eastern Connecticut Health" (Oracle eglz): 533 rows, 135 active, all CA
--     (Santa Barbara / Goleta); a live posting reads "within Cottage Medical
--     Group": COTTAGE HEALTH. 0 collisions. 3 hospital_cms_alias rows (Santa
--     Barbara, Goleta Valley, Santa Ynez Valley Cottage), 1 system_domains
--     row, 1 hospital_wages row.

-- ══════════════════════════════════════════════════════════════════════════
-- A. Counts first (all read-only).
-- ══════════════════════════════════════════════════════════════════════════
select m.old_label, m.new_label,
       (select count(*) from public.hospital_jobs j where j.hospital_system = m.old_label and j.is_active) as active_rows,
       (select count(*) from public.hospital_jobs j where j.hospital_system = m.old_label) as total_rows,
       (select count(*) from public.hospital_jobs j where j.hospital_system = m.old_label and j.is_active
          and exists (select 1 from public.hospital_jobs n where n.hospital_system = m.new_label and n.job_id = j.job_id)) as active_twins,
       (select count(*) from public.hospital_jobs j where j.hospital_system = m.old_label
          and exists (select 1 from public.hospital_jobs n where n.hospital_system = m.new_label and n.job_id = j.job_id)) as collisions_all,
       (select count(*) from public.hospital_cms_alias x where x.hospital_system_label = m.old_label) as alias_rows,
       (select count(*) from public.system_domains d where d.hospital_system = m.old_label) as domain_rows,
       (select count(*) from public.hospital_wages w where w.hospital_system = m.old_label) as wage_rows
from (values
  ('Paycor Hospital 2',          'Insight Health'),
  ('Kronos Hospital 2',          'Northern Regional Hospital'),
  ('Kronos Hospital 3',          'Pikeville Medical Center'),
  ('United Regional',            'Atlantic Health System'),
  ('Eastern Connecticut Health', 'Cottage Health')
) as m (old_label, new_label)
order by 1;

select count(*) filter (where is_active) as atria_active,
       count(*) as atria_total,
       count(*) filter (where url not ilike '%smartrecruiters.com/AtriaGroupLLC%') as atria_other_url
from public.hospital_jobs
where hospital_system = 'Atria Senior Living';

-- ══════════════════════════════════════════════════════════════════════════
-- B. Atria Group LLC: retire the 966 IT-staffing rows filed as Atria Senior
--    Living. The config entry is gone, so nothing re-adds them.
-- ══════════════════════════════════════════════════════════════════════════
update public.hospital_jobs
   set is_active = false
 where hospital_system = 'Atria Senior Living'
   and url ilike '%smartrecruiters.com/AtriaGroupLLC%'
   and is_active;

-- ══════════════════════════════════════════════════════════════════════════
-- C. Placeholder twins. C1 relabels any row whose job_id does NOT yet exist
--    under the real label (67c semantics; expected 0 rows on 2026-10-07).
--    C2 then deactivates the colliding rows: the real-label row is the live
--    one, written nightly since push 8, and the old label is never written
--    again, so these 473 rows would otherwise sit behind the zero-yield guard
--    until the backstop (about 10-14) as duplicates on the board.
-- ══════════════════════════════════════════════════════════════════════════
update public.hospital_jobs j
   set hospital_system = m.new_label,
       hospital_name = case when j.hospital_name = m.old_label then m.new_label else j.hospital_name end
  from (values
  ('Paycor Hospital 2', 'Insight Health'),
  ('Kronos Hospital 2', 'Northern Regional Hospital'),
  ('Kronos Hospital 3', 'Pikeville Medical Center')
) as m (old_label, new_label)
 where j.hospital_system = m.old_label
   and not exists (select 1 from public.hospital_jobs n where n.hospital_system = m.new_label and n.job_id = j.job_id);

update public.hospital_jobs
   set is_active = false
 where hospital_system in ('Paycor Hospital 2', 'Kronos Hospital 2', 'Kronos Hospital 3')
   and is_active;

-- ══════════════════════════════════════════════════════════════════════════
-- D. Oracle tenant relabels: United Regional (erqh) -> Atlantic Health System,
--    Eastern Connecticut Health (eglz) -> Cottage Health. Active rows first
--    (892 + 135), then the inactive history (2,400 + 398) in a second
--    statement so neither runs long. A job_id already under the new label
--    (0 on 2026-10-07; possible if a run with the new config lands first) is
--    deactivated in D3 instead.
-- ══════════════════════════════════════════════════════════════════════════
-- D1. Active rows.
update public.hospital_jobs j
   set hospital_system = m.new_label,
       hospital_name = case when j.hospital_name = m.old_label then m.new_label else j.hospital_name end
  from (values
  ('United Regional',            'Atlantic Health System'),
  ('Eastern Connecticut Health', 'Cottage Health')
) as m (old_label, new_label)
 where j.hospital_system = m.old_label
   and j.is_active
   and not exists (select 1 from public.hospital_jobs n where n.hospital_system = m.new_label and n.job_id = j.job_id);

-- D2. Inactive rows (history follows the tenant).
update public.hospital_jobs j
   set hospital_system = m.new_label,
       hospital_name = case when j.hospital_name = m.old_label then m.new_label else j.hospital_name end
  from (values
  ('United Regional',            'Atlantic Health System'),
  ('Eastern Connecticut Health', 'Cottage Health')
) as m (old_label, new_label)
 where j.hospital_system = m.old_label
   and not j.is_active
   and not exists (select 1 from public.hospital_jobs n where n.hospital_system = m.new_label and n.job_id = j.job_id);

-- D3. Collisions left under the old labels (expected 0): the new-label row is live.
update public.hospital_jobs
   set is_active = false
 where hospital_system in ('United Regional', 'Eastern Connecticut Health')
   and is_active;

-- D4. CMS alias rows (6 Atlantic Health NJ hospitals, 3 Cottage CA hospitals):
--     the aliases already point at the right hospitals, only the label was wrong.
update public.hospital_cms_alias x
   set hospital_system_label = m.new_label,
       note = x.note || ' (relabelled 2026-10-07 push 10 from ' || m.old_label || ')'
  from (values
  ('United Regional',            'Atlantic Health System'),
  ('Eastern Connecticut Health', 'Cottage Health')
) as m (old_label, new_label)
 where x.hospital_system_label = m.old_label
   and not exists (select 1 from public.hospital_cms_alias y
                   where y.cms_facility_id = x.cms_facility_id and y.hospital_system_label = m.new_label and y.mode = x.mode);

-- D5. system_domains (one row per old label; skipped if the new label already has one).
update public.system_domains d
   set hospital_system = m.new_label
  from (values
  ('United Regional',            'Atlantic Health System'),
  ('Eastern Connecticut Health', 'Cottage Health')
) as m (old_label, new_label)
 where d.hospital_system = m.old_label
   and not exists (select 1 from public.system_domains e where e.hospital_system = m.new_label);

-- D6. hospital_wages: NOT changed here. One user-submitted row sits under each
--     old label; whether to move them is the owner's call (counts in A).

-- D7. Coverage views, as after 67c.
select public.refresh_cms_coverage_detail();
select public.refresh_cms_coverage_snapshot();

-- ══════════════════════════════════════════════════════════════════════════
-- E. Verify: expect 0 active rows under every old label and under Atria.
-- ══════════════════════════════════════════════════════════════════════════
select hospital_system,
       count(*) filter (where is_active) as active,
       count(*) as total
from public.hospital_jobs
where hospital_system in ('Paycor Hospital 2', 'Kronos Hospital 2', 'Kronos Hospital 3',
                          'Insight Health', 'Northern Regional Hospital', 'Pikeville Medical Center',
                          'Atria Senior Living',
                          'United Regional', 'Atlantic Health System',
                          'Eastern Connecticut Health', 'Cottage Health')
group by 1
order by 1;

select hospital_system_label, count(*)
from public.hospital_cms_alias
where hospital_system_label in ('United Regional', 'Atlantic Health System', 'Eastern Connecticut Health', 'Cottage Health')
group by 1
order by 1;
