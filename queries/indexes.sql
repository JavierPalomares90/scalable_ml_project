-- explain
-- select count(*)
-- from pitch_data pd
--     join pitch_data_stage ps
--         on pd.p1_pitch_id = ps.p1_pitch_id;
/*
 Aggregate  (cost=711136.10..711136.11 rows=1 width=8)
  ->  Hash Join  (cost=320506.97..703869.54 rows=2906621 width=0)
        Hash Cond: ((ps.p1_pitch_id)::text = (pd.p1_pitch_id)::text)
        ->  Seq Scan on pitch_data_stage ps  (cost=0.00..267984.30 rows=2907530 width=37)
        ->  Hash  (cost=261466.21..261466.21 rows=2906621 width=37)
              ->  Seq Scan on pitch_data pd  (cost=0.00..261466.21 rows=2906621 width=37)

 */
--need pitch_id idx's on both tables
create index idx_pd_p1_pitch_id on pitch_data (p1_pitch_id);
create index idx_ps_p1_pitch_id on pitch_data_stage (p1_pitch_id);

--update column types
alter table pitch_data drop column at_bat_outs;
alter table pitch_data drop column pitch_des;
/*
  p0_at_bat_o integer,
    p0_pitch_des text,

 */
 alter table pitch_data add column p0_at_bat_o integer default 0;
 alter table pitch_data add column p0_pitch_des text default '';


--run update
update pitch_data
set p0_at_bat_o = ps.p0_at_bat_o
from pitch_data_stage ps join pitch_data pd on ps.p1_pitch_id = pd.p1_pitch_id
