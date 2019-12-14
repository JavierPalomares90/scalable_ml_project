--todO use a counter so there's only one statement

--2016
with counts as (
    select p.pitch_id,
        --ab.pitcher, p.id, p.tfs_zulu, p.at_bat_id, gpp.team_id, p.des,--for debug
        row_number() over (partition by p.game_id, p.at_bat_id order by p.id asc) as pitch_count_atbat,
        row_number() over (partition by p.game_id, gpp.team_id order by p.id asc) as pitch_count_team,
        case when lag(p.type_simple) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id) = 'B' then 1 else 0 end as p0_is_ball,
        case when lag(p.type_simple) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id) = 'S' then 1 else 0 end as p0_is_strike
   from pitch p
        join game g on p.game_id = g.game_id
        join at_bat ab on p.at_bat_id = ab.at_bat_id
        join game_player gpp on ab.pitcher = gpp.id
            and g.gid = gpp.gid
    where substring(g.gid, 1, 8) = 'gid_2016'
)
insert into pitch_data_stage (season, pitcher_id, team_id, team_abbrev, era, wins,
                             losses, throws, b1_id, b1_team_id, b1_stand, b1_height,
                             b1_bats, b1_avg, b1_hr, b1_rbi, b1_bat_order, b1_game_position,
                             p1_pitch_id, p0_pitch_id, p1_pitch_seqno, p0_pitch_seqno, p0_at_bat_o, p0_pitch_des,
                             p0_inning, result_type, result_type_simple, x, y, start_speed,
                             end_speed, sz_top, sz_bot, pfx_x, pfx_z, px, pz, x0, y0, z0,
                             vx0, vy0, vz0, ax, ay, az, break_y, break_angle, break_length,
                             p0_pitch_type, type_confidence, zone, nasty, spin_dir, spin_rate,
                             outcome, inning, pitch_count_atbat, pitch_count_team, balls, strikes,
                             outs, is_runner_on_first, is_runner_on_second, is_runner_on_third,
                             runs_pitcher_team, runs_batter_team, game_id, inning_id,
                             half_inning_id, at_bat_id, gid, p1_pitch_type)
select
    coalesce(date_part('year', p.tfs_zulu), 2016), --as season,
    --p1 pitcher data
    cast(gpp.id as integer), --as pitcher_id, --game_player.id
    gpp.team_id, --as team_id,
    gpp.team_abbrev, --as team_abbrev,
    coalesce(gpp.era, 4.5),--as era,
    gpp.wins, --as wins,
    gpp.losses, --as losses,
    ab.p_throws, --as throws,
    --b1 data
    cast(gpb.id as integer), --as b1_id, --game_player.id
    gpb.team_id, --as b1_team_id,
    ab.stand, --as b1_stand,
    ab.b_height, --as b1_height,
    gpb.bats, --as b1_bats,
    gpb.avg, --as b1_avg,
    gpb.hr, --as b1_hr,
    gpb.rbi, --as b1_rbi,
    gpb.bat_order, --as b1_bat_order,
    gpb.game_position, --as b1_game_position,
    --ids to verify p1/p0
    p.pitch_id, --as p1_pitch_id,
    lag(p.pitch_id) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as p0_pitch_id,
    p.id, --as p1_pitch_seqno,
    lag(p.id) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as p0_pitch_seqno,
    --p0 pitch data
    lag(ab.o) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as p0_at_bat_o,
    lag(p.des) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as p0_pitch_des,
    lag(i.num) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as p0_inning,
    lag(p.type) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as result_type,
    lag(p.type_simple) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as result_type_simple,
    lag(p.x) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as x,
    lag(p.y) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as y,
    lag(p.start_speed) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as start_speed,
    lag(p.end_speed) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as end_speed,
    lag(p.sz_top) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as sz_top,
    lag(p.sz_bot) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as sz_bot,
    lag(p.pfx_x) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as pfx_x,
    lag(p.pfx_z) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as pfx_z,
    lag(p.px) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as px,
    lag(p.pz) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as pz,
    lag(p.x0) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as x0,
    lag(p.y0) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as y0,
    lag(p.z0) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as z0,
    lag(p.vx0) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as vx0,
    lag(p.vy0) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as vy0,
    lag(p.vz0) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as vz0,
    lag(p.ax) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as ax,
    lag(p.ay) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as ay,
    lag(p.az) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as az,
    lag(p.break_y) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as break_y,
    lag(p.break_angle) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as break_angle,
    lag(p.break_length) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as break_length,
    lag(p.pitch_type) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as p0_pitch_type,
    lag(p.type_confidence) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as type_confidence,
    lag(p.zone) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as zone,
    lag(p.nasty) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as nasty,
    lag(p.spin_dir) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as spin_dir,
    lag(p.spin_rate) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as spin_rate,
    lag(p.outcome) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as outcome,
    --situational data
    i.num, --as inning,
    c.pitch_count_atbat,
    c.pitch_count_team,
    sum(c.p0_is_ball) over (partition by p.at_bat_id order by p.id) as balls,
    sum(c.p0_is_strike) over (partition by p.at_bat_id order by p.id) as strikes,
    --TODO
    0, --as outs,
	false, --as is_runner_on_first,
	false, --as is_runner_on_second,
	false, --as is_runner_on_third,
    0, --as runs_pitcher_team,
    0, --as runs_batter_team,
    --keys
    p.game_id,
    ab.inning_id,
	ab.half_inning_id,
	ab.at_bat_id,
    g.gid,
    --predicted value
    p.pitch_type --as p1_pitch_type
from pitch p
    join game g on p.game_id = g.game_id
    join at_bat ab on p.at_bat_id = ab.at_bat_id
    join inning i on p.inning_id = i.inning_id
    join counts c on p.pitch_id = c.pitch_id
    join game_player gpp on ab.pitcher = gpp.id
        and g.gid = gpp.gid
    join game_player gpb on ab.batter = gpb.id
        and g.gid = gpb.gid
where substring(g.gid, 1, 8) = 'gid_2016';


--2017
with counts as (
    select p.pitch_id,
        --ab.pitcher, p.id, p.tfs_zulu, p.at_bat_id, gpp.team_id, p.des,--for debug
        row_number() over (partition by p.game_id, p.at_bat_id order by p.id asc) as pitch_count_atbat,
        row_number() over (partition by p.game_id, gpp.team_id order by p.id asc) as pitch_count_team,
        --case when p.type_simple = 'B' then 1 else 0 end as is_ball,
        --case when p.type_simple = 'S' then 1 else 0 end as is_strike
        case when lag(p.type_simple) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id) = 'B' then 1 else 0 end as p0_is_ball,
        case when lag(p.type_simple) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id) = 'S' then 1 else 0 end as p0_is_strike

    from pitch p
        join game g on p.game_id = g.game_id
        join at_bat ab on p.at_bat_id = ab.at_bat_id
        join game_player gpp on ab.pitcher = gpp.id
            and g.gid = gpp.gid
--     where p.game_id = '8aeda4c6-03a8-4d98-b18c-e2f808c0d0f3'
    where substring(g.gid, 1, 8) = 'gid_2017'
          --date_part('year', p.tfs_zulu) = 2018
)
insert into pitch_data_stage (season, pitcher_id, team_id, team_abbrev, era, wins,
                             losses, throws, b1_id, b1_team_id, b1_stand, b1_height,
                             b1_bats, b1_avg, b1_hr, b1_rbi, b1_bat_order, b1_game_position,
                             p1_pitch_id, p0_pitch_id, p1_pitch_seqno, p0_pitch_seqno, p0_at_bat_o, p0_pitch_des,
                             p0_inning, result_type, result_type_simple, x, y, start_speed,
                             end_speed, sz_top, sz_bot, pfx_x, pfx_z, px, pz, x0, y0, z0,
                             vx0, vy0, vz0, ax, ay, az, break_y, break_angle, break_length,
                             p0_pitch_type, type_confidence, zone, nasty, spin_dir, spin_rate,
                             outcome, inning, pitch_count_atbat, pitch_count_team, balls, strikes,
                             outs, is_runner_on_first, is_runner_on_second, is_runner_on_third,
                             runs_pitcher_team, runs_batter_team, game_id, inning_id,
                             half_inning_id, at_bat_id, gid, p1_pitch_type)
select
    coalesce(date_part('year', p.tfs_zulu), 2017), --as season,
    --p1 pitcher data
    cast(gpp.id as integer), --as pitcher_id, --game_player.id
    gpp.team_id, --as team_id,
    gpp.team_abbrev, --as team_abbrev,
    coalesce(gpp.era, 4.5),--as era,
    gpp.wins, --as wins,
    gpp.losses, --as losses,
    ab.p_throws, --as throws,

    --b1 data
    cast(gpb.id as integer), --as b1_id, --game_player.id
    gpb.team_id, --as b1_team_id,
    ab.stand, --as b1_stand,
    ab.b_height, --as b1_height,
    gpb.bats, --as b1_bats,
    gpb.avg, --as b1_avg,
    gpb.hr, --as b1_hr,
    gpb.rbi, --as b1_rbi,
    gpb.bat_order, --as b1_bat_order,
    gpb.game_position, --as b1_game_position,

    --ids to verify p1/p0
    p.pitch_id, --as p1_pitch_id,
    lag(p.pitch_id) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as p0_pitch_id,
    p.id, --as p1_pitch_seqno,
    lag(p.id) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as p0_pitch_seqno,

    --p0 pitch data
    lag(ab.o) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as p0_at_bat_o,
    lag(p.des) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as p0_pitch_des,
    lag(i.num) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as p0_inning,
    lag(p.type) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as result_type,
    lag(p.type_simple) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as result_type_simple,
--     lag(p.tfs_zulu) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as tfs_zulu,
    lag(p.x) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as x,
    lag(p.y) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as y,
    lag(p.start_speed) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as start_speed,
    lag(p.end_speed) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as end_speed,
    lag(p.sz_top) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as sz_top,
    lag(p.sz_bot) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as sz_bot,
    lag(p.pfx_x) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as pfx_x,
    lag(p.pfx_z) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as pfx_z,
    lag(p.px) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as px,
    lag(p.pz) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as pz,
    lag(p.x0) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as x0,
    lag(p.y0) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as y0,
    lag(p.z0) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as z0,
    lag(p.vx0) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as vx0,
    lag(p.vy0) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as vy0,
    lag(p.vz0) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as vz0,
    lag(p.ax) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as ax,
    lag(p.ay) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as ay,
    lag(p.az) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as az,
    lag(p.break_y) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as break_y,
    lag(p.break_angle) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as break_angle,
    lag(p.break_length) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as break_length,
    lag(p.pitch_type) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as p0_pitch_type,
    lag(p.type_confidence) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as type_confidence,
    lag(p.zone) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as zone,
    lag(p.nasty) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as nasty,
    lag(p.spin_dir) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as spin_dir,
    lag(p.spin_rate) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as spin_rate,
    lag(p.outcome) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as outcome,

    --situational data
    i.num, --as inning,
    c.pitch_count_atbat,
    c.pitch_count_team,
    --sum(c.is_ball) over (partition by p.at_bat_id order by p.id), --as balls,
    --sum(c.is_strike) over (partition by p.at_bat_id order by p.id), --as strikes,
    --sum(c.p0_is_ball) over (partition by p.at_bat_id order by p.id)::varchar || '-' || sum(c.p0_is_strike) over (partition by p.at_bat_id order by p.id)::varchar as count,
    sum(c.p0_is_ball) over (partition by p.at_bat_id order by p.id) as balls,
    sum(c.p0_is_strike) over (partition by p.at_bat_id order by p.id) as strikes,

    --TODO
    0, --as outs,
	false, --as is_runner_on_first,
	false, --as is_runner_on_second,
	false, --as is_runner_on_third,
    0, --as runs_pitcher_team,
    0, --as runs_batter_team,

    --keys
    p.game_id,
    ab.inning_id,
	ab.half_inning_id,
	ab.at_bat_id,
    g.gid,

    --predicted value
    p.pitch_type --as p1_pitch_type

from pitch p
    join game g on p.game_id = g.game_id
    join at_bat ab on p.at_bat_id = ab.at_bat_id
    join inning i on p.inning_id = i.inning_id
    join counts c on p.pitch_id = c.pitch_id
    join game_player gpp on ab.pitcher = gpp.id
        and g.gid = gpp.gid
    join game_player gpb on ab.batter = gpb.id
        and g.gid = gpb.gid
where substring(g.gid, 1, 8) = 'gid_2017'; -- ('gid_2018_05', 'gid_2018_06', 'gid_2018_07', 'gid_2018_08', 'gid_2018_09', 'gid_2018_10');
    --and p.game_id <> '8aeda4c6-03a8-4d98-b18c-e2f808c0d0f3';
--order by p.game_id, p.id

--2018
with counts as (
    select p.pitch_id,
        --ab.pitcher, p.id, p.tfs_zulu, p.at_bat_id, gpp.team_id, p.des,--for debug
        row_number() over (partition by p.game_id, p.at_bat_id order by p.id asc) as pitch_count_atbat,
        row_number() over (partition by p.game_id, gpp.team_id order by p.id asc) as pitch_count_team,
        --case when p.type_simple = 'B' then 1 else 0 end as is_ball,
        --case when p.type_simple = 'S' then 1 else 0 end as is_strike
        case when lag(p.type_simple) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id) = 'B' then 1 else 0 end as p0_is_ball,
        case when lag(p.type_simple) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id) = 'S' then 1 else 0 end as p0_is_strike

    from pitch p
        join game g on p.game_id = g.game_id
        join at_bat ab on p.at_bat_id = ab.at_bat_id
        join game_player gpp on ab.pitcher = gpp.id
            and g.gid = gpp.gid
--     where p.game_id = '8aeda4c6-03a8-4d98-b18c-e2f808c0d0f3'
    where substring(g.gid, 1, 8) = 'gid_2018'
          --date_part('year', p.tfs_zulu) = 2018
)
insert into pitch_data_stage (season, pitcher_id, team_id, team_abbrev, era, wins,
                             losses, throws, b1_id, b1_team_id, b1_stand, b1_height,
                             b1_bats, b1_avg, b1_hr, b1_rbi, b1_bat_order, b1_game_position,
                             p1_pitch_id, p0_pitch_id, p1_pitch_seqno, p0_pitch_seqno, p0_at_bat_o, p0_pitch_des,
                             p0_inning, result_type, result_type_simple, x, y, start_speed,
                             end_speed, sz_top, sz_bot, pfx_x, pfx_z, px, pz, x0, y0, z0,
                             vx0, vy0, vz0, ax, ay, az, break_y, break_angle, break_length,
                             p0_pitch_type, type_confidence, zone, nasty, spin_dir, spin_rate,
                             outcome, inning, pitch_count_atbat, pitch_count_team, balls, strikes,
                             outs, is_runner_on_first, is_runner_on_second, is_runner_on_third,
                             runs_pitcher_team, runs_batter_team, game_id, inning_id,
                             half_inning_id, at_bat_id, gid, p1_pitch_type)
select
    coalesce(date_part('year', p.tfs_zulu), 2018), --as season,
    --p1 pitcher data
    cast(gpp.id as integer), --as pitcher_id, --game_player.id
    gpp.team_id, --as team_id,
    gpp.team_abbrev, --as team_abbrev,
    coalesce(gpp.era, 4.5),--as era,
    gpp.wins, --as wins,
    gpp.losses, --as losses,
    ab.p_throws, --as throws,

    --b1 data
    cast(gpb.id as integer), --as b1_id, --game_player.id
    gpb.team_id, --as b1_team_id,
    ab.stand, --as b1_stand,
    ab.b_height, --as b1_height,
    gpb.bats, --as b1_bats,
    gpb.avg, --as b1_avg,
    gpb.hr, --as b1_hr,
    gpb.rbi, --as b1_rbi,
    gpb.bat_order, --as b1_bat_order,
    gpb.game_position, --as b1_game_position,

    --ids to verify p1/p0
    p.pitch_id, --as p1_pitch_id,
    lag(p.pitch_id) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as p0_pitch_id,
    p.id, --as p1_pitch_seqno,
    lag(p.id) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as p0_pitch_seqno,

    --p0 pitch data
    lag(ab.o) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as p0_at_bat_o,
    lag(p.des) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as p0_pitch_des,
    lag(i.num) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as p0_inning,
    lag(p.type) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as result_type,
    lag(p.type_simple) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as result_type_simple,
--     lag(p.tfs_zulu) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as tfs_zulu,
    lag(p.x) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as x,
    lag(p.y) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as y,
    lag(p.start_speed) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as start_speed,
    lag(p.end_speed) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as end_speed,
    lag(p.sz_top) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as sz_top,
    lag(p.sz_bot) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as sz_bot,
    lag(p.pfx_x) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as pfx_x,
    lag(p.pfx_z) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as pfx_z,
    lag(p.px) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as px,
    lag(p.pz) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as pz,
    lag(p.x0) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as x0,
    lag(p.y0) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as y0,
    lag(p.z0) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as z0,
    lag(p.vx0) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as vx0,
    lag(p.vy0) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as vy0,
    lag(p.vz0) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as vz0,
    lag(p.ax) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as ax,
    lag(p.ay) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as ay,
    lag(p.az) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as az,
    lag(p.break_y) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as break_y,
    lag(p.break_angle) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as break_angle,
    lag(p.break_length) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as break_length,
    lag(p.pitch_type) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as p0_pitch_type,
    lag(p.type_confidence) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as type_confidence,
    lag(p.zone) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as zone,
    lag(p.nasty) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as nasty,
    lag(p.spin_dir) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as spin_dir,
    lag(p.spin_rate) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as spin_rate,
    lag(p.outcome) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as outcome,

    --situational data
    i.num, --as inning,
    c.pitch_count_atbat,
    c.pitch_count_team,
    --sum(c.is_ball) over (partition by p.at_bat_id order by p.id), --as balls,
    --sum(c.is_strike) over (partition by p.at_bat_id order by p.id), --as strikes,
    --sum(c.p0_is_ball) over (partition by p.at_bat_id order by p.id)::varchar || '-' || sum(c.p0_is_strike) over (partition by p.at_bat_id order by p.id)::varchar as count,
    sum(c.p0_is_ball) over (partition by p.at_bat_id order by p.id) as balls,
    sum(c.p0_is_strike) over (partition by p.at_bat_id order by p.id) as strikes,

    --TODO
    0, --as outs,
	false, --as is_runner_on_first,
	false, --as is_runner_on_second,
	false, --as is_runner_on_third,
    0, --as runs_pitcher_team,
    0, --as runs_batter_team,

    --keys
    p.game_id,
    ab.inning_id,
	ab.half_inning_id,
	ab.at_bat_id,
    g.gid,

    --predicted value
    p.pitch_type --as p1_pitch_type

from pitch p
    join game g on p.game_id = g.game_id
    join at_bat ab on p.at_bat_id = ab.at_bat_id
    join inning i on p.inning_id = i.inning_id
    join counts c on p.pitch_id = c.pitch_id
    join game_player gpp on ab.pitcher = gpp.id
        and g.gid = gpp.gid
    join game_player gpb on ab.batter = gpb.id
        and g.gid = gpb.gid
where substring(g.gid, 1, 8) = 'gid_2018'; -- ('gid_2018_05', 'gid_2018_06', 'gid_2018_07', 'gid_2018_08', 'gid_2018_09', 'gid_2018_10');
    --and p.game_id <> '8aeda4c6-03a8-4d98-b18c-e2f808c0d0f3';
--order by p.game_id, p.id

--2019
with counts as (
    select p.pitch_id,
        --ab.pitcher, p.id, p.tfs_zulu, p.at_bat_id, gpp.team_id, p.des,--for debug
        row_number() over (partition by p.game_id, p.at_bat_id order by p.id asc) as pitch_count_atbat,
        row_number() over (partition by p.game_id, gpp.team_id order by p.id asc) as pitch_count_team,
        --case when p.type_simple = 'B' then 1 else 0 end as is_ball,
        --case when p.type_simple = 'S' then 1 else 0 end as is_strike
        case when lag(p.type_simple) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id) = 'B' then 1 else 0 end as p0_is_ball,
        case when lag(p.type_simple) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id) = 'S' then 1 else 0 end as p0_is_strike

    from pitch p
        join game g on p.game_id = g.game_id
        join at_bat ab on p.at_bat_id = ab.at_bat_id
        join game_player gpp on ab.pitcher = gpp.id
            and g.gid = gpp.gid
--     where p.game_id = '8aeda4c6-03a8-4d98-b18c-e2f808c0d0f3'
    where substring(g.gid, 1, 8) = 'gid_2019'
          --date_part('year', p.tfs_zulu) = 2018
)
insert into pitch_data_stage (season, pitcher_id, team_id, team_abbrev, era, wins,
                             losses, throws, b1_id, b1_team_id, b1_stand, b1_height,
                             b1_bats, b1_avg, b1_hr, b1_rbi, b1_bat_order, b1_game_position,
                             p1_pitch_id, p0_pitch_id, p1_pitch_seqno, p0_pitch_seqno, p0_at_bat_o, p0_pitch_des,
                             p0_inning, result_type, result_type_simple, x, y, start_speed,
                             end_speed, sz_top, sz_bot, pfx_x, pfx_z, px, pz, x0, y0, z0,
                             vx0, vy0, vz0, ax, ay, az, break_y, break_angle, break_length,
                             p0_pitch_type, type_confidence, zone, nasty, spin_dir, spin_rate,
                             outcome, inning, pitch_count_atbat, pitch_count_team, balls, strikes,
                             outs, is_runner_on_first, is_runner_on_second, is_runner_on_third,
                             runs_pitcher_team, runs_batter_team, game_id, inning_id,
                             half_inning_id, at_bat_id, gid, p1_pitch_type)
select
    coalesce(date_part('year', p.tfs_zulu), 2019), --as season,
    --p1 pitcher data
    cast(gpp.id as integer), --as pitcher_id, --game_player.id
    gpp.team_id, --as team_id,
    gpp.team_abbrev, --as team_abbrev,
    coalesce(gpp.era, 4.5),--as era,
    gpp.wins, --as wins,
    gpp.losses, --as losses,
    ab.p_throws, --as throws,

    --b1 data
    cast(gpb.id as integer), --as b1_id, --game_player.id
    gpb.team_id, --as b1_team_id,
    ab.stand, --as b1_stand,
    ab.b_height, --as b1_height,
    gpb.bats, --as b1_bats,
    gpb.avg, --as b1_avg,
    gpb.hr, --as b1_hr,
    gpb.rbi, --as b1_rbi,
    gpb.bat_order, --as b1_bat_order,
    gpb.game_position, --as b1_game_position,

    --ids to verify p1/p0
    p.pitch_id, --as p1_pitch_id,
    lag(p.pitch_id) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as p0_pitch_id,
    p.id, --as p1_pitch_seqno,
    lag(p.id) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as p0_pitch_seqno,

    --p0 pitch data
    lag(ab.o) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as p0_at_bat_o,
    lag(p.des) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as p0_pitch_des,
    lag(i.num) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as p0_inning,
    lag(p.type) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as result_type,
    lag(p.type_simple) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as result_type_simple,
--     lag(p.tfs_zulu) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as tfs_zulu,
    lag(p.x) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as x,
    lag(p.y) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as y,
    lag(p.start_speed) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as start_speed,
    lag(p.end_speed) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as end_speed,
    lag(p.sz_top) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as sz_top,
    lag(p.sz_bot) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as sz_bot,
    lag(p.pfx_x) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as pfx_x,
    lag(p.pfx_z) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as pfx_z,
    lag(p.px) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as px,
    lag(p.pz) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as pz,
    lag(p.x0) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as x0,
    lag(p.y0) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as y0,
    lag(p.z0) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as z0,
    lag(p.vx0) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as vx0,
    lag(p.vy0) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as vy0,
    lag(p.vz0) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as vz0,
    lag(p.ax) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as ax,
    lag(p.ay) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as ay,
    lag(p.az) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as az,
    lag(p.break_y) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as break_y,
    lag(p.break_angle) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as break_angle,
    lag(p.break_length) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as break_length,
    lag(p.pitch_type) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as p0_pitch_type,
    lag(p.type_confidence) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as type_confidence,
    lag(p.zone) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as zone,
    lag(p.nasty) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as nasty,
    lag(p.spin_dir) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as spin_dir,
    lag(p.spin_rate) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as spin_rate,
    lag(p.outcome) over (partition by p.game_id, gpp.team_id, ab.pitcher order by p.id asc), --as outcome,

    --situational data
    i.num, --as inning,
    c.pitch_count_atbat,
    c.pitch_count_team,
    --sum(c.is_ball) over (partition by p.at_bat_id order by p.id), --as balls,
    --sum(c.is_strike) over (partition by p.at_bat_id order by p.id), --as strikes,
    --sum(c.p0_is_ball) over (partition by p.at_bat_id order by p.id)::varchar || '-' || sum(c.p0_is_strike) over (partition by p.at_bat_id order by p.id)::varchar as count,
    sum(c.p0_is_ball) over (partition by p.at_bat_id order by p.id) as balls,
    sum(c.p0_is_strike) over (partition by p.at_bat_id order by p.id) as strikes,

    --TODO
    0, --as outs,
	false, --as is_runner_on_first,
	false, --as is_runner_on_second,
	false, --as is_runner_on_third,
    0, --as runs_pitcher_team,
    0, --as runs_batter_team,

    --keys
    p.game_id,
    ab.inning_id,
	ab.half_inning_id,
	ab.at_bat_id,
    g.gid,

    --predicted value
    p.pitch_type --as p1_pitch_type

from pitch p
    join game g on p.game_id = g.game_id
    join at_bat ab on p.at_bat_id = ab.at_bat_id
    join inning i on p.inning_id = i.inning_id
    join counts c on p.pitch_id = c.pitch_id
    join game_player gpp on ab.pitcher = gpp.id
        and g.gid = gpp.gid
    join game_player gpb on ab.batter = gpb.id
        and g.gid = gpb.gid
where substring(g.gid, 1, 8) = 'gid_2019'; -- ('gid_2018_05', 'gid_2018_06', 'gid_2018_07', 'gid_2018_08', 'gid_2018_09', 'gid_2018_10');
    --and p.game_id <> '8aeda4c6-03a8-4d98-b18c-e2f808c0d0f3';
--order by p.game_id, p.id

explain
update pitch_data
set p0_at_bat_o = ps.p0_at_bat_o
from pitch_data_stage ps join pitch_data pd on ps.p1_pitch_id = pd.p1_pitch_id
where coalesce(ps.p0_at_bat_o, 0) <> 0;