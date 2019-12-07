## Queries used to get the data

# pitcher data summary from final game listed in game_player table
PITCHER_DATA_SUMMARY_QUERY='''
with pitcher_finalgames as (
    select gp.id as player_id, max(gp.gid) as gid_final, count(*) as games_played
    from game_player gp
    where gp.position = 'P'
        and gp.game_position = 'P'
        and substring(gp.gid, 1, 8) = 'gid_2018' --2018 season only
    group by gp.id
)
select pf.player_id, gp.boxname, gp.rl, gp.bats, gp.status, gp.team_abbrev,
    pf.games_played, gp.avg, gp.hr, gp.rbi, gp.wins, gp.losses, gp.era,
    pf.gid_final
from pitcher_finalgames pf
    join game_player gp on pf.player_id = gp.id
        and pf.gid_final = gp.gid
    join game g on pf.gid_final = g.gid
;
'''

# list of pitches in a game
# pass in the gid into the query 
PITCHES_IN_GAME_QUERY='''
select p.tfs_zulu, p.id, p.type, p.pitch_type, p.type_confidence, p.start_speed, p.end_speed, p.spin_rate, p.spin_dir,  
       p.break_angle, p.break_length, p.break_y, p.x, p.y, p.x0, p.y0, p.z0, p.vx0, p.vy0, p.vz0, p.ax, p.ay, p.az, 
       p.px, p.pz, p.pfx_x, p.pfx_z, p.sz_top, p.sz_bot, p.zone, p.nasty, p.outcome as outcome_pitch, p.game_id,
       ab.pitcher, ab.batter, ab.stand, ab.b_height, ab.b, ab.s, ab.o, ab.outcome as outcome_at_bat, ab.score, 
       ab.home_team_runs, ab.away_team_runs, ab.num as num_at_bat, ab.runner_ids, i.num as num_inning,
       gp.gid, gp.id as player_id, gp.boxname, gp.rl, gp.bats, gp.team_abbrev
from pitch p
    join game g on p.game_id = g.game_id
    join at_bat ab on p.at_bat_id = ab.at_bat_id
    join inning i on p.inning_id = i.inning_id
    join game_player gp on ab.pitcher = gp.id
        and g.gid = gp.gid
where g.gid = '{gid}' 
order by p.tfs_zulu;
'''

# all pitches for a specific pitcher
# pass in the pitcher id
PITCHES_BY_PITCHER_ID_QUERY='''
-- all pitches for a specific pitcher
select p.tfs_zulu, p.id, p.type, p.pitch_type, p.type_confidence, p.start_speed, p.end_speed, p.spin_rate, p.spin_dir,  
       p.break_angle, p.break_length, p.break_y, p.x, p.y, p.x0, p.y0, p.z0, p.vx0, p.vy0, p.vz0, p.ax, p.ay, p.az, 
       p.px, p.pz, p.pfx_x, p.pfx_z, p.sz_top, p.sz_bot, p.zone, p.nasty, p.outcome as outcome_pitch, p.game_id,
       ab.pitcher, ab.batter, ab.stand, ab.b_height, ab.b, ab.s, ab.o, ab.outcome as outcome_at_bat, ab.score, 
       ab.home_team_runs, ab.away_team_runs, ab.num as num_at_bat, ab.runner_ids, i.num as num_inning,      
       gp.gid, gp.id as player_id, gp.boxname, gp.rl, gp.bats, gp.era, gp.wins, gp.losses, gp.team_abbrev
from pitch p
    join game g on p.game_id = g.game_id
    join at_bat ab on p.at_bat_id = ab.at_bat_id
    join inning i on p.inning_id = i.inning_id
    join game_player gp on ab.pitcher = gp.id
        and g.gid = gp.gid
where gp.id = '{pitcher_id}'
order by gp.gid, p.tfs_zulu;
'''


# all pitches for a specific pitcher with batter info
# 
PITCHES_WITH_BATTER_INFO_QUERY='''
select pitcher.gid, p.tfs_zulu, p.id, p.type, p.pitch_type, p.type_confidence, p.start_speed, p.end_speed, p.spin_rate, p.spin_dir,  
       p.break_angle, p.break_length, p.break_y, p.x, p.y, p.x0, p.y0, p.z0, p.vx0, p.vy0, p.vz0, p.ax, p.ay, p.az, 
       p.px, p.pz, p.pfx_x, p.pfx_z, p.sz_top, p.sz_bot, p.zone, p.nasty, p.outcome as outcome_pitch, p.game_id,
       ab.pitcher, ab.batter, ab.stand, ab.b_height, ab.b, ab.s, ab.o, ab.outcome as outcome_at_bat, ab.score, 
       ab.home_team_runs, ab.away_team_runs, ab.num as num_at_bat, ab.runner_ids, i.num as num_inning,      
       pitcher.id as pitcher_id, pitcher.boxname as pitcher_boxname, pitcher.rl as pitcher_rl, pitcher.bats as pitcher_bats, 
       pitcher.era as pitcher_era, pitcher.wins as pitcher_wins, pitcher.losses as pitcher_losses, 
       batter.id as batter_id, batter.boxname as batter_boxname, batter.rl as batter_rl, batter.bats as batter_bats, 
       batter.avg as batter_avg, batter.hr as batter_hr, batter.rbi as batter_rbi, batter.position as batter_position,  batter.bat_order
from pitch p
    join game g on p.game_id = g.game_id
    join at_bat ab on p.at_bat_id = ab.at_bat_id
    join inning i on p.inning_id = i.inning_id
    join game_player pitcher on ab.pitcher = pitcher.id
        and g.gid = pitcher.gid
    join game_player batter on ab.batter = batter.id
        and g.gid = batter.gid
where pitcher.id = '{pitcher_id}'
order by pitcher.gid, p.tfs_zulu;
'''

#TODO: Remove limit
PITCH_DATA_QUERY='''
select * from pitch_data LIMIT 10000;
'''

PITCH_DATA_BY_PITCHER_QUERY='''
select * from pitch_data 
where pitch_data.pitcher_id = '{pitcher_id}' 
order by pitch_data.gid, pitch_data.p1_pitch_seqno;
'''
