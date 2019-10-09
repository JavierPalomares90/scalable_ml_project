-- pitcher data summary from final game listed in game_player table
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

--list of pitches in a game
select p.tfs_zulu, p.type, p.pitch_type, p.start_speed, p.end_speed, p.outcome as outcome_pitch, p.des, p.game_id,
       ab.pitcher, ab.batter, ab.outcome as outcome_at_bat, ab.event, ab.des,
       gp.gid, gp.id as player_id, gp.boxname, gp.rl, gp.bats, gp.team_abbrev
from pitch p
    join game g on p.game_id = g.game_id
    join at_bat ab on p.at_bat_id = ab.at_bat_id
    join game_player gp on ab.pitcher = gp.id
        and g.gid = gp.gid
where g.gid = 'gid_2018_03_29_chnmlb_miamlb_1'
order by p.tfs_zulu;

-- all pitches for a specific pitcher
select p.tfs_zulu, p.type, p.pitch_type, p.start_speed, p.end_speed, p.outcome as outcome_pitch, p.des, p.game_id,
       ab.pitcher, ab.batter, ab.outcome as outcome_at_bat, ab.event, ab.des,
       gp.gid, gp.id as player_id, gp.boxname, gp.rl, gp.bats, gp.team_abbrev
from pitch p
    join game g on p.game_id = g.game_id
    join at_bat ab on p.at_bat_id = ab.at_bat_id
    join game_player gp on ab.pitcher = gp.id
        and g.gid = gp.gid
where gp.id = '570632'
order by gp.gid, p.tfs_zulu;

-- all pitches for a specific pitcher with batter info
select gp.gid, p.tfs_zulu, p.type, p.pitch_type, p.start_speed, p.end_speed, p.outcome as outcome_pitch, p.des,
       ab.outcome as outcome_at_bat, ab.event, ab.des,
       gp.id as pitcher_id, gp.boxname as pitcher_boxname, gp.rl as pitcher_rl, gp.bats as pitcher_bats, gp.team_abbrev as pitcher_team,
       gp2.id as batter_id, gp2.boxname as batter_boxname, gp2.rl as batter_rl, gp2.bats as batter_bats, gp2.team_abbrev as batter_team, gp2.*
from pitch p
    join game g on p.game_id = g.game_id
    join at_bat ab on p.at_bat_id = ab.at_bat_id
    join game_player gp on ab.pitcher = gp.id
        and g.gid = gp.gid
    join game_player gp2 on ab.batter = gp2.id
        and g.gid = gp2.gid
where gp.id = '570632'
order by gp.gid, p.tfs_zulu;

