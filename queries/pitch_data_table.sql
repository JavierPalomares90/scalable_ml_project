drop table pitch_data_stage;

create table pitch_data_stage
(
    pitch_data_id serial not null
        constraint pitch_data_key2
            primary key,
    season integer not null, --year of pitch.tfs_zulu
    --pitcher data
    pitcher_id integer, --game_player.id casted as int
	team_id text,
	team_abbrev text,
	era numeric,
	wins integer,
	losses integer,
	throws varchar(1),

    --batter
    b1_id integer, --game_player.id casted as int
	b1_team_id text,
	--TODO add team abbrev? Do we need batter team info?
	b1_stand varchar(1),
	b1_height integer,
	b1_bats text,
	b1_avg numeric,
	b1_hr integer,
	b1_rbi integer,
	b1_bat_order integer,
	b1_game_position text,

    --ids to verify p1/p0
    p1_pitch_id varchar(36),
	p0_pitch_id varchar(36),
	p1_pitch_seqno integer, --pitch.id
	p0_pitch_seqno integer,

	--p0 pitch data
	--the associated data for p0 of at_bat.o, and pitch.des?
    p0_at_bat_o integer,
    p0_pitch_des text,
	p0_inning integer,
	result_type varchar(10),
	result_type_simple varchar(1),
	x numeric,
	y numeric,
	start_speed numeric,
	end_speed numeric,
	sz_top numeric,
	sz_bot numeric,
	pfx_x numeric,
	pfx_z numeric,
	px numeric,
	pz numeric,
	x0 numeric,
	y0 numeric,
	z0 numeric,
	vx0 numeric,
	vy0 numeric,
	vz0 numeric,
	ax numeric,
	ay numeric,
	az numeric,
	break_y numeric,
	break_angle numeric,
	break_length numeric,
	p0_pitch_type varchar(10),
	type_confidence numeric,
	zone integer,
	nasty integer,
	spin_dir numeric,
	spin_rate numeric,
	outcome numeric,

	--current situational data
	inning integer,
	pitch_count_atbat integer,
	pitch_count_team integer,
	balls integer,
	strikes integer,
	outs integer,
	is_runner_on_first boolean default false,
	is_runner_on_second boolean default false,
	is_runner_on_third boolean default false,
    runs_pitcher_team integer,
    runs_batter_team integer,

    --keys
    game_id varchar(36),
    inning_id varchar(36),
	half_inning_id varchar(36),
	at_bat_id varchar(36),
    gid text,

    --predicted value
    p1_pitch_type varchar(10)
);

alter table pitch_data_stage owner to postgres;
