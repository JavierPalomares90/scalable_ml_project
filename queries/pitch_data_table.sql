create table if not exists pitch_data
(
    pitch_data_id 		serial not null
        constraint pitch_data_key primary key,
    season 			integer not null,
    pitch_id 			varchar(36),
    game_id 			varchar(36),
    inning_id 			varchar(36),
    half_inning_id 		varchar(36),
    at_bat_id 			varchar(36),
    --p0 pitch data
    result_type 		varchar(10),
    tfs_zulu 			timestamp,
    x 				numeric,
    y 				numeric,
    start_speed 		numeric,
    end_speed 			numeric,
    sz_top 			numeric,
    sz_bot 			numeric,
    pfx_x 			numeric,
    pfx_z 			numeric,
    px 				numeric,
    pz 				numeric,
    x0 				numeric,
    y0 				numeric,
    z0 				numeric,
    vx0 			numeric,
    vy0 			numeric,
    vz0 			numeric,
    ax 				numeric,
    ay 				numeric,
    az 				numeric,
    break_y 			numeric,
    break_angle 		numeric,
    break_length 		numeric,
    p0_pitch_type 		varchar(10),
    type_confidence 		numeric,
    zone 			integer,
    nasty 			integer,
    spin_dir 			numeric,
    spin_rate 			numeric,
    outcome 			numeric,
    --p0 pitcher data
    p0_id 			integer, --game_player.id
    p0_era 			numeric default 0,
    p0_wins 			integer default 0,
    p0_losses 			integer default 0,
    --current situational data
    pitch_count_atbat 		integer default 0,
    pitch_count_team 		integer default 0,
    balls 			integer default 0,
    strikes 			integer default 0,
    outs 			integer default 0,
    is_runner_on_first 		bit default 0,
    is_runner_on_second 	bit default 0,
    is_runner_on_third 		bit default 0,
    runs_pitcher_team 		integer default 0,
    runs_batter_team 		integer default 0,
    inning 			integer,
    --p1 pitcher data
    p1_id 			integer, --game_player.id
    p1_era 			numeric default 0,
    p1_wins 			integer default 0,
    p1_losses 			integer default 0,
    --b1 data
    b1_id 			integer, --game_player.id
    b1_stand 			varchar(1),
    b1_height 			integer,
    b1_bats 			text,
    b1_avg 			numeric,
    b1_hr 			integer,
    b1_rbi 			integer,
    b1_bat_order 		integer,
    b1_game_position 		text,
    --predicted value
    p1_pitch_type 		varchar(10)
);

alter table pitch_data owner to postgres;
