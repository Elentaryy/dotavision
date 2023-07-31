CREATE SCHEMA IF NOT EXISTS dota_dds;

CREATE SCHEMA IF NOT EXISTS dota_ods;

CREATE TABLE IF NOT EXISTS dota_dds.pro_series (
    series_id SERIAL PRIMARY KEY,
    team1_id int,
    team2_id int,
    series_type int,
    team1_score int,
    team2_score int,
    is_live boolean,
    raw_dt date DEFAULT CURRENT_DATE
);

CREATE TABLE IF NOT EXISTS dota_dds.pro_matches (
    match_id bigint PRIMARY KEY NOT NULL,
    series_id int,
    match_data json,
    is_live boolean,
    raw_dt date DEFAULT CURRENT_DATE
);

CREATE TABLE IF NOT EXISTS dota_dds.pro_matches_statuses (
    status_id SERIAL PRIMARY KEY,
    match_id bigint NOT NULL,
    match_data json,
    ingame_dttm int NOT NULL 
)

CREATE TABLE IF NOT EXISTS dota_dds.public_matches (
    match_id bigint PRIMARY KEY NOT NULL,
    start_time bigint,
    duration int,
    game_mode int,
    avg_rank_tier int,
    radiant_team int[],
    dire_team int[],
    radiant_win boolean NOT NULL,
    raw_dt date DEFAULT CURRENT_DATE
);

CREATE TABLE IF NOT EXISTS dota_ods.predictions (
    match_id bigint NOT NULL,
    model text NOT NULL,
    prediction int, 
    probability float,
    result int DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS dota_dds.leagues (
	league_id int PRIMARY KEY NOT NULL,
	league_name text NOT NULL,
	tier text NOT NULL,
	allowed boolean DEFAULT false
);





