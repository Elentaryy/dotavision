CREATE SCHEMA dota_dds;

CREATE SCHEMA dota_ods;

CREATE TABLE dota_dds.pro_matches (
    match_id SERIAL PRIMARY KEY,
    team1_id int,
    team2_id int,
    series_type int,
    team1_score int,
    team2_score int,
    is_live boolean
);

CREATE TABLE dota_dds.pro_games (
    game_id bigint PRIMARY KEY NOT NULL,
    match_id int NOT NULL,
    game_data json,
    is_live boolean
)
