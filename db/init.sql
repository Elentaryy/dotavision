CREATE SCHEMA dota_dds;

CREATE SCHEMA dota_ods;

CREATE TABLE dota_dds.pro_series (
    series_id SERIAL PRIMARY KEY,
    team1_id int,
    team2_id int,
    series_type int,
    team1_score int,
    team2_score int,
    is_live boolean,
    created_at date DEFAULT CURRENT_DATE
);

CREATE TABLE dota_dds.pro_matches (
    match_id bigint PRIMARY KEY NOT NULL,
    series_id int NOT NULL,
    match_data json,
    is_live boolean,
    created_at date DEFAULT CURRENT_DATE
);

CREATE TABLE dota_dds.pro_matches_statuses (
    status_id SERIAL PRIMARY KEY,
    match_id bigint NOT NULL,
    matche_data json,
    status_dt timestamp NOT NULL
)