CREATE TABLE IF NOT EXISTS dota_ods.pro_matches_ml AS (
WITh teams_matches AS (
SELECT 
	match_id,
	match_data ->> 'radiant_team_id' AS team_id,
	match_data ->> 'dire_team_id' AS opp_id,
	CASE
		WHEN (match_data ->> 'radiant_win'::text) = 'true'::text THEN 1
		ELSE 0
	END AS result,
	to_timestamp((match_data ->> 'start_time')::int) at time zone 'Europe/Moscow' AS match_time
FROM dota_dds.pro_matches
WHERE match_data ->> 'radiant_team_id' IS NOT NULL AND match_data ->> 'dire_team_id' IS NOT NULL

UNION ALL 

SELECT 
	match_id,
	match_data ->> 'dire_team_id' AS team_id,
	match_data ->> 'radiant_team_id' AS opp_id,
	CASE
		WHEN (match_data ->> 'radiant_win'::text) = 'true'::text THEN 0
		ELSE 1
	END AS result,
	to_timestamp((match_data ->> 'start_time')::int) at time zone 'Europe/Moscow' AS match_time
FROM dota_dds.pro_matches
WHERE match_data ->> 'radiant_team_id' IS NOT NULL AND match_data ->> 'dire_team_id' IS NOT NULL),

teams_stats AS (

SELECT 
	match_id, 
	team_id::int,
	opp_id::int,
	match_time,
    COUNT(*) OVER (
        PARTITION BY team_id, opp_id
        ORDER BY match_time
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS previous_games,
    COALESCE(SUM(result) OVER (
        PARTITION BY team_id, opp_id
        ORDER BY match_time
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ), 0) AS previous_wins,
    (COUNT(*) OVER (
        PARTITION BY team_id
        ORDER BY match_time
        RANGE BETWEEN INTERVAL '2 weeks' PRECEDING AND CURRENT ROW
    ) - 1) AS t1_games_last_2_weeks,
    (SUM(result) OVER (
        PARTITION BY team_id
        ORDER BY match_time
        RANGE BETWEEN INTERVAL '2 weeks' PRECEDING AND CURRENT ROW
    ) - result) AS t1_wins_last_2_weeks,
    (COUNT(*) OVER (
        PARTITION BY opp_id
        ORDER BY match_time
        RANGE BETWEEN INTERVAL '2 weeks' PRECEDING AND CURRENT ROW
    ) - 1) AS t2_games_last_2_weeks,
    (SUM(result) OVER (
        PARTITION BY opp_id
        ORDER BY match_time
        RANGE BETWEEN INTERVAL '2 weeks' PRECEDING AND CURRENT ROW
    ) - result) AS t2_wins_last_2_weeks,
    (COUNT(*) OVER (
        PARTITION BY team_id
        ORDER BY match_time
        RANGE BETWEEN INTERVAL '3 months' PRECEDING AND CURRENT ROW
    ) - 1) AS t1_games_last_3_months,
    (SUM(result) OVER (
        PARTITION BY team_id
        ORDER BY match_time
        RANGE BETWEEN INTERVAL '3 months' PRECEDING AND CURRENT ROW
    ) - result) AS t1_wins_last_3_months,
    (COUNT(*) OVER (
        PARTITION BY opp_id
        ORDER BY match_time
        RANGE BETWEEN INTERVAL '3 months' PRECEDING AND CURRENT ROW
    ) - 1) AS t2_games_last_3_months,
    (SUM(result) OVER (
        PARTITION BY opp_id
        ORDER BY match_time
        RANGE BETWEEN INTERVAL '3 months' PRECEDING AND CURRENT ROW
    ) - result) AS t2_wins_last_3_months,
    (COUNT(*) OVER (
        PARTITION BY team_id
        ORDER BY match_time
        RANGE BETWEEN INTERVAL '12 months' PRECEDING AND CURRENT ROW
    ) - 1) AS t1_games_last_12_months,
    (SUM(result) OVER (
        PARTITION BY team_id
        ORDER BY match_time
        RANGE BETWEEN INTERVAL '12 months' PRECEDING AND CURRENT ROW
    ) - result) AS t1_wins_last_12_months,
    (COUNT(*) OVER (
        PARTITION BY opp_id
        ORDER BY match_time
        RANGE BETWEEN INTERVAL '12 months' PRECEDING AND CURRENT ROW
    ) - 1) AS t2_games_last_12_months,
    (SUM(result) OVER (
        PARTITION BY opp_id
        ORDER BY match_time
        RANGE BETWEEN INTERVAL '12 months' PRECEDING AND CURRENT ROW
    ) - result) AS t2_wins_last_12_months
FROM teams_matches),

allowed AS (
	SELECT
		match_id,
		match_data,
		is_live
	FROM dota_dds.pro_matches pm 
	LEFT JOIN dota_dds.leagues l 
	ON l.league_id = (pm.match_data ->> 'leagueid')::int
--WHERE l.allowed = True
),



matches AS (
SELECT 
 	pm.match_id,
 	to_timestamp((match_data ->> 'start_time')::int) at time zone 'Europe/Moscow' AS match_time,
	(player.value ->> 'account_id'::text)::integer AS account_id,
	(player.value ->> 'hero_id'::text)::integer AS hero_id,
	(player.value ->> 'kills'::text)::integer AS kills,
	(player.value ->> 'deaths'::text)::integer AS deaths,
	(player.value ->> 'assists'::text)::integer AS assists,
	(player.value ->> 'team_number'::text)::integer AS team_number,
	CASE
		WHEN (pm.match_data ->> 'radiant_win'::text) = 'true'::text THEN 1
		ELSE 0
	END AS result
FROM allowed pm,
LATERAL json_array_elements(pm.match_data -> 'players'::text) player(value)
   
WHERE ('1970-01-01 00:00:00'::timestamp without time zone + ((pm.match_data ->> 'start_time'::text)::bigint)::double precision * '00:00:01'::interval) > (CURRENT_DATE - '1 year'::interval)
AND (player.value ->> 'team_number'::text)::integer IN (0, 1)
AND is_live = False
),



playerhero_winrates AS (
    SELECT 
        match_id,
        match_time,
        account_id,
        hero_id,
        team_number,
        previous_games,
        CASE
            WHEN previous_games = 0 THEN 0
            ELSE previous_wins::float / previous_games
        END AS win_rate,
        CASE
            WHEN previous_games = 0 THEN 0
            ELSE total_kills::float / previous_games
        END AS kills,
        CASE
            WHEN previous_games = 0 THEN 0
            ELSE total_deaths::float / previous_games
        END AS deaths,
        CASE
            WHEN previous_games = 0 THEN 0
            ELSE total_assists::float / previous_games
        END AS assists
    FROM (
        SELECT 
            match_id,
            account_id,
            hero_id,
            match_time,
            team_number,
            COUNT(*) OVER (
                PARTITION BY account_id, hero_id
                ORDER BY match_time
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) AS previous_games,
            SUM(result::int) OVER (
                PARTITION BY account_id, hero_id
                ORDER BY match_time
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) AS previous_wins,
            SUM(kills::int) OVER (
                PARTITION BY account_id, hero_id
                ORDER BY match_time
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) AS total_kills,
            SUM(deaths::int) OVER (
                PARTITION BY account_id, hero_id
                ORDER BY match_time
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) AS total_deaths,
            SUM(assists::int) OVER (
                PARTITION BY account_id, hero_id
                ORDER BY match_time
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) AS total_assists
        FROM matches
    ) subq
),

player_winrates AS (
    SELECT 
        match_id,
        match_time,
        account_id,
        team_number,
        previous_games,
        CASE
            WHEN previous_games = 0 THEN 0
            ELSE previous_wins::float / previous_games
        END AS win_rate,
        CASE
            WHEN previous_games = 0 THEN 0
            ELSE total_kills::float / previous_games
        END AS kills,
        CASE
            WHEN previous_games = 0 THEN 0
            ELSE total_deaths::float / previous_games
        END AS deaths,
        CASE
            WHEN previous_games = 0 THEN 0
            ELSE total_assists::float / previous_games
        END AS assists
    FROM (
        SELECT 
            match_id,
            match_time,
            account_id,
            team_number,
       
            COUNT(*) OVER (
                PARTITION BY account_id
                ORDER BY match_time
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) AS previous_games,
            SUM(result::int) OVER (
                PARTITION BY account_id
                ORDER BY match_time
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) AS previous_wins,
            SUM(kills::int) OVER (
                PARTITION BY account_id, hero_id
                ORDER BY match_time
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) AS total_kills,
            SUM(deaths::int) OVER (
                PARTITION BY account_id, hero_id
                ORDER BY match_time
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) AS total_deaths,
            SUM(assists::int) OVER (
                PARTITION BY account_id, hero_id
                ORDER BY match_time
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) AS total_assists
        FROM matches
    ) subq
),

hero_winrates AS (
    SELECT 
        match_id,
        match_time,
        hero_id,
        team_number,
        previous_games,
        CASE
            WHEN previous_games = 0 THEN 0
            ELSE previous_wins::float / previous_games
        END AS win_rate,
        CASE
            WHEN previous_games = 0 THEN 0
            ELSE total_kills::float / previous_games
        END AS kills,
        CASE
            WHEN previous_games = 0 THEN 0
            ELSE total_deaths::float / previous_games
        END AS deaths,
        CASE
            WHEN previous_games = 0 THEN 0
            ELSE total_assists::float / previous_games
        END AS assists
    FROM (
        SELECT 
            match_id,
            match_time,
            hero_id,
            team_number,
            COUNT(*) OVER (
                PARTITION BY hero_id
                ORDER BY match_time
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) AS previous_games,
            SUM(result::int) OVER (
                PARTITION BY hero_id
                ORDER BY match_time
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) AS previous_wins,
            SUM(kills::int) OVER (
                PARTITION BY account_id, hero_id
                ORDER BY match_time
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) AS total_kills,
            SUM(deaths::int) OVER (
                PARTITION BY account_id, hero_id
                ORDER BY match_time
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) AS total_deaths,
            SUM(assists::int) OVER (
                PARTITION BY account_id, hero_id
                ORDER BY match_time
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) AS total_assists
        FROM matches
    ) subq
),

teams_players AS (
SELECT 
    m.match_id,
    m.match_time,
    AVG(ph.kills) FILTER (WHERE ph.team_number = 0) AS team1_playerhero_kills,
    AVG(ph.deaths) FILTER (WHERE ph.team_number = 0) AS team1_playerhero_deaths,
    AVG(ph.assists) FILTER (WHERE ph.team_number = 0) AS team1_playerhero_assists,
    AVG(ph.previous_games) FILTER (WHERE ph.team_number = 0) AS team1_playerhero_games,
    AVG(ph.win_rate) FILTER (WHERE ph.team_number = 0) AS team1_playerhero_winrate,
    AVG(ph.kills) FILTER (WHERE ph.team_number = 1) AS team2_playerhero_kills,
    AVG(ph.deaths) FILTER (WHERE ph.team_number = 1) AS team2_playerhero_deaths,
    AVG(ph.assists) FILTER (WHERE ph.team_number = 1) AS team2_playerhero_assists,
    AVG(ph.previous_games) FILTER (WHERE ph.team_number =1) AS team2_playerhero_games,
    AVG(ph.win_rate) FILTER (WHERE ph.team_number = 1) AS team2_playerhero_winrate,
    AVG(p.kills) FILTER (WHERE p.team_number = 0) AS team1_player_kills,
    AVG(p.deaths) FILTER (WHERE p.team_number = 0) AS team1_player_deaths,
    AVG(p.assists) FILTER (WHERE p.team_number = 0) AS team1_player_assists,
    AVG(p.previous_games) FILTER (WHERE p.team_number = 0) AS team1_player_games,
    AVG(p.win_rate) FILTER (WHERE p.team_number = 0) AS team1_player_winrate,
    AVG(p.kills) FILTER (WHERE p.team_number = 1) AS team2_player_kills,
    AVG(p.deaths) FILTER (WHERE p.team_number = 1) AS team2_player_deaths,
    AVG(p.assists) FILTER (WHERE p.team_number = 1) AS team2_player_assists,
    AVG(p.previous_games) FILTER (WHERE p.team_number =1) AS team2_player_games,
    AVG(p.win_rate) FILTER (WHERE p.team_number = 1) AS team2_player_winrate,
    AVG(h.kills) FILTER (WHERE h.team_number = 0) AS team1_hero_kills,
    AVG(h.deaths) FILTER (WHERE h.team_number = 0) AS team1_hero_deaths,
    AVG(h.assists) FILTER (WHERE h.team_number = 0) AS team1_hero_assists,
    AVG(h.previous_games) FILTER (WHERE h.team_number = 0) AS team1_hero_games,
    AVG(h.win_rate) FILTER (WHERE h.team_number = 0) AS team1_hero_winrate,
    AVG(h.kills) FILTER (WHERE h.team_number = 1) AS team2_hero_kills,
    AVG(h.deaths) FILTER (WHERE h.team_number = 1) AS team2_hero_deaths,
    AVG(h.assists) FILTER (WHERE h.team_number = 1) AS team2_hero_assists,
    AVG(h.previous_games) FILTER (WHERE h.team_number =1) AS team2_hero_games,
    AVG(h.win_rate) FILTER (WHERE h.team_number = 1) AS team2_hero_winrate
    
FROM matches m
LEFT JOIN playerhero_winrates ph ON m.match_id = ph.match_id AND m.account_id = ph.account_id AND m.hero_id = ph.hero_id
LEFT JOIN player_winrates p ON m.match_id = p.match_id AND m.account_id = p.account_id
LEFT JOIN hero_winrates h ON m.match_id = h.match_id AND m.hero_id = h.hero_id
GROUP BY m.match_id, m.match_time)

SELECT 
	pm.match_data,
	pm.match_id,
	l.league_id,
	l.allowed,
	(pm.match_data ->> 'radiant_team_id')::int AS radiant_team_id,
	(pm.match_data ->> 'dire_team_id')::int AS dire_team_id,
	CASE
		WHEN (pm.match_data ->> 'radiant_win'::text) = 'true'::text THEN 1
		ELSE 0
	END AS result,
	to_timestamp((match_data ->> 'start_time')::int) at time zone 'Europe/Moscow' AS datetime,
	ts.t1_games_last_12_months,
	ts.t1_wins_last_12_months,
	ts.t2_games_last_12_months,
	ts.t2_wins_last_12_months,
	ts.t1_games_last_3_months,
	ts.t1_wins_last_3_months,
	ts.t2_games_last_3_months,
	ts.t2_wins_last_3_months,
	ts.t1_games_last_2_weeks,
	ts.t1_wins_last_2_weeks,
	ts.t2_games_last_2_weeks,
	ts.t2_wins_last_2_weeks,
	ts.previous_games,
	ts.previous_wins,
	tp.team1_playerhero_kills,
	tp.team1_playerhero_deaths,
	tp.team1_playerhero_assists,
	tp.team1_playerhero_games,
	tp.team1_playerhero_winrate,
	tp.team2_playerhero_kills,
	tp.team2_playerhero_deaths,
	tp.team2_playerhero_assists,
	tp.team2_playerhero_games,
	tp.team2_playerhero_winrate,
	tp.team1_hero_kills,
	tp.team1_hero_deaths,
	tp.team1_hero_assists,
	tp.team1_hero_games,
	tp.team1_hero_winrate,
	tp.team2_hero_kills,
	tp.team2_hero_deaths,
	tp.team2_hero_assists,
	tp.team2_hero_games,
	tp.team2_hero_winrate,
	tp.team1_player_kills,
	tp.team1_player_deaths,
	tp.team1_player_assists,
	tp.team1_player_games,
	tp.team1_player_winrate,
	tp.team2_player_kills,
	tp.team2_player_deaths,
	tp.team2_player_assists,
	tp.team2_player_games,
	tp.team2_player_winrate
FROM dota_dds.pro_matches pm 
LEFT JOIN dota_dds.leagues l 
	ON l.league_id = (pm.match_data ->> 'leagueid')::int
LEFT JOIN teams_stats ts 
	ON ts.team_id = (pm.match_data ->> 'radiant_team_id')::int
	AND ts.opp_id = (pm.match_data ->> 'dire_team_id')::int
	AND ts.match_id = pm.match_id
LEFT JOIN teams_players tp
	ON tp.match_id = pm.match_id
WHERE is_live = False
AND match_data ->> 'radiant_team_id' IS NOT NULL AND match_data ->> 'dire_team_id' IS NOT NULL
AND team1_playerhero_kills IS NOT NULL

)

