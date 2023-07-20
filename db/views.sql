CREATE MATERIALIZED VIEW IF NOT EXISTS dota_ods.teams_stats
TABLESPACE pg_default
AS WITH matches AS (
         SELECT '1970-01-01 00:00:00'::timestamp without time zone + ((pm.match_data ->> 'start_time'::text)::bigint)::double precision * '00:00:01'::interval AS datetime,
            pm.match_data,
            pm.match_data ->> 'match_id'::text AS match_id,
            pm.match_data ->> 'radiant_team_id'::text AS radiant_id,
            pm.match_data ->> 'dire_team_id'::text AS dire_id,
                CASE
                    WHEN (pm.match_data ->> 'radiant_win'::text) = 'true'::text THEN 1
                    ELSE 0
                END AS result
           FROM dota_dds.pro_matches pm
             JOIN dota_dds.leagues l ON l.league_id = ((pm.match_data ->> 'leagueid'::text)::integer)
          WHERE pm.is_live = false AND (pm.match_data ->> 'radiant_team_id'::text) IS NOT NULL AND (pm.match_data ->> 'dire_team_id'::text) IS NOT NULL
        ), full_matches AS (
         SELECT matches.match_data,
            matches.radiant_id AS team_id,
            matches.dire_id AS opp_id,
            matches.result,
            matches.datetime
           FROM matches
        UNION ALL
         SELECT matches.match_data,
            matches.dire_id AS team_id,
            matches.radiant_id AS opp_id,
                CASE
                    WHEN matches.result = 1 THEN 0
                    ELSE 1
                END AS result,
            matches.datetime
           FROM matches
        ), team_stats AS (
         SELECT full_matches.team_id,
            count(
                CASE
                    WHEN full_matches.datetime >= (CURRENT_DATE - '1 year'::interval) THEN 1
                    ELSE NULL::integer
                END) AS total_matches_past_year,
            count(
                CASE
                    WHEN full_matches.datetime >= (CURRENT_DATE - '1 year'::interval) AND full_matches.result = 1 THEN 1
                    ELSE NULL::integer
                END) AS total_wins_past_year,
            count(
                CASE
                    WHEN full_matches.datetime >= (CURRENT_DATE - '3 mons'::interval) THEN 1
                    ELSE NULL::integer
                END) AS total_matches_past_3months,
            count(
                CASE
                    WHEN full_matches.datetime >= (CURRENT_DATE - '3 mons'::interval) AND full_matches.result = 1 THEN 1
                    ELSE NULL::integer
                END) AS total_wins_past_3months,
            count(
                CASE
                    WHEN full_matches.datetime >= (CURRENT_DATE - '14 days'::interval) THEN 1
                    ELSE NULL::integer
                END) AS total_matches_past_2weeks,
            count(
                CASE
                    WHEN full_matches.datetime >= (CURRENT_DATE - '14 days'::interval) AND full_matches.result = 1 THEN 1
                    ELSE NULL::integer
                END) AS total_wins_past_2weeks
           FROM full_matches
          GROUP BY full_matches.team_id
        ), current_winstreak AS (
         SELECT rn.team_id,
            sum(rn.result) AS total_wins,
            count(rn.result) AS total_games
           FROM ( SELECT full_matches.team_id,
                    full_matches.result,
                    row_number() OVER (PARTITION BY full_matches.team_id ORDER BY full_matches.datetime DESC) AS rn
                   FROM full_matches) rn
          WHERE rn.rn <= 10
          GROUP BY rn.team_id
        )
 SELECT ts.team_id::integer AS team_id,
    ts.total_matches_past_year,
    ts.total_wins_past_year,
    ts.total_matches_past_3months,
    ts.total_wins_past_3months,
    ts.total_matches_past_2weeks,
    ts.total_wins_past_2weeks,
    cw.total_games,
    cw.total_wins
   FROM team_stats ts
     LEFT JOIN current_winstreak cw ON ts.team_id = cw.team_id
WITH DATA;

CREATE MATERIALIZED VIEW dota_ods.team_vs_team
TABLESPACE pg_default
AS WITH matches AS (
         SELECT '1970-01-01 00:00:00'::timestamp without time zone + ((pm.match_data ->> 'start_time'::text)::bigint)::double precision * '00:00:01'::interval AS datetime,
            pm.match_data,
            pm.match_data ->> 'match_id'::text AS match_id,
            pm.match_data ->> 'radiant_team_id'::text AS radiant_id,
            pm.match_data ->> 'dire_team_id'::text AS dire_id,
                CASE
                    WHEN (pm.match_data ->> 'radiant_win'::text) = 'true'::text THEN 1
                    ELSE 0
                END AS result
           FROM dota_dds.pro_matches pm
             JOIN dota_dds.leagues l ON l.league_id = ((pm.match_data ->> 'leagueid'::text)::integer)
          WHERE pm.is_live = false AND (pm.match_data ->> 'radiant_team_id'::text) IS NOT NULL AND (pm.match_data ->> 'dire_team_id'::text) IS NOT NULL
        ), full_matches AS (
         SELECT matches.match_data,
            matches.radiant_id AS team_id,
            matches.dire_id AS opp_id,
            matches.result,
            matches.datetime
           FROM matches
        UNION ALL
         SELECT matches.match_data,
            matches.dire_id AS team_id,
            matches.radiant_id AS opp_id,
                CASE
                    WHEN matches.result = 1 THEN 0
                    ELSE 1
                END AS result,
            matches.datetime
           FROM matches
        ), team_vs_team AS (
         SELECT full_matches.team_id,
            full_matches.opp_id,
            count(*) AS total_games,
            sum(full_matches.result) AS total_wins
           FROM full_matches
          WHERE full_matches.datetime >= (CURRENT_DATE - '1 year'::interval)
          GROUP BY full_matches.team_id, full_matches.opp_id
        )
 SELECT team_vs_team.team_id::integer AS team_id,
    team_vs_team.opp_id::integer AS opp_id,
    team_vs_team.total_games,
    team_vs_team.total_wins
   FROM team_vs_team
WITH DATA;

CREATE MATERIALIZED VIEW dota_ods.hero_stats
TABLESPACE pg_default
AS WITH player_hero_games AS (
         SELECT (player.value ->> 'account_id'::text)::integer AS account_id,
            (player.value ->> 'hero_id'::text)::integer AS hero_id,
            (player.value ->> 'kills'::text)::integer AS kills,
            (player.value ->> 'deaths'::text)::integer AS deaths,
            (player.value ->> 'assists'::text)::integer AS assists,
            (player.value ->> 'team_number'::text)::integer AS team_number,
                CASE
                    WHEN (pro_matches.match_data ->> 'radiant_win'::text) = 'true'::text THEN 1
                    ELSE 0
                END AS result
           FROM dota_dds.pro_matches,
            LATERAL json_array_elements(pro_matches.match_data -> 'players'::text) player(value)
          WHERE ('1970-01-01 00:00:00'::timestamp without time zone + ((pro_matches.match_data ->> 'start_time'::text)::bigint)::double precision * '00:00:01'::interval) > (CURRENT_DATE - '1 year'::interval)
        )
 SELECT player_hero_games.hero_id,
    avg(player_hero_games.kills) AS avg_kills,
    avg(player_hero_games.deaths) AS avg_deaths,
    avg(player_hero_games.assists) AS avg_assists,
    count(*) AS games_played,
    count(
        CASE
            WHEN player_hero_games.result = 1 AND player_hero_games.team_number = 0 OR player_hero_games.result = 0 AND player_hero_games.team_number = 1 THEN player_hero_games.account_id
            ELSE NULL::integer
        END)::double precision / count(*)::double precision AS winrate
   FROM player_hero_games
  GROUP BY player_hero_games.hero_id
WITH DATA;

CREATE MATERIALIZED VIEW dota_ods.player_hero_stats
TABLESPACE pg_default
AS WITH player_hero_games AS (
         SELECT (player.value ->> 'account_id'::text)::integer AS account_id,
            (player.value ->> 'hero_id'::text)::integer AS hero_id,
            (player.value ->> 'kills'::text)::integer AS kills,
            (player.value ->> 'deaths'::text)::integer AS deaths,
            (player.value ->> 'assists'::text)::integer AS assists,
            (player.value ->> 'team_number'::text)::integer AS team_number,
                CASE
                    WHEN (pro_matches.match_data ->> 'radiant_win'::text) = 'true'::text THEN 1
                    ELSE 0
                END AS result
           FROM dota_dds.pro_matches,
            LATERAL json_array_elements(pro_matches.match_data -> 'players'::text) player(value)
          WHERE ('1970-01-01 00:00:00'::timestamp without time zone + ((pro_matches.match_data ->> 'start_time'::text)::bigint)::double precision * '00:00:01'::interval) > (CURRENT_DATE - '1 year'::interval)
        )
 SELECT player_hero_games.account_id,
    player_hero_games.hero_id,
    avg(player_hero_games.kills) AS avg_kills,
    avg(player_hero_games.deaths) AS avg_deaths,
    avg(player_hero_games.assists) AS avg_assists,
    count(*) AS games_played,
    count(
        CASE
            WHEN player_hero_games.result = 1 AND player_hero_games.team_number = 0 OR player_hero_games.result = 0 AND player_hero_games.team_number = 1 THEN player_hero_games.account_id
            ELSE NULL::integer
        END)::double precision / count(*)::double precision AS winrate
   FROM player_hero_games
  GROUP BY player_hero_games.account_id, player_hero_games.hero_id
WITH DATA;

CREATE MATERIALIZED VIEW dota_ods.player_stats
TABLESPACE pg_default
AS WITH player_hero_games AS (
         SELECT (player.value ->> 'account_id'::text)::integer AS account_id,
            (player.value ->> 'hero_id'::text)::integer AS hero_id,
            (player.value ->> 'kills'::text)::integer AS kills,
            (player.value ->> 'deaths'::text)::integer AS deaths,
            (player.value ->> 'assists'::text)::integer AS assists,
            (player.value ->> 'team_number'::text)::integer AS team_number,
                CASE
                    WHEN (pro_matches.match_data ->> 'radiant_win'::text) = 'true'::text THEN 1
                    ELSE 0
                END AS result
           FROM dota_dds.pro_matches,
            LATERAL json_array_elements(pro_matches.match_data -> 'players'::text) player(value)
          WHERE ('1970-01-01 00:00:00'::timestamp without time zone + ((pro_matches.match_data ->> 'start_time'::text)::bigint)::double precision * '00:00:01'::interval) > (CURRENT_DATE - '1 year'::interval)
        )
 SELECT player_hero_games.account_id,
    avg(player_hero_games.kills) AS avg_kills,
    avg(player_hero_games.deaths) AS avg_deaths,
    avg(player_hero_games.assists) AS avg_assists,
    count(*) AS games_played,
    count(
        CASE
            WHEN player_hero_games.result = 1 AND player_hero_games.team_number = 0 OR player_hero_games.result = 0 AND player_hero_games.team_number = 1 THEN player_hero_games.account_id
            ELSE NULL::integer
        END)::double precision / count(*)::double precision AS winrate
   FROM player_hero_games
  GROUP BY player_hero_games.account_id
WITH DATA;

CREATE TABLE IF NOT EXISTS dota_ods.pro_matches_ml_full_v2 AS (
WITH teams_matches AS (
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
),



matches AS (
SELECT 
 	pm.match_id,
 	to_timestamp((match_data ->> 'start_time')::integer) at time zone 'Europe/Moscow' AS match_time,
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
   
WHERE ('1970-01-01 00:00:00'::timestamp without time zone + ((pm.match_data ->> 'start_time'::text)::bigint)::double precision * '00:00:01'::interval) > (CURRENT_DATE - '2 years'::interval)
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
                PARTITION BY account_id
                ORDER BY match_time
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) AS total_kills,
            SUM(deaths::int) OVER (
                PARTITION BY account_id
                ORDER BY match_time
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) AS total_deaths,
            SUM(assists::int) OVER (
                PARTITION BY account_id
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
                PARTITION BY hero_id
                ORDER BY match_time
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) AS total_kills,
            SUM(deaths::int) OVER (
                PARTITION BY hero_id
                ORDER BY match_time
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) AS total_deaths,
            SUM(assists::int) OVER (
                PARTITION BY hero_id
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
GROUP BY m.match_id, m.match_time),

hero_pairs_together AS (
SELECT
	a.match_id,
	a.match_time,
	a.hero_id AS hero1,
	b.hero_id AS hero2,
	a.team_number,
	CASE WHEN a.team_number = 0 THEN a.result ELSE 1 - a.result END AS result
FROM matches a
JOIN matches b ON a.match_id = b.match_id AND a.team_number = b.team_number AND a.hero_id != b.hero_id
),

pair_winrates_together AS (
SELECT
	match_id,
	hero1,
	hero2,
	team_number,
	result,
	COUNT(*) OVER (
	  PARTITION BY hero1, hero2
	  ORDER BY match_time
	  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
	) AS games,
	SUM(result) OVER (
      PARTITION BY hero1, hero2
      ORDER BY match_time
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS wins
  FROM hero_pairs_together
),

hero_pairs_against AS (
SELECT
	a.match_id,
	a.match_time,
	a.hero_id AS hero1,
	b.hero_id AS hero2,
	a.team_number,
	CASE WHEN a.team_number = 0 THEN a.result ELSE 1 - a.result END AS result
FROM matches a
JOIN matches b ON a.match_id = b.match_id AND a.team_number != b.team_number AND a.hero_id != b.hero_id
),

pair_winrates_against AS (
SELECT
	match_id,
	hero1,
	hero2,
	team_number,
	result,
	COUNT(*) OVER (
	  PARTITION BY hero1, hero2
	  ORDER BY match_time
	  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
	) AS games,
	SUM(result) OVER (
      PARTITION BY hero1, hero2
      ORDER BY match_time
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS wins
  FROM hero_pairs_against
),

matches_together AS (
SELECT
	pwt.match_id,
	AVG(CASE WHEN pwt.team_number = 0 THEN CASE WHEN pwt.games = 0 THEN 0 ELSE pwt.wins::float / pwt.games END END) AS avg_winrate_together_1,
	AVG(CASE WHEN pwt.team_number = 1 THEN CASE WHEN pwt.games = 0 THEN 0 ELSE pwt.wins::float / pwt.games END END) AS avg_winrate_together_2
FROM pair_winrates_together pwt
GROUP BY
	1),
	
matches_against AS (
SELECT
	pwa.match_id,
	AVG(CASE WHEN pwa.team_number = 0 THEN CASE WHEN pwa.games = 0 THEN 0 ELSE pwa.wins::float / pwa.games END END) AS avg_winrate_against_1,
	AVG(CASE WHEN pwa.team_number = 1 THEN CASE WHEN pwa.games = 0 THEN 0 ELSE pwa.wins::float / pwa.games END END) AS avg_winrate_against_2
FROM pair_winrates_against pwa
GROUP BY
	1
)

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
	tp.team2_player_winrate,
	mt.avg_winrate_together_1,
	mt.avg_winrate_together_2,
	ma.avg_winrate_against_1,
	ma.avg_winrate_against_2
FROM dota_dds.pro_matches pm 
LEFT JOIN dota_dds.leagues l 
	ON l.league_id = (pm.match_data ->> 'leagueid')::int
LEFT JOIN matches_together mt
	ON mt.match_id = pm.match_id
LEFT JOIN matches_against ma
	ON ma.match_id = pm.match_id
LEFT JOIN teams_stats ts 
	ON ts.team_id = (pm.match_data ->> 'radiant_team_id')::int
	AND ts.opp_id = (pm.match_data ->> 'dire_team_id')::int
	AND ts.match_id = pm.match_id
LEFT JOIN teams_players tp
	ON tp.match_id = pm.match_id
WHERE is_live = False
AND match_data ->> 'radiant_team_id' IS NOT NULL AND match_data ->> 'dire_team_id' IS NOT NULL
AND to_timestamp((match_data ->> 'start_time')::int) at time zone 'Europe/Moscow' > current_date - interval '13 months'
)