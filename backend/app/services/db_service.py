import logging
import os
import pandas as pd
from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values, Json
from psycopg2 import OperationalError
from psycopg2.extensions import register_adapter
from time import sleep

register_adapter(dict, Json)
load_dotenv()

logger = logging.getLogger('db_service')

db_name = os.getenv('POSTGRES_DB')
user = os.getenv('POSTGRES_USER')
password = os.getenv('POSTGRES_PASSWORD')

class DatabaseService:
    def __init__(self, db_name, user, password, host='db', port='5432'):
        self.db_name = db_name
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.connection = None
        self.connect()

    def connect(self):
        for i in range(1, 7):
            try:
                self.connection = psycopg2.connect(
                        dbname=self.db_name,
                        user=self.user,
                        password=self.password,
                        host=self.host,
                        port=self.port
                    )
                self.connection.autocommit = True
                logger.info(f'Successfully connected to db')
                break
            except OperationalError as e:
                logger.error(f'Error {str(e)} occured, trying to reconnect in {i**2} seconds')
                sleep(i**2)
        else:
            raise Exception("Unable to connect to the database after several attempts")

    def close(self):
        if self.connection is not None:
            self.connection.close()
            logger.info(f'Connection to the database has been closed')

    def get_live_matches(self):
        try:
            with self.connection.cursor() as cursor:
                check_live_query = sql.SQL("""
                    SELECT 
                        match_id, 
                        match_data 
                    FROM dota_dds.pro_matches 
                    WHERE is_live = True
                """)
                cursor.execute(check_live_query)
                games = [{'match_id': row[0], 'match_data': row[1]} for row in cursor.fetchall()]
                return {'games': games}
        except psycopg2.Error as e:
            logger.error(f'Error checking live games: {str(e)}')
            return {}
        
    def create_match(self, match_id, series_id, match_data):
        try:
            with self.connection.cursor() as cursor:
                insert_game_query = sql.SQL("""
                    INSERT INTO dota_dds.pro_matches (match_id, series_id, match_data, is_live)
                    VALUES (%s, %s, %s, True)
                """)
                cursor.execute(insert_game_query, (match_id, series_id, match_data))
                logger.info(f'Successfully created match {match_id}')
        except psycopg2.Error as e:
            logger.error(f'Error created game: {str(e)}')

    def update_match(self, match_id, data, is_live):
        try:
            with self.connection.cursor() as cursor:
                update_game_query = sql.SQL("""
                    UPDATE dota_dds.pro_matches
                    SET is_live = %s,
                        match_data = %s
                    WHERE match_id = %s
                """)
                cursor.execute(update_game_query, (is_live, data, match_id))
        except psycopg2.Error as e:
            logger.error(f'Error updating game: {str(e)}')

    def get_match_info(self, match_id):
        try:
            with self.connection.cursor() as cursor:
                get_game_query = sql.SQL("""
                    SELECT match_id, result
                    FROM dota_ods.predictions
                    WHERE match_id = %s
                """)
                cursor.execute(get_game_query, (match_id,))
                row = cursor.fetchone()  # fetches one row from the result
                if row:
                    return {'match_id': row[0], 'result': row[1]}  # pack results into dictionary
                else:
                    return {}
        except psycopg2.Error as e:
            logger.error(f'Error fetching match info: {str(e)}')
            return {}

    def add_match_status(self, match_id, match_data, ingame_dttm):
        try:
            with self.connection.cursor() as cursor:
                insert_game_query = sql.SQL("""
                    INSERT INTO dota_dds.pro_matches_statuses (match_id, match_data, ingame_dttm)
                    SELECT %s, %s, %s
                    WHERE NOT EXISTS (
                        SELECT 1 FROM dota_dds.pro_matches_statuses
                        WHERE match_id = %s AND ingame_dttm = %s
                    )
                """)
                cursor.execute(insert_game_query, (match_id, match_data, ingame_dttm, match_id, ingame_dttm))
        except psycopg2.Error as e:
            logger.error(f'Error created game: {str(e)}')

    def get_match_statuses(self, match_id):
        try:
            with self.connection.cursor() as cursor:
                get_game_query = sql.SQL("""
                    SELECT match_id, match_data, ingame_dttm 
                    FROM dota_dds.pro_matches_statuses 
                    WHERE match_id = %s
                    ORDER BY ingame_dttm ASC
                """)
                cursor.execute(get_game_query, (match_id,))
                statuses = [{'match_data': row[1], 'ingame_dttm': row[2]} for row in cursor.fetchall()]
                return {'match_id': match_id, 'statuses': statuses}
        except psycopg2.Error as e:
            logger.error(f'Error fetching game statuses: {str(e)}')
            return {}
        

        

    def create_predictions(self, predictions):
        try:
            with self.connection.cursor() as cursor:
                insert_game_query = sql.SQL("""
                    INSERT INTO dota_ods.predictions (match_id, radiant_team, dire_team, model, prediction, probability)
                    VALUES (%(match_id)s, %(radiant_team)s, %(dire_team)s, %(model)s, %(prediction)s, %(probability)s)
                """)
                cursor.executemany(insert_game_query, predictions)
                logger.info(f'Successfully predicted matches {[match["match_id"] for match in predictions]}')
        except psycopg2.Error as e:
            logger.error(f'Error created game: {str(e)}')

    def update_predictions(self, match_id, result):
        try:
            with self.connection.cursor() as cursor:
                update_game_query = sql.SQL("""
                    UPDATE dota_ods.predictions
                    SET result = %s
                    WHERE match_id = %s
                """)
                cursor.execute(update_game_query, (result, match_id))
                logger.info(f'Successfully updated result in predictions {match_id}')
        except psycopg2.Error as e:
            logger.error(f'Error updating game: {str(e)}')

    def get_predictions(self, query, params=None, is_match=False):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                matches = {}

                for row in cursor.fetchall():
                    match_id = row[0]
                    radiant_team = row[1]
                    dire_team = row[2]
                    league_name = row[3]
                    model = row[4]
                    prediction = row[5]
                    probability = row[6]
                    result = row[7] if is_match else None

                    match_key = (match_id, radiant_team, dire_team, league_name)

                    if match_key not in matches:
                        matches[match_key] = {'predictions': [], 'result': result}

                    matches[match_key]['predictions'].append({
                        'model': model,
                        'prediction': prediction,
                        'probability': probability
                    })

                predictions = [
                    {
                        'match_id': key[0],
                        'radiant_team': key[1],
                        'dire_team': key[2],
                        'league_name': key[3],
                        'predictions': value['predictions'],
                        'result': value['result']
                    }
                    for key, value in matches.items()
                ]

                return predictions
        except psycopg2.Error as e:
            logger.error(f'Error returning predictions: {str(e)}')
            return {}

    def get_live_predictions(self):
        live_matches_query = sql.SQL("""
            SELECT 
                p.match_id, 
                p.radiant_team,
                p.dire_team,
                l.league_name,
                p.model,
                p.prediction,
                p.probability
            FROM dota_ods.predictions p
            INNER JOIN dota_dds.pro_matches pm
                ON p.match_id = pm.match_id
                AND pm.is_live = True
            INNER JOIN dota_dds.leagues l
                ON l.league_id = (pm.match_data ->> 'league_id')::int
                AND l.allowed = True
            WHERE p.raw_dt >= current_date - INTERVAL '1 day'
        """)

        predictions = self.get_predictions(live_matches_query)
        return {'matches': predictions}

    def get_match_prediction(self, match_id: int):
        match_query = sql.SQL("""
            SELECT 
                p.match_id, 
                p.radiant_team,
                p.dire_team,
                l.league_name,
                p.model,
                p.prediction,
                p.probability,
                p.result
            FROM dota_ods.predictions p
            LEFT JOIN dota_dds.pro_matches pm
                ON p.match_id = pm.match_id
            INNER JOIN dota_dds.leagues l
                ON l.league_id = COALESCE((pm.match_data ->> 'league_id')::int, (pm.match_data ->> 'leagueid')::int)
                AND l.allowed = True
            WHERE p.match_id = %s
        """)

        predictions = self.get_predictions(match_query, (match_id,), is_match=True)
        return predictions[0] if predictions else None


    def get_max_value(self, table, column):
        try:
            with self.connection.cursor() as cursor:
                schema, table = table.split('.')
                query = sql.SQL("SELECT MAX({}) FROM {}.{}").format(sql.Identifier(column), sql.Identifier(schema), sql.Identifier(table))
                cursor.execute(query)
                max_value = cursor.fetchone()[0]
                logger.info(f'Successfully retrieved the maximum value from {table}.{column}')
                return max_value   
        except psycopg2.Error as e:
            logger.error(f'Error retrieving maximum value: {str(e)}')
            return None
        
    def get_tournaments_stats(self):
        try:
            with self.connection.cursor() as cursor:
                query = sql.SQL("""
                    SELECT 
                        league_name,
                        model,
                        COUNT(*) AS total_games,
                        COUNT(CASE WHEN prediction = result THEN p.match_id ELSE NULL END) AS total_correct,
                        COUNT(CASE WHEN prediction != result THEN p.match_id ELSE NULL END) AS total_incorrect,
                        ROUND((COUNT(CASE WHEN prediction = result THEN p.match_id ELSE NULL END)::float / COUNT(*)::float)::numeric, 2) AS winrate
                    FROM dota_ods.predictions p
                    LEFT JOIN dota_dds.pro_matches pm 
                        ON pm.match_id = p.match_id 
                    LEFT JOIN dota_dds.leagues l 
                        ON l.league_id = (pm.match_data ->> 'leagueid')::int
                    WHERE league_name IS NOT NULL
                    AND is_live = false 
                    AND prediction IS NOT NULL
                    AND probability > 0.57
                    AND allowed = True
                    AND league_id NOT IN (14783)
                    GROUP BY 
                        1, 2
                """)
                cursor.execute(query)
                stats = {}

                rows = cursor.fetchall()
                for row in rows:
                    league_name = row[0]
                    model_name = row[1]
                    total_games = row[2]
                    total_correct = row[3]
                    total_incorrect = row[4]
                    winrate = row[5]

                    if league_name in stats:
                        stats[league_name]['predictions'].append({
                            'model_name': model_name,
                            'total_games': total_games,
                            'total_correct': total_correct,
                            'total_incorrect': total_incorrect,
                            'winrate': winrate
                        })
                    else:
                        stats[league_name] = {
                            'predictions': [{
                                'model_name': model_name,
                                'total_games': total_games,
                                'total_correct': total_correct,
                                'total_incorrect': total_incorrect,
                                'winrate': winrate
                            }]
                        }

                logger.info(f'Successfully retrieved tournament stats.')
        except psycopg2.Error as e:
            logger.error(f'Error retrieving tournament stats: {str(e)}')
        
        return {'tournaments': [{'league_name': k, 'predictions': v['predictions']} for k, v in stats.items()]}
    
    def get_recent_stats(self):
        try:
            with self.connection.cursor() as cursor:
                query = sql.SQL("""
                    SELECT 
                        COUNT(*) AS total_predictions,
                        COUNT(CASE WHEN prediction = result THEN p.match_id ELSE NULL END) AS total_correct,
                        COUNT(CASE WHEN prediction != result THEN p.match_id ELSE NULL END) AS total_incorrect,
                        ROUND((COUNT(CASE WHEN prediction = result THEN p.match_id ELSE NULL END)::float / NULLIF(COUNT(*)::float, 0))::numeric, 2) AS winrate
                    FROM dota_ods.predictions p
                    LEFT JOIN dota_dds.pro_matches pm
                        ON pm.match_id = p.match_id
                    LEFT JOIN dota_dds.leagues l 
                        ON l.league_id = (pm.match_data ->> 'leagueid')::int
                    WHERE 
                        p.raw_dt = current_date - interval '1 day'
                        AND p.prediction IS NOT NULL
                        AND p.probability > 0.59
                        AND l.allowed = True
                        AND model = 'heroes_standard'
                """)
                cursor.execute(query)

                result = cursor.fetchone()
                column_names = [desc[0] for desc in cursor.description]
                stats = dict(zip(column_names, result))
                if stats['total_predictions'] == 0:
                    raise ValueError("No predictions were made.")
                return stats
        except psycopg2.Error as e:
            logger.error(f'Error retrieving recent stats: {str(e)}')
            return None
        
    def insert_public_matches(self, matches):
        try:
            with self.connection.cursor() as cursor:
                insert_query = sql.SQL("""
                    INSERT INTO dota_dds.public_matches (match_id, start_time, duration, game_mode, avg_rank_tier, radiant_team, dire_team, radiant_win, raw_dt)
                    VALUES (%(match_id)s, %(start_time)s, %(duration)s, %(game_mode)s, %(avg_rank_tier)s, %(radiant_team)s, %(dire_team)s, %(radiant_win)s, CURRENT_DATE)
                    ON CONFLICT (match_id) DO NOTHING
                """)
                psycopg2.extras.execute_batch(cursor, insert_query, matches)
                self.connection.commit()
                logger.info(f'Successfully inserted new matches')
        except psycopg2.Error as e:
            logger.error(f'Error inserting new matches: {str(e)}')
    
    def get_allowed_leagues(self):
        try:
            with self.connection.cursor() as cursor:
                query = sql.SQL("""
                    SELECT league_id
                    FROM dota_dds.leagues
                    WHERE allowed = True
                """)
                cursor.execute(query)
                rows = cursor.fetchall()
                league_ids = [row[0] for row in rows]
                return league_ids
        except psycopg2.Error as e:
            print(f'Error retrieving allowed leagues: {str(e)}')
            return []
        
    def get_league_names(self, match_ids):
        try:
            with self.connection.cursor() as cursor:
                query = sql.SQL("""
                    SELECT
                        pm.match_id,
                        l.league_name
                    FROM dota_dds.pro_matches pm
                    LEFT JOIN dota_dds.leagues l 
                        ON l.league_id = COALESCE(pm.match_data ->> 'leagueid', pm.match_data ->> 'league_id')::int
                    WHERE match_id IN (SELECT unnest(%s))
                """)
                cursor.execute(query, (match_ids,))
                df = pd.DataFrame(cursor.fetchall(), columns=['match_id', 'league_name'])
                return df
        except psycopg2.Error as e:
            logger.error(f'Error fetching league names: {str(e)}')
            return pd.DataFrame()
        
    def refresh_materialized_views(self):
        view_names = ['dota_ods.hero_stats',
                    'dota_ods.player_hero_stats',
                    'dota_ods.player_stats',
                    'dota_ods.teams_stats',
                    'dota_ods.team_vs_team']
        try:
            with self.connection.cursor() as cursor:
                for view_name in view_names:
                    query = f"REFRESH MATERIALIZED VIEW {view_name}"
                    cursor.execute(query)
            logger.info("Successfully refreshed all materialized views.")
        except psycopg2.Error as e:
            logger.error(f'Error refreshing materialized views: {str(e)}')

    def get_stats_for_prediction(self, match_ids):
        try:
            with self.connection.cursor() as cursor:       
                query = sql.SQL("""
                    WITH game AS (
                        SELECT 
                            match_id,
                            match_data,
                            current_date AS game_dt,
                            (match_data -> 'radiant_team' ->> 'team_id')::int AS radiant_id,
                            (match_data -> 'dire_team' ->> 'team_id')::int AS dire_id,
                            (match_data -> 'radiant_team' ->> 'team_name') AS radiant_name,
                            (match_data -> 'dire_team' ->> 'team_name') AS dire_name
                        FROM dota_dds.pro_matches
                        WHERE match_id IN (SELECT unnest(%s))
                    ),

                    players AS (
                        SELECT 
                            match_id,
                            (json_array_elements(pm.match_data->'players')->>'account_id')::int AS player_id,
                            (json_array_elements(pm.match_data->'players')->>'hero_id')::int AS hero_id,
                            (json_array_elements(pm.match_data->'players')->>'team')::int AS team_number
                        FROM game pm
                        
                    ),

                    player_hero_stats AS (

                    SELECT 
                        match_id,
                        AVG(CASE WHEN team_number = 0 THEN phs.avg_kills ELSE NULL END) AS avg_kills1,
                        AVG(CASE WHEN team_number = 0 THEN phs.avg_deaths ELSE NULL END) AS avg_deaths1,
                        AVG(CASE WHEN team_number = 0 THEN phs.avg_assists ELSE NULL END) AS avg_assists1,
                        AVG(CASE WHEN team_number = 0 THEN phs.games_played ELSE NULL END) AS avg_games_played1,
                        AVG(CASE WHEN team_number = 0 THEN phs.winrate ELSE NULL END) AS avg_winrate1,
                        AVG(CASE WHEN team_number = 1 THEN phs.avg_kills ELSE NULL END) AS avg_kills2,
                        AVG(CASE WHEN team_number = 1 THEN phs.avg_deaths ELSE NULL END) AS avg_deaths2,
                        AVG(CASE WHEN team_number = 1 THEN phs.avg_assists ELSE NULL END) AS avg_assists2,
                        AVG(CASE WHEN team_number = 1 THEN phs.games_played ELSE NULL END) AS avg_games_played2,
                        AVG(CASE WHEN team_number = 1 THEN phs.winrate ELSE NULL END) AS avg_winrate2,
                        AVG(CASE WHEN team_number = 0 THEN hs.avg_kills ELSE NULL END) AS h_avg_kills1,
                        AVG(CASE WHEN team_number = 0 THEN hs.avg_deaths ELSE NULL END) AS h_avg_deaths1,
                        AVG(CASE WHEN team_number = 0 THEN hs.avg_assists ELSE NULL END) AS h_avg_assists1,
                        AVG(CASE WHEN team_number = 0 THEN hs.games_played ELSE NULL END) AS h_avg_games_played1,
                        AVG(CASE WHEN team_number = 0 THEN hs.winrate ELSE NULL END) AS h_avg_winrate1,
                        AVG(CASE WHEN team_number = 1 THEN hs.avg_kills ELSE NULL END) AS h_avg_kills2,
                        AVG(CASE WHEN team_number = 1 THEN hs.avg_deaths ELSE NULL END) AS h_avg_deaths2,
                        AVG(CASE WHEN team_number = 1 THEN hs.avg_assists ELSE NULL END) AS h_avg_assists2,
                        AVG(CASE WHEN team_number = 1 THEN hs.games_played ELSE NULL END) AS h_avg_games_played2,
                        AVG(CASE WHEN team_number = 1 THEN hs.winrate ELSE NULL END) AS h_avg_winrate2,
                        AVG(CASE WHEN team_number = 0 THEN ps.avg_kills ELSE NULL END) AS p_avg_kills1,
                        AVG(CASE WHEN team_number = 0 THEN ps.avg_deaths ELSE NULL END) AS p_avg_deaths1,
                        AVG(CASE WHEN team_number = 0 THEN ps.avg_assists ELSE NULL END) AS p_avg_assists1,
                        AVG(CASE WHEN team_number = 0 THEN ps.games_played ELSE NULL END) AS p_avg_games_played1,
                        AVG(CASE WHEN team_number = 0 THEN ps.winrate ELSE NULL END) AS p_avg_winrate1,
                        AVG(CASE WHEN team_number = 1 THEN ps.avg_kills ELSE NULL END) AS p_avg_kills2,
                        AVG(CASE WHEN team_number = 1 THEN ps.avg_deaths ELSE NULL END) AS p_avg_deaths2,
                        AVG(CASE WHEN team_number = 1 THEN ps.avg_assists ELSE NULL END) AS p_avg_assists2,
                        AVG(CASE WHEN team_number = 1 THEN ps.games_played ELSE NULL END) AS p_avg_games_played2,
                        AVG(CASE WHEN team_number = 1 THEN ps.winrate ELSE NULL END) AS p_avg_winrate2
                    FROM players p
                    LEFT JOIN dota_ods.player_hero_stats phs 
                        ON phs.account_id = p.player_id
                        AND phs.hero_id = p.hero_id
                    LEFT JOIN dota_ods.hero_stats hs 
                        ON hs.hero_id = p.hero_id
                    LEFT JOIN dota_ods.player_stats ps
                        ON ps.account_id = p.player_id
                    WHERE team_number IN (0, 1)
                    GROUP BY
                        p.match_id)
                        
                    SELECT 
                        g.match_id,
                        radiant_name AS radiant_team,
                        dire_name AS dire_team,
                        EXTRACT(YEAR FROM game_dt)::int AS game_year,
                        EXTRACT(MONTH FROM game_dt)::int AS game_month,
                        EXTRACT(DAY FROM game_dt)::int AS game_day,
                        ts.total_matches_past_year AS t1_games_year,
                        ts.total_wins_past_year AS t1_wins_year,
                        ts2.total_matches_past_year AS t2_games_year,
                        ts2.total_wins_past_year AS t2_wins_year,
                        ts.total_matches_past_3months AS t1_games_3months,
                        ts.total_wins_past_3months AS t1_wins_3months,
                        ts2.total_matches_past_3months AS t2_games_3months,
                        ts2.total_wins_past_3months AS t2_wins_3months,
                        ts.total_matches_past_2weeks AS t1_games_2weeks,
                        ts.total_wins_past_2weeks AS t1_wins_2weeks,
                        ts2.total_matches_past_2weeks AS t2_games_2weeks,
                        ts2.total_wins_past_2weeks AS t2_wins_2weeks,
                        tvt.total_games,
                        tvt.total_wins,
                        avg_kills1,
                        avg_deaths1,
                        avg_assists1,
                        avg_games_played1,
                        avg_winrate1,
                        avg_kills2,
                        avg_deaths2,
                        avg_assists2,
                        avg_games_played2,
                        avg_winrate2,
                        h_avg_kills1,
                        h_avg_deaths1,
                        h_avg_assists1,
                        h_avg_games_played1,
                        h_avg_winrate1,
                        h_avg_kills2,
                        h_avg_deaths2,
                        h_avg_assists2,
                        h_avg_games_played2,
                        h_avg_winrate2,
                        p_avg_kills1,
                        p_avg_deaths1,
                        p_avg_assists1,
                        p_avg_games_played1,
                        p_avg_winrate1,
                        p_avg_kills2,
                        p_avg_deaths2,
                        p_avg_assists2,
                        p_avg_games_played2,
                        p_avg_winrate2
                    FROM game g
                    LEFT JOIN dota_ods.teams_stats ts 
                        ON ts.team_id = g.radiant_id
                    LEFT JOIN dota_ods.teams_stats ts2 
                        ON ts2.team_id = g.dire_id
                    LEFT JOIN dota_ods.team_vs_team tvt 
                        ON tvt.team_id = g.radiant_id
                        AND tvt.opp_id = g.dire_id
                    LEFT JOIN player_hero_stats phs
                        ON phs.match_id = g.match_id
                """)

                cursor.execute(query, (match_ids,))  
                column_names = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                stats = [dict(zip(column_names, row)) for row in rows]
                return {'stats': stats}
        except psycopg2.Error as e:
            logger.error(f'Error retrieving stats for match {match_ids}: {str(e)}')
            return {}


db = DatabaseService(db_name = db_name, user = user, password = password)

