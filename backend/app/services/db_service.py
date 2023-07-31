import os
import logging
import psycopg2
from psycopg2 import sql, OperationalError
from psycopg2.extras import execute_values, Json, DictCursor
from psycopg2.extensions import register_adapter
from time import sleep
from dotenv import load_dotenv
from collections import defaultdict
import pandas as pd

register_adapter(dict, Json)
load_dotenv()

logger = logging.getLogger('db_service')

class DatabaseService:
    MAX_RETRIES = 6

    def __init__(self, host='db', port='5432'):
        self.connection_parameters = {
            'dbname': os.getenv('POSTGRES_DB'),
            'user': os.getenv('POSTGRES_USER'),
            'password': os.getenv('POSTGRES_PASSWORD'),
            'host': host,
            'port': port,
        }
        self.connection = None
        self.connect()

    def connect(self):
        for i in range(1, self.MAX_RETRIES + 1):
            try:
                self.connection = psycopg2.connect(**self.connection_parameters)
                self.connection.autocommit = True
                logger.info('Successfully connected to db')
                return
            except OperationalError as e:
                logger.error(f'Error {str(e)} occured, trying to reconnect in {i**2} seconds')
                sleep(i**2)

        raise Exception("Unable to connect to the database after several attempts")

    def close(self):
        if self.connection:
            self.connection.close()
            logger.info('Connection to the database has been closed')

    def _execute_query(self, query, method, error_message, params=None):
        try:
            with self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                if method == 'commit':
                    cursor.execute(query, params)
                    self.connection.commit()
                elif method == 'commitmany':
                    cursor.executemany(query, params)
                    self.connection.commit()
                elif method == 'fetchall':
                    cursor.execute(query, params)
                    return cursor.fetchall()
                elif method == 'fetchone':
                    cursor.execute(query, params)
                    return cursor.fetchone()
        except psycopg2.Error as e:
            logger.error(f'{error_message}: {str(e)}')
            return []
        
    def _execute_batch_query(self, query, params, error_message):
        try:
            with self.connection.cursor() as cursor:
                psycopg2.extras.execute_batch(cursor, query, params)
                self.connection.commit()
                return True
        except psycopg2.Error as e:
            logger.error(f'{error_message}: {str(e)}')
            return None

    def get_live_matches(self):
        query = """
            SELECT 
                match_id, 
                match_data 
            FROM dota_dds.pro_matches 
            WHERE is_live = True
        """
        games = self._execute_query(query, 'fetchall', 'Error checking live games')
        return {'games': games}

    def create_match(self, match_id, series_id, match_data):
        query = """
            INSERT INTO dota_dds.pro_matches (match_id, series_id, match_data, is_live)
            VALUES (%s, %s, %s, True)
        """
        self._execute_query(query, 'commit', 'Error created game', (match_id, series_id, match_data))
        logger.info(f'Successfully created match {match_id}')

    def update_match(self, match_id, data, is_live):
        query = """
            UPDATE dota_dds.pro_matches
            SET is_live = %s,
                match_data = %s
            WHERE match_id = %s
        """
        self._execute_query(query, 'commit', 'Error updating game', (is_live, data, match_id))

    def get_match_info(self, match_id):
        query = """
            SELECT match_id, match_data
            FROM dota_dds.pro_matches
            WHERE match_id = %s
        """
        result = self._execute_query(query, 'fetchone', 'Error fetching match info', (match_id,))
        return result if result else {}
    
    def add_match_status(self, match_id, match_data, ingame_dttm):
        query = """
            INSERT INTO dota_dds.pro_matches_statuses (match_id, match_data, ingame_dttm)
            SELECT %s, %s, %s
            WHERE NOT EXISTS (
                SELECT 1 FROM dota_dds.pro_matches_statuses
                WHERE match_id = %s AND ingame_dttm = %s
            )
        """
        self._execute_query(query, 'commit', 'Error created game', (match_id, match_data, ingame_dttm, match_id, ingame_dttm))

    def get_match_statuses(self, match_id):
        query = """
            SELECT match_id, match_data, ingame_dttm 
            FROM dota_dds.pro_matches_statuses 
            WHERE match_id = %s
            ORDER BY ingame_dttm ASC
        """
        result = self._execute_query(query, 'fetchall', 'Error fetching game statuses', match_id)
        return {'match_id': match_id, 'statuses': result}

    def create_predictions(self, predictions):
        query = """
            INSERT INTO dota_ods.predictions (match_id, radiant_team, dire_team, model, prediction, probability)
            VALUES (%(match_id)s, %(radiant_team)s, %(dire_team)s, %(model)s, %(prediction)s, %(probability)s)
        """
        self._execute_query(query, 'commitmany', 'Error creating game', predictions)
        logger.info(f'Successfully predicted matches {[match["match_id"] for match in predictions]}')

    def update_predictions(self, match_id, result):
        query = """
            UPDATE dota_ods.predictions
            SET result = %s
            WHERE match_id = %s
        """
        self._execute_query(query, 'commit', 'Error updating game', (result, match_id))
        logger.info(f'Successfully updated result in predictions {match_id}')

    def get_predictions(self, query, params=None, is_match=False):
        rows = self._execute_query(query, 'fetchall', 'Error returning predictions', params)
        
        matches = {}
        for row in rows:
            match_id = row['match_id']
            radiant_team = row['radiant_team']
            dire_team = row['dire_team']
            league_name = row['league_name']
            model = row['model']
            prediction = row['prediction']
            probability = row['probability']
            result = row['result'] if is_match else None
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
            FROM 
                dota_ods.predictions p
                INNER JOIN dota_dds.pro_matches pm ON p.match_id = pm.match_id AND pm.is_live = True
                INNER JOIN dota_dds.leagues l ON l.league_id = (pm.match_data ->> 'league_id')::int AND l.allowed = True
            WHERE 
                p.raw_dt >= current_date - INTERVAL '1 day'
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
            FROM 
                dota_ods.predictions p
                LEFT JOIN dota_dds.pro_matches pm ON p.match_id = pm.match_id
                INNER JOIN dota_dds.leagues l ON l.league_id = COALESCE((pm.match_data ->> 'league_id')::int, (pm.match_data ->> 'leagueid')::int) AND l.allowed = True
            WHERE 
                p.match_id = %s
        """)

        predictions = self.get_predictions(match_query, (match_id,), is_match=True)
        return predictions[0] if predictions else None


    def get_max_value(self, table, column):
        schema, table = table.split('.')
        query = sql.SQL("SELECT MAX({}) FROM {}.{}").format(sql.Identifier(column), sql.Identifier(schema), sql.Identifier(table))
        max_value = self._execute_query(query, 'fetchone', 'Error retrieving maximum value')
        return max_value[column] if max_value else None
        
    def get_tournaments_stats(self):
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

        rows = self._execute_query(query, 'fetchall', 'Error retrieving tournament stats')
        stats = defaultdict(lambda: {'predictions': []})

        for row in rows:
            league_name = row['league_name']
            model_name = row['model']
            total_games = row['total_games']
            total_correct = row['total_correct']
            total_incorrect = row['total_incorrect']
            winrate = row['winrate']

            stats[league_name]['predictions'].append({
                'model_name': model_name,
                'total_games': total_games,
                'total_correct': total_correct,
                'total_incorrect': total_incorrect,
                'winrate': winrate
            })

        return {'tournaments': [{'league_name': k, 'predictions': v['predictions']} for k, v in stats.items()]}

    def get_recent_stats(self):
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

        result = self._execute_query(query, 'fetchone', 'Error retrieving recent stats')
        if not result:
            return None
        if result['total_predictions'] == 0:
            raise ValueError("No predictions were made.")
        return result
        
    def insert_public_matches(self, matches):
        insert_query = sql.SQL("""
            INSERT INTO dota_dds.public_matches (match_id, start_time, duration, game_mode, avg_rank_tier, radiant_team, dire_team, radiant_win, raw_dt)
            VALUES (%(match_id)s, %(start_time)s, %(duration)s, %(game_mode)s, %(avg_rank_tier)s, %(radiant_team)s, %(dire_team)s, %(radiant_win)s, CURRENT_DATE)
            ON CONFLICT (match_id) DO NOTHING
        """)
        return self._execute_batch_query(insert_query, matches, 'Error inserting new matches')

    def get_allowed_leagues(self):
        query = sql.SQL("""
            SELECT league_id
            FROM dota_dds.leagues
            WHERE allowed = True
        """)
        rows = self._execute_query(query, 'fetchall', 'Error retrieving allowed leagues')
        return [row['league_id'] for row in rows] if rows else []

    def get_league_names(self, match_ids):
        query = sql.SQL("""
            SELECT
                pm.match_id,
                l.league_name
            FROM dota_dds.pro_matches pm
            LEFT JOIN dota_dds.leagues l 
                ON l.league_id = COALESCE(pm.match_data ->> 'leagueid', pm.match_data ->> 'league_id')::int
            WHERE match_id IN (SELECT unnest(%s))
        """)
        rows = self._execute_query(query, 'fetchall', 'Error fetching league names', (match_ids,))
        return pd.DataFrame(rows) if rows else pd.DataFrame()

    def refresh_materialized_views(self):
        view_names = ['dota_ods.hero_stats',
                    'dota_ods.player_hero_stats',
                    'dota_ods.player_stats',
                    'dota_ods.teams_stats',
                    'dota_ods.team_vs_team']
        for view_name in view_names:
            query = f"REFRESH MATERIALIZED VIEW {view_name}"
            if not self._execute_query(query, 'commit', f'Error refreshing materialized view {view_name}'):
                return
        logger.info("Successfully refreshed all materialized views.")
        
    def get_stats_for_prediction(self, match_ids):
    
        query = """
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
        """
        error_message = f'Error retrieving stats for matches {match_ids}'
        rows = self._execute_query(query, 'fetchall', error_message, [match_ids])

        if rows:
            stats = rows
            return {'stats': stats}
        else:
            return {}



db = DatabaseService()

