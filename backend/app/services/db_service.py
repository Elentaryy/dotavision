import logging
import os
from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values, Json
from psycopg2 import OperationalError
from psycopg2.extensions import register_adapter, AsIs
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
            logger.info(f'Error checking live games: {str(e)}')
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
            logger.info(f'Error created game: {str(e)}')

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
            logger.info(f'Error updating game: {str(e)}')

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
            logger.info(f'Error created game: {str(e)}')

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
            logger.info(f'Error fetching game statuses: {str(e)}')
            return {}
        
    def get_live_predictions(self):
        try:
            with self.connection.cursor() as cursor:
                check_predictions_query = sql.SQL("""
                    SELECT 
                        p.match_id, 
                        model,
                        prediction,
                        probability
                    FROM dota_ods.predictions p
                    INNER JOIN dota_dds.pro_matches pm
                        ON p.match_id = pm.match_id
                        AND pm.is_live = True
                """)
                cursor.execute(check_predictions_query)
                predictions = [{'match_id': row[0], 'model': row[1], 'prediction' : row[2], 'probability' : row[3]} for row in cursor.fetchall()]
                return {'predictions': predictions}
        except psycopg2.Error as e:
            logger.info(f'Error returning predictions: {str(e)}')
            return {}
        
    def create_predictions2(self, match_id, radiant_team, dire_team, model, prediction, probability):
        try:
            with self.connection.cursor() as cursor:
                insert_game_query = sql.SQL("""
                    INSERT INTO dota_ods.predictions (match_id, radiant_team, dire_team, model, prediction, probability)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """)
                cursor.execute(insert_game_query, (match_id, radiant_team, dire_team, model, prediction, probability))
                logger.info(f'Successfully created prediction {match_id}')
        except psycopg2.Error as e:
            logger.info(f'Error created game: {str(e)}')

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
            logger.info(f'Error created game: {str(e)}')

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
            logger.info(f'Error updating game: {str(e)}')

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
            logger.info(f'Error retrieving maximum value: {str(e)}')
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
            logger.info(f'Error inserting new matches: {str(e)}')

    

    
    


db = DatabaseService(db_name = db_name, user = user, password = password)

