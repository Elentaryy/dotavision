import pandas as pd
import logging
import json 
import os
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values, Json
from psycopg2 import OperationalError
from psycopg2.extensions import register_adapter, AsIs
from time import sleep

register_adapter(dict, Json)

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
                check_live_query = sql.SQL("SELECT match_id FROM dota_dds.pro_matches WHERE is_live = True")
                cursor.execute(check_live_query)
                games = [game[0] for game in cursor.fetchall()]
                logger.info(f'returning live matches -- {games}')
                return games
        except psycopg2.Error as e:
            logger.info(f'Error checking live games: {str(e)}')
            return []

    def check_live_series_exists(self, team1_id, team2_id, series_type):
        try:
            with self.connection.cursor() as cursor:
                check_series_query = sql.SQL("""
                    SELECT series_id FROM dota_dds.pro_series 
                    WHERE is_live = True AND series_type = %s AND
                    ((team1_id = %s AND team2_id = %s) OR (team1_id = %s AND team2_id = %s))
                """)
                cursor.execute(check_series_query, (series_type, team1_id, team2_id, team2_id, team1_id))
                series = cursor.fetchone()
                logger.info(series)
                return series[0] if series else None
        except psycopg2.Error as e:
            logger.info(f'Error checking series existence: {str(e)}')
            return None

    def create_series(self, team1_id, team2_id, series_type, team1_score, team2_score):
        try:
            with self.connection.cursor() as cursor:
                insert_series_query = sql.SQL("""
                    INSERT INTO dota_dds.pro_series (team1_id, team2_id, series_type, team1_score, team2_score, is_live)
                    VALUES (%s, %s, %s, %s, %s, True) RETURNING series_id
                """)
                cursor.execute(insert_series_query, (team1_id, team2_id, series_type, team1_score, team2_score))
                series_id = cursor.fetchone()[0]
                logger.info(f'Successfully created series {series_id}')
                return series_id
        except psycopg2.Error as e:
            logger.info(f'Error creating series: {str(e)}')
            return None

    def insert_game(self, match_id, series_id, match_data):
        try:
            with self.connection.cursor() as cursor:
                insert_game_query = sql.SQL("""
                    INSERT INTO dota_dds.pro_matches (match_id, series_id, match_data, is_live)
                    VALUES (%s, %s, %s, True)
                """)
                cursor.execute(insert_game_query, (match_id, series_id, match_data))
                logger.info(f'Successfully inserted match {match_id}')
        except psycopg2.Error as e:
            logger.info(f'Error inserting game: {str(e)}')

    def update_game(self, match_id, data, is_live):
        try:
            with self.connection.cursor() as cursor:
                update_game_query = sql.SQL("""
                    UPDATE dota_dds.pro_matches
                    SET is_live = %s,
                    match_data = %s
                    WHERE match_id = %s
                    RETURNING series_id
                """)
                cursor.execute(update_game_query, (is_live, data, match_id))
                series_id = cursor.fetchone()[0]
                logger.info(f'Successfully updated game {match_id}')
                return series_id
        except psycopg2.Error as e:
            logger.info(f'Error updating game: {str(e)}')

    def check_and_update_series_status(self, series_id, winner):
        try:
            with self.connection.cursor() as cursor:
                select_series_query = sql.SQL("""
                    SELECT team1_id, team2_id, team1_score, team2_score, series_type FROM dota_dds.pro_series WHERE series_id = %s
                """)
                cursor.execute(select_series_query, (series_id,))
                team1_id, team2_id, team1_score, team2_score, series_type = cursor.fetchone()

                team1_points = 1 if winner == team1_id else 0
                team2_points = 1 if winner == team2_id else 0
                team1_score, team2_score = team1_score + team1_points, team2_score + team2_points

                is_live = 1
                if series_type == 1:
                    if team1_score >= 2 or team2_score >= 2:
                        is_live = False
                    else:
                        is_live = True
                elif series_type == 0:
                    if team1_score >= 1 or team2_score >= 1:
                        is_live = False
                    else:
                        is_live = True

                update_series_query = sql.SQL("""
                    UPDATE dota_dds.pro_series 
                    SET is_live = %s,
                    team1_score = %s, 
                    team2_score = %s
                    WHERE series_id = %s
                """)
                cursor.execute(update_series_query, (is_live, team1_score, team2_score, series_id))
                logger.info(f'Successfully update series {series_id}')
        except psycopg2.Error as e:
            logger.info(f'Error updating series status: {str(e)}')