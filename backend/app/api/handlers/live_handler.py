from services.db_service import DatabaseService
from dotenv import load_dotenv
import logging
import os

logger = logging.getLogger('get_live_matches')

load_dotenv()

db_name = os.getenv('POSTGRES_DB')
user = os.getenv('POSTGRES_USER')
password = os.getenv('POSTGRES_PASSWORD')

def get_live_games_info():
    db = DatabaseService(db_name = db_name, user = user, password = password)
    try:
        data = db.get_live_matches()
        return data 
    except Exception as e:
        logger.info(f'Something went wrong while querying live match info {str(e)}')
        return {"error": "Something went wrong"}