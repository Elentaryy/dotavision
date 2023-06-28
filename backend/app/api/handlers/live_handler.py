from fastapi import HTTPException
from services.db_service import DatabaseService
from dotenv import load_dotenv
import logging
import os

logger = logging.getLogger('live_handler')

load_dotenv()

db_name = os.getenv('POSTGRES_DB')
user = os.getenv('POSTGRES_USER')
password = os.getenv('POSTGRES_PASSWORD')

db = DatabaseService(db_name = db_name, user = user, password = password)


def get_live_games_info():
    try:
        data = db.get_live_matches()
        return data 
    except Exception as e:
        logger.info(f'Something went wrong while querying live match info {str(e)}')
        raise HTTPException(status_code=500, detail="Something went wrong")
    
def get_live_series_info():
    try:
        data = db.get_live_series()
        return data 
    except Exception as e:
        logger.info(f'Something went wrong while querying live series info {str(e)}')
        raise HTTPException(status_code=500, detail="Something went wrong")