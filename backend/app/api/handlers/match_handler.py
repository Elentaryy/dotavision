from fastapi import HTTPException
from services.db_service import DatabaseService
from dotenv import load_dotenv
from datetime import date
import logging
import os

logger = logging.getLogger('match_handler')

load_dotenv()

db_name = os.getenv('POSTGRES_DB')
user = os.getenv('POSTGRES_USER')
password = os.getenv('POSTGRES_PASSWORD')

db = DatabaseService(db_name = db_name, user = user, password = password)


def get_match_info(match_id : int):
    try:
        data = db.get_game_statuses(match_id)
        return data 
    except Exception as e:
        logger.info(f'Something went wrong while querying historical series {str(e)}')
        raise HTTPException(status_code=500, detail="Something went wrong")