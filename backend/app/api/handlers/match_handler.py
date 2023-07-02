from fastapi import HTTPException
from services.db_service import db
from dotenv import load_dotenv
from datetime import date
import logging
import os

logger = logging.getLogger('match_handler')

def get_match_info(match_id : int):
    try:
        data = db.get_game_statuses(match_id)
        return data 
    except Exception as e:
        logger.info(f'Something went wrong while querying historical series {str(e)}')
        raise HTTPException(status_code=500, detail="Something went wrong")