from fastapi import HTTPException
from services.db_service import DatabaseService
from dotenv import load_dotenv
from datetime import date
import logging
import os

logger = logging.getLogger('history_handler')

load_dotenv()

db = DatabaseService()


def get_series_info(dt: date):
    dt_str = dt.strftime('%Y-%m-%d')
    try:
        data = db.get_series(dt_str)
        return data 
    except Exception as e:
        logger.info(f'Something went wrong while querying historical series {str(e)}')
        raise HTTPException(status_code=500, detail="Something went wrong")
    
