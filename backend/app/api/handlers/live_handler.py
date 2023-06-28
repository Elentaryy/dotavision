from fastapi import HTTPException
from services.db_service import DatabaseService
from dotenv import load_dotenv
import logging
import os
import pandas as pd
from utils import format_data

import pickle

logger = logging.getLogger('live_handler')

load_dotenv()

db_name = os.getenv('POSTGRES_DB')
user = os.getenv('POSTGRES_USER')
password = os.getenv('POSTGRES_PASSWORD')

db = DatabaseService(db_name = db_name, user = user, password = password)

with open('model/models/heroes_xgb.pkl', 'rb') as f:
    xgb = pickle.load(f)

with open('model/models/heroes_lr.pkl', 'rb') as f:
    lr = pickle.load(f)


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
    
def predict_live_games():
    try:
        data = format_data(db.get_live_matches())

        if data is not None:
            teams = data[['radiant_team', 'dire_team']]
            pred = data.drop(columns = ['radiant_team', 'dire_team', 'match_data']).values
            logger.info(data.drop(columns = ['radiant_team', 'dire_team', 'match_data']).columns)

            logger.info(data)
            teams['prediction'] = xgb.predict(pred)
            return teams.to_dict(orient="records")
        else:
            raise ValueError('wrong data format')
            

    except Exception as e:
        logger.info(f'Something went wrong while making a prediction {str(e)}')
        raise HTTPException(status_code=500, detail="Something went wrong")
        
    
