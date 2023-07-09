from fastapi import HTTPException
from services.db_service import db
import logging
import pandas as pd
from utils import format_data

import pickle

logger = logging.getLogger('live_handler')

with open('model/models/heroes_xgb_pub.pkl', 'rb') as f:
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
        data = db.get_live_matches()
        logger.info(f'Data -- {data}, len(data)' )
        if data.get('games'):
            data = format_data(data)  
            teams = data[['radiant_team', 'dire_team']]
            pred = data.drop(columns = ['radiant_team', 'dire_team']).values
            logger.info(f'Data2 -- {data}, teams -- {teams}, pred -- {pred}')
            teams['prediction'] = xgb.predict(pred)
            teams['proba'] = xgb.predict_proba(pred).max(axis=1)
            return teams.to_dict(orient="records")
        else:
            return {}
            

    except Exception as e:
        logger.info(f'Something went wrong while making a prediction {str(e)}')
        raise HTTPException(status_code=500, detail="Something went wrong")
        
    
