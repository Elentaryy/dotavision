import os
from services.db_service import db
from services.dota_service import ds
from utils import check_complete_drafts
from model.predict import predict_heroes
import logging
import pickle
import pandas as pd
from time import sleep

logger = logging.getLogger('db_update')


def check_live_matches():
    
    data =  ds.get_live_matches() # Live dota 2 matches via API
    db_data = [game['match_id'] for game in db.get_live_matches()['games']] # Live dota matches in DB

    games = [game for game in data['result']['games'] if game.get('radiant_team') and game.get('dire_team')] #and game.get('league_id') == 15438
    game_ids = [game['match_id'] for game in games]
    
    predicted_ids = [game['match_id'] for game in db.get_live_predictions()['predictions']] # Games which are already predicted

    games_to_predict = [game for game in games if check_complete_drafts(game) and game['match_id'] not in predicted_ids] # Live games via API which finished drafting and dont have a prediction yet
    games_ids_to_predict = [game['match_id'] for game in games_to_predict] 

    if games_ids_to_predict:
        df = pd.DataFrame({'match_id' : games_ids_to_predict, 'match_data' : games_to_predict})
        db.create_predictions(predict_heroes(df))
        
    for game in games:
        if game['match_id'] not in db_data and game['match_id'] != 0:
            db.create_match(match_id = game['match_id'], series_id = None, match_data = game)
        if game['match_id'] in db_data:
            db.update_match(match_id = game['match_id'], data = game, is_live = True)
            if game.get('scoreboard'):
                if game.get('scoreboard').get('duration'):
                    game_data = {i:game[i] for i in game if i!='players'}
                    db.add_match_status(match_id = game['match_id'], match_data = game_data, ingame_dttm = int(game.get('scoreboard').get('duration')))
                   
    for game_id in db_data:
        if game_id not in game_ids:
            match_info = ds.get_match_info(str(game_id))
            if match_info is not None:
                result = 1 if match_info['result'].get('radiant_win') == True else 0 
                db.update_match(match_id = game_id, data = match_info['result'], is_live = False)            
                db.update_predictions(match_id = game_id, result = result)

def update_public_matches():
    last_pro_match = db.get_max_value('dota_dds.pro_matches', 'match_id')
    last_public_match = db.get_max_value('dota_dds.public_matches', 'match_id')

    data = []
    for _ in range(100):
        try:
            sleep(1)
            match_data = ds.get_public_matches(last_pro_match)

            if match_data:
                if any(match['match_id'] <= last_public_match for match in match_data):
                    break
                else:
                    data.extend(match_data)
                    last_pro_match = data[-1]['match_id']
        except Exception as e:
            logger.info(f'Something went wrong while fetching new data {str(e)}')
            break
    
    df = pd.DataFrame(data)
    df = df[['match_id', 'start_time', 'duration', 'game_mode', 'avg_rank_tier', 'radiant_team', 'dire_team', 'radiant_win']]
    df['radiant_team'] = df['radiant_team'].apply(lambda x: [int(hero) for hero in x.split(',')])
    df['dire_team'] = df['dire_team'].apply(lambda x: [int(hero) for hero in x.split(',')])
    data = df.to_dict(orient='records')

    db.insert_public_matches(data)






            






    


