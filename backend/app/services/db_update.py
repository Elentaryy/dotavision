from services.db_service import db
from services.dota_service import ds
from services.google_service import gs
from utils import check_complete_drafts
from model.predict import predict_heroes, predict_teams
from typing import List, Any, Dict, Optional
from datetime import date
import logging
import pandas as pd
from time import sleep

logger = logging.getLogger('db_update')

MAX_ITERATIONS = 100
SLEEP_TIME = 1

def add_league_names(df):
    match_ids = df['match_id'].unique().tolist()
    league_names_df = db.get_league_names(match_ids)

    df = df.merge(league_names_df, on='match_id', how='left')

    cols = df.columns.tolist()
    cols.insert(1, cols.pop(cols.index('league_name')))
    df = df.reindex(columns=cols)

    return df

def get_live_match_ids_from_db():
    return [game['match_id'] for game in db.get_live_matches()['games']]

def get_complete_games(data: dict) -> List[dict]:
    return [game for game in data['result']['games'] if game.get('radiant_team') and game.get('dire_team')]

def get_match_ids(games: List[dict]) -> List[int]:
    return [game['match_id'] for game in games]

def get_predicted_match_ids() -> List[int]:
    return [game['match_id'] for game in db.get_live_predictions()['matches']]

def filter_games_to_predict(games: List[dict], predicted_ids: List[int], allowed_leagues: List[int]) -> List[dict]:
    return [game for game in games 
            if check_complete_drafts(game) 
            and game['match_id'] not in predicted_ids 
            and game.get('league_id') in allowed_leagues]

def update_db_with_live_matches(games: List[dict], db_data: List[int]):
    for game in games:
        try:
            if game['match_id'] not in db_data and game['match_id'] != 0:
                db.create_match(match_id=game['match_id'], series_id=None, match_data=game)
            if game['match_id'] in db_data:
                db.update_match(match_id=game['match_id'], data=game, is_live=True)
        except Exception as e:
            logger.error(f'DB Error - {str(e)}')

def make_and_save_predictions(games_ids_to_predict: List[int], games_to_predict: List[dict]):
    df_heroes = pd.DataFrame({'match_id': games_ids_to_predict, 'match_data': games_to_predict})
    heroes_predictions = pd.DataFrame(predict_heroes(df_heroes))

    df_teams = pd.DataFrame(db.get_stats_for_prediction(games_ids_to_predict).get('stats'))        
    df_teams['probability'] = heroes_predictions['probability'].where(heroes_predictions['prediction']==1, 1-heroes_predictions['probability'])
    teams_predictions = pd.DataFrame(predict_teams(df_teams))
 
    db.create_predictions(pd.concat((heroes_predictions, teams_predictions)).to_dict(orient='records'))

    google_predictions = add_league_names(heroes_predictions)
    google_predictions['date'] = date.today().strftime('%Y-%m-%d')

    try:
        gs.write_prediction(google_predictions, 'DotaVision')
    except Exception as e:
        logger.error(f'Error writing prediction to google sheet {str(e)}')

def update_ended_games(db_data: List[int], game_ids: List[int], allowed_leagues: List[int]):
    for game_id in db_data:
        if game_id not in game_ids:
            match_info = fetch_match_info(game_id)
            if match_info is not None:
                result = 1 if match_info['result'].get('radiant_win') else 0 
                db.update_match(match_id=game_id, data=match_info['result'], is_live=False)  
                if match_info['result'].get('leagueid') in allowed_leagues:
                    db.update_predictions(match_id=game_id, result=result)
                    gs.update_result(match_id=game_id, result=result, sheet_name='DotaVision')

def fetch_match_info(game_id: int) -> Optional[dict]:
    try:
        return ds.get_match_info(str(game_id))
    except Exception as e:
        logger.error(f'GGs Error - {str(e)}')
        return None
    
def check_live_matches():
    
    data =  ds.get_live_matches()
   
    if data:
        db_data = get_live_match_ids_from_db()
        games = get_complete_games(data)
        game_ids = get_match_ids(games)
        allowed_leagues = db.get_allowed_leagues()
        predicted_ids = get_predicted_match_ids()
        games_to_predict = filter_games_to_predict(games, predicted_ids, allowed_leagues)
        games_ids_to_predict = get_match_ids(games_to_predict)
        update_db_with_live_matches(games, db_data)

        if games_ids_to_predict:
            make_and_save_predictions(games_ids_to_predict, games_to_predict)
            
        update_ended_games(db_data, game_ids, allowed_leagues)

def transform_teams(team):
    return [int(hero) for hero in team.split(',')]

def update_public_matches():
    try:
        last_pro_match = db.get_max_value('dota_dds.pro_matches', 'match_id')
        last_public_match = db.get_max_value('dota_dds.public_matches', 'match_id')

        data = []
        for _ in range(MAX_ITERATIONS):
            try:
                sleep(SLEEP_TIME)
                match_data = ds.get_public_matches(last_pro_match)
                if any(match['match_id'] <= last_public_match for match in match_data):
                    break
                
                data.extend(match_data)
                last_pro_match = data[-1]['match_id']
            except Exception as e:
                logger.error(f'Exception while fetching data for match_id {last_pro_match}. Error: {str(e)}')
                break

        data = [{k: v for k, v in match.items() if k in ['match_id', 'start_time', 'duration', 'game_mode', 'avg_rank_tier', 'radiant_team', 'dire_team', 'radiant_win']} for match in data]

        df = pd.DataFrame(data)
        df['radiant_team'] = df['radiant_team'].apply(transform_teams)
        df['dire_team'] = df['dire_team'].apply(transform_teams)

        db.insert_public_matches(df.to_dict(orient='records'))

    except Exception as e:
        logger.error(f'Update public matches error: {str(e)}')






            






    


