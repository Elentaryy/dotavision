from services.db_service import db
from services.dota_service import ds
from services.google_service import gs
from utils import check_complete_drafts
from model.predict import predict_heroes, predict_teams
import logging
import pandas as pd
from time import sleep

logger = logging.getLogger('db_update')

def add_league_names(df):
    match_ids = df['match_id'].unique().tolist()

    # Fetch league names for these match_ids
    league_names_df = db.get_league_names(match_ids)

    df = df.merge(league_names_df, on='match_id', how='left')

    cols = df.columns.tolist()
    cols.insert(1, cols.pop(cols.index('league_name')))
    df = df.reindex(columns=cols)

    return df

def check_live_matches():
    
    data =  ds.get_live_matches() # Live dota 2 matches via API
    db_data = [game['match_id'] for game in db.get_live_matches()['games']] # Live dota matches in DB
    games = [game for game in data['result']['games'] if game.get('radiant_team') and game.get('dire_team')] #and game.get('league_id') == 15438
    game_ids = [game['match_id'] for game in games]
    
    allowed_leagues = db.get_allowed_leagues() #Leagues to predict
    
    predicted_ids = [game['match_id'] for game in db.get_live_predictions()['matches']] # Games which are already predicted
    games_to_predict = [game for game in games if check_complete_drafts(game) and game['match_id'] not in predicted_ids and game.get('league_id') in allowed_leagues] # Live games via API which finished drafting and dont have a prediction yet
    games_ids_to_predict = [game['match_id'] for game in games_to_predict] 

    if games_ids_to_predict:
        df_heroes = pd.DataFrame({'match_id' : games_ids_to_predict, 'match_data' : games_to_predict}) #Heroes df to predict using heroes model
        heroes_predictions = pd.DataFrame(predict_heroes(df_heroes))

        df_teams = pd.DataFrame(db.get_stats_for_prediction(games_ids_to_predict).get('stats'))
        
        df_teams['probability'] = heroes_predictions['probability'].where(heroes_predictions['prediction']==1, 1-heroes_predictions['probability'])

        teams_predictions = pd.DataFrame(predict_teams(df_teams))

        google_predictions = add_league_names(teams_predictions)
        gs.write_prediction(google_predictions, 'DotaVision')
        
        db.create_predictions(heroes_predictions.to_dict(orient='records'))
        db.create_predictions(teams_predictions.to_dict(orient='records'))
     
    for game in games:
        if game['match_id'] not in db_data and game['match_id'] != 0:
            db.create_match(match_id = game['match_id'], series_id = None, match_data = game)
        if game['match_id'] in db_data:
            db.update_match(match_id = game['match_id'], data = game, is_live = True)
            if game.get('scoreboard'):
                if game.get('scoreboard').get('duration'):
                    game_data = {i:game[i] for i in game if i!='players'}
                    #db.add_match_status(match_id = game['match_id'], match_data = game_data, ingame_dttm = int(game.get('scoreboard').get('duration')))
                   
    for game_id in db_data:
        if game_id not in game_ids:
            match_info = ds.get_match_info(str(game_id))
            if match_info is not None:
                result = 1 if match_info['result'].get('radiant_win') == True else 0 
                db.update_match(match_id = game_id, data = match_info['result'], is_live = False)  
                if match_info['result'].get('leagueid') in allowed_leagues: 
                    logger.info('result')       
                    db.update_predictions(match_id = game_id, result = result)
                    gs.update_result(match_id = game_id, result = result, sheet_name = 'DotaVision')

def update_public_matches():
    last_pro_match = db.get_max_value('dota_dds.pro_matches', 'match_id')
    last_public_match = db.get_max_value('dota_dds.public_matches', 'match_id')

    data = []
    for _ in range(10):
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






            






    


