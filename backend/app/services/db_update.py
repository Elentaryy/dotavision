import os
from services.db_service import db
from services.dota_service import ds
import logging

logger = logging.getLogger('db_update')

def check_live_matches():

    data =  ds.get_live_matches()
    db_data = [game['match_id'] for game in db.get_live_matches()['games']]

    games = [game for game in data['result']['games'] if game.get('radiant_team') and game.get('dire_team') and game.get('league_id') == 15438]
    game_ids = [game['match_id'] for game in games]

    for game in games:
        if game['match_id'] not in db_data:
            logger.info(game['match_id'])
            logger.info(f'Current live db games - {db_data}')
            series_id = db.check_live_series_exists(game['radiant_team']['team_id'], game['dire_team']['team_id'], game['series_type'])
            if series_id is None:
                series_id = db.create_series(game['radiant_team']['team_id'], 
                                             game['dire_team']['team_id'], 
                                             game['series_type'], 
                                             game['radiant_series_wins'],
                                             game['dire_series_wins'])
            db.create_game(match_id = game['match_id'], series_id = series_id, match_data = game)

        if game['match_id'] in db_data:
            series_id = db.update_game(match_id = game['match_id'], data = game, is_live = True)
            if game['scoreboard'].get('duration'):
                game_data = {i:game[i] for i in game if i!='players'}
                #db.add_game_status(match_id = game['match_id'], match_data = game_data, ingame_dttm = int(game['scoreboard'].get('duration')))
    

    for game_id in db_data:
        if game_id not in game_ids:
            logger.info(f'games ---- {game_ids}, game_id ---- {game_id}')
            match_info = ds.get_match_info(str(game_id))
            if match_info is not None:
                series_id = db.update_game(match_id = game_id, data = match_info['result'], is_live = False)
                winner = match_info['result']['radiant_team_id'] if match_info['result']['radiant_win'] else match_info['result']['dire_team_id']

                #db.check_and_update_series_status(series_id = series_id, winner = winner)


            






    


