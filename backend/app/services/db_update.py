import os
from services.db_service import DatabaseService
from services.dota_service import DotaService
from dotenv import load_dotenv
import logging

logger = logging.getLogger('db_update')

load_dotenv()

api_key = os.getenv('STEAM_API_KEY')
db_name = os.getenv('POSTGRES_DB')
user = os.getenv('POSTGRES_USER')
password = os.getenv('POSTGRES_PASSWORD')


db = DatabaseService(db_name = db_name, user = user, password = password)
ds = DotaService(api_key=api_key)



def check_live_matches():
    print('we cool')
    data =  ds.get_live_matches()
    print('we suck')
    db_data = db.get_live_matches()
    print('we dog')
    games = [game for game in data['result']['games']]

    for game in games:
        if game.get('radiant_team') and game.get('dire_team'):
            logger.info(game)
            if game['match_id'] not in db_data:
                series_id = db.check_live_series_exists(game['radiant_team']['team_id'], game['dire_team']['team_id'], game['series_type'])
                if series_id is None:
                    series_id = db.create_series(game['radiant_team']['team_id'], game['dire_team']['team_id'], game['series_type'])
                db.insert_game(match_id = game['match_id'], series_id = series_id, match_data = game)
    
    for game_id in db_data:
        if game_id not in games:
            match_info = ds.get_match_info(game_id)
            if match_info is not None:
                series_id = db.update_game(game_id, match_info['result'])
                winner = match_info['result']['radiant_team_id'] if match_info['result']['radiant_win'] else match_info['result']['dire_team_id']

                db.check_and_update_series_status(series_id = series_id, winner = winner)


            






    


