import pandas as pd
import logging
import warnings
from datetime import datetime
warnings.filterwarnings('ignore')

logger = logging.getLogger('utils')

def check_complete_drafts(match):
    team_counts = {0: 0, 1: 0}  
        
    for player in match['players']:
        if player['hero_id'] != 0 and player['team'] in [0, 1]:
            team_counts[player['team']] += 1
        
    if team_counts[0] >= 5 and team_counts[1] >= 5:
        return True
    
    return False
     

def format_data(data):
    try:
        df = pd.DataFrame(data['games'])
        df['radiant_team'] = df['match_data'].apply(lambda x: x.get('radiant_team').get('team_name'))
        df['dire_team'] = df['match_data'].apply(lambda x: x.get('dire_team').get('team_name'))
        df['day']= datetime.today().strftime('%d')
        df['month']= datetime.today().strftime('%m')
        df['year']= datetime.today().strftime('%Y')

        df = df.drop(columns = ['match_id'])
        

        all_hero_ids = set(range(1, 141))
        for team_number in range(2):
            for hero_id in all_hero_ids:
                    column_name = f'hero_{hero_id}_team_{team_number}'
                    df[column_name] = df['match_data'].apply(lambda x: 1 if any(player.get('hero_id') == hero_id \
                                            and player.get('team') == team_number for player in x.get('players', {})) else 0)
                
        team1_cols = [f'hero_{hero_id}_team_0' for hero_id in all_hero_ids]
        team2_cols = [f'hero_{hero_id}_team_1' for hero_id in all_hero_ids]

        mask = (df[team1_cols].sum(axis=1) == 5) & (df[team2_cols].sum(axis=1) == 5)

        df = df[mask]
                
        df = df.drop(columns=['match_data'])
        
        return df
    except Exception as e:
        logger.info(f'Something went wrong {str(e)}')
        return None