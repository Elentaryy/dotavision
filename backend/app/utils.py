import pandas as pd
import logging
import warnings
import ast
from datetime import datetime
warnings.filterwarnings('ignore')

logger = logging.getLogger('utils')

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
        for hero_id in all_hero_ids:
            for team_number in range(2):
                column_name = f'hero_{hero_id}_team_{team_number}'
                df[column_name] = df['match_data'].apply(lambda x: 1 if any(player.get('hero_id') == hero_id \
                                        and player.get('team') == team_number for player in x.get('players', {})) else 0)
                
        team1_cols = [f'hero_{hero_id}_team_0' for hero_id in all_hero_ids]
        team2_cols = [f'hero_{hero_id}_team_1' for hero_id in all_hero_ids]

        # Create a mask for rows with exactly 5 heroes in each team
        mask = (df[team1_cols].sum(axis=1) == 5) & (df[team2_cols].sum(axis=1) == 5)

        # Apply mask to keep only valid rows
        df = df[mask]
                
        df = df.drop(columns=['match_data'])
        
        return df
    except Exception as e:
        logger.info(f'Something went wrong {str(e)}')
        return None