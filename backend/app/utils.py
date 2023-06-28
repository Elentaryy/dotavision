import pandas as pd
import logging
import warnings
warnings.filterwarnings('ignore')

logger = logging.getLogger('utils')

def format_data(data):
    try:
        df = pd.DataFrame(data['games'])
        for index, row in df.iterrows():
            logger.info(row['match_data'])
        df['radiant_team'] = df['match_data'].apply(lambda x: x.get('radiant_team').get('team_name'))
        df['dire_team'] = df['match_data'].apply(lambda x: x.get('dire_team').get('team_name'))
        df['dt'] = df['match_data'].apply(lambda x: x.get('result', {}).get('start_time'))
        df['dt'] = pd.to_datetime(df['dt'], unit='s')
        df['year'] = df['dt'].dt.year
        df['month'] = df['dt'].dt.month
        df['day'] = df['dt'].dt.day

        df = df.drop(columns = ['dt', 'match_id'])

        all_hero_ids = set(range(1, 141))
        for hero_id in all_hero_ids:
            for team_number in range(2):
                column_name = f'hero_{hero_id}_team_{team_number}'
                df[column_name] = df['match_data'].apply(lambda x: 1 if any(player.get('hero_id') == hero_id \
                                        and player.get('team_number') == team_number for player in x.get('result', {}).get('players', [])) else 0)
        return df
    except Exception as e:
        logger.info(f'Something went wrong {str(e)}')
        return None