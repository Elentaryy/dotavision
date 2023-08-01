import logging
import pickle
import pandas as pd
from datetime import datetime

logger = logging.getLogger('model')

with open('model/train/models/heroes_model_bali.pkl', 'rb') as f:
    heroes_xgb_v1 = pickle.load(f)

with open('model/train/models/teams_model.pkl', 'rb') as f:
    wtf_xgb = pickle.load(f)

model = heroes_xgb_v1
model_name = 'heroes_standard'

def get_team_data(df):
    df['radiant_team'] = df['match_data'].apply(lambda x: x.get('radiant_team').get('team_name'))
    df['dire_team'] = df['match_data'].apply(lambda x: x.get('dire_team').get('team_name'))
    df['day'] = datetime.today().strftime('%d')
    df['month'] = datetime.today().strftime('%m')
    df['year'] = datetime.today().strftime('%Y')
    return df

def get_hero_data(df):
    all_hero_ids = set(range(1, 141))
    for team_number in range(2):
        for hero_id in all_hero_ids:
            column_name = f'hero_{hero_id}_team_{team_number}'
            df[column_name] = df['match_data'].apply(
                lambda x: 1 if any(player.get('hero_id') == hero_id
                                    and player.get('team') == team_number 
                                    for player in x.get('players', {})) 
                else 0)
    return df

def predict_heroes(df):
    df_predict = get_hero_data(get_team_data(df.copy()))
    pred = df_predict.drop(columns=['radiant_team', 'dire_team', 'match_id', 'match_data']).values

    predictions_df = pd.DataFrame({
        'match_id': df_predict['match_id'],
        'radiant_team': df_predict['radiant_team'],
        'dire_team': df_predict['dire_team'],
        'model': 'heroes_standard',
        'prediction': model.predict(pred),
        'probability': model.predict_proba(pred).max(axis=1)
    })

    return predictions_df.to_dict(orient="records")

def predict_teams(df):
    df_pred = df.drop(columns=['match_id', 'radiant_team', 'dire_team']).fillna(0)
    model_df = pd.DataFrame({
        'match_id': df['match_id'],
        'radiant_team': df['radiant_team'],
        'dire_team': df['dire_team'],
        'model': 'teams_model',
        'prediction': wtf_xgb.predict(df_pred.values),
        'probability': wtf_xgb.predict_proba(df_pred.values).max(axis=1)
    })

    return model_df.to_dict(orient="records")
    
