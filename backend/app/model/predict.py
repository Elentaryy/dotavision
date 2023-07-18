import pandas as pd
import logging
import warnings
import ast
from datetime import datetime
import pickle

logger = logging.getLogger('model')

with open('model/models/heroes_xgb_pub.pkl', 'rb') as f:
    heroes_xgb_v1 = pickle.load(f)
with warnings.catch_warnings():
    warnings.filterwarnings('ignore')
    with open('model/train/models/teams_modelv2.pkl', 'rb') as f:
        wtf_xgb = pickle.load(f)

models = [heroes_xgb_v1]
model_names = ['heroes_standard']

def predict_heroes(df):
    df_predict = df.copy()
    df_predict['radiant_team'] = df_predict['match_data'].apply(lambda x: x.get('radiant_team').get('team_name'))
    df_predict['dire_team'] = df_predict['match_data'].apply(lambda x: x.get('dire_team').get('team_name'))
    df_predict['day']= datetime.today().strftime('%d')
    df_predict['month']= datetime.today().strftime('%m')
    df_predict['year']= datetime.today().strftime('%Y') 

    all_hero_ids = set(range(1, 141))
    for team_number in range(2):
        for hero_id in all_hero_ids:
                column_name = f'hero_{hero_id}_team_{team_number}'
                df_predict[column_name] = df_predict['match_data'].apply(lambda x: 1 if any(player.get('hero_id') == hero_id \
                                        and player.get('team') == team_number for player in x.get('players', {})) else 0)
                
    teams = df_predict[['match_id', 'radiant_team', 'dire_team']]
    pred = df_predict.drop(columns = ['radiant_team', 'dire_team', 'match_id', 'match_data']).values

    predictions_df = pd.DataFrame()

    for model, model_name in zip(models, model_names):
        model_df = df_predict[['match_id', 'radiant_team', 'dire_team']].copy()
        model_df['model'] = model_name
        model_df['prediction'] = model.predict(pred)
        model_df['probability'] = model.predict_proba(pred).max(axis=1)
        predictions_df = pd.concat((predictions_df, model_df), ignore_index=True)

    return predictions_df.to_dict(orient="records")

def predict_teams(df):
    model_df = df[['match_id', 'radiant_team', 'dire_team']]
    df_pred = df.drop(columns = ['match_id', 'radiant_team', 'dire_team'])

    model_df['model'] = 'teams_model'
    model_df['prediction'] = wtf_xgb.predict(df_pred.values)
    model_df['probability'] = wtf_xgb.predict_proba(df_pred.values).max(axis=1)

    return model_df.to_dict(orient="records")
    
