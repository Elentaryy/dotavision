import os
import requests
from dotenv import load_dotenv
import pandas as pd
import logging
import json

load_dotenv()

logger = logging.getLogger('dota_api')

api_key = os.getenv('STEAM_API_KEY')

class DotaService:
    def __init__(self, api_key: str):
        self.api_key = api_key

    def get_live_matches(self, params=None):
        url = f'http://api.steampowered.com/IDOTA2Match_570/GetLiveLeagueGames/v1/?key={self.api_key}'
        try:
            response = requests.get(url, params=params)

            if response.status_code == 200:
                data = response.json()
                return data
            
            else:
                logger.info(f"Bad status code. {response.status_code}, {response.text}")
                return None
        
        except Exception as e:
            logger.info(f"Error occurred while sending request for pro matches. {str(e)}")
            return None  

    def get_match_info(self, match_id: str):
        url = f'http://api.steampowered.com/IDOTA2Match_570/GetMatchDetails/v1/?key={self.api_key}'
        params = {
            'match_id': match_id
        }       
        try:
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                if data['result'].get('error'):
                    return None
                return data
            else:
                logger.info(f"Error occurred while fetching match details for match ID {match_id}. {response.status_code}, {response.text}")
                return None
        except Exception as e:
            logger.info(f'Error occurred while sending request for match info. {str(e)}')
            return None 
        
    def get_public_matches(self, max_match_id):
        params = {
            'less_than_match_id' : max_match_id,
            'min_rank': 80,
            'mmr_descending': 1
        }
        try:
            response = requests.get(f'https://api.opendota.com/api/publicMatches', params=params)
            
            if response.status_code == 200:
                logger.info(f'Successfully fetched public matches')
                return response.json()
            else:
                return None
        except Exception as e:
            logger.info(f'Error occurred while fetching new public matches. {str(e)}')
            return None 

        
ds = DotaService(api_key=api_key)
        
    
    