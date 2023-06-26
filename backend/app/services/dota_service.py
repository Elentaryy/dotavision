from dotenv import load_dotenv
import os
import requests
import pandas as pd
import logging
import json 

load_dotenv()
STEAM_API_KEY = os.getenv('STEAM_API_KEY')

logger = logging.getLogger('dota_api')

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

    def get_match_info(self, match_id: int):
        url = f"https://api.steampowered.com/IDOTA2Match_570/GetMatchDetails/v1/?key={self.api_key}&match_id={match_id}"
        try:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                return data
            else:
                logger.info(f"Error occurred while fetching match details for match ID {match_id}. {response.status_code}, {response.text}")
                return None
        except Exception as e:
            logger.info(f'Error occurred while sending request for match info. {str(e)}')
            return None 
        
    
    