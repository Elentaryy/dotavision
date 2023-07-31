import os
import logging
import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger('dota_api')

class DotaService:
    BASE_URL = 'http://api.steampowered.com/IDOTA2Match_570'
    PUBLIC_MATCHES_URL = 'https://api.opendota.com/api/publicMatches'
    
    def __init__(self, api_key: str):
        self.api_key = api_key

    def _send_get_request(self, url, params=None):
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            logger.error(f"Bad status code. {response.status_code}, {response.text}")
        except Exception as e:
            logger.error(f"Error occurred while sending request. {str(e)}")
        return None

    def get_live_matches(self):
        url = f'{self.BASE_URL}/GetLiveLeagueGames/v1/?key={self.api_key}'
        return self._send_get_request(url)

    def get_match_info(self, match_id: str):
        url = f'{self.BASE_URL}/GetMatchDetails/v1/?key={self.api_key}'
        params = {'match_id': match_id}
        response = self._send_get_request(url, params=params)

        if response and response['result'].get('error'):
            return None
        return response

    def get_public_matches(self, max_match_id):
        params = {
            'less_than_match_id' : max_match_id,
            'min_rank': 80,
            'mmr_descending': 1
        }
        return self._send_get_request(self.PUBLIC_MATCHES_URL, params=params)


api_key = os.getenv('STEAM_API_KEY')
ds = DotaService(api_key=api_key)
    
    