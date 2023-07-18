import os
import json
import logging
from dotenv import load_dotenv
import gspread
from oauth2client.service_account import ServiceAccountCredentials

logger = logging.getLogger('Google')
load_dotenv()

google_creds = os.getenv('GOOGLE_CREDS')
json_creds = json.loads(google_creds)

class GoogleService:
    def __init__(self, json_creds=json_creds):
        self.creds_json = json_creds
        self.scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
        self.client = self.authenticate()

    def authenticate(self):
        creds = ServiceAccountCredentials.from_json_keyfile_dict(self.creds_json, self.scope)
        client = gspread.authorize(creds)
        return client
    
    def write_prediction(self, dataframe, sheet_name):
        sheet = self.client.open(sheet_name)
        worksheet = sheet.sheet1  

        data = dataframe.values.tolist()

        for row in data:
            worksheet.append_row(row)

    def update_result(self, match_id, result, sheet_name):
        sheet = self.client.open(sheet_name)
        worksheet = sheet.get_worksheet(0)
        
        try:
            cell = worksheet.find(str(match_id))
            result_col_number = worksheet.find("Result").col
        except gspread.CellNotFound:
            logger.info(f'Match ID {match_id} or Result column not found.')
            return

        result_cell = worksheet.cell(cell.row, result_col_number)
        
        result_cell.value = result
        worksheet.update_cells([result_cell])

gs = GoogleService()

