import os
import json
import logging
from dotenv import load_dotenv
import gspread
from oauth2client.service_account import ServiceAccountCredentials

logger = logging.getLogger('Google')

class GoogleService:
    def __init__(self):
        load_dotenv()
        google_creds = os.getenv('GOOGLE_CREDS')
        if not google_creds:
            raise ValueError("Missing GOOGLE_CREDS environment variable")

        self.creds_json = json.loads(google_creds)
        self.scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
        self.client = self._authenticate()

    def _authenticate(self):
        creds = ServiceAccountCredentials.from_json_keyfile_dict(self.creds_json, self.scope)
        client = gspread.authorize(creds)
        return client
    
    def write_prediction(self, dataframe, sheet_name):
        sheet = self.client.open(sheet_name)
        worksheet = sheet.sheet1  

        data = dataframe.values.tolist()
        
        data.reverse()

        for row in data:
            prediction = row[6]  
            worksheet.insert_row(row, 2)
            if prediction < 0.59:  
                pred_skip_col_number = worksheet.find("Prediction result").col
                worksheet.update_cell(2, pred_skip_col_number, "Skip")  

    def update_result(self, match_id, result, sheet_name):
        sheet = self.client.open(sheet_name)
        worksheet = sheet.get_worksheet(0)

        try:
            match_cells = worksheet.findall(str(match_id))
            result_col_number = worksheet.find("Result").col
            prediction_col_number = worksheet.find("Prediction").col
            pred_result_col_number = worksheet.find("Prediction result").col
        except gspread.CellNotFound:
            logger.error(f'Match ID {match_id} or columns not found.')
            return

        for cell in match_cells:
            worksheet.update_cell(cell.row, result_col_number, result)
            prediction = worksheet.cell(cell.row, prediction_col_number).value
            prediction_skip = worksheet.cell(cell.row, pred_result_col_number).value

            if prediction_skip != "Skip":  
                if int(prediction) == int(result):
                    pred_result = "Win"
                else:
                    pred_result = "Lose"

                worksheet.update_cell(cell.row, pred_result_col_number, pred_result)

gs = GoogleService()

