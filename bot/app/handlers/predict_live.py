from telegram import Update
from telegram.ext import CommandHandler, CallbackContext
import requests
import logging

logger = logging.getLogger('predict_handler')

def format_predictions(predictions):
    formatted_string = "Predictions for current live matches :\n"
    for prediction in predictions:
        winner = prediction["dire_team"] if prediction["prediction"] == 0 else prediction["radiant_team"]
        formatted_string += f"{prediction['radiant_team']} - {prediction['dire_team']}      Winner - {winner}\n"
    return formatted_string

async def predict(update: Update, context: CallbackContext) -> None:
    response = requests.get('http://backend/live/predict')
    data = response.json()
    logger.info(data)

    text = format_predictions(data)

    await context.bot.send_message(chat_id=update.effective_chat.id, text=text)

predict_handler = CommandHandler('predict', predict)