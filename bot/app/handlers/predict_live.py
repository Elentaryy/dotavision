from telegram import Update
import requests
import logging
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import CallbackContext, CallbackQueryHandler, CommandHandler

logger = logging.getLogger('predict_handler')

def format_match(match):
    return f"{match['radiant_team']} - {match['dire_team']}"

# Prepare matches data
def get_predictions():
    response = requests.get('http://backend/live/predict')  # This endpoint returns all matches and predictions
    return response.json()

# Handlers
async def live_matches(update: Update, context: CallbackContext) -> None:
    predictions = get_predictions()

    keyboard = [
        [InlineKeyboardButton(format_match(prediction), callback_data=str(i))] for i, prediction in enumerate(predictions)
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)

    # Store the predictions in the context user data
    context.user_data['predictions'] = predictions

    await update.message.reply_text('Please choose a match:', reply_markup=reply_markup)

async def match_callback(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    match_index = int(query.data)

    # Get the prediction for the chosen match
    prediction = context.user_data['predictions'][match_index]

    winner = prediction["dire_team"] if prediction["prediction"] == 0 else prediction["radiant_team"]
    proba = prediction["proba"]
    text = f"{prediction['radiant_team']} - {prediction['dire_team']}\nWinner: {winner} with a probability of {proba*100:.1f}%"

    await query.edit_message_text(text=text)

# Create handlers
live_matches_handler = CommandHandler('live_matches', live_matches)
match_callback_handler = CallbackQueryHandler(match_callback)

# def format_predictions(predictions):
#     formatted_string = "Predictions for current live matches :\n\n"
#     for prediction in predictions:
#         winner = prediction["dire_team"] if prediction["prediction"] == 0 else prediction["radiant_team"]
#         proba = prediction["proba"]
#         formatted_string += f"{prediction['radiant_team']} - {prediction['dire_team']}\n"
#         formatted_string += f"Winner: {winner} with a probability of {proba*100:.1f}%\n\n\n"
#     return formatted_string

# async def predict(update: Update, context: CallbackContext) -> None:
#     response = requests.get('http://backend/live/predict')
#     data = response.json()
#     logger.info(data)
#     text = format_predictions(data)

#     await context.bot.send_message(chat_id=update.effective_chat.id, text=text)

# predict_handler = CommandHandler('predict', predict)