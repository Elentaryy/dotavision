from telegram import InlineKeyboardMarkup, InlineKeyboardButton, Update
from telegram.ext import CommandHandler, CallbackQueryHandler, ContextTypes

import requests

async def get_predictions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query.data == "predictions":
        keyboard = [
            [
                InlineKeyboardButton("Live", callback_data='live'),
                InlineKeyboardButton("Match ID", callback_data='id_match'),
            ],
            [
                InlineKeyboardButton("Back", callback_data='back_to_start'),
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await query.edit_message_text('Please, choose:\nPredictions for live games or by Match ID', reply_markup=reply_markup)

async def get_match_prediction(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query.data == "id_match":
        keyboard = [[InlineKeyboardButton("Back", callback_data='predictions')]]
        
        reply_markup = InlineKeyboardMarkup(keyboard)

        await query.edit_message_text('Coming soon!', reply_markup=reply_markup)

async def get_live_predictions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query.data == "live":
        r = requests.get("http://backend/api/predictions/live")
        data = r.json()
        
        if not data['matches']:
            keyboard = [[InlineKeyboardButton("Back", callback_data='predictions')]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text('No live tracked games going on.', reply_markup=reply_markup)
        else:
            leagues = list(set([match['league_name'] for match in data['matches']]))

            keyboard = [[InlineKeyboardButton(f"{league}", callback_data=f'league_{league}')] for league in leagues]
            keyboard.append([InlineKeyboardButton("Back", callback_data='predictions')])
            reply_markup = InlineKeyboardMarkup(keyboard)
            context.user_data['matches'] = data['matches']

            await query.edit_message_text('Select a league:', reply_markup=reply_markup)

async def get_matches_by_league(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    league = query.data.split("_", 1)[1]
    matches = [match for match in context.user_data['matches'] if match['league_name'] == league]

    keyboard = [[InlineKeyboardButton(f"{match['radiant_team']} vs {match['dire_team']}", callback_data=f'match_{match["match_id"]}')] for match in matches]
    keyboard.append([InlineKeyboardButton("Back", callback_data='live')])
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text('Select a match:', reply_markup=reply_markup)

async def get_match_details(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    match_id = int(query.data.split("_", 1)[1])
    match = [match for match in context.user_data['matches'] if match['match_id'] == match_id][0]
    text = '\n\n'.join([f"🤖 Model: {prediction['model']}\n🏆 Winner: {match['radiant_team'] if prediction['prediction'] == 1 else match['dire_team']}\n📈 Probability: {prediction['probability']*100:.2f}%" for prediction in match['predictions']])

    keyboard = [[InlineKeyboardButton("Back", callback_data=f'league_{match["league_name"]}')]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(text, reply_markup=reply_markup)

predictions_handler = CallbackQueryHandler(get_predictions, pattern='^predictions$')
live_predictions_handler = CallbackQueryHandler(get_live_predictions, pattern='^live$')
matches_by_league_handler = CallbackQueryHandler(get_matches_by_league, pattern='^league_')
match_details_handler = CallbackQueryHandler(get_match_details, pattern='^match_')
match_prediction_handler = CallbackQueryHandler(get_match_prediction, pattern='^id_match$')