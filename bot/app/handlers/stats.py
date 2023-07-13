from telegram import InlineKeyboardMarkup, InlineKeyboardButton, Update
from telegram.ext import CommandHandler, CallbackQueryHandler, ContextTypes

import requests

async def get_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query.data == "stats":
        r = requests.get("http://backend/api/stats/tournaments")
        data = r.json()

        tournaments = data['tournaments']
        keyboard = [[InlineKeyboardButton(t['league_name'], callback_data=f'tournament_{t["league_name"]}')] for t in tournaments]
        keyboard.append([InlineKeyboardButton("Back", callback_data='back_to_start')])

        reply_markup = InlineKeyboardMarkup(keyboard)

        context.user_data['tournaments'] = data['tournaments']
        await query.edit_message_text('Select a tournament:', reply_markup=reply_markup)

async def get_tournament_details(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    league_name = query.data.split('_', 1)[1]

    tournaments = context.user_data['tournaments']
    tournament = next((t for t in tournaments if t['league_name'] == league_name), None)
    
    if tournament:
        model = next((p for p in tournament['predictions'] if p['model_name'] == 'heroes_standard'), None)
        if model:
            stats = f"Model: {model['model_name']}\n" \
                     f"Total Games: {model['total_games']}\n" \
                     f"Total Correct: {model['total_correct']}\n" \
                     f"Total Incorrect: {model['total_incorrect']}\n" \
                     f"🥇 Win Rate: {model['winrate']*100:.2f}%"
        else:
            stats = 'No stats available for heroes_standard model.'
    else:
        stats = 'No stats available for this tournament.'

    keyboard = [[InlineKeyboardButton("Back", callback_data='stats')]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(stats, reply_markup=reply_markup)

stats_handler = CallbackQueryHandler(get_stats, pattern='^stats$')
tournament_details_handler = CallbackQueryHandler(get_tournament_details, pattern='^tournament_')