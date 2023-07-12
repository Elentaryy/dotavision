from telegram import InlineKeyboardMarkup, InlineKeyboardButton, Update
from telegram.ext import CommandHandler, CallbackQueryHandler, ContextTypes

async def about(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query.data == "stats":
        keyboard = [[InlineKeyboardButton("Back", callback_data='back_to_start')]]
        
        reply_markup = InlineKeyboardMarkup(keyboard)

        await query.edit_message_text('Coming soon!', reply_markup=reply_markup)

stats_handler = CallbackQueryHandler(about, pattern='^stats$')