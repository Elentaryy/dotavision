from telegram import InlineKeyboardMarkup, InlineKeyboardButton, Update
from telegram.ext import CommandHandler, CallbackQueryHandler, ContextTypes

async def about(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query.data == "limits":
        keyboard = [[InlineKeyboardButton("Back", callback_data='back_to_start')]]
        
        reply_markup = InlineKeyboardMarkup(keyboard)

        await query.edit_message_text('Currently bot is completely free and has no limits\nMake sure to join the channel: <a href="https://t.me/dota_vision">https://t.me/dota_vision</a>', reply_markup=reply_markup, parse_mode='HTML')

limits_handler = CallbackQueryHandler(about, pattern='^limits$')