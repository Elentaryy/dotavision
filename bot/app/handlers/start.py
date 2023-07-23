from telegram import InlineKeyboardMarkup, InlineKeyboardButton, Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import CommandHandler, CallbackQueryHandler, ContextTypes

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [
            InlineKeyboardButton("About", callback_data='about'),
            InlineKeyboardButton("Stats", callback_data='stats'),
        ],
        [
            InlineKeyboardButton("Predictions", callback_data='predictions'),
            InlineKeyboardButton("Limits", callback_data='limits'),
        ]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text('Welcome to DotaVision bot!\nWhat do you want?', reply_markup=reply_markup)

async def back_to_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query.data == "back_to_start":
        keyboard = [
            [
                InlineKeyboardButton("About", callback_data='about'),
                InlineKeyboardButton("Stats", callback_data='stats'),
            ],
            [
                InlineKeyboardButton("Predictions", callback_data='predictions'),
                InlineKeyboardButton("Limits", callback_data='limits'),
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text('Please choose:', reply_markup=reply_markup)

start_handler = CommandHandler('start', start)
back_to_start_handler = CallbackQueryHandler(back_to_start, pattern='^back_to_start$')