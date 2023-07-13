from telegram import InlineKeyboardMarkup, InlineKeyboardButton, Update
from telegram.ext import CommandHandler, CallbackQueryHandler, ContextTypes

import requests

async def about(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query.data == "about":
        keyboard = [[InlineKeyboardButton("Back", callback_data='back_to_start')]]
        
        reply_markup = InlineKeyboardMarkup(keyboard)

        about_text = f"🎮 DotaVision Bot\n\n" \
                     f"🔮 Bot is making predictions for Dota 2 games and is completely free to use for now\n\n" \
                     f"🎯 Bot only tracks premium/high tier professional leagues, but anything can be added if needed.\n\n" \
                     f"🤖 It operates using AI models, predicting matches outcomes based on heroes performances and stats in pro and high ranked immortal games, as well as team strength and player performances (both recent dynamic and overall, performance on specific hero etc.). AI is constantly improving and will get better over time\n\n" \
                     f"📊 All the stats are opened, bot regularly posts all predictions in https://t.me/dota_vision. Also detailed statistics is available here https://docs.google.com/spreadsheets/d/1dsQ_1ljNI1oVk5yzjbD-6fQR-7CvHcUKPVNo05PSzrU\n\n" \
                     f"📡 Predictions are made as soon all the info appeared in Dota API\n\n" \
                     "\n" \
                     f"🎮 DotaVision Бот\n\n" \
                     f"🔮 Бот делает прогнозы для игр Dota 2 и на данный момент полностью бесплатен и не имеет ограничений\n\n" \
                     f"🎯 Бот отслеживает только DPC/хай тир лиги, но при необходимости может быть добавлено что-угодно, включая игры в ранкеде\n\n" \
                     f"🤖 Бот работает с помощью AI-моделей, предсказывая исходы матчей на основе статистики по героям, динамике в топ иммортал и про играх, перфомансу игроков на конкретных персонажах и т.д.. AI находится в стадии активного развития и постоянно переобучается на новых данных\n\n" \
                     f"📊 Вся статистика открыта, бот регулярно публикует все прогнозы в https://t.me/dota_vision. Также детальную статистику можно посмотреть тут https://docs.google.com/spreadsheets/d/1dsQ_1ljNI1oVk5yzjbD-6fQR-7CvHcUKPVNo05PSzrU\n\n" \
                     f"📡 Предикты появляются в реальном времени, как только необходимая инфа появилась в Dota API\n"

        await query.edit_message_text(about_text, reply_markup=reply_markup)

about_handler = CallbackQueryHandler(about, pattern='^about$')