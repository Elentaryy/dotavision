from dotenv import load_dotenv
import os
import logging
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler
# from handlers.predict_live import predict_handler
from handlers.predict_live import live_matches_handler, match_callback_handler

load_dotenv()
BOT_TOKEN= os.getenv('BOT_TOKEN')

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)


if __name__ == '__main__':
    application = ApplicationBuilder().token(BOT_TOKEN).build()

    application.add_handler(live_matches_handler)
    application.add_handler(match_callback_handler)
    
    application.run_polling()