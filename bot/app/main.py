from dotenv import load_dotenv
import os
import atexit
import logging
import requests
from telegram import Update
from telegram.ext import ApplicationBuilder
from telegram.error import BadRequest
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from handlers.predictions import predictions_handler, live_predictions_handler, matches_by_league_handler, match_details_handler, match_prediction_handler
from handlers.start import start_handler, back_to_start_handler
from handlers.about import about_handler
from handlers.limits import limits_handler
from handlers.stats import stats_handler

load_dotenv()
BOT_TOKEN= os.getenv('BOT_TOKEN')
CHANNEL_ID = os.getenv('CHANNEL_ID')

logging.basicConfig(
    filename='app.log',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

logger = logging.getLogger('bot_main')

seen_matches = set()

async def fetch_predictions():
    try:
        logger.info('We fetching')
        r = requests.get("http://backend/api/predictions/live")
        data = r.json()
        if r.status_code == 200:
            if data['matches']:
                for match in data['matches']:
                    for prediction in match['predictions']:
                        if prediction['model'] == 'heroes_standard':
                            team_name = match['radiant_team'] if prediction['prediction'] == 1 else match['dire_team']
                            probability = prediction['probability']
                            message = f"🏆 {match['radiant_team']} - {match['dire_team']}\n\n"
                            message += f"🎮 League name: {match['league_name']}\n\n"
                            message += f"🥇 Winner: *{team_name}*\n\n"
                            message += f"📈 Probability: {probability*100:.2f}%\n"
                            if match['match_id'] not in seen_matches:
                                seen_matches.add(match['match_id'])
                                await application.bot.send_message(chat_id=CHANNEL_ID, text=message, parse_mode='HTML')
    except Exception as e:
        logger.info(f'Something went wrong while fetching predictions {str(e)}')
    await asyncio.sleep(30)

if __name__ == '__main__':
    application = ApplicationBuilder().token(BOT_TOKEN).build()

    application.add_handler(start_handler)
    application.add_handler(predictions_handler)
    application.add_handler(live_predictions_handler)
    application.add_handler(back_to_start_handler)
    application.add_handler(matches_by_league_handler)
    application.add_handler(match_details_handler)
    application.add_handler(about_handler)
    application.add_handler(limits_handler)
    application.add_handler(stats_handler)
    application.add_handler(match_prediction_handler)

    scheduler = AsyncIOScheduler()
    scheduler.add_job(fetch_predictions, 'interval', seconds=40)
    scheduler.start()
    
    application.run_polling()

    atexit.register(lambda: scheduler.shutdown())