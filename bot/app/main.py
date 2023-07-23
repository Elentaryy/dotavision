from dotenv import load_dotenv
import os
import atexit
import logging
from pytz import timezone
from telegram.ext import ApplicationBuilder
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from handlers.predictions import predictions_handler, live_predictions_handler, matches_by_league_handler, match_details_handler, match_prediction_handler
from handlers.start import start_handler, back_to_start_handler
from handlers.about import about_handler
from handlers.limits import limits_handler
from handlers.stats import stats_handler, tournament_details_handler
from jobs.fetch_predictions import fetch_predictions
from jobs.recent_stats import fetch_stats

load_dotenv()
BOT_TOKEN= os.getenv('BOT_TOKEN')
CHANNEL_ID = os.getenv('CHANNEL_ID')

logging.basicConfig(
    filename='app.log',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

logger = logging.getLogger('bot_main')

moscow_tz = timezone('Europe/Moscow')

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
    application.add_handler(tournament_details_handler)

    scheduler = AsyncIOScheduler(timezone=moscow_tz)
    scheduler.add_job(fetch_predictions, 'interval', seconds=30, args=(application, CHANNEL_ID))
    scheduler.add_job(fetch_stats, 'cron', hour=0, minute=5, args=(application, CHANNEL_ID))
    scheduler.start()
    
    application.run_polling()


    atexit.register(lambda: scheduler.shutdown())