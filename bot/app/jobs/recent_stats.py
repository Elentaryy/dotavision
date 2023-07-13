from datetime import datetime, timedelta
import requests
import logging

logger = logging.getLogger('recent_stats')

yesterday = datetime.now() - timedelta(days=1)
formatted_date = yesterday.strftime('%Y-%m-%d')

def format_prediction_stats(data):
    message = "📊 Prediction stats for {formatted_date}:\n\n\n"
    message += f"🎮 Total predictions: {data['total_predictions']}\n\n"
    message += f"✅ Correct: {data['total_correct']}\n\n"
    message += f"❌ Incorrect: {data['total_incorrect']}\n\n"
    message += f"📈 Winrate: {data['winrate']*100:.2f}%\n\n"
    message += f"You can check detailed stats here: "
    return message

async def fetch_stats(app, channel_id):
    try:
        logger.info('Fetching stats')
        r = requests.get("http://backend/api/stats/recent")
        if r.status_code == 200:
            data = r.json()
            message = format_prediction_stats(data)
            await app.bot.send_message(chat_id=channel_id, text=message, parse_mode='HTML')
        else:
            logger.info('No stats available')
    except Exception as e:
        logger.info(f'Something went wrong while fetching stats {str(e)}')