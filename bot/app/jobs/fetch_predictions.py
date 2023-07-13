import asyncio
import requests
import logging

logger = logging.getLogger('predictions_msg')

seen_matches = set()

async def fetch_predictions(app, channel_id):
    try:
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
                                await app.bot.send_message(chat_id=channel_id, text=message, parse_mode='HTML')
    except Exception as e:
        logger.info(f'Something went wrong while fetching predictions {str(e)}')
    await asyncio.sleep(30)