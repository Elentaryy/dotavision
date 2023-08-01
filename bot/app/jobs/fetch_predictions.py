import asyncio
import requests
import logging

logger = logging.getLogger('predictions_msg')

seen_matches = set()

match_messages = {}  

BASE_API_URL = "http://backend/api/predictions"

def fetch_live_predictions():
    try:
        r = requests.get(f"{BASE_API_URL}/live")
        r.raise_for_status()
        return r.json()
    except requests.HTTPError as errh:
        logger.error(f"HTTP Error occurred while fetching live predictions: {errh}")
        return None
    except requests.ConnectionError as errc:
        logger.error(f"Error Connecting occurred while fetching live predictions: {errc}")
        return None
    except Exception as err:
        logger.error(f"An error occurred while fetching live predictions: {err}")
        return None

async def process_prediction(app, channel_id, match):
    try:
        for prediction in match.get('predictions', []):
            if prediction.get('model') == 'heroes_standard':
                probability = prediction['probability']

                team_name = match['radiant_team'] if prediction['prediction'] == 1 else match['dire_team']
                message = (f"🏆 {match['radiant_team']} - {match['dire_team']}\n\n"
                           f"🎯 Match ID: {match['match_id']}\n\n"
                           f"🎮 League name: {match['league_name']}\n\n"
                           f"🥇 Winner: <b>{team_name}</b>\n\n"
                           f"📈 Probability: {probability*100:.2f}%\n\n"
                           f"🔮 Prediction: {team_name if probability > 0.59 else 'Skip'}\n")

                seen_matches.add(match['match_id'])

                message_id = await app.bot.send_message(chat_id=channel_id, text=message, parse_mode='HTML')

                if probability > 0.59:
                    match_messages[match['match_id']] = {
                        'message_id': message_id.message_id,
                        'message': message,
                        'prediction': prediction['prediction']
                    }

    except Exception as e:
        logger.error(f"An error occurred while processing prediction: {e}")

async def update_result(app, channel_id, prev_match_id):
    try:
        match_result_req = requests.get(f"{BASE_API_URL}/match/{prev_match_id}")
        match_result_req.raise_for_status()
        match_result = match_result_req.json()
        if match_result.get('result') is not None and prev_match_id in match_messages:
            prediction = match_messages[prev_match_id]['prediction']
            prediction_correct = (match_result['result'] == prediction)

            update_message = f"Upd: {'Win ✅' if prediction_correct else 'Lose ❌'}"
            original_message = match_messages[prev_match_id]['message']
            await app.bot.edit_message_text(
                chat_id=channel_id,
                message_id=match_messages[prev_match_id]['message_id'],
                text=original_message + "\n" + update_message,
                parse_mode='HTML'
            )

            seen_matches.remove(prev_match_id)
            del match_messages[prev_match_id]

    except requests.HTTPError as errh:
        logger.error(f"HTTP Error occurred while fetching match result for {prev_match_id}: {errh}")
    except requests.ConnectionError as errc:
        logger.error(f"Error Connecting occurred while fetching match result for {prev_match_id}: {errc}")
    except Exception as e:
        logger.error(f"An error occurred while updating message for match_id {prev_match_id}: {e}")

async def fetch_predictions(app, channel_id):
    try:
        data = fetch_live_predictions()
        if not data:
            return

        current_matches = set()
        if 'matches' in data:
            for match in data['matches']:
                if 'match_id' not in match:
                    logger.warning("Missing match_id in match data")
                    continue
                if match['match_id'] in seen_matches:
                    current_matches.add(match['match_id'])
                    continue
                await process_prediction(app, channel_id, match)

        for prev_match_id in seen_matches:
            if prev_match_id not in current_matches:
                await update_result(app, channel_id, prev_match_id)

    except Exception as e:
        logger.error(f'Something went wrong while fetching predictions {e}')
