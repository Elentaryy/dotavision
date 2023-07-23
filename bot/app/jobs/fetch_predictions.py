import asyncio
import requests
import logging

logger = logging.getLogger('predictions_msg')

seen_matches = set()

match_messages = {}  

async def fetch_predictions(app, channel_id):
    try:
        r = requests.get("http://backend/api/predictions/live")
        if r.status_code != 200:
            logger.error(f"Error fetching live predictions: Status code {r.status_code}")
            return
        data = r.json()
        current_matches = set()
        if 'matches' in data:
            for match in data['matches']:
                if 'match_id' not in match:
                    logger.warning("Missing match_id in match data")
                    continue
                if match['match_id'] in seen_matches:
                    current_matches.add(match['match_id'])
                    continue
                for prediction in match.get('predictions', []):
                    if prediction.get('model') == 'heroes_standard':
                        probability = prediction['probability']
                        
                        team_name = match['radiant_team'] if prediction['prediction'] == 1 else match['dire_team']
                        message = f"🏆 {match['radiant_team']} - {match['dire_team']}\n\n"
                        message += f"🎯 Match ID: {match['match_id']}\n\n"
                        message += f"🎮 League name: {match['league_name']}\n\n"
                        message += f"🥇 Winner: {team_name}\n\n"
                        message += f"📈 Probability: {probability*100:.2f}%\n\n"
                        message += f"🥇 Prediction: {team_name if probability > 0.59 else 'Skip'}\n\n"  
                        current_matches.add(match['match_id'])
                        seen_matches.add(match['match_id'])
                        message_id = await app.bot.send_message(chat_id=channel_id, text=message, parse_mode='HTML')

                        if probability > 0.59: 
                            match_messages[match['match_id']] = {
                                'message_id': message_id.message_id,
                                'message': message,
                                'prediction': prediction['prediction']
                            }

        for prev_match_id in list(seen_matches):
            if prev_match_id not in current_matches:
                match_result_req = requests.get(f"http://backend/api/match/{prev_match_id}")
                if match_result_req.status_code != 200:
                    logger.error(f"Error fetching match result for {prev_match_id}: Status code {match_result_req.status_code}")
                    continue
                match_result = match_result_req.json()
                if match_result.get('result') is not None and prev_match_id in match_messages:  
                    prediction = match_messages[prev_match_id]['prediction']
                    prediction_correct = (match_result['result'] == prediction)

                    update_message = f"Upd: {'Win ✅' if prediction_correct else 'Lose ❌'}"
                    original_message = match_messages[prev_match_id]['message']
                    try:
                        await app.bot.edit_message_text(
                            chat_id=channel_id, 
                            message_id=match_messages[prev_match_id]['message_id'], 
                            text=original_message + "\n" + update_message
                        )
                    except Exception as e:
                        logger.info(f'Error while updating msg {str(e)}')
                        
                    seen_matches.remove(prev_match_id)
                    if prev_match_id in match_messages: 
                        del match_messages[prev_match_id]
    except Exception as e:
        logger.error(f'Something went wrong while fetching predictions {str(e)}')
