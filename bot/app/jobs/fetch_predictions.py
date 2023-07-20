import asyncio
import requests
import logging

logger = logging.getLogger('predictions_msg')

seen_matches = set()

# async def fetch_predictions(app, channel_id):
#     try:
#         r = requests.get("http://backend/api/predictions/live")
#         data = r.json()
#         if r.status_code == 200:
#             current_matches = set()
#             if data['matches']:
#                 for match in data['matches']:
#                     for prediction in match['predictions']:
#                         if prediction['model'] == 'heroes_standard':
#                             team_name = match['radiant_team'] if prediction['prediction'] == 1 else match['dire_team']
#                             probability = prediction['probability']
#                             message = f"🏆 {match['radiant_team']} - {match['dire_team']}\n\n"
#                             message += f"🎮 League name: {match['league_name']}\n\n"
#                             message += f"🥇 Winner: <b>{team_name}</b>\n\n"
#                             message += f"📈 Probability: {probability*100:.2f}%\n"
#                             if match['match_id'] not in seen_matches:
#                                 seen_matches.add(match['match_id'])
#                                 await app.bot.send_message(chat_id=channel_id, text=message, parse_mode='HTML')
#     except Exception as e:
#         logger.info(f'Something went wrong while fetching predictions {str(e)}')

match_messages = {}  # add this line to your code before the fetch_predictions function

async def fetch_predictions(app, channel_id):
    try:
        r = requests.get("http://backend/api/predictions/live")
        if r.status_code != 200:
            logger.error(f"Error fetching live predictions: Status code {r.status_code}")
            return
        data = r.json()
        current_matches = set()  # stores the match_ids in the current round of predictions
        if 'matches' in data:
            for match in data['matches']:
                if 'match_id' not in match:
                    logger.warning("Missing match_id in match data")
                    continue
                if match['match_id'] in seen_matches:
                    current_matches.add(match['match_id'])
                    continue  # if we've already seen this match, no need to predict it again
                for prediction in match.get('predictions', []):
                    if prediction.get('model') == 'heroes_standard':
                        team_name = match['radiant_team'] if prediction['prediction'] == 1 else match['dire_team']
                        probability = prediction['probability']
                        message = f"🏆 {match['radiant_team']} - {match['dire_team']}\n\n"
                        message += f"🎮 League name: {match['league_name']}\n\n"
                        message += f"🥇 Winner: {team_name}\n\n"
                        message += f"📈 Probability: {probability*100:.2f}%\n"
                        current_matches.add(match['match_id'])
                        seen_matches.add(match['match_id'])
                        message_id = await app.bot.send_message(chat_id=channel_id, text=message, parse_mode='HTML')
                        match_messages[match['match_id']] = {
                            'message_id': message_id.message_id,
                            'message': message
                        }
        # Update results for matches that have ended
        for prev_match_id in list(seen_matches):
            if prev_match_id not in current_matches:  # only update if the match is not in the current prediction list
                match_result_req = requests.get(f"http://backend/api/match/{prev_match_id}")
                if match_result_req.status_code != 200:
                    logger.error(f"Error fetching match result for {prev_match_id}: Status code {match_result_req.status_code}")
                    continue
                match_result = match_result_req.json()
                if match_result.get('result') is not None:
                    # Add to the existing Telegram message with match result
                    prediction_correct = (match_result['result'] == 1 and prediction['prediction'] == 1) or (match_result['result'] == 0 and prediction['prediction'] == 0)

                    update_message = f"Upd: {'Win ✅' if prediction_correct else 'Lose ❌'}"
                    original_message = match_messages[prev_match_id]['message']
                    await app.bot.edit_message_text(
                        chat_id=channel_id, 
                        message_id=match_messages[prev_match_id]['message_id'], 
                        text=original_message + "\n" + update_message,
                        parse_mode='HTML'
                        
                    )
                    seen_matches.remove(prev_match_id)
                    del match_messages[prev_match_id]  # remove from the dictionary after updating
    except Exception as e:
        logger.error(f'Something went wrong while fetching predictions {str(e)}')