from fastapi import APIRouter, HTTPException
from services.db_service import db

predictions_router = APIRouter()

@predictions_router.get("/predictions/live")
async def get_live_predictions():
    try:
        return db.get_live_predictions()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@predictions_router.get("/predictions/match/{match_id}")
async def get_match_predictions(match_id: int):
    prediction = db.get_match_prediction(match_id)
    if prediction:
        return prediction
    else:
        raise HTTPException(status_code=404, detail="Match not found")