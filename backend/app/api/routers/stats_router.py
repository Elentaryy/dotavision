from fastapi import APIRouter, HTTPException
from services.db_service import db
import logging

logger = logging.getLogger('stats_router')

stats_router = APIRouter(tags=["Stats"])

@stats_router.get("/tournaments")
async def get_tournaments_stats():
    try:
        stats = db.get_tournaments_stats()
        if not stats:
            raise HTTPException(status_code=404, detail="No tournament stats found")
        return stats
    except Exception as e:
        logger.info(f'Something went wrong {str(e)}')
        raise HTTPException(status_code=500, detail="Something went wrong")
    
@stats_router.get("/recent")
async def get_recent_stats():
    try:
        stats = db.get_recent_stats()
        if stats is None:
            raise HTTPException(status_code=500, detail="Server error occurred.")
        return stats
    except ValueError as ve:
        raise HTTPException(status_code=404, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail='Something went wrong')