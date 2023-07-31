from fastapi import APIRouter, HTTPException
from services.db_service import db
import logging

logger = logging.getLogger('match_router')

match_router = APIRouter(tags=["Match"])

@match_router.get("/{match_id}")
async def get_match_info(match_id: int):
    try:
        stats = db.get_match_info(match_id)
        if not stats:
            raise HTTPException(status_code=404, detail="Match not found")
        return stats
    except HTTPException as http_error:
        raise http_error
    except Exception as e:
        logger.error(f'Something went wrong {str(e)}')
        raise HTTPException(status_code=500, detail="Something went wrong")
    
