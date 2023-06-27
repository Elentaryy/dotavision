from api.handlers import live_handler
from api.models.live_model import Matches
from fastapi import APIRouter
from typing import List

router = APIRouter(
    prefix="/live",
    tags=["live"],
)

@router.get("/matches", response_model=Matches)
def read_live_games():
    return live_handler.get_live_games_info()
