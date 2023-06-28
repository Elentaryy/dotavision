from api.handlers.live_handler import get_live_games_info, get_live_series_info
from api.models.live_model import Matches, LiveSeries
from api.models.error import Error
from fastapi import APIRouter
from typing import List, Union

router = APIRouter(
    prefix="/live",
    tags=["live"],
)

@router.get("/matches", response_model=Union[Matches, Error])
def read_live_games():
    return get_live_games_info()

@router.get("/series", response_model=Union[LiveSeries, Error])
def read_live_series():
    return get_live_series_info()
