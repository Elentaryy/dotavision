from api.handlers.match_handler import get_match_info
from api.models.live_model import Matches, LiveSeries
from api.models.error import Error
from fastapi import APIRouter
from typing import List, Union

router = APIRouter(
    prefix="/match",
    tags=["match"],
)

@router.get("")
def read_match_info(match_id: int):
    return get_match_info(match_id)

