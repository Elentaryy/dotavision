from api.handlers.history_handler import get_series_info
from api.models.live_model import Matches, LiveSeries
from api.models.error import Error
from fastapi import APIRouter
from typing import List, Union
from datetime import date

router = APIRouter(
    prefix="/history",
    tags=["history"],
)

@router.get("/series", response_model=Union[LiveSeries, Error])
def read_old_series(date: date):
    return get_series_info(date)
