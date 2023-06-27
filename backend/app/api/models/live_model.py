from typing import List, Optional
from pydantic import BaseModel


class Player(BaseModel):
    account_id: int
    name: str
    hero_id: int
    team: int


class Team(BaseModel):
    team_name: str
    team_id: int
    team_logo: Optional[int]
    complete: bool


class Scoreboard(BaseModel):
    duration: float
    roshan_respawn_timer: int
    radiant: dict
    dire: dict


class MatchData(BaseModel):
    players: List[Player]
    radiant_team: Team
    dire_team: Team
    lobby_id: int
    match_id: int
    spectators: int
    league_id: int
    league_node_id: int
    stream_delay_s: int
    radiant_series_wins: int
    dire_series_wins: int
    series_type: int
    scoreboard: Optional[Scoreboard]


class Match(BaseModel):
    match_id: int
    match_data: MatchData


class Matches(BaseModel):
    games: List[Match]
