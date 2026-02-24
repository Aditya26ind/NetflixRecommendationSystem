from pydantic import BaseModel
from typing import Literal, Optional, Dict


class Event(BaseModel):
    event_id: Optional[str] = None
    event_type: Literal["play", "pause", "stop", "impression", "click"]
    user_id: str
    movie_id: str
    device: Optional[str] = None
    timestamp: Optional[int] = None


class RecommendationResponse(BaseModel):
    user_id: str
    items: list[str]
