from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field # Reverted to Field

class UnwrappedUser(BaseModel):
    id_hash: str
    country: Optional[str] = None
    product: Optional[str] = None

class UnwrappedStats(BaseModel):
    total_minutes: int
    track_count: int
    unique_artists_count: int
    activity_period_days: int
    first_listen: Optional[str] = None # ISO datetime string
    last_listen: Optional[str] = None  # ISO datetime string

class UnwrappedPlayedTrack(BaseModel):
    track_id: str
    artist_id: str # Primary artist ID
    duration_ms: int
    listened_at: str # ISO datetime string

class UnwrappedArtistImage(BaseModel):
    url: str
    height: Optional[int] = None
    width: Optional[int] = None

class UnwrappedArtistFollowers(BaseModel):
    href: Optional[str] = None
    total: int

class UnwrappedTopArtist(BaseModel):
    id: str # Artist ID
    name: str
    popularity: Optional[int] = None
    play_count: Optional[str] = None
    last_played: Optional[str] = None

class UnwrappedData(BaseModel):
    user: UnwrappedUser
    stats: UnwrappedStats
    tracks: List[UnwrappedPlayedTrack] = Field(default_factory=list)
    top_artists_medium_term: Optional[List[UnwrappedTopArtist]] = Field(default_factory=list)

class Metadata(BaseModel):
    source: str
    collectionDate: str
    dataType: str