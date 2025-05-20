from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional

class Settings(BaseSettings):
    """Global settings configuration using environment variables"""

    INPUT_DIR: str = Field(
        default="/input",
        description="Directory containing input files to process"
    )

    OUTPUT_DIR: str = Field(
        default="/output",
        description="Directory where output files will be written"
    )

    REFINEMENT_ENCRYPTION_KEY: str = Field(
        default=None,
        description="Key to symmetrically encrypt the refinement. This is derived from the original file encryption key"
    )

    SCHEMA_NAME: str = Field(
        default="Unwrapped Spotify Data",
        description="Schema name for Unwrapped Spotify listening data"
    )

    SCHEMA_VERSION: str = Field(
        default="0.1.2",
        description="Version of the Unwrapped Spotify schema"
    )

    SCHEMA_DESCRIPTION: str = Field(
        default="Refined schema for Spotify listening history and derived top artists. Artist details are enriched via Spotify Web API.",
        description="Description of the Unwrapped Spotify schema"
    )

    SCHEMA_DIALECT: str = Field(
        default="sqlite",
        description="Dialect of the schema"
    )

    # Optional, required if using https://pinata.cloud (IPFS pinning service)
    PINATA_API_KEY: Optional[str] = Field(
        default=None,
        description="Pinata API key"
    )

    PINATA_API_SECRET: Optional[str] = Field(
        default=None,
        description="Pinata API secret"
    )

    PINATA_API_GATEWAY: Optional[str] = Field(
        default="https://gateway.pinata.cloud/ipfs",
        description="Pinata API gateway URL. Note: This is the gateway to access, not the API endpoint for upload."
    )

    # Spotify Web API Credentials
    SPOTIFY_CLIENT_ID: Optional[str] = Field(
        default=None,
        description="Spotify Web API Client ID"
    )
    SPOTIFY_CLIENT_SECRET: Optional[str] = Field(
        default=None,
        description="Spotify Web API Client Secret"
    )
    SPOTIFY_API_URL: str = Field(
        default="https://api.spotify.com/v1",
        description="Base URL for Spotify Web API"
    )
    SPOTIFY_TOKEN_URL: str = Field(
        default="https://accounts.spotify.com/api/token",
        description="Token URL for Spotify Web API"
    )
    SPOTIFY_MAX_IDS_PER_BATCH: int = Field(
        default=50,
        description="Max IDs for Spotify batch API calls (artists/tracks)"
    )
    SPOTIFY_API_CALL_DELAY_SECONDS: float = Field(
        default=0.1, # Slightly increased default for Spotify API
        description="Delay in seconds between individual Spotify API calls."
    )
    # Deprecating ARTIST_API_BASE_URL as SpotifyAPIClient handles its own base URL
    # API_CALL_DELAY_SECONDS is now SPOTIFY_API_CALL_DELAY_SECONDS

    class Config:
        env_file = ".env"
        case_sensitive = True

settings = Settings()