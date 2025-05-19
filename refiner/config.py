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
        default="0.1.1",
        description="Version of the Unwrapped Spotify schema"
    )

    SCHEMA_DESCRIPTION: str = Field(
        default="Refined schema for Spotify listening history and top artists contributed via the Unwrapped platform. Artist details are enriched via an external API.",
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
        default="https://api.pinata.cloud",
        description="Pinata API gateway URL"
    )

    FILE_ID: Optional[int] = Field(
        default=None,
        description="File ID of the input file being processed, injected by the refinement service."
    )

    # ARTIST_API_BASE_URL: str = Field(
    #     default="http://localhost:3000/api", # Default for local Unwrapped UI API
    #     description="Base URL for the API to fetch artist and track details."
    # )
    DATA_REFINEMENT_API_BASE_URL: str = Field()

    API_CALL_DELAY_SECONDS: float = Field(
        default=0.05, # Small delay to be nice to local API
        description="Delay in seconds between API calls to fetch artist/track details."
    )

    class Config:
        env_file = ".env"
        case_sensitive = True

settings = Settings()