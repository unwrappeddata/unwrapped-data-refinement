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
        default="Unwrapped Spotify Contribution Proof",
        description="Name of the schema"
    )

    SCHEMA_VERSION: str = Field(
        default="1.0.0",
        description="Version of the schema"
    )

    SCHEMA_DESCRIPTION: str = Field(
        default="Schema for refined Unwrapped Spotify listening data contribution proofs.",
        description="Description of the schema"
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

    FILE_ID: Optional[int] = Field(
        default=None,
        description="File ID of the input file being processed, injected by the refinement service."
    )

    PINATA_API_GATEWAY: Optional[str] = Field(
        default="https://tan-absent-magpie-105.mypinata.cloud/ipfs",
        description="Pinata API gateway URL"
    )

    class Config:
        env_file = ".env"
        case_sensitive = True

settings = Settings()