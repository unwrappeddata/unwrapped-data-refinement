from typing import Optional, Dict, Any, Union
from pydantic import BaseModel, Field

class PointsBreakdownInput(BaseModel):
    volume_points: int
    volume_reason: str
    diversity_points: int
    diversity_reason: str
    history_points: int
    history_reason: str

class AttributesInputValid(BaseModel):
    account_id_hash: str
    track_count: int
    total_minutes: int
    data_validated: bool
    activity_period_days: int
    unique_artists: int
    previously_contributed: bool
    times_rewarded: int
    total_points: int
    differential_points: int
    points_breakdown: PointsBreakdownInput

class AttributesInputError(BaseModel):
    error: str

AttributesInput = Union[AttributesInputValid, AttributesInputError]


class FileChecksumsInput(BaseModel):
    encrypted: str
    decrypted: str

class FileInput(BaseModel):
    id: int
    source: str
    url: str
    checksums: FileChecksumsInput

class MetadataInput(BaseModel):
    dlp_id: int
    version: str
    file_id: int = Field(alias="file_id") # metadata.file_id
    job_id: int
    owner_address: str
    file: FileInput

class UnwrappedProofInput(BaseModel):
    dlp_id: int
    valid: bool
    score: float
    authenticity: float
    ownership: float
    quality: float
    uniqueness: float
    attributes: AttributesInput
    metadata: MetadataInput