from typing import Optional
from pydantic import BaseModel

from refiner.models.offchain_schema import OffChainSchema

class Output(BaseModel):
    refinement_url: Optional[str] = None
    schema: Optional[OffChainSchema] = None