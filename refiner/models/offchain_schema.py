from pydantic import BaseModel

class OffChainSchema(BaseModel):
    name: str
    version: str
    description: str
    dialect: str
    schema_definition: str