from pydantic import BaseModel

from models.type import CardinalitiesType


class Cardinalities(BaseModel):
    source: str
    destination: str
    type: CardinalitiesType
