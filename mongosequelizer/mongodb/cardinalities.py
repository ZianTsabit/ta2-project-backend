from pydantic import BaseModel

from mongosequelizer.type import CardinalitiesType


class Cardinalities(BaseModel):
    source: str
    destination: str
    type: CardinalitiesType
