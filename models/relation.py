from typing import List

from pydantic import BaseModel

from models.attribute import Attribute


class Relation(BaseModel):
    name: str
    attributes: List[Attribute]
    relation_id: Attribute
    foreign_keys: Attribute
