from typing import List

from pydantic import BaseModel

from models.rdbms.attribute import Attribute


class Relation(BaseModel):
    name: str
    attributes: List[Attribute]
    primary_key: Attribute
    foreign_key: Attribute
