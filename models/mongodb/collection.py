from typing import List

from pydantic import BaseModel

from models.mongodb.field import Field


class Collection(BaseModel):
    name: str
    fields: List[Field]
