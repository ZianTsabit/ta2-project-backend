from typing import List

from pydantic import BaseModel

from mongosequelizer.mongodb.field import Field


class Collection(BaseModel):
    name: str
    fields: List[Field] = []
