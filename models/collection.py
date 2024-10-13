from typing import List

from pydantic import BaseModel

from models.field import Field


class Collection(BaseModel):
    name: str
    fields: List[Field]