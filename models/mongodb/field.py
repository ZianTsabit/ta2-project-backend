from typing import List

from pydantic import BaseModel

from models.type import MongoType


class Field(BaseModel):
    name: str
    data_type: MongoType
    