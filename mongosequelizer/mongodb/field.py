from pydantic import BaseModel

from mongosequelizer.type import MongoType


class Field(BaseModel):
    name: str
    data_type: MongoType
    not_null: bool
    unique: bool
