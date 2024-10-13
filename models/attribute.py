from pydantic import BaseModel

from models.type import PsqlType


class Attribute(BaseModel):
    name: str
    data_type: PsqlType
    null: bool
    unique: bool
